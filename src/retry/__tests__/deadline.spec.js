const { inspect } = require('util')
const createRetry = require('../index')
const testDefaults = require('../defaults.test')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')

const REQUEST_TIMED_OUT = 7
const INVALID_RECORD = 87

/** No jitter, so every delay is exact: 100, 200, 400, 800 ms ... */
const STEADY = { initialRetryTime: 100, factor: 0, multiplier: 2, maxRetryTime: 10000 }

/**
 * Runs one retrier over a function that fails with REQUEST_TIMED_OUT (or `failWith`'s error) on
 * every attempt but `succeedOnAttempt`, driving fake time until it settles.
 * @param {object} config the retrier options
 * @param {{ succeedOnAttempt?: number, failWith?: () => Error }} [options]
 * @returns {Promise<{ outcome: { value?: string, error?: Error }, attempts: { at: number, retryTime: number }[], errors: Error[] }>}
 */
const run = async (config, { succeedOnAttempt = null, failWith } = {}) => {
  const startedAt = Date.now()
  const attempts = []
  const errors = []
  const settled = createRetry(config)(async (bail, retryCount, retryTime) => {
    attempts.push({ at: Date.now() - startedAt, retryTime })
    if (attempts.length === succeedOnAttempt) {
      return 'done'
    }

    const error = failWith ? failWith() : createErrorFromCode(REQUEST_TIMED_OUT)
    errors.push(error)
    throw error
  }).then(
    (value) => ({ value }),
    (error) => ({ error })
  )

  await jest.runAllTimersAsync()
  return { outcome: await settled, attempts, errors }
}

describe('Retry > maxTotalTime, the delay clamp and the shared backoff', () => {
  beforeEach(() => {
    jest.useFakeTimers()
  })

  afterEach(() => {
    // Restore the spies first: restoring a setTimeout spy after the real timers are back would
    // reinstall the fake one.
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test('rejects with KafkaJSDeliveryTimeout once the next attempt would start after the deadline', async () => {
    const { outcome, attempts, errors } = await run({ ...STEADY, retries: 10, maxTotalTime: 1000 })

    // 0, +100, +200, +400; the next wait, 800 ms from 700, would end at 1500.
    expect(attempts.map(({ at }) => at)).toEqual([0, 100, 300, 700])
    expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(outcome.error).toMatchObject({
      name: 'KafkaJSDeliveryTimeout',
      retriable: false,
      category: 'abortable',
      message: 'Gave up after 4 attempts: the next one would start after the 1000 ms deadline',
    })
    expect(outcome.error.cause).toBe(errors[3])
    expect(jest.getTimerCount()).toEqual(0)
  })

  test('an attempt may start exactly at the deadline', async () => {
    const { outcome, attempts } = await run({ ...STEADY, retries: 10, maxTotalTime: 1500 })

    expect(attempts.map(({ at }) => at)).toEqual([0, 100, 300, 700, 1500])
    expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(outcome.error.message).toEqual(
      'Gave up after 5 attempts: the next one would start after the 1500 ms deadline'
    )
  })

  test('an attempt that would start one millisecond after the deadline is refused', async () => {
    const { outcome, attempts } = await run({ ...STEADY, retries: 10, maxTotalTime: 1499 })

    expect(attempts.map(({ at }) => at)).toEqual([0, 100, 300, 700])
    expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(outcome.error.message).toEqual(
      'Gave up after 4 attempts: the next one would start after the 1499 ms deadline'
    )
  })

  test("the cause is the error the last attempt threw, not that error's own cause", async () => {
    const inner = new Error('inner')
    const { outcome, errors } = await run(
      { ...STEADY, retries: 10, maxTotalTime: 1000 },
      { failWith: () => Object.assign(createErrorFromCode(REQUEST_TIMED_OUT), { cause: inner }) }
    )

    expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(outcome.error.cause).toBe(errors[3])
    expect(outcome.error.cause.cause).toBe(inner)
  })

  test('exhausting the retries first still rejects with KafkaJSNumberOfRetriesExceeded', async () => {
    // The third failure, at 300 ms, would cross the deadline (300 + 400 > 500) if it were checked first.
    const { outcome, attempts } = await run({ ...STEADY, retries: 2, maxTotalTime: 500 })

    expect(attempts).toHaveLength(3)
    expect(outcome.error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    expect(outcome.error).toMatchObject({ retryCount: 2, type: 'REQUEST_TIMED_OUT' })
  })

  test('a non-retriable error is not turned into a timeout', async () => {
    const { outcome, attempts } = await run(
      { ...STEADY, retries: 10, maxTotalTime: 1 },
      { failWith: () => createErrorFromCode(INVALID_RECORD) }
    )

    expect(attempts).toHaveLength(1)
    expect(outcome.error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(outcome.error).not.toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(outcome.error.cause).toMatchObject({ type: 'INVALID_RECORD' })
  })

  test('a success before the deadline resolves', async () => {
    const { outcome, attempts } = await run(
      { ...STEADY, retries: 10, maxTotalTime: 1000 },
      { succeedOnAttempt: 3 }
    )

    expect(attempts).toHaveLength(3)
    expect(outcome).toEqual({ value: 'done' })
  })

  test('maxTotalTime: undefined is the same retrier as no maxTotalTime', async () => {
    const omitted = await run({ ...STEADY, retries: 5 })
    const undefinedDeadline = await run({ ...STEADY, retries: 5, maxTotalTime: undefined })

    expect(undefinedDeadline.attempts).toEqual(omitted.attempts)
    expect(undefinedDeadline.outcome.error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    expect(undefinedDeadline.outcome.error.retryCount).toEqual(5)
  })

  test.each([0, -5, 1.5, '5000'])(
    'a maxTotalTime of %p is refused when the retrier is built',
    (maxTotalTime) => {
      const build = () => createRetry({ maxTotalTime })

      expect(build).toThrow(KafkaJSInvalidConfigurationError)
      expect(build).toThrow(
        `retry.maxTotalTime must be an integer of at least 1, got ${inspect(maxTotalTime)}`
      )
      expect(build).toThrow(expect.objectContaining({ category: 'invalidConfiguration' }))
    }
  )

  test('builds a retrier from an integer deadline of at least 1, from none, and from no options', () => {
    const accepted = [{ maxTotalTime: 1 }, { maxTotalTime: null }, { maxTotalTime: undefined }, {}]

    for (const opts of [...accepted, undefined, null]) {
      expect(() => createRetry(opts)).not.toThrow()
    }
  })

  test('a maxTotalTime is accepted up to 2147483647 ms, the longest timer delay, and refused above it', () => {
    const build = () => createRetry({ maxTotalTime: 2147483648 })

    expect(() => createRetry({ maxTotalTime: 2147483647 })).not.toThrow()
    expect(build).toThrow(KafkaJSInvalidConfigurationError)
    expect(build).toThrow(
      'retry.maxTotalTime must be an integer of at least 1 and at most 2147483647, got 2147483648'
    )
    expect(build).toThrow(expect.objectContaining({ category: 'invalidConfiguration' }))
  })

  test('a factor above 1 never schedules a negative delay', async () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const setTimeoutSpy = jest.spyOn(global, 'setTimeout')

    const { attempts } = await run({
      retries: 3,
      initialRetryTime: 100,
      factor: 3,
      multiplier: 2,
      maxRetryTime: 1000,
    })

    expect(setTimeoutSpy.mock.calls.map(([, delay]) => delay)).toEqual([0, 0, 0])
    expect(attempts.map(({ retryTime }) => retryTime)).toEqual([0, 0, 0, 0])
  })

  test('a negative multiplier never schedules a negative delay', async () => {
    const setTimeoutSpy = jest.spyOn(global, 'setTimeout')

    await run({ retries: 3, initialRetryTime: 100, factor: 0, multiplier: -2, maxRetryTime: 1000 })

    expect(setTimeoutSpy.mock.calls.map(([, delay]) => delay)).toEqual([100, 0, 0])
  })

  test("createRetry.backoff is the retrier's own schedule, draw for draw", async () => {
    const config = {
      retries: 2,
      initialRetryTime: 100,
      factor: 0.2,
      multiplier: 2,
      maxRetryTime: 10000,
    }
    const draws = [0.1, 0.9, 0.3, 0.7]
    const random = jest.spyOn(Math, 'random')

    draws.forEach((draw) => random.mockReturnValueOnce(draw))
    const { attempts } = await run(config)

    draws.forEach((draw) => random.mockReturnValueOnce(draw))
    const backoff = createRetry.backoff(config)
    const first = backoff.initial()
    const second = backoff.next(first)
    const third = backoff.next(second)

    expect(attempts.map(({ retryTime }) => retryTime)).toEqual([first, second, third])
  })

  test("createRetry.backoff takes the retrier's defaults and is clamped too", () => {
    jest.spyOn(Math, 'random').mockReturnValue(0.5)
    const backoff = createRetry.backoff()

    expect(backoff.initial()).toEqual(testDefaults.initialRetryTime)
    expect(backoff.next(50)).toEqual(50 * testDefaults.multiplier)
    expect(backoff.next(testDefaults.maxRetryTime)).toEqual(testDefaults.maxRetryTime)

    jest.spyOn(Math, 'random').mockReturnValue(0)
    expect(createRetry.backoff({ initialRetryTime: 100, factor: 3 }).initial()).toEqual(0)
  })
})
