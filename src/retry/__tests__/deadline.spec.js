const { inspect } = require('util')
const createRetry = require('../index')
const testDefaults = require('../defaults.test')
const { createErrorFromCode } = require('../../protocol/error')
const { skipMonotonicTo } = require('../../../testHelpers/monotonicSkip')
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
    expect(outcome.error).not.toHaveProperty('outcome')
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

/** Every delay is 100 ms and the whole call may take 500 ms. */
const LATE = {
  initialRetryTime: 100,
  factor: 0,
  multiplier: 1,
  maxRetryTime: 100,
  retries: 5,
  maxTotalTime: 500,
}
const fence = { register: () => () => {} }
const PLAIN = ['plain', (options) => createRetry(options)]
const FENCED = ['fenced', (options) => createRetry({ ...options, fence })]

describe('Retry > the budget on a late or stepped clock', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0 })
  })

  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  /**
   * @param {...any} outcomes what each call does, in order: an error to throw or a value to return
   * @returns {jest.Mock} the retried function
   */
  const scripted = (...outcomes) =>
    jest.fn(async () => {
      const outcome = outcomes.shift()
      if (outcome instanceof Error) throw outcome
      return outcome
    })

  /**
   * @param {Promise<any>} promise
   * @returns {Promise<{ value?: any, error?: Error }>} how the promise ended
   */
  const outcomeOf = (promise) =>
    promise.then(
      (value) => ({ value }),
      (error) => ({ error })
    )

  test.each([
    ['a retry whose timer fires after the deadline is not started', PLAIN],
    ['a fenced retry whose timer fires after the deadline is not started', FENCED],
  ])('%s', async (_title, [, retrier]) => {
    const first = createErrorFromCode(REQUEST_TIMED_OUT)
    const fn = scripted(first, 'done')
    const settled = outcomeOf(retrier(LATE)(fn))

    await jest.advanceTimersByTimeAsync(0)
    expect(fn).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(1)

    skipMonotonicTo(1000)
    await jest.advanceTimersByTimeAsync(100)

    const outcome = await settled
    expect(fn).toHaveBeenCalledTimes(1)
    expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(outcome.error.message).toEqual(
      'Gave up after 1 attempts: the next one would start after the 500 ms deadline'
    )
    expect(outcome.error.cause).toBe(first)
    expect(jest.getTimerCount()).toBe(0)
  })

  test.each([PLAIN, FENCED])(
    'a forward wall step does not end the budget early (%s)',
    async (_kind, retrier) => {
      const fn = scripted(createErrorFromCode(REQUEST_TIMED_OUT), 'done')
      const settled = outcomeOf(retrier(LATE)(fn))
      jest.setSystemTime(1000)

      await jest.advanceTimersByTimeAsync(100)

      expect(await settled).toEqual({ value: 'done' })
      expect(fn).toHaveBeenCalledTimes(2)
    }
  )

  test.each([PLAIN, FENCED])(
    'a backward wall step does not extend the budget (%s)',
    async (_kind, retrier) => {
      const startedAt = []
      const fn = jest.fn(async () => {
        startedAt.push(performance.now())
        throw createErrorFromCode(REQUEST_TIMED_OUT)
      })
      const settled = outcomeOf(retrier({ ...LATE, retries: 20, maxTotalTime: 350 })(fn))

      await jest.advanceTimersByTimeAsync(0)
      jest.setSystemTime(-100000)
      await jest.runAllTimersAsync()

      const outcome = await settled
      expect(startedAt).toEqual([0, 100, 200, 300])
      expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(outcome.error.message).toEqual(
        'Gave up after 4 attempts: the next one would start after the 350 ms deadline'
      )
    }
  )

  test.each([PLAIN, FENCED])(
    'an attempt whose timer fires exactly at the deadline still starts (%s)',
    async (_kind, retrier) => {
      const fn = scripted(createErrorFromCode(REQUEST_TIMED_OUT), 'done')
      const settled = outcomeOf(retrier({ ...LATE, maxTotalTime: 100 })(fn))

      await jest.advanceTimersByTimeAsync(100)

      expect(await settled).toEqual({ value: 'done' })
      expect(fn).toHaveBeenCalledTimes(2)
    }
  )

  describe('at the deadline, on a fractional clock', () => {
    /** The budget is 101 ms and the first wait is 100 ms, so 1 ms is left for the wait to overrun. */
    const TIGHT = { ...LATE, maxTotalTime: 101 }
    const timedOutAfterOne =
      'Gave up after 1 attempts: the next one would start after the 101 ms deadline'

    /**
     * Starts a call whose first attempt fails and whose second would succeed, then shows the
     * monotonic clock at `failedAt` ms before that failure is handled.
     * @param {Function} retrier
     * @param {number} failedAt
     * @returns {{ fn: jest.Mock, first: Error, settled: Promise<{ value?: any, error?: Error }> }}
     */
    const failingAt = (retrier, failedAt) => {
      const first = createErrorFromCode(REQUEST_TIMED_OUT)
      const fn = scripted(first, 'done')
      const settled = outcomeOf(retrier(TIGHT)(fn))
      skipMonotonicTo(failedAt)
      return { fn, first, settled }
    }

    /**
     * Starts a call whose first attempt fails and whose second would succeed, lets the failure be
     * handled, then shows the monotonic clock `lateBy` ms after the wait's end when the timer fires.
     * @param {Function} retrier
     * @param {number} lateBy
     * @returns {Promise<{ fn: jest.Mock, first: Error, settled: Promise<{ value?: any, error?: Error }> }>}
     */
    const wokenLate = async (retrier, lateBy) => {
      const first = createErrorFromCode(REQUEST_TIMED_OUT)
      const fn = scripted(first, 'done')
      const settled = outcomeOf(retrier(TIGHT)(fn))
      await jest.advanceTimersByTimeAsync(0)
      expect(jest.getTimerCount()).toBe(1)
      skipMonotonicTo(lateBy)
      await jest.advanceTimersByTimeAsync(100)
      return { fn, first, settled }
    }

    test.each([PLAIN, FENCED])(
      'a retry whose wait ends 0.5 ms before the deadline is scheduled (%s)',
      async (_kind, retrier) => {
        const { fn, settled } = failingAt(retrier, 0.5)

        await jest.advanceTimersByTimeAsync(100)

        expect(await settled).toEqual({ value: 'done' })
        expect(fn).toHaveBeenCalledTimes(2)
      }
    )

    test.each([PLAIN, FENCED])(
      'a retry whose wait ends exactly at the deadline is scheduled (%s)',
      async (_kind, retrier) => {
        const { fn, settled } = failingAt(retrier, 1)

        await jest.advanceTimersByTimeAsync(100)

        expect(await settled).toEqual({ value: 'done' })
        expect(fn).toHaveBeenCalledTimes(2)
      }
    )

    test.each([PLAIN, FENCED])(
      'a retry whose wait would end 0.5 ms past the deadline is not scheduled (%s)',
      async (_kind, retrier) => {
        const { fn, first, settled } = failingAt(retrier, 1.5)

        await jest.advanceTimersByTimeAsync(0)

        expect(jest.getTimerCount()).toBe(0)
        const outcome = await settled
        expect(fn).toHaveBeenCalledTimes(1)
        expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(outcome.error.message).toEqual(timedOutAfterOne)
        expect(outcome.error.cause).toBe(first)
      }
    )

    test.each([PLAIN, FENCED])(
      'a retry whose timer fires 0.5 ms before the deadline still starts (%s)',
      async (_kind, retrier) => {
        const { fn, settled } = await wokenLate(retrier, 0.5)

        expect(await settled).toEqual({ value: 'done' })
        expect(fn).toHaveBeenCalledTimes(2)
      }
    )

    test.each([PLAIN, FENCED])(
      'a retry whose timer fires exactly at the deadline still starts (%s)',
      async (_kind, retrier) => {
        const { fn, settled } = await wokenLate(retrier, 1)

        expect(await settled).toEqual({ value: 'done' })
        expect(fn).toHaveBeenCalledTimes(2)
      }
    )

    test.each([PLAIN, FENCED])(
      'a retry whose timer fires 0.5 ms after the deadline is not started (%s)',
      async (_kind, retrier) => {
        const { fn, first, settled } = await wokenLate(retrier, 1.5)

        const outcome = await settled
        expect(fn).toHaveBeenCalledTimes(1)
        expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(outcome.error.message).toEqual(timedOutAfterOne)
        expect(outcome.error.cause).toBe(first)
        expect(jest.getTimerCount()).toBe(0)
      }
    )
  })

  test('a fenced retry whose budget is spent after its timer fired and before its attempt is not started', async () => {
    let registrations = 0
    const spendBudgetOnThirdRegistration = {
      register: () => {
        registrations += 1
        if (registrations === 3) Promise.resolve().then(() => skipMonotonicTo(1000))
        return () => {}
      },
    }
    const first = createErrorFromCode(REQUEST_TIMED_OUT)
    const fn = scripted(first, 'done')
    const settled = outcomeOf(createRetry({ ...LATE, fence: spendBudgetOnThirdRegistration })(fn))

    await jest.advanceTimersByTimeAsync(0)
    expect(fn).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(100)

    const outcome = await settled
    expect(registrations).toBe(3)
    expect(fn).toHaveBeenCalledTimes(1)
    expect(outcome.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(outcome.error.message).toEqual(
      'Gave up after 1 attempts: the next one would start after the 500 ms deadline'
    )
    expect(outcome.error.cause).toBe(first)
    expect(jest.getTimerCount()).toBe(0)
  })

  test.each([
    ['plain', undefined, PLAIN],
    ['plain', null, PLAIN],
    ['fenced', undefined, FENCED],
    ['fenced', null, FENCED],
  ])(
    'a %s call with maxTotalTime %p never reads the monotonic clock',
    async (_kind, maxTotalTime, [, retrier]) => {
      const now = jest.spyOn(performance, 'now')
      const failure = createErrorFromCode(REQUEST_TIMED_OUT)
      const fn = scripted(failure, failure, 'done')
      const settled = outcomeOf(retrier({ ...LATE, maxTotalTime })(fn))

      await jest.advanceTimersByTimeAsync(300)

      expect(await settled).toEqual({ value: 'done' })
      expect(fn).toHaveBeenCalledTimes(3)
      expect(now).not.toHaveBeenCalled()
    }
  )
})
