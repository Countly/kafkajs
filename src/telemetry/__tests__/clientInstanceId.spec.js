const ClientTelemetry = require('../client')
const TelemetryMetrics = require('../metrics')
const {
  KafkaJSConnectionError,
  KafkaJSInvalidConfigurationError,
  KafkaJSTelemetryError,
} = require('../../errors')

const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

/**
 * Builds the real machine over an independent monotonic clock and a held first Get.
 * @returns {object} the machine, its scripted transport handle and the clock/answer controls
 */
const setup = () => {
  let now = 0
  let resolveGet
  const pendingGet = new Promise((resolve) => {
    resolveGet = resolve
  })
  const clock = { now: () => now, random: () => 0.5, setTimeout, clearTimeout }
  const metrics = new TelemetryMetrics({ kind: 'producer' })
  const handle = {
    isConnected: () => true,
    get: jest.fn(() => pendingGet),
    push: jest.fn(),
    retire: jest.fn(async () => {}),
  }
  const transport = {
    candidates: () => [
      {
        nodeId: 1,
        host: 'a',
        port: 1,
        endpointRevision: 1n,
        versions: { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } },
      },
    ],
    open: jest.fn(async (_, { onAllocated }) => {
      onAllocated(handle)
      return handle
    }),
  }
  const client = new ClientTelemetry({
    kind: 'producer',
    metrics,
    transport,
    clock,
    config: {
      enableMetricsPush: true,
      makeError: (reason) => new KafkaJSTelemetryError(`Telemetry ${reason}`, { reason }),
    },
  })
  return {
    client,
    handle,
    transport,
    advance: (value) => {
      now = value
    },
    resolveGet,
    answer: {
      clientInstanceId: ID,
      subscriptionId: 1,
      acceptedCompressionTypes: [0],
      pushIntervalMs: 1000,
      telemetryMaxBytes: 1048576,
      deltaTemporality: true,
      requestedMetrics: [],
    },
  }
}

const settle = async () => {
  for (let turn = 0; turn < 30; turn++) await Promise.resolve()
}

/**
 * Journals every timer the machine arms or clears while the fake clock still owns delivery.
 * @param {{setTimeout:Function,clearTimeout:Function}} clock the machine's own clock object
 * @returns {{armed:object[],cleared:object[]}} callbacks, delays and handles in call order
 */
const journalTimers = (clock) => {
  const journal = { armed: [], cleared: [] }
  const { setTimeout: arm, clearTimeout: clear } = clock
  clock.setTimeout = (callback, delay) => {
    const handle = arm(callback, delay)
    journal.armed.push({ callback, delay, handle })
    return handle
  }
  clock.clearTimeout = (handle) => {
    journal.cleared.push(handle)
    return clear(handle)
  }
  return journal
}

/**
 * Starts one public getter call and records whether it has settled.
 * @param {object} h the setup controls
 * @param {number} timeout public timeout in milliseconds
 * @returns {{settled:boolean,result:Promise<*>}} the call's outcome, with rejections as values
 */
const caller = (h, timeout) => {
  const call = { settled: false }
  call.result = h.client
    .clientInstanceId({ timeout })
    .catch((error) => error)
    .then((value) => {
      call.settled = true
      return value
    })
  return call
}

/**
 * Holds the first Get and admits one short (10 ms) and sixty-three long (5000 ms) waiters.
 * @param {object} h the setup controls
 * @returns {Promise<{short:object,long:object[],journal:object}>} the callers and the timer journal
 */
const fillWithOneShort = async (h) => {
  h.client.resume()
  await jest.advanceTimersByTimeAsync(0)
  const journal = journalTimers(h.client.clock)
  const short = caller(h, 10)
  const long = Array.from({ length: 63 }, () => caller(h, 5000))
  expect(h.client.waiters.size).toBe(64)
  expect(h.handle.get).toHaveBeenCalledTimes(1)
  return { short, long, journal }
}

beforeEach(() => jest.useFakeTimers({ now: 0 }))
afterEach(() => jest.useRealTimers())

describe('public instance ID contract', () => {
  test.each([
    null,
    [],
    'SIGTERM',
    1,
    { timeout: null },
    { timeout: -1 },
    { timeout: 0.1 },
    { timeout: NaN },
    { timeout: Infinity },
    { timeout: 2147483648 },
    { timeout: '1' },
    {
      get timeout() {
        throw Error('private')
      },
    },
  ])('invalid options reject asynchronously before any waiter %#', async (options) => {
    const h = setup()
    let promise
    expect(() => {
      promise = h.client.clientInstanceId(options)
    }).not.toThrow()
    await expect(promise).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(h.client.waiters.size).toBe(0)
    expect(h.transport.open).not.toHaveBeenCalled()
  })

  test.each([0, 2147483647])('accepts the boundary timeout %p', async (timeout) => {
    const h = setup()
    h.client.connected = true
    h.client.state = 'GETTING'
    const result = h.client.clientInstanceId({ timeout }).catch((e) => e)
    try {
      if (timeout === 0) expect(await result).toMatchObject({ reason: 'timeout' })
      else expect([...h.client.waiters][0].deadline).toBe(2147483647)
    } finally {
      h.client.suspend()
    }
    await result
    expect(jest.getTimerCount()).toBe(0)
  })

  test('the default waits 60000 from entry and late timer delivery cannot extend authority', async () => {
    const h = setup()
    h.client.connected = true
    h.client.state = 'GETTING'
    const result = h.client.clientInstanceId().catch((e) => e)
    try {
      expect([...h.client.waiters][0].deadline).toBe(60000)
      h.advance(60000)
      h.client.suspend()
      expect(await result).toMatchObject({ reason: 'timeout' })
    } finally {
      h.client.suspend()
    }
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a supported held connection setup admits a waiter under its original deadline', async () => {
    const h = setup()
    let releaseOpen
    const opening = new Promise((resolve) => {
      releaseOpen = resolve
    })
    h.transport.open.mockImplementation(async (_, { onAllocated }) => {
      onAllocated(h.handle)
      await opening
      return h.handle
    })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.transport.open).toHaveBeenCalledTimes(1)
    expect(h.handle.get).not.toHaveBeenCalled()
    const result = h.client.clientInstanceId({ timeout: 100 }).catch((e) => e)
    await settle()
    expect(h.client.waiters.size).toBe(1)
    expect([...h.client.waiters][0].deadline).toBe(100)
    h.advance(40)
    releaseOpen()
    await settle()
    h.resolveGet(h.answer)
    await expect(result).resolves.toBe(ID)
    h.client.suspend()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('an established unsupported outcome refuses without opening a transport', async () => {
    const h = setup()
    h.transport.candidates = () => []
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await expect(h.client.clientInstanceId({ timeout: 100 })).rejects.toMatchObject({
      reason: 'unsupported',
    })
    expect(h.transport.open).not.toHaveBeenCalled()
    h.client.suspend()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('independent waiters detach on expiry without cancelling the shared Get', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const first = h.client.clientInstanceId({ timeout: 10 }).catch((e) => e)
    const second = h.client.clientInstanceId({ timeout: 20 }).catch((e) => e)
    h.advance(10)
    await jest.advanceTimersByTimeAsync(10)
    expect(await first).toMatchObject({ reason: 'timeout' })
    expect(h.client.waiters.size).toBe(1)
    h.resolveGet(h.answer)
    await settle()
    expect(await second).toBe(ID)
    expect(h.handle.get).toHaveBeenCalledTimes(1)
    h.client.suspend()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('suspension settles expired and pending waiters before removal and an old Get cannot settle a replacement', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const expired = h.client.clientInstanceId({ timeout: 10 }).catch((e) => e)
    const pending = h.client.clientInstanceId({ timeout: 30 }).catch((e) => e)
    h.advance(11)
    h.client.suspend()
    expect(await expired).toMatchObject({ reason: 'timeout' })
    expect(await pending).toMatchObject({ reason: 'not-connected' })
    expect(h.client.waiters.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
    await expect(h.client.clientInstanceId()).rejects.toMatchObject({ reason: 'not-connected' })
    h.client.resume()
    const replacement = h.client.clientInstanceId({ timeout: 30 }).catch((e) => e)
    h.resolveGet(h.answer)
    await settle()
    expect(h.client.uuid).toBeNull()
    expect(h.client.waiters.size).toBe(1)
    h.client.suspend()
    expect(await replacement).toMatchObject({ reason: 'not-connected' })
    expect(jest.getTimerCount()).toBe(0)
  })

  test('the sixty-fifth waiter is refused and an expired slot can be reused', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const pending = Array.from({ length: 64 }, () =>
      h.client.clientInstanceId({ timeout: 10 }).catch((e) => e)
    )
    const refused = h.client.clientInstanceId().catch((e) => e)
    try {
      expect(h.client.waiters.size).toBe(64)
      expect(await refused).toMatchObject({ reason: 'limit' })
    } finally {
      if (h.client.waiters.size > 64) h.client.suspend()
    }
    h.advance(10)
    await jest.advanceTimersByTimeAsync(10)
    expect((await Promise.all(pending)).every((e) => e.reason === 'timeout')).toBe(true)
    expect(h.client.waiters.size).toBe(0)
    const replacement = h.client.clientInstanceId().catch((e) => e)
    expect(h.client.waiters.size).toBe(1)
    h.client.suspend()
    expect(await replacement).toMatchObject({ reason: 'not-connected' })
    expect(jest.getTimerCount()).toBe(0)
  })

  test('admission reclaims an expired slot before any Get release, timer delivery or settlement', async () => {
    const h = setup()
    const { short, long, journal } = await fillWithOneShort(h)
    const obsolete = journal.armed.at(-1)
    expect(h.client.waiterTimer).toBe(obsolete.handle)
    h.advance(11)
    const timers = jest.getTimerCount()
    const armed = journal.armed.length
    const cleared = journal.cleared.length
    const replacement = caller(h, 3000)
    expect(h.client.waiters.size).toBe(64)
    expect([...h.client.waiters].map((waiter) => waiter.deadline).sort((a, b) => a - b)).toEqual([
      3011,
      ...Array(63).fill(5000),
    ])
    expect(journal.armed).toHaveLength(armed + 1)
    expect(journal.armed.at(-1).delay).toBe(3000)
    expect(journal.cleared).toHaveLength(cleared + 1)
    expect(journal.cleared.at(-1)).toBe(obsolete.handle)
    expect(h.client.waiterTimer).toBe(journal.armed.at(-1).handle)
    expect(jest.getTimerCount()).toBe(timers)
    expect(await short.result).toMatchObject({ reason: 'timeout' })
    await settle()
    expect([...long, replacement].some((call) => call.settled)).toBe(false)
    expect(h.handle.get).toHaveBeenCalledTimes(1)
    expect(h.transport.open).toHaveBeenCalledTimes(1)
    obsolete.callback()
    expect(h.client.waiters.size).toBe(64)
    expect(journal.armed).toHaveLength(armed + 2)
    expect(journal.armed.at(-1).delay).toBe(3000)
    expect(jest.getTimerCount()).toBe(timers)
    await settle()
    expect([...long, replacement].some((call) => call.settled)).toBe(false)
    h.client.suspend()
    const outcomes = await Promise.all([...long, replacement].map((call) => call.result))
    expect(outcomes.every((error) => error.reason === 'not-connected')).toBe(true)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('public close after a reclaiming admission rejects every uncached caller and a stale answer cannot settle a replacement', async () => {
    const h = setup()
    const { short, long } = await fillWithOneShort(h)
    h.advance(11)
    const admitted = caller(h, 3000)
    await h.client.close()
    expect(await short.result).toMatchObject({ reason: 'timeout' })
    const closed = await Promise.all([...long, admitted].map((call) => call.result))
    expect(closed).toHaveLength(64)
    expect(closed.every((error) => error.reason === 'closed')).toBe(true)
    expect(h.client.waiters.size).toBe(0)
    expect(h.client.waiterTimer).toBeNull()
    expect(jest.getTimerCount()).toBe(0)
    h.client.resume()
    const replacement = caller(h, 3000)
    expect(h.client.waiters.size).toBe(1)
    h.resolveGet(h.answer)
    await settle()
    expect(h.client.uuid).toBeNull()
    expect(h.client.waiters.size).toBe(1)
    expect(replacement.settled).toBe(false)
    h.client.suspend()
    expect(await replacement.result).toMatchObject({ reason: 'not-connected' })
    expect(jest.getTimerCount()).toBe(0)
  })

  test('the sixty-fifth live waiter is refused without touching a timer and the exact deadline instant frees one slot', async () => {
    const h = setup()
    const { short, long, journal } = await fillWithOneShort(h)
    const members = [...h.client.waiters]
    const timer = h.client.waiterTimer
    const timers = jest.getTimerCount()
    const armed = journal.armed.length
    const cleared = journal.cleared.length
    h.advance(9)
    await expect(h.client.clientInstanceId({ timeout: 100 })).rejects.toMatchObject({
      reason: 'limit',
    })
    expect([...h.client.waiters]).toHaveLength(64)
    expect([...h.client.waiters].every((waiter, index) => waiter === members[index])).toBe(true)
    expect(h.client.waiterTimer).toBe(timer)
    expect(journal.armed).toHaveLength(armed)
    expect(journal.cleared).toHaveLength(cleared)
    expect(jest.getTimerCount()).toBe(timers)
    expect(short.settled).toBe(false)
    h.advance(10)
    const next = caller(h, 100)
    expect(h.client.waiters.size).toBe(64)
    expect(h.client.waiters.has(members[0])).toBe(false)
    expect(await short.result).toMatchObject({ reason: 'timeout' })
    expect(journal.armed).toHaveLength(armed + 1)
    h.client.suspend()
    const outcomes = await Promise.all([...long, next].map((call) => call.result))
    expect(outcomes.every((error) => error.reason === 'not-connected')).toBe(true)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('one admission reclaims every expired waiter and arms a single timer for the live set', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const journal = journalTimers(h.client.clock)
    const expired = Array.from({ length: 64 }, () => caller(h, 10))
    expect(h.client.waiters.size).toBe(64)
    h.advance(10)
    const armed = journal.armed.length
    const live = caller(h, 500)
    expect(h.client.waiters.size).toBe(1)
    expect(journal.armed).toHaveLength(armed + 1)
    expect(journal.armed.at(-1).delay).toBe(500)
    const outcomes = await Promise.all(expired.map((call) => call.result))
    expect(outcomes.every((error) => error.reason === 'timeout')).toBe(true)
    expect(live.settled).toBe(false)
    expect(h.handle.get).toHaveBeenCalledTimes(1)
    h.client.suspend()
    expect(await live.result).toMatchObject({ reason: 'not-connected' })
    expect(jest.getTimerCount()).toBe(0)
  })

  test('validation, cache and timeout-zero outcomes never reclaim another caller slot', async () => {
    const h = setup()
    const { short, long, journal } = await fillWithOneShort(h)
    h.advance(11)
    const armed = journal.armed.length
    const cleared = journal.cleared.length
    await expect(h.client.clientInstanceId({ timeout: 0 })).rejects.toMatchObject({
      reason: 'timeout',
    })
    await expect(h.client.clientInstanceId({ timeout: -1 })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    h.client.uuid = ID
    await expect(h.client.clientInstanceId()).resolves.toBe(ID)
    h.client.uuid = null
    expect(h.client.waiters.size).toBe(64)
    expect(short.settled).toBe(false)
    expect(journal.armed).toHaveLength(armed)
    expect(journal.cleared).toHaveLength(cleared)
    h.client.suspend()
    expect(await short.result).toMatchObject({ reason: 'timeout' })
    await Promise.all(long.map((call) => call.result))
    expect(jest.getTimerCount()).toBe(0)
  })

  test('validation, disabled state, cache and lifecycle precedence are exact', async () => {
    const h = setup()
    h.client.uuid = ID
    h.client.state = 'STOPPED'
    h.client.connected = false
    await expect(h.client.clientInstanceId({ timeout: -1 })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    await expect(h.client.clientInstanceId({ timeout: 0 })).resolves.toBe(ID)
    h.client.config.enableMetricsPush = false
    await expect(h.client.clientInstanceId()).rejects.toMatchObject({ reason: 'disabled' })
    h.client.config.enableMetricsPush = true
    h.client.uuid = null
    await expect(h.client.clientInstanceId()).rejects.toMatchObject({ reason: 'not-connected' })
    h.client.state = 'CLOSED'
    await expect(h.client.clientInstanceId()).rejects.toMatchObject({ reason: 'closed' })
    h.client.state = 'STOPPED'
    h.client.connected = true
    await expect(h.client.clientInstanceId()).rejects.toMatchObject({ reason: 'stopped' })
    h.client.state = 'GETTING'
    await expect(h.client.clientInstanceId({ timeout: 0 })).rejects.toMatchObject({
      reason: 'timeout',
    })
  })

  test('a cached ID stays available while closed, suspended and stopped, with no network', async () => {
    const h = setup()
    h.client.uuid = ID
    for (const [state, connected] of [
      ['CLOSED', false],
      ['DORMANT', false],
      ['STOPPED', true],
    ]) {
      h.client.state = state
      h.client.connected = connected
      await expect(h.client.clientInstanceId({ timeout: 0 })).resolves.toBe(ID)
    }
    expect(h.transport.open).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a slow timeout accessor consumes the entry deadline', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const result = h.client
      .clientInstanceId({
        get timeout() {
          h.advance(21)
          return 20
        },
      })
      .catch((e) => e)
    try {
      expect(h.client.waiters.size).toBe(0)
      expect(await result).toMatchObject({ reason: 'timeout' })
    } finally {
      h.client.suspend()
    }
    expect(jest.getTimerCount()).toBe(0)
  })

  test('ordinary transport retry preserves the original waiter deadline', async () => {
    const h = setup()
    h.handle.get.mockRejectedValueOnce(new KafkaJSConnectionError('Telemetry transport lost'))
    h.client.resume()
    const pending = h.client.clientInstanceId({ timeout: 5000 }).catch((e) => e)
    const original = [...h.client.waiters][0]
    await jest.advanceTimersByTimeAsync(0)
    expect(h.client.state).toBe('BACKOFF')
    expect([...h.client.waiters][0]).toBe(original)
    expect(original.deadline).toBe(5000)
    h.advance(5000)
    await jest.advanceTimersByTimeAsync(5000)
    expect(await pending).toMatchObject({ reason: 'timeout' })
    h.client.suspend()
    expect(jest.getTimerCount()).toBe(0)
  })
})
