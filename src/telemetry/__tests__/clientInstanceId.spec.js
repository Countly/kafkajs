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
