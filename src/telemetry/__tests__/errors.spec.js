const ClientTelemetry = require('../client')
const TelemetryMetrics = require('../metrics')
const {
  KafkaJSConnectionError,
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')
const response = require('../../protocol/requests/getTelemetrySubscriptions/v0/response')
const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const subscription = (fields = {}) => ({
  clientInstanceId: ID,
  subscriptionId: -7,
  acceptedCompressionTypes: [0],
  pushIntervalMs: 1000,
  telemetryMaxBytes: 1048576,
  deltaTemporality: true,
  requestedMetrics: ['*'],
  ...fields,
})
const failure = (code, extra = {}) =>
  Object.assign(new Error('private diagnostic'), { code, clientSideThrottleTime: 0, ...extra })
/** @param {object} options deterministic controls @returns {object} real collector and finite transport */
const setup = (options = {}) => {
  const clock = {
    now: () => jest.now(),
    random: () => options.random ?? 0.5,
    setTimeout,
    clearTimeout,
  }
  const metrics = new TelemetryMetrics({
    kind: 'producer',
    clock: { wallMs: () => 1000, monotonicNs: () => BigInt(jest.now()) * 1000000n },
  })
  const events = []
  const gets = []
  const pushes = []
  const handles = []
  let candidates = options.candidates ?? [
    {
      nodeId: 1,
      host: 'a',
      port: 9092,
      endpointRevision: 1n,
      versions: { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } },
    },
  ]
  const transport = {
    candidates: () => candidates,
    open: jest.fn(async (candidate, { guard, onAllocated }) => {
      const handle = {
        retired: false,
        isConnected: () => !handle.retired,
        get: jest.fn(async (payload, authority) => {
          const error = authority.guard()
          if (error) throw error
          events.push({ api: 71, ...payload })
          const answer = gets.shift()
          return typeof answer === 'function'
            ? answer(payload, authority)
            : (answer ?? subscription())
        }),
        push: jest.fn(async (payload, authority) => {
          const error = authority.guard()
          if (error) throw error
          authority.onWrite()
          events.push({ api: 72, ...payload })
          const answer = pushes.shift()
          return typeof answer === 'function'
            ? answer(payload, authority)
            : (answer ?? { errorCode: 0, clientSideThrottleTime: 0 })
        }),
        retire: jest.fn(async () => {
          handle.retired = true
        }),
      }
      handles.push(handle)
      onAllocated(handle)
      const refusal = guard()
      if (refusal) throw refusal
      return handle
    }),
  }
  const client = new ClientTelemetry({
    kind: 'producer',
    metrics,
    transport,
    clock,
    logger: options.logger,
    config: { enableMetricsPush: true, ...options.config },
  })
  return {
    client,
    metrics,
    transport,
    events,
    gets,
    pushes,
    handles,
    setCandidates: (value) => {
      candidates = value
    },
  }
}
beforeEach(() => {
  jest.useFakeTimers({ now: 0 })
})
afterEach(() => {
  jest.useRealTimers()
})

describe('bounded numeric telemetry error recovery', () => {
  it.each([42, 87, 58, 999])('stops ordinary streams on fatal code %i', async (code) => {
    const h = setup()
    h.pushes.push(() => Promise.reject(failure(code)))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.state).toBe('STOPPED')
    expect(jest.getTimerCount()).toBe(0)
  })
  it.each([117, 76])('allows one immediate refresh for push error %i', async (code) => {
    const h = setup()
    h.pushes.push(
      () => Promise.reject(failure(code)),
      () => Promise.reject(failure(code))
    )
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    await jest.advanceTimersByTimeAsync(1)
    expect(h.events.filter((e) => e.api === 71)).toHaveLength(2)
    expect(h.client.recoveryUsed).toBe(true)
    await jest.advanceTimersByTimeAsync(999)
    expect(h.events.filter((e) => e.api === 71)).toHaveLength(2)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.events.filter((e) => e.api === 71)).toHaveLength(2)
    await jest.advanceTimersByTimeAsync(1)
    expect(h.events.filter((e) => e.api === 71)).toHaveLength(3)
    h.client.suspend()
  })
  it.each([117, 76])('backs off a failed Get without recursive refresh: %i', async (code) => {
    const h = setup()
    h.gets.push(() => Promise.reject(failure(code)))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.events).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(999)
    expect(h.events).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(h.events).toHaveLength(2)
    h.client.suspend()
  })
  it('halves the effective budget and retains its penalty across a larger refreshed cap', async () => {
    const h = setup()
    h.gets.push(subscription({ telemetryMaxBytes: 200 }))
    h.pushes.push(() => Promise.reject(failure(118)))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.penalty).toBe(100)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.penalty).toBe(100)
    expect(h.client.subscription.telemetryMaxBytes).toBe(1048576)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.penalty).toBeNull()
    h.client.suspend()
  })
  it('waits for both interval and quota after code89', async () => {
    const h = setup()
    h.pushes.push(() => Promise.reject(failure(89, { clientSideThrottleTime: 2000 })))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(2999)
    expect(h.events.filter((e) => e.api === 71)).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(h.events.filter((e) => e.api === 71)).toHaveLength(2)
    h.client.suspend()
  })
  it('caps the exponential backoff sequence after jitter', () => {
    const h = setup({ random: 1 })
    expect(Array.from({ length: 8 }, () => h.client.backoff())).toEqual([
      1200, 2400, 4800, 9600, 19200, 30000, 30000, 30000,
    ])
  })
  it('retires a held Get at the machine watchdog deadline', async () => {
    const h = setup()
    let release
    h.gets.push(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(30000)
    expect(h.client.state).toBe('BACKOFF')
    expect(h.handles[0].retired).toBe(true)
    release(subscription())
    await jest.advanceTimersByTimeAsync(0)
    expect(h.client.uuid).toBeNull()
    h.client.suspend()
  })
  it('retains unsent samples and backs off a local payload failure', async () => {
    const h = setup()
    h.client.payload.buildPayload = async () => {
      throw Error('secret')
    }
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.state).toBe('BACKOFF')
    expect(h.events.filter((e) => e.api === 72)).toHaveLength(0)
    expect(h.handles[0].retired).toBe(false)
    h.client.suspend()
  })
})
describe('provisional post-final reopen', () => {
  it.each(['beforeGet', 'afterGet', 'afterPush'])(
    'retains UUID with cache retirement %s',
    async (position) => {
      const h = setup()
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      await jest.advanceTimersByTimeAsync(1000)
      await h.client.close({ remaining: () => 1000 })
      let retired = position === 'beforeGet'
      const order = []
      h.gets.push(() => {
        order.push('get')
        if (position === 'afterGet') retired = true
        return subscription({ clientInstanceId: null })
      })
      h.pushes.push(() => {
        order.push('push')
        if (!retired) {
          retired = true
          throw failure(42)
        }
        return { errorCode: 0 }
      })
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      expect(h.client.reopen).not.toBeNull()
      expect(h.events.at(-1)).toMatchObject({ api: 71, clientInstanceId: ID })
      await jest.advanceTimersByTimeAsync(1000)
      if (position === 'afterPush') {
        expect(h.client.reopen.used).toBe(true)
        await jest.advanceTimersByTimeAsync(2000)
      }
      expect(retired).toBe(true)
      expect(order).toEqual(['get', 'push'])
      expect(h.client.reopen).toBeNull()
      expect(h.events.at(-1).clientInstanceId).toBe(ID)
      expect(h.events.filter((e) => e.terminating)).toHaveLength(1)
      h.client.suspend()
    }
  )
  it('consumes the single corrective allowance before retirement and keeps it through Get', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    await h.client.close({ remaining: () => 1000 })
    h.pushes.push(() => Promise.reject(failure(42)))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const reopen = h.client.reopen
    await jest.advanceTimersByTimeAsync(1000)
    expect(reopen.used).toBe(true)
    expect(h.client.reopen).toBe(reopen)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.events.at(-1)).toMatchObject({ api: 71, clientInstanceId: ID })
    expect(h.client.reopen).toBe(reopen)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.reopen).toBeNull()
    h.client.suspend()
  })
  it('stops after repeated corrective42 without another final', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    await h.client.close({ remaining: () => 1000 })
    h.pushes.push(
      () => Promise.reject(failure(42)),
      () => Promise.reject(failure(42))
    )
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(3000)
    expect(h.client.state).toBe('STOPPED')
    expect(h.events.filter((e) => e.terminating)).toHaveLength(1)
  })
  it('codec refresh never restores the corrective reopen allowance', () => {
    const h = setup()
    h.client.connected = true
    h.client.install(subscription())
    h.client.reopen = { revision: h.client.issue(), lifecycle: h.client.lifecycle, used: true }
    h.client.rejectedCodecs.add(1)
    const before = h.client.reopen
    h.client.install(subscription({ clientInstanceId: null, acceptedCompressionTypes: [1, 0] }))
    expect(h.client.reopen).toBe(before)
    expect(h.client.reopen.used).toBe(true)
    expect([...h.client.rejectedCodecs]).toEqual([1])
    h.client.handleError(failure(42), 'push', { lifecycle: h.client.lifecycle })
    expect(h.client.state).toBe('STOPPED')
  })
  it('corrective codec refresh writes fallback under the consumed reopen identity', async () => {
    for (const ending of ['success', 'repeated42']) {
      const h = setup()
      const built = []
      h.client.payload.buildPayload = async (snapshot, options) => {
        built.push([...options.acceptedCompressionTypes])
        return {
          metrics: Buffer.alloc(0),
          compressionType: options.acceptedCompressionTypes.includes(1) ? 1 : 0,
          includedIdentities: snapshot.points.map((point) => point.identity),
          omittedIdentities: [],
        }
      }
      const codecs = { acceptedCompressionTypes: [1, 0] }
      h.gets.push(subscription(codecs))
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      await jest.advanceTimersByTimeAsync(1000)
      await h.client.close({ remaining: () => 1000 })
      const start = h.events.length
      for (let get = 0; get < 3; get++)
        h.gets.push(subscription({ clientInstanceId: null, ...codecs }))
      h.pushes.push(
        () => Promise.reject(failure(42)),
        () => Promise.reject(failure(76)),
        ...(ending === 'repeated42' ? [() => Promise.reject(failure(42))] : [])
      )
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      const reopen = h.client.reopen
      expect(reopen).toMatchObject({ used: false })
      const tail = () => h.events.slice(start)
      for (let step = 0; step < 40 && tail().length < 6; step++)
        await jest.advanceTimersByTimeAsync(500)
      expect(tail().map((event) => event.api)).toEqual([71, 72, 71, 72, 71, 72])
      expect(
        tail()
          .filter((event) => event.api === 71)
          .map((event) => event.clientInstanceId)
      ).toEqual([ID, ID, ID])
      expect(
        tail()
          .filter((event) => event.api === 72)
          .map((event) => event.compressionType)
      ).toEqual([1, 1, 0])
      expect(built.at(-2)).toEqual([1, 0])
      expect(built.at(-1)).toEqual([0])
      expect([...h.client.rejectedCodecs]).toEqual([1])
      expect(reopen.used).toBe(true)
      expect(h.events.filter((event) => event.terminating)).toHaveLength(1)
      if (ending === 'success') {
        expect(h.client.reopen).toBeNull()
        expect(h.client.hadFinal).toBe(false)
        expect(h.client.state).toBe('READY')
      } else {
        expect(h.client.reopen).toBe(reopen)
        expect(h.client.state).toBe('STOPPED')
        await jest.advanceTimersByTimeAsync(60000)
        expect(tail()).toHaveLength(6)
      }
      h.client.suspend()
    }
  })
  it('skips another final if close interrupts a provisional reopen', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    await h.client.close({ remaining: () => 1000 })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await h.client.close({ remaining: () => 1000 })
    expect(h.events.filter((e) => e.terminating)).toHaveLength(1)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('corrective request authority', () => {
  it.each(['backoff', 'get', 'push'])(
    'revokes held corrective %s through another public reopen',
    async (phase) => {
      const h = setup()
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      await jest.advanceTimersByTimeAsync(1000)
      await h.client.close({ remaining: () => 1000 })
      h.pushes.push(() => Promise.reject(failure(42)))
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      await jest.advanceTimersByTimeAsync(1000)
      const old = h.client.reopen
      let release
      if (phase === 'backoff') await jest.advanceTimersByTimeAsync(999)
      if (phase === 'get') {
        h.gets.push(
          () =>
            new Promise((resolve) => {
              release = resolve
            })
        )
        await jest.advanceTimersByTimeAsync(1000)
      }
      if (phase === 'push') {
        await jest.advanceTimersByTimeAsync(1000)
        h.pushes.push(
          () =>
            new Promise((resolve) => {
              release = resolve
            })
        )
        await jest.advanceTimersByTimeAsync(1000)
      }
      await h.client.close({ zero: true })
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      const next = h.client.subscription
      if (release) {
        release(phase === 'get' ? subscription({ subscriptionId: 99 }) : { errorCode: 0 })
        await jest.advanceTimersByTimeAsync(0)
      }
      expect(h.client.reopen).not.toBe(old)
      expect(h.client.subscription).toBe(next)
      expect(h.client.uuid).toBe(ID)
      expect(h.events.filter((e) => e.terminating)).toHaveLength(1)
      h.client.suspend()
    }
  )
  it('keeps fatal telemetry stopped through public close and connect', async () => {
    const h = setup()
    h.client.stop()
    await h.client.close({ zero: true })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.transport.open).not.toHaveBeenCalled()
    expect(h.client.stopped).toBe(true)
  })
})

/**
 * Encodes an unsigned varint independently of the decoder under test.
 * @param {number} value non-negative integer below 2^32
 * @returns {Buffer} the 1-5 byte encoding
 */
const uvarint = (value) => {
  const bytes = []
  for (; value >= 0x80; value = Math.floor(value / 0x80)) bytes.push((value % 0x80) | 0x80)
  return Buffer.from([...bytes, value])
}

/**
 * Encodes a successful GetTelemetrySubscriptions body for the accepted instance ID.
 * @param {{codecs?:number, prefixes?:string[]}} fields codec count and requested-metric prefixes
 * @returns {Buffer} body after response header v1
 */
const wire = ({ codecs = 1, prefixes = ['*'] } = {}) =>
  Buffer.concat([
    Buffer.from([0, 0, 0, 0, 0, 0]),
    Buffer.from(ID.replaceAll('-', ''), 'hex'),
    Buffer.from([0, 0, 0, 7]),
    uvarint(codecs + 1),
    Buffer.alloc(codecs),
    Buffer.from([0, 0, 0x03, 0xe8, 0, 0x10, 0, 0, 1]),
    uvarint(prefixes.length + 1),
    ...prefixes.map((text) =>
      Buffer.concat([uvarint(Buffer.byteLength(text) + 1), Buffer.from(text)])
    ),
    Buffer.from([0]),
  ])

/** @param {Buffer} body raw response @returns {Function} a Get answer decoded and parsed by the real protocol response */
const decoded = (body) => () => response.decode(body).then(response.parse)

describe('over-limit subscription metadata from the wire', () => {
  const exact = {
    codecs: 128,
    prefixes: Array.from({ length: 256 }, (_, i) => String(i).padEnd(64, 'x')),
  }
  it('installs both selections at their exact limits', async () => {
    const h = setup()
    h.gets.push(decoded(wire(exact)))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.client.state).toBe('READY')
    expect(h.client.subscription.acceptedCompressionTypes).toHaveLength(128)
    expect(h.client.subscription.requestedMetrics).toEqual(exact.prefixes)
    h.client.suspend()
  })
  it.each([
    ['129 codecs', { codecs: 129 }],
    ['257 prefixes', { prefixes: Array(257).fill('a') }],
    ['a 257-byte prefix', { prefixes: ['a'.repeat(257)] }],
    ['16385 prefix bytes', { prefixes: [...Array(255).fill('a'.repeat(64)), 'a'.repeat(65)] }],
  ])('stops once on %s without retry and keeps the accepted selection', async (_, fields) => {
    const logger = { warn: jest.fn() }
    const h = setup({ logger })
    const install = jest.spyOn(h.client, 'install')
    const select = jest.spyOn(h.metrics, 'select')
    h.gets.push(subscription(), decoded(wire(fields)))
    h.pushes.push(() => Promise.reject(failure(117)))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const accepted = h.client.subscription
    expect(accepted.requestedMetrics).toEqual(['*'])
    await jest.advanceTimersByTimeAsync(1001)
    expect(h.events.filter((e) => e.api === 71)).toHaveLength(2)
    expect(h.client.state).toBe('STOPPED')
    expect(h.client.stopped).toBe(true)
    expect(install).toHaveBeenCalledTimes(1)
    expect(select).toHaveBeenCalledTimes(1)
    expect(h.client.uuid).toBe(ID)
    expect(h.client.subscription).toBe(accepted)
    expect(logger.warn).toHaveBeenCalledWith('Telemetry stopped')
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(600000)
    expect(h.events.filter((e) => e.api === 71)).toHaveLength(2)
    expect(h.events.filter((e) => e.api === 72)).toHaveLength(1)
    expect(h.handles).toHaveLength(1)
  })
})

describe('safe wrapped failure classification', () => {
  it('finds retriable transport causes with a finite cycle bound', () => {
    const h = setup()
    const inner = Object.assign(new Error('secret'), { retriable: true })
    const outer = Object.assign(new Error('wrapper'), { cause: inner })
    inner.cause = outer
    expect(h.client.classify(outer)).toMatchObject({ retriable: true, code: undefined })
    expect(h.client.classify({ cause: failure(58), retriable: true }).code).toBe(58)
  })
  it('never invokes thrown diagnostic accessors', () => {
    const h = setup()
    const getter = jest.fn(() => {
      throw Error('secret')
    })
    const error = {}
    Object.defineProperty(error, 'message', { get: getter })
    Object.defineProperty(error, 'code', { get: getter })
    Object.defineProperty(error, 'cause', { get: getter })
    expect(h.client.classify(error)).toMatchObject({ retriable: false, code: undefined })
    expect(getter).not.toHaveBeenCalled()
  })
})

describe('terminating provenance', () => {
  it('retires final provenance after a successful normal reopen push', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    await h.client.close({ remaining: () => 1000 })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.hadFinal).toBe(false)
    await h.client.close({ zero: true })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.client.reopen).toBeNull()
    h.pushes.push(() => Promise.reject(failure(42)))
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.state).toBe('STOPPED')
    expect(h.events.filter((e) => e.terminating)).toHaveLength(1)
  })
})

it.each([0, 20, 1000, 5000])('bounds primitive close with remaining budget %i', async (budget) => {
  const h = setup()
  h.client.resume()
  await jest.advanceTimersByTimeAsync(0)
  let release
  h.pushes.push(
    () =>
      new Promise((resolve) => {
        release = resolve
      })
  )
  const entered = jest.now()
  let ended = false
  const closing = h.client
    .close({ zero: budget === 0, remaining: () => budget - (jest.now() - entered) })
    .then(() => {
      ended = true
    })
  await jest.advanceTimersByTimeAsync(0)
  const cap = Math.min(1000, budget)
  if (cap > 0) {
    expect(ended).toBe(false)
    expect(h.events.filter((event) => event.terminating)).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(cap - 1)
    expect(ended).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
  } else {
    expect(h.events.filter((event) => event.terminating)).toHaveLength(0)
  }
  await closing
  expect(h.handles[0].retired).toBe(true)
  expect(h.client.state).toBe('CLOSED')
  expect(jest.getTimerCount()).toBe(0)
  if (release) release({ errorCode: 0 })
  await jest.advanceTimersByTimeAsync(0)
  expect(jest.getTimerCount()).toBe(0)
})

it.each(
  ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT'].flatMap((code) =>
    ['direct', 'wrapped', 'exhausted'].map((wrapper) => [code, wrapper])
  )
)('retries %s connection failure with retained UUID through %s error', async (code, wrapper) => {
  const h = setup()
  const transportError = new KafkaJSConnectionError('socket failure', { broker: 'a:9092', code })
  const error =
    wrapper === 'direct'
      ? transportError
      : wrapper === 'wrapped'
        ? new KafkaJSError(transportError, { cause: transportError })
        : new KafkaJSNumberOfRetriesExceeded(transportError, { retryCount: 1, retryTime: 0 })
  h.pushes.push(() => Promise.reject(error))
  h.client.resume()
  await jest.advanceTimersByTimeAsync(0)
  await jest.advanceTimersByTimeAsync(1000)
  expect(h.client.state).toBe('BACKOFF')
  expect(h.handles[0].retired).toBe(true)
  expect(h.client.uuid).toBe(ID)
  await jest.advanceTimersByTimeAsync(999)
  expect(h.events.map((event) => event.api)).toEqual([71, 72])
  await jest.advanceTimersByTimeAsync(1)
  expect(h.events.at(-1)).toMatchObject({ api: 71, clientInstanceId: ID })
  expect(h.handles).toHaveLength(2)
  await jest.advanceTimersByTimeAsync(1000)
  expect(h.client.state).toBe('READY')
  h.client.suspend()
})

it.each([
  [117, 'READY'],
  [999, 'STOPPED'],
])(
  'honors numeric protocol code %i beneath an OS-coded connection wrapper',
  async (code, state) => {
    const h = setup()
    h.pushes.push(() =>
      Promise.reject(
        new KafkaJSConnectionError('wrapped', { code: 'ECONNRESET', cause: failure(code) })
      )
    )
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1001)
    expect(h.client.state).toBe(state)
    expect(h.events.map((event) => event.api)).toEqual(code === 117 ? [71, 72, 71] : [71, 72])
    h.client.suspend()
  }
)

it.each(['push', 'empty-poll'])(
  'resets retry backoff only after a successful %s',
  async (reset) => {
    const h = setup()
    h.gets.push(() => Promise.reject(new KafkaJSConnectionError('reset', { code: 'ECONNRESET' })))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.client.failures).toBe(1)
    if (reset === 'empty-poll') h.gets.push(subscription({ requestedMetrics: [] }))
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.failures).toBe(reset === 'empty-poll' ? 0 : 1)
    if (reset === 'push') {
      await jest.advanceTimersByTimeAsync(1000)
      expect(h.client.failures).toBe(0)
    }
    h.client.suspend()
  }
)
