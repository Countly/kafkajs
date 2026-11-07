const ClientTelemetry = require('../client')
const TelemetryMetrics = require('../metrics')
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

describe('stable instance identity and absolute subscription replacement', () => {
  it('uses zero until the first current success and retains the accepted UUID', async () => {
    const h = setup()
    h.gets.push(
      () => Promise.reject(failure(undefined, { retriable: true })),
      subscription({ requestedMetrics: [] })
    )
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.events.map((e) => e.clientInstanceId)).toEqual([null, null])
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.events[2].clientInstanceId).toBe(ID)
    h.client.suspend()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.events.at(-1).clientInstanceId).toBe(ID)
    h.client.suspend()
  })
  it('replaces every subscription field and issues revisions for repeated numeric IDs', async () => {
    const h = setup()
    h.client.connected = true
    h.client.install(subscription())
    const a = h.client.subscription
    h.client.install(
      subscription({
        clientInstanceId: null,
        subscriptionId: 3,
        acceptedCompressionTypes: [1],
        pushIntervalMs: 30,
        telemetryMaxBytes: 512,
        deltaTemporality: false,
        requestedMetrics: ['custom.'],
      })
    )
    const b = h.client.subscription
    h.client.install(subscription({ clientInstanceId: null }))
    const c = h.client.subscription
    expect(a.revision < b.revision && b.revision < c.revision).toBe(true)
    expect(c.subscriptionId).toBe(-7)
    expect(c.requestedMetrics).toEqual(['*'])
    expect(b.pushIntervalMs).toBe(100)
    expect(b.deltaTemporality).toBe(false)
    expect(c.acceptedCompressionTypes).toEqual([0])
    h.client.suspend()
  })
  it.each([
    { clientInstanceId: null },
    { clientInstanceId: '00000000-0000-0000-0000-000000000000' },
    { telemetryMaxBytes: 0 },
    { requestedMetrics: null },
    { acceptedCompressionTypes: Array(129).fill(0) },
    { requestedMetrics: ['é'.repeat(129)] },
    { deltaTemporality: 1 },
  ])('refuses invalid fields atomically: %j', (fields) => {
    const h = setup()
    expect(() => h.client.install(subscription(fields))).toThrow()
    expect(h.client.uuid).toBeNull()
    expect(h.client.subscription).toBeNull()
  })
  it('rejects a changed nonzero UUID without changing accepted fields', () => {
    const h = setup()
    h.client.install(subscription())
    const old = h.client.subscription
    expect(() =>
      h.client.install(subscription({ clientInstanceId: '11111111-1111-1111-1111-111111111111' }))
    ).toThrow()
    expect(h.client.uuid).toBe(ID)
    expect(h.client.subscription).toBe(old)
  })
  it.each([0, -1, -2147483648])(
    'substitutes the fixed interval fallback for %i',
    (pushIntervalMs) => {
      const h = setup()
      h.client.install(subscription({ pushIntervalMs }))
      expect(h.client.interval()).toBe(300000)
    }
  )
})
describe('subscription pacing and selection', () => {
  it.each([
    [0, 500],
    [0.5, 1000],
    [1, 1500],
  ])('writes first push only at jitter due time %s', async (random, due) => {
    const h = setup({ random })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(due - 1)
    expect(h.events.filter((e) => e.api === 72)).toHaveLength(0)
    await jest.advanceTimersByTimeAsync(1)
    expect(h.events.filter((e) => e.api === 72)).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(999)
    expect(h.events.filter((e) => e.api === 72)).toHaveLength(1)
    h.client.suspend()
  })
  it('polls empty selections and writes a zero blob for nonmatching selections', async () => {
    const h = setup()
    h.gets.push(
      subscription({ requestedMetrics: [] }),
      subscription({ clientInstanceId: null, requestedMetrics: ['missing.'] })
    )
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.events.map((e) => e.api)).toEqual([71, 71])
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.events.at(-1)).toMatchObject({ api: 72, compressionType: 0, metrics: Buffer.alloc(0) })
    h.client.suspend()
  })
  it('throttles through stepped timers without timer overflow', async () => {
    const h = setup({ random: 1 })
    h.gets.push(subscription({ pushIntervalMs: 2147483647, clientSideThrottleTime: 2147483647 }))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.events).toHaveLength(1)
    expect(h.client.throttleUntil).toBe(2147483647)
    await jest.advanceTimersByTimeAsync(2147483647)
    expect(h.events.map((event) => event.api)).toEqual([71])
    await jest.advanceTimersByTimeAsync(1073741823)
    expect(h.events.map((event) => event.api)).toEqual([71])
    await jest.advanceTimersByTimeAsync(1)
    expect(h.events.map((event) => event.api)).toEqual([71, 72])
    await jest.advanceTimersByTimeAsync(2147483646)
    expect(h.events.map((event) => event.api)).toEqual([71, 72])
    await jest.advanceTimersByTimeAsync(1)
    expect(h.events.map((event) => event.api)).toEqual([71, 72, 72])
    h.client.suspend()
    expect(jest.getTimerCount()).toBe(0)
  })
})
describe('subscription revisions never reuse authority', () => {
  it('abandons held encoding when replacement repeats its numeric subscription ID', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    let release
    h.client.payload.buildPayload = () =>
      new Promise((resolve) => {
        release = resolve
      })
    await jest.advanceTimersByTimeAsync(1000)
    const op = h.client.running
    expect(op).not.toBeNull()
    h.client.install(subscription({ clientInstanceId: null }))
    release({
      metrics: Buffer.alloc(0),
      compressionType: 0,
      includedIdentities: [],
      omittedIdentities: [],
    })
    await jest.advanceTimersByTimeAsync(0)
    expect(h.events.filter((e) => e.api === 72)).toEqual([])
    expect(op.abandoned).toBe(true)
    h.client.suspend()
  })
  it('cannot install a held old Get after suspension and equal-clock reopen', async () => {
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
    const life = h.client.lifecycle
    h.client.suspend()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const next = h.client.subscription
    release(subscription({ subscriptionId: 99 }))
    await jest.advanceTimersByTimeAsync(0)
    expect(h.client.lifecycle > life).toBe(true)
    expect(h.client.subscription).toBe(next)
    h.client.suspend()
  })
  it('advances included samples at transmission even if the response is lost', async () => {
    const h = setup()
    const written = jest.spyOn(h.metrics, 'written')
    const abandon = jest.spyOn(h.metrics, 'abandon')
    h.pushes.push(() => Promise.reject(failure(undefined, { retriable: true })))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(written).toHaveBeenCalledTimes(1)
    expect(written.mock.calls[0][1]).toEqual([written.mock.calls[0][0].points[0].identity])
    expect(abandon).not.toHaveBeenCalled()
    h.client.suspend()
  })
})

describe('current metric authority at write', () => {
  it('abandons a revoked unsent sample without stopping the current stream', async () => {
    const h = setup()
    h.metrics.register({ name: 'custom.gauge', type: 'gauge', value: 3 })
    const written = jest.spyOn(h.metrics, 'written')
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    const push = h.handles[0].push
    h.handles[0].push = jest.fn(async (payload, authority) => {
      h.metrics.unregister({ name: 'custom.gauge' })
      const refusal = authority.guard()
      if (refusal) throw refusal
      authority.onWrite()
      return { errorCode: 0 }
    })
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.state).not.toBe('STOPPED')
    expect(written).not.toHaveBeenCalled()
    h.handles[0].push = push
    await jest.advanceTimersByTimeAsync(1000)
    expect(written).toHaveBeenCalledTimes(1)
    expect(h.client.state).toBe('READY')
    h.client.suspend()
  })
})

describe('delayed timer and deadline delivery', () => {
  it('late timer delivery sends one operation without a catch-up burst', async () => {
    const h = setup()
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    jest.setSystemTime(jest.now() + 5500)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.events.filter((e) => e.api === 72)).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(999)
    expect(h.events.filter((e) => e.api === 72)).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(h.events.filter((e) => e.api === 72)).toHaveLength(2)
    h.client.suspend()
  })

  it.each(['before-allocation', 'after-allocation', 'payload', 'pre-commit', 'commit'])(
    'retires expired machine work with delayed watchdog delivery at %s',
    async (phase) => {
      const h = setup()
      const written = jest.spyOn(h.metrics, 'written')
      let release
      const held = new Promise((resolve) => {
        release = resolve
      })
      const startup = phase === 'before-allocation' || phase === 'after-allocation'
      if (startup) {
        const open = h.transport.open
        h.transport.open = jest.fn(async (candidate, authority) => {
          if (phase === 'before-allocation') await held
          const handle = await open(candidate, authority)
          if (phase === 'after-allocation') await held
          return handle
        })
      }
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      const before = h.client.subscription
      if (!startup) {
        expect(before).not.toBeNull()
        if (phase === 'payload')
          h.client.payload.buildPayload = async () => {
            await held
            return {
              metrics: Buffer.alloc(0),
              compressionType: 0,
              includedIdentities: [],
              omittedIdentities: [],
            }
          }
        else
          h.handles[0].push = jest.fn(async (payload, authority) => {
            if (phase === 'pre-commit') await held
            const refusal = authority.guard()
            if (refusal) throw refusal
            if (phase === 'commit') await held
            authority.onWrite()
            h.events.push({ api: 72, ...payload })
            return { errorCode: 0 }
          })
        await jest.advanceTimersByTimeAsync(1000)
      }
      expect(h.client.running).not.toBeNull()
      jest.setSystemTime(jest.now() + 30001)
      release()
      await jest.advanceTimersByTimeAsync(0)
      expect(h.events.filter((e) => e.api === 72)).toEqual([])
      expect(written).not.toHaveBeenCalled()
      expect(h.client.state).toBe('BACKOFF')
      expect(h.transport.open).toHaveBeenCalledTimes(1)
      expect(h.handles).toHaveLength(1)
      expect(h.handles[0].retired).toBe(true)
      expect(h.client.subscription).toBe(before)
      if (startup) {
        expect(h.events).toEqual([])
        expect(h.client.uuid).toBeNull()
      }
      h.client.suspend()
      expect(jest.getTimerCount()).toBe(0)
    }
  )
})

it.each(['resolve', 'reject'].flatMap((outcome) => [false, true].map((stale) => [outcome, stale])))(
  'retires a payload transport after late %s with stale resources=%s before watchdog delivery',
  async (outcome, stale) => {
    let current = true
    const h = setup({ config: { resources: () => ({ values: [], isCurrent: () => current }) } })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    let release
    h.client.payload.buildPayload = () =>
      new Promise((resolve, reject) => {
        release = () =>
          outcome === 'reject' ? reject(new Error('late codec failure')) : resolve(null)
      })
    await jest.advanceTimersByTimeAsync(1000)
    const revision = h.client.transportRevision
    const op = h.client.running
    current = !stale
    jest.setSystemTime(31001)
    release()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.handles[0].retired).toBe(true)
    expect(h.client.handle).toBeNull()
    expect(h.client.transportRevision > revision).toBe(true)
    expect(h.client.state).toBe('BACKOFF')
    expect(op.abandoned).toBe(true)
    expect(h.events.map((event) => event.api)).toEqual([71])
    expect(h.client.watchdog).toBeNull()
    current = true
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.handles).toHaveLength(2)
    expect(h.events.at(-1)).toMatchObject({ api: 71, clientInstanceId: ID })
    h.client.suspend()
    expect(jest.getTimerCount()).toBe(0)
  }
)
