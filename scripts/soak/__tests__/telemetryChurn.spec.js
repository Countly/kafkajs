const { EventEmitter } = require('node:events')
const {
  KINDS,
  OrdinaryOracle,
  OrdinaryWork,
  ProcessResources,
  Registry,
  TelemetryChurn,
  TelemetryProcesses,
  TelemetrySchedule,
  TelemetryTap,
  TelemetryWorker,
  TelemetryWorkerProcess,
  WORKER_ENV,
  WORKER_TOKEN,
  recordId,
} = require('../telemetryChurn')
const {
  PROFILES,
  TELEMETRY,
  TelemetryResourceVerdict,
  Soak,
  WORKLOADS,
  buildTelemetryChurn,
  instrument,
  main,
  parseArgs,
} = require('../soak')

const mockReads = { symbols: 0 }
jest.mock('../../../index', () => {
  const actual = jest.requireActual('../../../index')
  return {
    ...actual,
    Kafka: jest.fn(function Kafka() {
      return new Proxy(
        {},
        {
          ownKeys() {
            mockReads.symbols += 1
            return []
          },
        }
      )
    }),
  }
})

/** Runs `work` with a stand-in for the collection function, as the inherited soak specs do. */
const withGc = async (work) => {
  const exposed = globalThis.gc
  globalThis.gc = () => {}
  try {
    return await work()
  } finally {
    globalThis.gc = exposed
  }
}

/** Synchronous `withGc`, for builders that throw. */
const gcSync = (work) => {
  const exposed = globalThis.gc
  globalThis.gc = () => {}
  try {
    return work()
  } finally {
    globalThis.gc = exposed
  }
}

/** Runs `work` with no collection function at all. */
const noGc = (work) => {
  const exposed = globalThis.gc
  delete globalThis.gc
  try {
    return work()
  } finally {
    if (exposed !== undefined) globalThis.gc = exposed
  }
}

const turn = () => new Promise((resolve) => setImmediate(resolve))

describe('telemetry profile routes and builds', () => {
  const owner = (over = {}) => ({
    run: jest.fn(async () => ({ ok: true, marker: 'verdict' })),
    close: jest.fn(async () => {}),
    ...over,
  })

  test('routes main through one owned lifecycle', async () => {
    const first = owner()
    const build = jest.fn(() => first)
    const verdict = await withGc(() => main('telemetry', { brokers: ['a:1'], telemetry: build }))
    expect(build).toHaveBeenCalledTimes(1)
    expect(build).toHaveBeenCalledWith({ brokers: ['a:1'], phases: PROFILES.telemetry })
    expect(first.run).toHaveBeenCalledTimes(1)
    expect(first.close).toHaveBeenCalledTimes(1)
    expect(verdict).toEqual({ ok: true, marker: 'verdict' })

    const falsy = owner({
      run: jest.fn(async () => {
        // eslint-disable-next-line no-throw-literal
        throw undefined
      }),
    })
    await expect(
      withGc(() => main('telemetry', { telemetry: () => falsy }))
    ).rejects.toBeUndefined()
    expect(falsy.close).toHaveBeenCalledTimes(1)

    const closing = new Error('cleanup')
    const cleanupFails = owner({ close: jest.fn(async () => Promise.reject(closing)) })
    await expect(withGc(() => main('telemetry', { telemetry: () => cleanupFails }))).rejects.toBe(
      closing
    )

    const both = owner({
      run: jest.fn(async () => Promise.reject(new Error('run'))),
      close: cleanupFails.close,
    })
    const failure = await withGc(() => main('telemetry', { telemetry: () => both })).catch((e) => e)
    expect(failure).toBeInstanceOf(AggregateError)
    expect(failure.errors.map((e) => e.message)).toEqual(['run', 'cleanup'])
  })

  test('keeps the inherited profiles and parser unchanged beside the telemetry route', async () => {
    expect(parseArgs(['--profile', 'producer-churn']).profile).toBe('producer-churn')
    const churn = owner()
    const build = jest.fn(() => churn)
    await withGc(() => main('producer-churn', { churn: build }))
    expect(build).toHaveBeenCalledTimes(1)
    expect(churn.run).toHaveBeenCalledTimes(1)
  })

  test('refuses another phase list and a missing collector', () => {
    expect(() => noGc(() => buildTelemetryChurn({ brokers: [], phases: [] }))).toThrow(
      '--expose-gc'
    )
    expect(() =>
      gcSync(() =>
        buildTelemetryChurn({ brokers: [], phases: [{ ...PROFILES.telemetry[0], ms: 1000 }] })
      )
    ).toThrow('one fixed phase')
  })

  test('starts exactly one worker for each client kind', () => {
    const spawned = []
    const processes = new TelemetryProcesses({
      script: 'driver.js',
      spawn: (script, args, options) => {
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
          send: jest.fn(),
          kill: jest.fn(),
        })
        spawned.push({ script, args, options, child })
        return child
      },
    })
    for (const kind of KINDS) processes.start(kind)
    expect(spawned.map(({ options }) => options.env[WORKER_ENV])).toEqual([...KINDS])
    expect(new Set(spawned.map(({ options }) => options.env[WORKER_TOKEN])).size).toBe(4)
    for (const { args, options } of spawned) {
      expect(args).toEqual(['--profile', 'telemetry'])
      expect(options.execArgv).toEqual(['--expose-gc'])
      expect(options.stdio).toEqual(['ignore', 'pipe', 'pipe', 'ipc'])
    }
    expect(() => processes.start('producer')).toThrow('already started')
    expect(() => processes.start('streams')).toThrow('Unknown telemetry worker kind')
    expect(processes.workers.size).toBe(4)
  })

  test('a worker process never starts another parent or a cluster', () => {
    const saved = { kind: process.env[WORKER_ENV], send: process.send }
    process.env[WORKER_ENV] = 'producer'
    try {
      delete process.send
      expect(() =>
        gcSync(() => buildTelemetryChurn({ brokers: [], phases: PROFILES.telemetry }))
      ).toThrow('needs its kind and its parent channel')
      process.send = () => true
      const built = gcSync(() => buildTelemetryChurn({ brokers: [], phases: PROFILES.telemetry }))
      expect(Object.keys(built).sort()).toEqual(['close', 'run'])
      process.env[WORKER_ENV] = 'streams'
      expect(() =>
        gcSync(() => buildTelemetryChurn({ brokers: [], phases: PROFILES.telemetry }))
      ).toThrow('needs its kind and its parent channel')
    } finally {
      if (saved.kind === undefined) delete process.env[WORKER_ENV]
      else process.env[WORKER_ENV] = saved.kind
      if (saved.send === undefined) delete process.send
      else process.send = saved.send
    }
  })

  test('rejects a duplicate, substituted or missing handshake', async () => {
    let now = 0
    const processes = new TelemetryProcesses({
      script: 'driver.js',
      now: () => (now += 400),
      spawn: () =>
        Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
        }),
    })
    const producer = processes.start('producer')
    processes.start('consumer')
    processes.receive('producer', { type: 'hello', kind: 'producer', token: producer.token })
    await expect(processes.message('producer', 'hello', 500)).resolves.toMatchObject({
      kind: 'producer',
    })
    processes.receive('consumer', { type: 'hello', kind: 'producer', token: producer.token })
    await expect(processes.message('consumer', 'hello', 500)).rejects.toThrow('Substituted')
    processes.start('admin')
    await expect(processes.message('admin', 'hello', 500)).rejects.toThrow('sent no hello')
  })
})

describe('telemetry profile schedule and resources', () => {
  test('holds the fixed measured action counts and the serialization order', () => {
    const schedule = new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    expect(schedule.required()).toEqual({ subscription: 30, retire: 10, reopen: 5 })
    expect(schedule.ticks.find((tick) => tick.at === 60000).actions).toEqual([
      'reopen',
      'retire',
      'subscription',
    ])
    expect(schedule.ticks.at(-1).at).toBe(350000)
    expect(
      schedule.ticks.filter((tick) => !schedule.measured(tick)).flatMap((t) => t.actions)
    ).toEqual(expect.arrayContaining(['retire', 'subscription']))
  })

  test('tracks same-count timer replacement by token, not by count', async () => {
    const resources = new ProcessResources()
    resources.start()
    try {
      const timers = () => resources.identities()['resource:Timeout'] ?? []
      const first = setTimeout(() => {}, 1e6)
      await turn()
      const before = timers()
      clearTimeout(first)
      await turn()
      await turn()
      const second = setTimeout(() => {}, 1e6)
      await turn()
      const after = timers()
      expect(after).toHaveLength(before.length)
      expect(after.filter((token) => before.includes(token))).toHaveLength(before.length - 1)
      clearTimeout(second)
    } finally {
      resources.stop()
    }
    expect(resources.identities()).toEqual({})
  })

  test('keeps the registry fingerprint through replacement and restore, and fails atomically', () => {
    const calls = []
    const client = {
      registerMetricForSubscription: jest.fn((metric) => calls.push(['add', metric.name])),
      unregisterMetricFromSubscription: jest.fn((id) => calls.push(['remove', id.name])),
    }
    const registry = new Registry(client, 'producer')
    registry.fill()
    expect(registry.registered).toHaveLength(128)
    const canonical = registry.digest('uuid')
    registry.replace()
    expect(registry.registered).toHaveLength(128)
    expect(registry.digest('uuid')).not.toBe(canonical)
    registry.restore()
    expect(registry.digest('uuid')).toBe(canonical)
    expect(registry.digest('other')).not.toBe(canonical)
    const bytes = registry.bytes
    client.registerMetricForSubscription.mockImplementationOnce(() => {
      throw new Error('refused')
    })
    expect(() => registry.replace()).toThrow('refused')
    expect(registry.bytes).toBeLessThan(bytes)
    expect(registry.registered).toHaveLength(127)
  })

  test('classifies the telemetry transport by its own frames and retires only that socket', () => {
    const tap = new TelemetryTap(() => {
      throw new Error('unused')
    })
    const socket = () =>
      Object.assign(new EventEmitter(), { write: jest.fn(() => true), destroy: jest.fn() })
    const frame = (api) => {
      const bytes = Buffer.alloc(16)
      bytes.writeInt32BE(12, 0)
      bytes.writeInt16BE(api, 4)
      return bytes
    }
    const core = tap.track(socket())
    const telemetry = tap.track(socket())
    core.write(frame(3))
    telemetry.write(frame(71))
    telemetry.write(frame(72))
    telemetry.write(frame(72))
    expect(tap.counts()).toEqual({ gets: 1, pushes: 2, sockets: 1 })
    expect(tap.open()).toBe(1)
    expect(tap.retire()).toBe(1)
    expect(telemetry.destroy).toHaveBeenCalledTimes(1)
    expect(core.destroy).not.toHaveBeenCalled()
    telemetry.emit('close')
    expect(tap.open()).toBe(0)
    expect(tap.retire()).toBe(0)
  })
})

describe('telemetry profile instrumentation', () => {
  test('uses public instrumentation without private symbols', async () => {
    mockReads.symbols = 0
    const sockets = []
    const { kafka, probe, socketFactory, listeners } = instrument(
      {
        clientId: 'public',
        brokers: ['a:1'],
        socketFactory: () => {
          const socket = Object.assign(new EventEmitter(), { destroyed: false })
          sockets.push(socket)
          return socket
        },
      },
      { publicOnly: true, activeResources: false }
    )
    expect(Object.getOwnPropertySymbols(kafka)).toEqual([])
    mockReads.symbols = 0
    const socket = socketFactory({ host: 'a', port: 1 })
    socket.on('data', () => {})
    socket.on('data', () => {})
    const row = await probe.snapshot()
    expect(mockReads.symbols).toBe(0)
    expect(row.sockets).toEqual({ created: 1, destroyed: 0, open: 1 })
    expect(row.identities.sockets).toHaveLength(1)
    expect(row.identities['listener:sockets:data']).toHaveLength(2)
    for (const omitted of ['queueDepth', 'ledgerSize', 'obligationSize'])
      expect(row).not.toHaveProperty(omitted)
    expect(listeners()).toEqual({ sockets: { data: 2 } })
    expect(() => instrument({ clientId: 'private', brokers: ['a:1'] }, {})).toThrow(
      'private createCluster'
    )
  })

  test('reads memory only when asked and refuses it without a collector', async () => {
    const exposed = globalThis.gc
    delete globalThis.gc
    try {
      expect(() =>
        instrument(
          { clientId: 'm', brokers: ['a:1'], socketFactory: () => new EventEmitter() },
          {
            publicOnly: true,
            activeResources: false,
            memory: true,
          }
        )
      ).toThrow('--expose-gc')
    } finally {
      globalThis.gc = exposed
    }
    await withGc(async () => {
      const { probe } = instrument(
        { clientId: 'm', brokers: ['a:1'], socketFactory: () => new EventEmitter() },
        { publicOnly: true, activeResources: false, memory: true }
      )
      const row = await probe.snapshot()
      expect(row.memory.retainedBytes).toBe(row.memory.heapUsed + row.memory.external)
    })
  })
})

/** A client double that logs its public calls in order. */
const fakeClient = (log, over = {}) => ({
  connect: jest.fn(async () => log.push('connect')),
  disconnect: jest.fn(async () => log.push('disconnect')),
  clientInstanceId: jest.fn(async () => {
    log.push('cache-read')
    return 'uuid-1'
  }),
  registerMetricForSubscription: jest.fn(),
  unregisterMetricFromSubscription: jest.fn(),
  ...over,
})

/** A tap double whose Push total moves when asked. */
const fakeTap = (state = { gets: 0, pushes: 0, open: 1 }) => ({
  state,
  counts: () => ({ gets: state.gets, pushes: state.pushes, sockets: 1 }),
  open: () => state.open,
  retire: jest.fn(() => {
    state.pushes += 1
    return 1
  }),
})

/**
 * Builds a worker over doubles and a small window.
 * @param {object} [over] replacements
 * @returns {{worker:object, log:string[], tap:object, ordinary:object, client:object}}
 */
const makeWorker = (over = {}) => {
  const log = []
  const client = fakeClient(log, over.client)
  const tap = over.tap ?? fakeTap()
  const ordinary = {
    start: jest.fn(async () => log.push('start')),
    stop: jest.fn(async () => log.push('stop')),
    drain: jest.fn(async () => log.push('drain')),
    resume: jest.fn(async () => log.push('resume')),
    ids: () => ({}),
    ...over.ordinary,
  }
  let now = 0
  const limits = over.limits ?? { warmupMs: 20, measureMs: 60 }
  const worker = new TelemetryWorker({
    kind: 'producer',
    client,
    tap,
    registry: over.registry ?? new Registry(client, 'producer', 2),
    clock: over.clock ?? { now: () => (now += 5) },
    sleep: async () => {},
    ordinary,
    limits,
    schedule: over.schedule ?? new TelemetrySchedule(limits),
  })
  if (!over.registry) worker.registry.fill()
  worker.uuid = 'uuid-1'
  worker.sampler = over.sampler ?? { endSampling: jest.fn(async () => log.push('end-sampling')) }
  client.connect.mockImplementation(async () => {
    log.push('connect')
    tap.state.pushes += 1
  })
  return { worker, log, tap, ordinary, client }
}

describe('telemetry profile worker', () => {
  test('retains immediate reopen ordering and normal pushes', async () => {
    const { worker, log, tap } = makeWorker()
    await worker.reopen()
    expect(log).toEqual(['drain', 'disconnect', 'connect', 'resume', 'cache-read'])
    expect(worker.reopens).toEqual([
      { order: ['disconnect-resolved', 'connect-called'], uuid: 'uuid-1', push: true, open: 1 },
    ])
    expect(tap.state.pushes).toBe(1)

    const changed = makeWorker({ client: { clientInstanceId: jest.fn(async () => 'uuid-2') } })
    await expect(changed.worker.reopen()).rejects.toThrow('instance ID changed')

    const silent = makeWorker()
    silent.client.connect.mockImplementation(async () => silent.log.push('connect'))
    await expect(silent.worker.reopen()).rejects.toThrow('No normal Push followed')
  })

  test('samples the full window and stops before cleanup', async () => {
    const order = []
    let samples = 0
    const sampler = {
      endSampling: jest.fn(async () => order.push(`end-sampling:${samples}`)),
    }
    const harness = makeWorker({
      sampler,
      ordinary: { stop: jest.fn(async () => order.push('cleanup-stop')) },
      limits: { warmupMs: 60000, measureMs: 300000 },
    })
    samples = 11
    const { worker } = harness
    await worker.work(360000)
    expect(order[0]).toBe('end-sampling:11')
    expect(order.indexOf('cleanup-stop')).toBeGreaterThan(0)
    expect(order.filter((entry) => entry.startsWith('end-sampling'))).toHaveLength(2)
    expect(worker.actions.measured).toEqual({ subscription: 30, retire: 10, reopen: 5 })
    expect(worker.requirements()).toEqual({ ok: true, violations: [] })
    expect(worker.timing.measurementMs).toBeGreaterThanOrEqual(300000)
    expect(worker.timing.cleanupMs).toBeGreaterThanOrEqual(0)
    await expect(worker.work(1000)).rejects.toThrow('fixed 360000 ms')
  })

  test('fails a run that missed a measured tick', () => {
    const { worker } = makeWorker()
    worker.actions.measured = { subscription: 30, retire: 10, reopen: 5 }
    expect(worker.requirements().ok).toBe(true)
    worker.missed.push({ at: 70000, actions: ['subscription'], late: 9000 })
    expect(worker.requirements().violations).toContain('a measured tick was missed')
    worker.missed.length = 0
    worker.actions.measured.reopen = 4
    expect(worker.requirements().violations).toContain('measured reopen actions 4')
  })

  test.each([
    [
      'sampler join',
      { sampler: { endSampling: jest.fn(async () => Promise.reject(new Error('sampler'))) } },
      'sampler',
    ],
    [
      'ordinary stop',
      { ordinary: { stop: jest.fn(async () => Promise.reject(new Error('stop'))) } },
      'stop',
    ],
    [
      'registry restore',
      {
        registry: {
          restore: jest.fn(() => {
            throw new Error('restore')
          }),
        },
      },
      'restore',
    ],
    [
      'disconnect',
      { client: { disconnect: jest.fn(async () => Promise.reject(new Error('disconnect'))) } },
      'disconnect',
    ],
  ])('restores owned resources after a %s failure', async (_name, over, message) => {
    const harness = makeWorker({ ...over, limits: { warmupMs: 10, measureMs: 20 } })
    const failure = await harness.worker.work(30).catch((error) => error)
    const messages =
      failure instanceof AggregateError ? failure.errors.map((e) => e.message) : [failure.message]
    expect(messages).toContain(message)
    expect(harness.ordinary.stop).toHaveBeenCalledTimes(1)
    expect(harness.client.disconnect).toHaveBeenCalledTimes(1)
  })

  test('keeps the primary workload failure ahead of cleanup failures', async () => {
    const harness = makeWorker({
      limits: { warmupMs: 10, measureMs: 20 },
      ordinary: {
        start: jest.fn(async () => Promise.reject(new Error('primary'))),
        stop: jest.fn(async () => Promise.reject(new Error('stop'))),
      },
    })
    const failure = await harness.worker.work(30).catch((error) => error)
    expect(failure).toBeInstanceOf(AggregateError)
    expect(failure.errors.map((e) => e.message)).toEqual(['primary', 'stop'])
  })
})

describe('telemetry profile sampling under the inherited Soak', () => {
  test('admits at least ten post-collection samples inside the window', async () => {
    jest.useFakeTimers()
    try {
      let samples = 0
      const probe = {
        snapshot: async () => ({
          sockets: { open: 0 },
          memory: { retainedBytes: 10, peakRss: 20 },
          identities: { sockets: [] },
          registryDigest: 'd',
          n: ++samples,
        }),
      }
      const clock = { now: () => Date.now() }
      const harness = makeWorker({
        clock,
        limits: { warmupMs: 60000, measureMs: 300000 },
      })
      harness.worker.sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
      let judged
      const soak = new Soak({
        kafka: {},
        probe,
        phases: PROFILES.telemetry,
        context: { telemetryChurn: harness.worker },
        sampleMs: TELEMETRY.sampleMs,
        warmupMs: TELEMETRY.warmupMs,
        settleMs: 0,
        clock,
        record: () => {},
        workloads: WORKLOADS,
        judge: (run) => {
          judged = run
          return { ok: true, violations: [] }
        },
      })
      harness.worker.sampler = soak
      const done = soak.run()
      for (let i = 0; i < 420; i++) await jest.advanceTimersByTimeAsync(1000)
      await done
      const measured = judged.samples.filter((s) => s.atMs >= TELEMETRY.warmupMs)
      expect(measured.length).toBeGreaterThanOrEqual(10)
      expect(Math.max(...judged.samples.map((s) => s.atMs))).toBeLessThanOrEqual(360000)
      expect(harness.worker.requirements().ok).toBe(true)
    } finally {
      jest.useRealTimers()
    }
  }, 60000)
})

describe('telemetry profile natural exit', () => {
  const rig = () => {
    jest.useFakeTimers()
    const spawned = []
    const processes = new TelemetryProcesses({
      script: 'driver.js',
      now: () => Date.now(),
      spawn: () => {
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
          kill: jest.fn(),
        })
        spawned.push(child)
        return child
      },
    })
    processes.start('producer')
    return { processes, child: spawned[0] }
  }
  afterEach(() => jest.useRealTimers())

  test('joins children naturally and treats timeouts as failures', async () => {
    const clean = rig()
    clean.processes.receive('producer', { type: 'receipt' })
    const joined = clean.processes.join('producer')
    await jest.advanceTimersByTimeAsync(9000)
    clean.child.emit('close', 0, null)
    await expect(joined).resolves.toMatchObject({
      event: 'close',
      code: 0,
      signal: null,
      stderrBytes: 0,
      unhandled: 0,
      watchdogExpired: false,
      afterDisconnectMs: 9000,
    })

    const late = rig()
    late.processes.receive('producer', { type: 'receipt' })
    const lateJoin = late.processes.join('producer')
    await jest.advanceTimersByTimeAsync(10001)
    late.child.emit('close', 0, null)
    await expect(lateJoin).resolves.toMatchObject({ event: 'timeout', watchdogExpired: true })

    const noReceipt = rig()
    noReceipt.child.emit('close', 0, null)
    await expect(noReceipt.processes.join('producer')).resolves.toMatchObject({
      event: 'close',
      afterDisconnectMs: null,
    })

    const killed = rig()
    killed.processes.receive('producer', { type: 'receipt' })
    killed.child.stderr.emit('data', 'boom')
    killed.processes.receive('producer', { type: 'unhandled' })
    const killedJoin = killed.processes.join('producer')
    killed.child.emit('close', null, 'SIGTERM')
    await expect(killedJoin).resolves.toMatchObject({
      code: null,
      signal: 'SIGTERM',
      stderrBytes: 4,
      unhandled: 1,
    })
  })
})

/** The identity of record `index` of a kind, as the parent's corpus names it. */
const ids = (kind, count = 10000) => Array.from({ length: count }, (_, i) => recordId('s', kind, i))

describe('telemetry profile parent', () => {
  const flat = () => {
    const snapshot = () => ({
      memory: { retainedBytes: 10, peakRss: 20 },
      registryDigest: 'd',
      identities: { sockets: [] },
    })
    return {
      baseline: snapshot(),
      samples: Array.from({ length: 11 }, (_, i) => ({
        atMs: 60000 + i * 30000,
        snapshot: snapshot(),
      })),
      teardown: snapshot(),
      measurementMs: 300000,
      cleanupMs: 1000,
    }
  }
  const natural = (kind, over = {}) => ({
    kind,
    event: 'close',
    code: 0,
    signal: null,
    stderrBytes: 0,
    unhandled: 0,
    watchdogExpired: false,
    afterDisconnectMs: 100,
    ...over,
  })
  const result = (kind, over = {}) => ({
    type: 'result',
    kind,
    clientId: `s-${kind}`,
    uuid: `uuid-${kind}`,
    input: flat(),
    requirements: { ok: true, violations: [] },
    rotations: Array.from({ length: 30 }, (_, index) => ({ index, confirmed: true })),
    ordinary: {
      offered: ids(kind),
      handled: ids(kind),
      processed: { 0: '3' },
    },
    ...over,
  })
  const build = (over = {}) => {
    const rows = []
    const sends = []
    let t = 0
    const fixture = {
      topic: (kind) => `t-${kind}`,
      group: (kind) => `g-${kind}`,
      open: jest.fn(async () => {}),
      inResource: jest.fn(async (work) => work()),
      rotate: jest.fn(async () => 'value'),
      produce: jest.fn(async () => []),
      durable: jest.fn(async (kind) => ids(kind)),
      committed: jest.fn(async () => ({ 0: '3' })),
      close: jest.fn(async () => {}),
      ...over.fixture,
    }
    const processes = {
      workers: new Map(
        KINDS.map((kind) => [kind, { child: { send: (m) => sends.push([kind, m]) }, exit: null }])
      ),
      start: jest.fn(),
      message: jest.fn(async (kind, type) =>
        type === 'result' ? (over.results?.[kind] ?? result(kind)) : { type, kind }
      ),
      join: jest.fn(async (kind) => over.exits?.[kind] ?? natural(kind)),
      taint: jest.fn(),
      ...over.processes,
    }
    const churn = new TelemetryChurn({
      fixture,
      processes,
      record: (row) => rows.push(row),
      clock: { now: () => (t += 250) },
      sleep: async () => {},
      limits: TELEMETRY,
      verdict: (input) => TelemetryResourceVerdict.evaluate(input),
      brokers: ['a:1'],
      stem: 's',
    })
    return { churn, fixture, processes, rows, sends }
  }
  /** A corpus where the consumer committed 3 and processed 3 on partition 0. */
  const patched = (over) => build(over)

  test('starts one worker for each kind and writes the four-and-four rows with one verdict', async () => {
    const { churn, processes, rows, sends } = patched({})
    const outcome = await churn.run()
    expect(outcome.ok).toBe(true)
    expect(outcome.clientKinds).toEqual([...KINDS])
    expect(processes.start.mock.calls.map(([kind]) => kind)).toEqual([...KINDS])
    expect(sends.filter(([, message]) => message.type === 'config').map(([kind]) => kind)).toEqual([
      ...KINDS,
    ])
    expect(
      rows.filter((row) => row.kind === 'resource-verdict').map((row) => row.clientKind)
    ).toEqual([...KINDS])
    expect(rows.filter((row) => row.kind === 'natural-exit')).toHaveLength(4)
    expect(rows.filter((row) => row.kind === 'oracle')).toHaveLength(1)
    expect(rows.filter((row) => row.kind === 'verdict')).toHaveLength(1)
    for (const row of rows.filter((r) => r.kind === 'resource-verdict')) {
      expect(row.input.clientCount).toBe(1)
      expect(row.result.ok).toBe(true)
    }
    expect(sends.filter(([, m]) => m.type === 'rotated')).toHaveLength(4 * 35)
  })

  test.each([
    ['a signal', { signal: 'SIGTERM', code: null }],
    ['a nonzero code', { code: 1 }],
    ['unexpected stderr', { stderrBytes: 1 }],
    ['an unhandled rejection', { unhandled: 1 }],
    ['a watchdog expiry', { watchdogExpired: true, event: 'timeout' }],
    ['a late close', { afterDisconnectMs: 10001 }],
    ['a missing receipt', { afterDisconnectMs: null }],
  ])('rejects %s as a clean exit', async (_name, over) => {
    const { churn } = patched({ exits: { consumer: natural('consumer', over) } })
    const outcome = await churn.run()
    expect(outcome.ok).toBe(false)
    expect(outcome.violations).toContain('consumer: the child did not exit naturally')
  })

  test('does not let an aggregate or a missing kind stand in for one worker', async () => {
    const bad = flat()
    bad.samples[3].snapshot.memory.retainedBytes += 100 * 1024 * 1024
    const { churn } = patched({ results: { admin: result('admin', { input: bad }) } })
    const outcome = await churn.run()
    expect(outcome.ok).toBe(false)
    expect(outcome.violations.join('\n')).toMatch(/admin: .*active retained ceiling/)
    const missing = patched({
      processes: {
        message: jest.fn(async (kind, type) => {
          if (kind === 'shareConsumer' && type === 'result') throw new Error('no result')
          return type === 'result' ? result(kind) : { type, kind }
        }),
      },
    })
    await expect(missing.churn.run()).rejects.toThrow('no result')
  })

  test.each([
    ['open', { fixture: { open: jest.fn(async () => Promise.reject(new Error('open'))) } }],
    [
      'resource',
      { fixture: { inResource: jest.fn(async () => Promise.reject(new Error('resource'))) } },
    ],
    [
      'rotation',
      { fixture: { rotate: jest.fn(async () => Promise.reject(new Error('rotation'))) } },
    ],
    ['oracle', { fixture: { durable: jest.fn(async () => Promise.reject(new Error('oracle'))) } }],
  ])('restores owned resources after a %s failure', async (name, over) => {
    const { churn, fixture, processes } = patched(over)
    await expect(churn.run()).rejects.toThrow(name)
    processes.workers.get('producer').exit = null
    processes.join.mockImplementation(async () => ({ event: 'timeout' }))
    await churn.close()
    expect(fixture.close).toHaveBeenCalledTimes(1)
    expect(processes.taint).toHaveBeenCalled()
    await churn.close()
    expect(fixture.close).toHaveBeenCalledTimes(1)
  })

  test('preserves a falsy primary failure and a failed cleanup', async () => {
    const { churn } = patched({
      fixture: {
        open: jest.fn(async () => {
          // eslint-disable-next-line no-throw-literal
          throw undefined
        }),
        close: jest.fn(async () => Promise.reject(new Error('closing'))),
      },
    })
    await expect(churn.run()).rejects.toBeUndefined()
    await expect(churn.close()).rejects.toThrow('closing')
  })
})

describe('telemetry profile oracle', () => {
  const sets = () => ({
    expected: ids('producer'),
    durable: ids('producer'),
    handled: ids('producer'),
  })

  test('preserves record commit and handler identities', () => {
    const control = OrdinaryOracle.check({
      ...sets(),
      committed: { 0: '5' },
      processed: { 0: '5' },
    })
    expect(control.ok).toBe(true)
    expect(Object.keys(control.hashes)).toEqual(['expected', 'durable', 'handled'])
    const cases = {
      'a dropped durable record': { durable: sets().durable.slice(1) },
      'a duplicated durable record': { durable: [...sets().durable.slice(1), sets().durable[1]] },
      'a same-count substitution': {
        handled: [...sets().handled.slice(1), recordId('s', 'consumer', 0)],
      },
      'a replayed callback': { handled: [...sets().handled, sets().handled[0]] },
      'an incorrect committed end': { committed: { 0: '4' }, processed: { 0: '5' } },
      'a misclassified share suffix': {
        handled: [...sets().handled.slice(0, -1), recordId('s', 'shareConsumer', 5)],
      },
    }
    for (const [name, change] of Object.entries(cases)) {
      const outcome = OrdinaryOracle.check({
        ...sets(),
        committed: { 0: '5' },
        processed: { 0: '5' },
        ...change,
      })
      expect({ name, ok: outcome.ok }).toEqual({ name, ok: false })
      expect(outcome.violations.length).toBeGreaterThan(0)
    }
  })

  test('counts the identities an ordinary producer resolved, never the ones it only offered', async () => {
    const make = (send) => {
      const work = new OrdinaryWork({
        kind: 'producer',
        client: { send },
        stem: 's',
        topic: 't',
        clock: { now: () => 0 },
        sleep: async () => {
          work.running = false
        },
      })
      return work
    }
    const ok = jest.fn(async () => {})
    const resolved = make(ok)
    await resolved.start()
    await resolved.stop()
    expect(ok).toHaveBeenCalledTimes(1)
    expect(ok.mock.calls[0][0]).toMatchObject({ topic: 't', acks: -1 })
    expect(ok.mock.calls[0][0].messages).toHaveLength(100)
    expect(ok.mock.calls[0][0].messages.every((m) => m.value.length === 200)).toBe(true)
    expect(resolved.ids().offered).toEqual(ids('producer', 100))

    const refused = make(jest.fn(async () => Promise.reject(new Error('send failed'))))
    await refused.start()
    await expect(refused.stop()).rejects.toThrow('send failed')
    expect(refused.ids().offered).toEqual([])
  })
})

describe('telemetry profile worker process', () => {
  test('refuses to run without its parent channel', async () => {
    const worker = new TelemetryWorkerProcess({ kind: 'producer', soak: {}, proc: {} })
    await expect(worker.run()).rejects.toThrow('parent channel')
  })
})
