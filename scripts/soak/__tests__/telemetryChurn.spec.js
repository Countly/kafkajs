const { EventEmitter } = require('node:events')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const zlib = require('node:zlib')
const {
  CORPUS,
  DecoderCoordinator,
  KILL_JOIN_MS,
  KINDS,
  OrdinaryOracle,
  OrdinaryWork,
  ParentFixture,
  ProcessResources,
  Registry,
  ShareOracle,
  TelemetryChurn,
  TelemetryProcesses,
  TelemetryProofs,
  TelemetrySchedule,
  TelemetryTap,
  TelemetryWorker,
  TelemetryWorkerProcess,
  WORKER_ENV,
  WORKER_TOKEN,
  compactRuns,
  controlManifest,
  labelOf,
  logInputs,
  recordId,
  runCatalogue,
  sourceStamp,
  valueOf,
  writeFence,
} = require('../telemetryChurn')
const { DecoderMemory } = require('../../../testHelpers/telemetryFixtures')
const { SEGMENT_FORMAT, checkSegmentFiles } = require('../../../testHelpers/telemetrySegments')
const {
  PROFILES,
  TelemetryResourceVerdict,
  buildTelemetryChurn,
  instrument,
  main,
  parseArgs,
} = require('../soak')
const {
  verifyChurn,
  verifyChurnFence,
  verifyChurnHandshakes,
  verifyChurnLifecycles,
  verifyChurnReference,
  verifyChurnReopen,
  verifyChurnRetirement,
  verifyChurnStages,
  verifyChurnStream,
  verifyChurnTransition,
  verifyDecoderMemory,
} = require('../../e2e/telemetry/evidence')

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

const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

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

/** Waits for a condition across event loop turns, never longer than a thousand of them. */
const until = async (condition) => {
  for (let i = 0; i < 1000 && !condition(); i++) await turn()
  expect(condition()).toBe(true)
}

const U = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
const STEM = 's'

const fakeChild = () =>
  Object.assign(new EventEmitter(), {
    stdout: new EventEmitter(),
    stderr: new EventEmitter(),
    connected: true,
    send: jest.fn(),
    kill: jest.fn(),
  })

/** A supervisor over fake children, and a way for each to speak with its own handshake token. */
const rig = (options = {}) => {
  const children = []
  const processes = new TelemetryProcesses({
    script: 'driver.js',
    spawn: () => {
      const child = fakeChild()
      children.push(child)
      return child
    },
    ...options,
  })
  const say = (kind, message) =>
    processes.receive(kind, { kind, token: processes.workers.get(kind).token, ...message })
  return { processes, children, say }
}

describe('telemetry profile', () => {
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
        const child = fakeChild()
        spawned.push({ script, args, options, child })
        return child
      },
    })
    for (const kind of KINDS) processes.start(kind)
    expect(spawned.map(({ options }) => options.env[WORKER_ENV])).toEqual([...KINDS])
    expect(new Set(spawned.map(({ options }) => options.env[WORKER_TOKEN])).size).toBe(4)
    for (const { args, options } of spawned) {
      expect(args).toEqual(['--profile', 'telemetry'])
      expect(options.execArgv).toEqual([
        '--expose-gc',
        '--min-semi-space-size=1',
        '--max-semi-space-size=1',
      ])
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

  test('rejects a duplicate, substituted, early, uncatalogued or oversized message', async () => {
    let now = 0
    const onFailure = jest.fn()
    const { processes, say } = rig({ now: () => (now += 400), onFailure })
    for (const kind of KINDS) processes.start(kind)
    say('producer', { type: 'hello' })
    await expect(processes.message('producer', 'hello', 500)).resolves.toMatchObject({
      kind: 'producer',
    })
    processes.receive('consumer', { type: 'hello', kind: 'consumer', token: 'wrong' })
    await expect(processes.message('consumer', 'hello', 500)).rejects.toThrow('substituted')
    say('producer', { type: 'hello' })
    await expect(processes.message('producer', 'result', 500)).rejects.toThrow('a duplicate hello')
    say('admin', { type: 'ready' })
    await expect(processes.message('admin', 'ready', 500)).rejects.toThrow('before hello')
    say('shareConsumer', { type: 'hello' })
    say('shareConsumer', { type: 'surprise' })
    await expect(processes.message('shareConsumer', 'ready', 500)).rejects.toThrow(
      'unknown message surprise'
    )
    expect(onFailure.mock.calls.map(([kind]) => kind).sort()).toEqual([
      'admin',
      'consumer',
      'producer',
      'shareConsumer',
    ])
    const big = rig()
    big.processes.start('producer')
    big.say('producer', { type: 'hello' })
    big.say('producer', { type: 'stage-done', pad: 'x'.repeat(8 * 1024 * 1024 + 1) })
    await expect(big.processes.message('producer', 'ready', 500)).rejects.toThrow('IPC overflow')
    const silent = rig({ now: () => (now += 400) })
    silent.processes.start('admin')
    await expect(silent.processes.message('admin', 'hello', 500)).rejects.toThrow('sent no hello')
    const failing = rig()
    failing.processes.start('producer')
    failing.say('producer', { type: 'hello' })
    failing.say('producer', { type: 'failure', message: 'boom' })
    await expect(failing.processes.message('producer', 'result', 500)).rejects.toThrow(
      'Telemetry worker producer failed: boom'
    )
  })

  test('releases every subscription batch between lifecycle windows', () => {
    for (let measureMs = 60000; measureMs <= 300000; measureMs += 10000) {
      const schedule = new TelemetrySchedule({ warmupMs: 60000, measureMs })
      const transitions = schedule.ticks.flatMap((tick) => tick.transitions)
      expect(transitions).toEqual(
        Array.from({ length: (60000 + measureMs) / 10000 - 1 }, (_, n) => n + 1)
      )
      expect(
        schedule.ticks.filter((t) => t.actions.includes('reopen')).length + 2
      ).toBeLessThanOrEqual(16)
      for (const tick of schedule.ticks.filter((t) => t.transitions.length)) {
        expect(tick.actions).toContain('reopen')
        expect(schedule.stages(tick)[0]).toBe('confirm')
        expect(tick.transitions.every((index) => index * 10000 <= tick.at)).toBe(true)
      }
    }
  })

  test('holds the fixed measured action counts, the serialization order and the corpus slots', () => {
    const schedule = new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    expect(schedule.required()).toEqual({ subscription: 35, retire: 10, reopen: 5 })
    expect(schedule.ticks).toHaveLength(13)
    expect(
      schedule.ticks.filter((t) => t.transitions.length).map((t) => [t.at, t.transitions])
    ).toEqual([
      [70000, [1, 2, 3, 4, 5, 6, 7]],
      [120000, [8, 9, 10, 11, 12]],
      [180000, [13, 14, 15, 16, 17, 18]],
      [240000, [19, 20, 21, 22, 23, 24]],
      [350000, [25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35]],
    ])
    expect(schedule.ticks.filter((t) => t.actions.includes('retire')).map((t) => t.at)).toEqual(
      Array.from({ length: 11 }, (_, n) => (n + 1) * 30000)
    )
    schedule.ticks.forEach((tick, number) => {
      expect(tick.number).toBe(number)
      expect(tick.index).toBe(tick.at / 10000)
    })
    const sixty = schedule.ticks.find((tick) => tick.at === 60000)
    expect(sixty.actions).toEqual(['retire'])
    expect(schedule.stages(sixty)).toEqual(['retire'])
    const firstBatch = schedule.ticks.find((tick) => tick.transitions.length)
    expect(schedule.selection(firstBatch.index)).toBe('com.example.')
    expect(schedule.stages(firstBatch)).toEqual(['confirm'])
    expect(schedule.ticks.at(-1).at).toBe(350000)
    expect(
      schedule.ticks.filter((tick) => !schedule.measured(tick)).flatMap((t) => t.actions)
    ).toEqual(['retire'])
    expect([1, 2, 3, 4, 5, 6].map((i) => schedule.selection(i))).toEqual([
      'com.example.',
      '',
      'org.apache.kafka.',
      'com.example.',
      '',
      'org.apache.kafka.',
    ])
    for (const tick of schedule.ticks.filter(
      (t) => t.actions.includes('retire') && t.transitions.length
    ))
      expect(schedule.selection(tick.index)).not.toBe('')
    const slots = schedule.corpusSlots()
    expect(slots).toHaveLength(CORPUS.records / CORPUS.batch)
    expect(slots[0]).toBe(60000)
    expect(slots.at(-1)).toBe(357000)
    expect(slots.every((slot) => slot >= 60000 && slot < 360000)).toBe(true)
  })

  test('tracks resources by creating role and trigger, and a same-count swap by token', async () => {
    const resources = new ProcessResources()
    resources.start()
    const timers = []
    try {
      expect(Object.keys(resources.identities()).sort()).toEqual([
        'resources:fixture',
        'resources:target',
        'resources:unknown',
      ])
      const target = resources.as('target', () => setTimeout(() => {}, 1e6))
      timers.push(target)
      timers.push(resources.as('fixture:sampler', () => setTimeout(() => {}, 1e6)))
      timers.push(setTimeout(() => {}, 1e6))
      await turn()
      const roles = resources.identities()
      expect(roles['resources:target']).toHaveLength(1)
      expect(roles['resources:fixture']).toHaveLength(1)
      expect(roles['resources:unknown']).toHaveLength(1)
      expect(roles['resources:target'][0]).toMatchObject({ type: 'Timeout', role: 'target' })
      expect(roles['resources:fixture'][0].role).toBe('fixture:sampler')
      const before = roles['resources:target']
      clearTimeout(target)
      await turn()
      await turn()
      timers.push(resources.as('target', () => setTimeout(() => {}, 1e6)))
      await turn()
      const after = resources.identities()['resources:target']
      expect(after).toHaveLength(before.length)
      expect(after.filter((token) => before.includes(token))).toHaveLength(0)
    } finally {
      for (const timer of timers) clearTimeout(timer)
      resources.stop()
    }
    expect(resources.identities()).toEqual({
      'resources:target': [],
      'resources:fixture': [],
      'resources:unknown': [],
    })
  })

  test('keeps the registry fingerprint through replacement and restore, and fails atomically', () => {
    const calls = []
    const client = {
      registerMetricForSubscription: jest.fn((metric) => calls.push(['add', metric.name])),
      unregisterMetricFromSubscription: jest.fn((id) => calls.push(['remove', id.name])),
    }
    let now = 100
    let cursor = 0
    const fence = jest.fn(() => ({ cursors: { 'producer-lc-1': cursor } }))
    const registry = new Registry(client, 'producer', 128, { now: () => (now += 1) }, fence)
    registry.fill()
    expect(registry.registered).toHaveLength(128)
    const canonical = registry.digest('uuid')
    client.unregisterMetricFromSubscription.mockImplementationOnce((id) => {
      calls.push(['remove', id.name])
      cursor = 4
    })
    client.registerMetricForSubscription.mockImplementationOnce((metric) => {
      calls.push(['add', metric.name])
      cursor = 5
    })
    const entry = registry.replace(7)
    expect(entry).toEqual({
      type: 'replace',
      tick: 7,
      removedVariant: 'a',
      addedVariant: 'b',
      unregisteredAt: 101,
      unregisterFence: { cursors: { 'producer-lc-1': 4 } },
      registeredAt: 102,
      registerFence: { cursors: { 'producer-lc-1': 5 } },
      at: 102,
    })
    expect(calls.slice(-2).map(([op]) => op)).toEqual(['remove', 'add'])
    expect(registry.registered).toHaveLength(128)
    expect(registry.digest('uuid')).not.toBe(canonical)
    registry.restore()
    expect(registry.digest('uuid')).toBe(canonical)
    expect(registry.digest('other')).not.toBe(canonical)
    expect(registry.replacements.map((r) => [r.tick, r.addedVariant])).toEqual([
      [7, 'b'],
      [null, 'a'],
    ])
    const bytes = registry.bytes
    client.registerMetricForSubscription.mockImplementationOnce(() => {
      throw new Error('refused')
    })
    expect(() => registry.replace()).toThrow('refused')
    expect(registry.bytes).toBeLessThan(bytes)
    expect(registry.registered).toHaveLength(127)
  })

  test('journals the cursor of every segment that can still record a request', () => {
    const segments = {
      undischarged: () => [
        { id: 'producer-lc-1', observer: { checkpoint: () => 9 } },
        { id: 'producer-lc-2', observer: { checkpoint: () => 3 } },
        { id: 'producer-lc-0', observer: null },
      ],
    }
    expect(writeFence(segments)).toEqual({ cursors: { 'producer-lc-1': 9, 'producer-lc-2': 3 } })
  })

  test('freezes one foreground catalogue per kind and records every outcome of a replay', async () => {
    const config = { topic: 's-producer', ctl: 's-ctl' }
    const producer = controlManifest('producer', config)
    expect(producer).toMatchObject({ kind: 'producer', topic: 's-producer', joins: false })
    expect(producer.operations).toHaveLength(20)
    expect(producer.operations[0]).toEqual({
      n: 0,
      op: 'send',
      topic: 's-ctl',
      valueBytes: 7,
      expected: 'fulfilled',
    })
    expect(controlManifest('admin', config).operations.map((o) => o.op)).toEqual(
      Array(10).fill('describeCluster')
    )
    expect(controlManifest('shareConsumer', config)).toMatchObject({ joins: true, operations: [] })
    let now = 0
    const send = jest.fn(async () => [])
    const outcomes = await runCatalogue({ send }, producer, 's:ctl-on', () => ++now)
    expect(outcomes).toHaveLength(20)
    expect(outcomes[19]).toEqual({ n: 19, status: 'fulfilled', at: 20 })
    expect(send.mock.calls[3][0]).toMatchObject({
      topic: 's-ctl',
      acks: -1,
      messages: [{ key: 's:ctl-on:3', value: Buffer.alloc(7, 99) }],
    })
    const refusal = new Error('refused')
    const failing = {
      describeCluster: jest.fn().mockResolvedValueOnce({}).mockRejectedValueOnce(refusal),
    }
    await expect(
      runCatalogue(failing, controlManifest('admin', config), 'x', () => 1)
    ).rejects.toBe(refusal)
    expect(failing.describeCluster).toHaveBeenCalledTimes(2)
  })

  describe('transport tap', () => {
    const frame = (api) => {
      const bytes = Buffer.alloc(16)
      bytes.writeInt32BE(12, 0)
      bytes.writeInt16BE(api, 4)
      return bytes
    }
    const socket = () =>
      Object.assign(new EventEmitter(), { write: jest.fn(() => true), destroy: jest.fn() })
    const make = () => {
      const owners = []
      const observer = {
        socketOwnership: () => owners,
        declareFault: jest.fn(),
      }
      const segment = { id: 'producer-lc-1', observer }
      let now = 0
      const tap = new TelemetryTap(
        { socketFactory: jest.fn(), current: () => segment },
        { clock: { now: () => (now += 1) } }
      )
      const add = () => {
        owners.push({})
        return tap.track(socket(), segment)
      }
      return { tap, observer, add }
    }

    test('classifies the telemetry transport by its own frames and faults only the next exchange', async () => {
      const { tap, observer, add } = make()
      const core = add()
      const telemetry = add()
      core.write(frame(3))
      telemetry.write(frame(71))
      expect(tap.telemetry()).toHaveLength(1)
      expect(tap.open()).toBe(2)
      const fault = tap.retire()
      expect(fault).toMatchObject({ segment: 'producer-lc-1', ordinal: 2 })
      expect(observer.declareFault).toHaveBeenCalledWith(2)
      expect(telemetry.destroy).not.toHaveBeenCalled()
      telemetry.write(frame(3))
      expect(telemetry.destroy).not.toHaveBeenCalled()
      core.write(frame(3))
      expect(core.destroy).not.toHaveBeenCalled()
      telemetry.write(frame(72))
      expect(telemetry.destroy).toHaveBeenCalledTimes(1)
      expect(core.destroy).not.toHaveBeenCalled()
      await expect(fault.hit).resolves.toBeGreaterThan(fault.declaredAt)
      telemetry.write(frame(72))
      expect(telemetry.destroy).toHaveBeenCalledTimes(1)
      telemetry.emit('close')
      expect(tap.telemetry()).toHaveLength(0)
      expect(tap.records[1].socket).toBeNull()
      expect(() => tap.retire()).toThrow('exactly one open telemetry socket')
    })

    test('releases closed factory sockets while the tap stays alive and preserves scalar fault facts', async () => {
      const owners = []
      const faults = []
      let now = 0
      const segment = {
        id: 'producer-lc-1',
        observer: {
          socketOwnership: () => owners,
          declareFault: (ordinal) => faults.push({ ordinal, at: now }),
        },
      }
      const tap = new TelemetryTap(
        {
          current: () => segment,
          socketFactory: () => {
            owners.push({})
            const inner = new EventEmitter()
            inner.destroyed = false
            inner.write = () => true
            inner.destroy = (error) => {
              inner.destroyed = true
              if (error) inner.emit('error', error)
              inner.emit('close')
            }
            return inner
          },
        },
        { clock: { now: () => ++now } }
      )
      const core = tap.factory({ host: 'a', port: 1 })
      let telemetry = tap.factory({ host: 'a', port: 1 })
      telemetry.on('error', () => {})
      telemetry.write(frame(71))
      const fault = tap.retire()
      expect(fault).toMatchObject({ segment: 'producer-lc-1', ordinal: 2, declaredAt: 1 })
      const released = new WeakRef(telemetry)
      telemetry.write(frame(72))
      await expect(fault.hit).resolves.toBe(2)
      expect(faults).toEqual([{ ordinal: 2, at: 1 }])
      expect(tap.open()).toBe(1)
      expect(tap.telemetry()).toEqual([])
      expect(tap.chaos.sockets).toEqual([core])
      expect(tap.records[1]).toMatchObject({
        segment: 'producer-lc-1',
        ordinal: 2,
        telemetry: true,
        closed: true,
        closedAt: 3,
        socket: null,
        restore: null,
        faulted: null,
      })
      telemetry = null

      if (typeof global.gc === 'function') {
        for (let i = 0; i < 10; i++) {
          await turn()
          global.gc()
        }
        expect(released.deref()).toBeUndefined()
        expect(tap.open()).toBe(1)
        expect(faults).toEqual([{ ordinal: 2, at: 1 }])
      }
      core.destroy()
      expect(tap.chaos.sockets).toEqual([])
      expect(tap.records.map(({ socket }) => socket)).toEqual([null, null])
      tap.records.length = 1024
      expect(() => tap.factory({ host: 'a', port: 1 })).toThrow('socket history bound exceeded')
      expect(owners).toHaveLength(2)
      jest.isolateModules(() => {
        const segments = jest.requireActual('../../../testHelpers/telemetrySegments')
        const schedule = jest.spyOn(segments, 'scheduleOf')
        try {
          const { TelemetryTap } = require('../telemetryChurn')
          const capped = new TelemetryTap({ socketFactory: jest.fn() })
          capped.records.length = 1024
          expect(() => capped.factory({ host: 'a', port: 1 })).toThrow(
            'socket history bound exceeded'
          )
          expect(schedule).toHaveBeenCalledWith('telemetry-churn')
          expect(schedule.mock.results[0].value.cap).toBe(16)
          expect(capped.segments.socketFactory).not.toHaveBeenCalled()
        } finally {
          schedule.mockRestore()
        }
      })
    })

    test('refuses to fault when more than one telemetry socket is open', () => {
      const { tap, add } = make()
      add().write(frame(71))
      add().write(frame(72))
      expect(() => tap.retire()).toThrow('exactly one open telemetry socket')
    })
  })

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
    expect(row.identities['listener:socket1:data']).toHaveLength(2)
    expect(Object.keys(row.identities)).not.toContain('listener:sockets:data')
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
          { publicOnly: true, activeResources: false, memory: true }
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

describe('telemetry wire proofs', () => {
  const get = (index, over = {}) => ({
    type: 'request',
    apiKey: 71,
    index,
    requestKey: `r${index}`,
    socketId: 1,
    uuid: U,
    writeAt: index * 10,
    response: {
      errorCode: 0,
      uuid: U,
      subscriptionId: 7,
      pushIntervalMs: 1000,
      requestedMetrics: ['org.apache.kafka.'],
      responseAt: index * 10 + 5,
    },
    ...over,
  })
  const push = (index, over = {}) => ({
    type: 'request',
    apiKey: 72,
    index,
    requestKey: `r${index}`,
    socketId: 1,
    uuid: U,
    subscriptionId: 7,
    terminating: false,
    metricsBytes: 10,
    writeAt: index * 10,
    response: { errorCode: 0, responseAt: index * 10 + 5 },
    ...over,
  })

  test('counts only a successful Get that carried and returned the assigned UUID', () => {
    const zero = get(1, { uuid: '00000000-0000-0000-0000-000000000000' })
    const failed = get(3, { response: { errorCode: 3 } })
    const foreign = get(4, { response: { ...get(4).response, uuid: 'other' } })
    expect(
      TelemetryProofs.gets([zero, get(2), failed, foreign], U).map((r) => r.requestKey)
    ).toEqual(['r2'])
  })

  test('proves a replacement only with a later accepted nonempty Push on the same socket', () => {
    const records = [get(1), push(2)]
    expect(TelemetryProofs.provingPush(records, records[0]).requestKey).toBe('r2')
    const rejected = [
      ['a Push at the Get response time', push(2, { writeAt: 15 })],
      ['a Push on another socket', push(2, { socketId: 2 })],
      ['a final', push(2, { terminating: true })],
      ['an empty Push', push(2, { metricsBytes: 0 })],
      ['a rejected Push', push(2, { response: { errorCode: 42, responseAt: 25 } })],
      ['a Push of another client', push(2, { uuid: 'other' })],
      ['a Push before the Get', push(0, { writeAt: 1, index: 0 })],
    ]
    for (const [, other] of rejected)
      expect(TelemetryProofs.provingPush([get(1), other], get(1))).toBeUndefined()
  })

  test('requires the exact requested list and interval for a selection Get', () => {
    const custom = get(2, {
      response: { ...get(2).response, requestedMetrics: ['com.example.'], subscriptionId: 8 },
    })
    const empty = get(3, { response: { ...get(3).response, requestedMetrics: [] } })
    const slow = get(4, { response: { ...get(4).response, pushIntervalMs: 300000 } })
    const records = [get(1), custom, empty, slow]
    expect(
      TelemetryProofs.selectionGet(records, U, { selection: ['com.example.'] }).requestKey
    ).toBe('r2')
    expect(TelemetryProofs.selectionGet(records, U, { selection: [] }).requestKey).toBe('r3')
    expect(TelemetryProofs.selectionGet(records, U, { selection: ['org.'] })).toBeUndefined()
    expect(
      TelemetryProofs.selectionGet(records, U, { selection: ['com.example.'], after: 21 })
    ).toBeUndefined()
    expect(TelemetryProofs.selectionGet([slow], U, { selection: ['org.apache.kafka.'] })).toBe(
      undefined
    )
  })

  test('declares the proof selection from the transcript alone', () => {
    const records = [
      get(1),
      push(2),
      push(3),
      get(4, {
        response: { ...get(4).response, requestedMetrics: ['com.example.'], subscriptionId: 8 },
      }),
      push(5, { subscriptionId: 8 }),
      push(6, { subscriptionId: 8 }),
      get(7, { socketId: 2 }),
      push(8, { socketId: 2 }),
      push(9, { terminating: true, socketId: 2 }),
      push(10, { socketId: 2, response: { errorCode: 42, responseAt: 105 } }),
    ]
    expect(TelemetryProofs.selection(records)).toEqual(['r2', 'r5', 'r8', 'r9'])
    expect(TelemetryProofs.selection([get(1)])).toEqual([])
  })

  test('selects the terminal segment final like every other accepted final', () => {
    const records = [get(1), push(2), push(3, { terminating: true })]
    expect(TelemetryProofs.selection(records)).toEqual(['r2', 'r3'])
    const empty = [get(1), push(2), push(3, { terminating: true, metricsBytes: 0 })]
    expect(TelemetryProofs.selection(empty)).toEqual(['r2'])
  })
})

describe('telemetry profile oracles', () => {
  const logFor = (kind, tamper = () => {}) => {
    const parts = [[], [], []]
    for (let i = 0; i < CORPUS.records; i++) {
      const id = recordId(STEM, kind, i)
      parts[i % 3].push({ offset: String(parts[i % 3].length), key: id, value: valueOf(id) })
    }
    tamper(parts)
    return new Map(parts.map((records, partition) => [partition, { records }]))
  }
  const inputsOf = (kind, tamper) => logInputs(logFor(kind, tamper), STEM, kind)
  const handledOf = (log) =>
    Object.entries(log.partitions).flatMap(([partition, rows]) =>
      rows.map(([offset, index]) => [Number(partition), offset, index, 12, 13])
    )

  test('compacts neighbouring settlements into ranges without changing any field', () => {
    const fact = (offset, over = {}) => ({
      topicId: 'T',
      partition: 0,
      offset: String(offset),
      type: 1,
      errorCode: 0,
      at: 5,
      ...over,
    })
    expect(compactRuns([fact(0), fact(1), fact(2), fact(4), fact(5, { type: 2 })])).toEqual([
      {
        topicId: 'T',
        partition: 0,
        type: 1,
        errorCode: 0,
        at: 5,
        firstOffset: '0',
        lastOffset: '2',
      },
      {
        topicId: 'T',
        partition: 0,
        type: 1,
        errorCode: 0,
        at: 5,
        firstOffset: '4',
        lastOffset: '4',
      },
      {
        topicId: 'T',
        partition: 0,
        type: 2,
        errorCode: 0,
        at: 5,
        firstOffset: '5',
        lastOffset: '5',
      },
    ])
  })

  test('preserves record commit and handler identities', () => {
    const log = inputsOf('consumer')
    const handled = handledOf(log)
    const good = () => ({
      kind: 'consumer',
      stem: STEM,
      log,
      handled: handled.map((e) => [...e]),
      processed: { ...log.ends },
      committed: { ...log.ends },
    })
    const ok = OrdinaryOracle.check(good())
    expect(ok).toMatchObject({ ok: true, violations: [] })
    const same = OrdinaryOracle.check(good())
    expect(same.hashes).toEqual(ok.hashes)
    const control = (change) => {
      const input = good()
      change(input)
      return OrdinaryOracle.check(input)
    }
    expect(control((i) => i.handled.pop()).violations).toContain(
      'handler identities are incomplete'
    )
    expect(control((i) => i.handled.push([...i.handled[0]])).violations).toContain(
      'handler repeats an identity'
    )
    expect(
      control((i) => {
        i.handled[5][2] = i.handled[6][2]
      }).violations
    ).toEqual(
      expect.arrayContaining([
        'handler repeats an identity',
        'a handler entry differs from the raw log at its offset',
      ])
    )
    expect(
      control((i) => {
        i.committed['1'] = String(Number(i.committed['1']) - 1)
      }).violations
    ).toContain('committed end differs on 1')
    expect(
      control((i) => {
        i.processed['0'] = String(Number(i.processed['0']) + 1)
      }).violations
    ).toEqual(
      expect.arrayContaining([
        'processed end differs from the log end on 0',
        'committed end differs on 0',
      ])
    )
    const altered = inputsOf('consumer', (parts) => {
      parts[0][3].value = Buffer.from('changed')
    })
    expect(OrdinaryOracle.check({ ...good(), log: altered }).violations).toContain(
      'log 0 value bytes differ from the corpus'
    )
    const missing = inputsOf('consumer', (parts) => parts[2].pop())
    expect(OrdinaryOracle.check({ ...good(), log: missing }).violations).toContain(
      'durable identities are incomplete'
    )
  })

  test('keeps the producer offer and resolution ledgers apart from the raw log', () => {
    const log = inputsOf('producer')
    const all = Array.from({ length: CORPUS.records }, (_, i) => i)
    expect(
      OrdinaryOracle.check({ kind: 'producer', stem: STEM, log, offered: all, resolved: all })
    ).toMatchObject({ ok: true })
    const dropped = OrdinaryOracle.check({
      kind: 'producer',
      stem: STEM,
      log,
      offered: all,
      resolved: all.slice(1),
    })
    expect(dropped.violations).toEqual(['resolved identities are incomplete'])
  })

  describe('share', () => {
    const G = 'G'
    let n = 0
    const exchange = (apiKey, at, request, response) => ({
      n: ++n,
      segment: 'shareConsumer-lc-1',
      socket: 1,
      apiKey,
      apiVersion: 2,
      correlationId: n,
      at,
      responseAt: at + 1,
      requestSha256: 'a'.repeat(64),
      responseSha256: response ? 'b'.repeat(64) : null,
      request: { groupId: G, isRenewAck: false, topics: request },
      response,
    })
    /** One ShareFetch acquiring `[partition, first, last, deliveryCount, fetchErrorCode?]` ranges. */
    const fetchAt = (at, ranges) =>
      exchange(78, at, [], {
        errorCode: 0,
        lockMs: 15000,
        topics: [
          {
            topicId: 'T',
            partitions: ranges.map(([partition, first, last, deliveryCount, code = 0]) => ({
              partition,
              fetchErrorCode: code,
              acknowledgeErrorCode: 0,
              acquired: [{ firstOffset: String(first), lastOffset: String(last), deliveryCount }],
            })),
          },
        ],
      })
    /** One ShareAcknowledge of `[partition, first, last, type, acknowledgeErrorCode?]` batches. */
    const ackAt = (at, batches) => {
      const parts = [...new Set(batches.map(([p]) => p))]
      return exchange(
        79,
        at,
        [
          {
            topicId: 'T',
            partitions: parts.map((partition) => ({
              partition,
              batches: batches
                .filter(([p]) => p === partition)
                .map(([, first, last, type]) => ({
                  firstOffset: String(first),
                  lastOffset: String(last),
                  types: [type],
                })),
            })),
          },
        ],
        {
          errorCode: 0,
          lockMs: 0,
          topics: [
            {
              topicId: 'T',
              partitions: parts.map((partition) => ({
                partition,
                fetchErrorCode: null,
                acknowledgeErrorCode:
                  batches.find(([p, , , , code]) => p === partition && code)?.[4] ?? 0,
                acquired: [],
              })),
            },
          ],
        }
      )
    }
    const event = (partition, first, last, type, at) => ({
      topic: 'share',
      partition,
      firstOffset: String(first),
      lastOffset: String(last),
      type,
      error: null,
      at,
    })
    const ends = (log) =>
      Object.entries(log.partitions).map(([p, rows]) => [Number(p), rows.length - 1])
    /** Every offset fetched at 10, handled at 12/13, accepted by the broker at 21 and publicly. */
    const build = () => {
      n = 0
      const log = inputsOf('shareConsumer')
      const last = ends(log)
      return {
        stem: STEM,
        log,
        topicId: 'T',
        group: G,
        handled: handledOf(log),
        exchanges: [
          fetchAt(
            10,
            last.map(([p, l]) => [p, 0, l, 1])
          ),
          ackAt(
            20,
            last.map(([p, l]) => [p, 0, l, 1])
          ),
        ],
        settled: last.map(([p, l]) => event(p, 0, l, 'accept', 21)),
        closures: [],
      }
    }
    /**
     * A captured close at 15..18 with partition 0's first 100 records handled: the close accepts
     * that prefix and releases every other acquired record; the suffix is acquired again with the
     * next delivery count, handled and accepted once.
     */
    const mixed = () => {
      n = 0
      const log = inputsOf('shareConsumer')
      const last = ends(log)
      const prefix = new Set(Array.from({ length: 100 }, (_, o) => `0:${o}`))
      const handled = Object.entries(log.partitions).flatMap(([partition, rows]) =>
        rows.map(([offset, index]) =>
          prefix.has(`${partition}:${offset}`)
            ? [Number(partition), offset, index, 12, 13]
            : [Number(partition), offset, index, 22, 23]
        )
      )
      const suffix = last.map(([p, l]) => [p, p === 0 ? 100 : 0, l])
      return {
        stem: STEM,
        log,
        topicId: 'T',
        group: G,
        handled,
        exchanges: [
          fetchAt(
            10,
            last.map(([p, l]) => [p, 0, l, 1])
          ),
          ackAt(16, [[0, 0, 99, 1], ...suffix.map(([p, f, l]) => [p, f, l, 2])]),
          fetchAt(
            20,
            suffix.map(([p, f, l]) => [p, f, l, 2])
          ),
          ackAt(
            30,
            suffix.map(([p, f, l]) => [p, f, l, 1])
          ),
        ],
        settled: [
          event(0, 0, 99, 'accept', 17),
          ...suffix.map(([p, f, l]) => event(p, f, l, 'release', 17)),
          ...suffix.map(([p, f, l]) => event(p, f, l, 'accept', 31)),
        ],
        closures: [{ at: 15, end: 18 }],
      }
    }
    const check = (change, base = build) => {
      const input = base()
      change(input)
      return ShareOracle.check(input)
    }

    test('accepts every offset once at the broker and publicly with no duplicate delivery', () => {
      const verdict = ShareOracle.check(build())
      expect(verdict).toMatchObject({ ok: true, violations: [], failedAcknowledgements: 0 })
      expect(Object.keys(verdict.hashes)).toEqual(['durable', 'handled', 'exchanges'])
    })

    test('accepts the handled prefix, releases the suffix and accepts its one redelivery', () => {
      const verdict = ShareOracle.check(mixed())
      expect(verdict).toMatchObject({ ok: true, violations: [], failedAcknowledgements: 0 })
      expect(verdict.hashes.durable).toBe(ShareOracle.check(build()).hashes.durable)
      expect(verdict.hashes.exchanges).not.toBe(ShareOracle.check(build()).hashes.exchanges)
    })

    test.each([
      [
        'accepting the unhandled suffix at the close',
        (i) => {
          i.exchanges[1] = ackAt(16, [
            [0, 0, 99, 1],
            [1, 0, 0, 1],
          ])
        },
        'the close did not accept the handled prefix and release the rest',
      ],
      [
        'releasing the handled prefix',
        (i) => {
          i.exchanges[1].request.topics[0].partitions[0].batches[0].types = [2]
        },
        'the close did not accept the handled prefix and release the rest',
      ],
      [
        'repeating accepted work',
        (i) => i.handled.push([0, 0, 0, 22, 23]),
        'accepted work was delivered again',
      ],
      [
        'omitting the permitted suffix redelivery',
        (i) => i.exchanges.splice(2, 1),
        'deliveries differ from one plus the releases',
      ],
      [
        'a failed release acknowledgement',
        (i) => {
          i.exchanges[1].response.topics[0].partitions[1].acknowledgeErrorCode = 9
        },
        'public releases differ from broker releases',
      ],
      [
        'a failed redelivery fetch',
        (i) => {
          i.exchanges[2].response.topics[0].partitions[1].fetchErrorCode = 9
        },
        'a handler entry is not joined to its acquisition',
      ],
      [
        'a release settled after the next acquisition',
        (i) => {
          i.exchanges[1].responseAt = 25
        },
        'a decision is attached to another acquisition',
      ],
    ])('rejects %s', (_name, change, label) => {
      expect(check(change, mixed).violations).toContain(label)
    })

    test.each([
      [
        'a second accept',
        (i) => i.exchanges.push(ackAt(30, [[0, 0, 0, 1]])),
        'an offset lacks exactly one broker accept',
      ],
      [
        'a settlement after the accept',
        (i) => i.exchanges.push(ackAt(30, [[0, 0, 0, 2]])),
        'an offset was settled again after its accept',
      ],
      [
        'a failed acknowledgement, which is never a settlement',
        (i) => {
          i.exchanges[1].response.topics[0].partitions[0].acknowledgeErrorCode = 9
        },
        'an offset lacks exactly one broker accept',
      ],
      [
        'a missing public accept',
        (i) => i.settled.pop(),
        'an offset lacks exactly one public accept',
      ],
      [
        'a public acknowledgement error',
        (i) => (i.settled[0].error = { name: 'KafkaJSShareAcknowledgementError', reason: 'x' }),
        'a public acknowledgement carried an error',
      ],
      [
        'accepted work delivered again',
        (i) => i.handled.push([0, 0, 0, 25, 26]),
        'accepted work was delivered again',
      ],
      [
        'a release without a redelivery',
        (i) => {
          i.exchanges.splice(1, 0, ackAt(14, [[0, 0, 0, 2]]))
          i.settled.push(event(0, 0, 0, 'release', 16))
        },
        'deliveries differ from one plus the releases',
      ],
      [
        'a foreign topic',
        (i) => (i.exchanges[0].response.topics[0].topicId = 'X'),
        'an acquisition names a foreign topic',
      ],
      [
        'a same-offset handler identity with the wrong key',
        (i) => (i.handled[0][2] = 5),
        'a handler entry differs from the raw log at its offset',
      ],
      [
        'a missing handler identity',
        (i) => i.handled.shift(),
        'handler entries differ from one plus the releases',
      ],
      [
        'extra foreign handler work',
        (i) => i.handled.push([7, 0, 0, 12, 13]),
        'a handler entry names foreign work',
      ],
      [
        'an acquisition outside the corpus',
        (i) => i.exchanges.push(fetchAt(40, [[0, 90000, 90000, 1]])),
        'a share fact names an offset outside the corpus',
      ],
      [
        'one acquisition handled twice',
        (i) => i.handled.push([0, 0, 0, 12.5, 12.6]),
        'an acquisition was handled twice',
      ],
      [
        'a decision settled before its acquisition answered',
        (i) => (i.exchanges[1].responseAt = 10.5),
        'a decision is attached to another acquisition',
      ],
    ])('rejects %s', (_name, change, label) => {
      expect(check(change).violations).toContain(label)
    })

    test('counts a failed acknowledgement apart from the settlements', () => {
      const verdict = check((i) => {
        i.exchanges[1].response.topics[0].partitions[2].acknowledgeErrorCode = 9
      })
      expect(verdict.failedAcknowledgements).toBe(1)
      expect(verdict.ok).toBe(false)
    })

    test('requires the close to accept the handled prefix and release the rest', () => {
      const closed = (change = () => {}) => {
        const input = build()
        input.closures = [{ at: 15, end: 18 }]
        input.exchanges[1].at = 15.5
        input.exchanges[1].responseAt = 16
        change(input)
        return ShareOracle.check(input)
      }
      expect(closed()).toMatchObject({ ok: true })
      expect(
        closed((i) => {
          i.handled.forEach((e) => (e[4] = null))
        }).violations
      ).toContain('the close did not accept the handled prefix and release the rest')
      expect(closed((i) => (i.exchanges[1].responseAt = 40)).violations).toContain(
        'an offset was unsettled when its close ended'
      )
    })
  })
})

describe('telemetry rotation readback', () => {
  const entries = { 'interval.ms': '1000', match: 'client_id=^s-.*$' }
  const answer = (values) => ({
    resources: [
      {
        resourceType: 16,
        resourceName: 's',
        errorCode: 0,
        configEntries: Object.entries(values).map(([configName, configValue]) => ({
          configName,
          configValue,
        })),
      },
    ],
  })
  const make = () => {
    const admin = {
      incrementalAlterConfigs: jest.fn(async () => answer({})),
      describeConfigs: jest.fn(async () => answer({ metrics: 'com.example.', ...entries })),
    }
    const fixture = new ParentFixture({
      stem: 's',
      kafka: { admin: () => admin, producer: () => ({}) },
      clock: { now: () => Date.now() },
    })
    return { fixture, admin }
  }
  beforeEach(() => jest.useFakeTimers({ now: 1000 }))
  afterEach(() => jest.useRealTimers())

  test('converges after a stale successful read and records attempts', async () => {
    const { fixture, admin } = make()
    admin.describeConfigs.mockResolvedValueOnce(
      answer({ metrics: 'org.apache.kafka.', ...entries })
    )
    const result = fixture.rotate(1, 'com.example.', 300)
    result.catch(() => {})
    await jest.advanceTimersByTimeAsync(50)
    await expect(result).resolves.toEqual({
      index: 1,
      selection: 'com.example.',
      setAt: 1000,
      readbackAt: 1050,
      readback: 'com.example.',
      readbackAttempts: 2,
    })
    expect(admin.describeConfigs).toHaveBeenCalledTimes(2)
    expect(admin.describeConfigs).toHaveBeenLastCalledWith({
      timeout: 250,
      resources: [{ type: 16, name: 's', configNames: ['metrics', 'interval.ms', 'match'] }],
    })
    expect(jest.getTimerCount()).toBe(0)
  })

  test('fails API errors immediately without another read', async () => {
    const transport = new Error('describe failed')
    const failed = answer({ metrics: 'com.example.', ...entries })
    failed.resources[0].errorCode = 42
    for (const response of [transport, failed]) {
      const { fixture, admin } = make()
      if (response === transport) admin.describeConfigs.mockRejectedValueOnce(transport)
      else admin.describeConfigs.mockResolvedValueOnce(response)
      const result = fixture.rotate(1, 'com.example.', 300)
      if (response === transport) await expect(result).rejects.toBe(transport)
      else await expect(result).rejects.toThrow('Client metrics resource operation failed')
      expect(Date.now()).toBe(1000)
      expect(admin.describeConfigs).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    }
  })

  test('fails persistent mismatch at the original deadline with expected and observed values', async () => {
    const { fixture, admin } = make()
    admin.incrementalAlterConfigs.mockImplementationOnce(
      () => new Promise((resolve) => setTimeout(() => resolve(answer({})), 75))
    )
    admin.describeConfigs.mockResolvedValueOnce(
      answer({ metrics: 'org.apache.kafka.', ...entries })
    )
    let failure
    const result = fixture.rotate(14, '', 300).catch((error) => {
      failure = error
    })
    await jest.advanceTimersByTimeAsync(299)
    expect(failure).toBeUndefined()
    await jest.advanceTimersByTimeAsync(1)
    await result
    expect(failure).toBeInstanceOf(Error)
    expect(failure.message).toBe(
      'The subscription readback differs: ' +
        JSON.stringify({
          index: 14,
          expected: { metrics: '', ...entries },
          observed: { metrics: 'com.example.', ...entries },
        })
    )
    expect(Date.now()).toBe(1300)
    expect(admin.describeConfigs).toHaveBeenCalledTimes(5)
    const attempts = admin.describeConfigs.mock.calls.length
    await jest.advanceTimersByTimeAsync(500)
    expect(admin.describeConfigs).toHaveBeenCalledTimes(attempts)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('compares empty metrics exactly without accepting missing or null values', async () => {
    const { fixture, admin } = make()
    admin.describeConfigs
      .mockResolvedValueOnce(answer(entries))
      .mockResolvedValueOnce(answer({ metrics: null, ...entries }))
      .mockResolvedValueOnce(answer({ metrics: '', 'interval.ms': '999', match: entries.match }))
      .mockResolvedValueOnce(answer({ metrics: '', 'interval.ms': '1000', match: 'foreign' }))
      .mockResolvedValueOnce(answer({ metrics: '', ...entries }))
    const result = fixture.rotate(2, '', 300)
    result.catch(() => {})
    await jest.advanceTimersByTimeAsync(200)
    await expect(result).resolves.toMatchObject({
      selection: '',
      readback: '',
      readbackAt: 1200,
      readbackAttempts: 5,
    })
    expect(admin.describeConfigs).toHaveBeenCalledTimes(5)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('telemetry share corpus configuration', () => {
  const make = () => {
    let source = 5
    let value = 'latest'
    const resource = { resourceType: 32, resourceName: 's-share-group', errorCode: 0 }
    const admin = {
      connect: jest.fn(async () => {}),
      createTopics: jest.fn(async () => {}),
      incrementalAlterConfigs: jest.fn(async () => ({ resources: [resource] })),
      describeConfigs: jest.fn(async () => ({
        resources: [
          {
            ...resource,
            configEntries: [
              { configName: 'share.auto.offset.reset', configValue: value, configSource: source },
            ],
          },
        ],
      })),
      disconnect: jest.fn(async () => {}),
    }
    const producer = { connect: jest.fn(async () => {}), disconnect: jest.fn(async () => {}) }
    const fixture = new ParentFixture({
      stem: 's',
      kafka: { admin: () => admin, producer: () => producer },
    })
    return {
      fixture,
      admin,
      producer,
      apply: (nextSource, nextValue) => {
        source = nextSource
        value = nextValue
      },
    }
  }
  beforeEach(() => jest.useFakeTimers({ now: 1000 }))
  afterEach(() => jest.useRealTimers())

  test('reads back earliest share offsets before setup completes and restores the override before disconnect', async () => {
    const { fixture, admin, apply } = make()
    let ready = false
    const opening = fixture.open().then(() => {
      ready = true
    })
    opening.catch(() => {})
    await jest.advanceTimersByTimeAsync(0)
    expect(ready).toBe(false)
    expect(admin.incrementalAlterConfigs).toHaveBeenCalledWith({
      resources: [
        {
          type: 32,
          name: 's-share-group',
          configEntries: [{ name: 'share.auto.offset.reset', operation: 0, value: 'earliest' }],
        },
      ],
    })
    apply(5, 'earliest')
    await jest.advanceTimersByTimeAsync(250)
    expect(ready).toBe(false)
    apply(8, 'earliest')
    await jest.advanceTimersByTimeAsync(250)
    await opening
    expect(ready).toBe(true)
    const closing = fixture.close()
    closing.catch(() => {})
    await jest.advanceTimersByTimeAsync(0)
    expect(admin.disconnect).not.toHaveBeenCalled()
    expect(admin.incrementalAlterConfigs).toHaveBeenLastCalledWith({
      resources: [
        {
          type: 32,
          name: 's-share-group',
          configEntries: [{ name: 'share.auto.offset.reset', operation: 1 }],
        },
      ],
    })
    apply(5, 'latest')
    await jest.advanceTimersByTimeAsync(100)
    await closing
    expect(admin.disconnect).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('restores a refused share setup and disconnects both clients even when restoration fails', async () => {
    const { fixture, admin, producer } = make()
    const refused = new Error('config set failed')
    const restoration = new Error('config reset failed')
    admin.incrementalAlterConfigs.mockRejectedValueOnce(refused).mockRejectedValueOnce(restoration)
    await expect(fixture.open()).rejects.toBe(refused)
    const closing = fixture.close()
    await expect(closing).rejects.toMatchObject({ errors: [restoration] })
    expect(admin.incrementalAlterConfigs).toHaveBeenLastCalledWith({
      resources: [
        {
          type: 32,
          name: 's-share-group',
          configEntries: [{ name: 'share.auto.offset.reset', operation: 1 }],
        },
      ],
    })
    expect(admin.disconnect).toHaveBeenCalledTimes(1)
    expect(producer.disconnect).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('telemetry profile ordinary work', () => {
  test('subscribes every owned worker suffix and excludes another run', async () => {
    let config
    const fixture = new ParentFixture({
      stem: 'telemetry-churn-owned',
      kafka: { admin: () => ({}), producer: () => ({}) },
      withResource: async (_admin, resource, work) => {
        config = resource
        await work()
      },
    })
    await fixture.inResource(async () => {})
    const pattern = new RegExp(`^(?:${config.entries.match.slice('client_id='.length)})$`)
    for (const kind of KINDS) expect(pattern.test(`telemetry-churn-owned-${kind}`)).toBe(true)
    expect(pattern.test('telemetry-churn-other-producer')).toBe(false)
    expect(pattern.test('foreign-telemetry-churn-owned-producer')).toBe(false)
  })

  const consumer = () => {
    const calls = []
    const handlers = []
    const client = {
      subscribe: jest.fn(async () => calls.push('subscribe')),
      run: jest.fn(async ({ eachMessage }) => {
        handlers.push(eachMessage)
        calls.push('run')
      }),
      pause: jest.fn(async () => calls.push('pause')),
      resume: jest.fn(async () => calls.push('resume')),
      commitOffsets: jest.fn(async (offsets) => calls.push(['commit', offsets])),
    }
    let now = 0
    const work = new OrdinaryWork({
      kind: 'consumer',
      client,
      stem: STEM,
      topic: 't',
      ctl: 'c',
      clock: { now: () => (now += 1) },
      sleep: async () => {},
    })
    return { work, client, calls, handlers }
  }
  const message = (offset) => ({
    partition: 0,
    message: { offset: String(offset), key: recordId(STEM, 'consumer', offset) },
  })

  test('pauses admission, drains in-flight work and confirms commits before a close, resuming after', async () => {
    const { work, calls, handlers } = consumer()
    await work.start()
    await handlers[0](message(4))
    const receipt = await work.pause()
    expect(calls).toEqual([
      'subscribe',
      'run',
      'pause',
      ['commit', [{ topic: 't', partition: 0, offset: '5' }]],
    ])
    expect(receipt.pausedAt).toBeLessThanOrEqual(receipt.idleAt)
    expect(receipt.idleAt).toBeLessThanOrEqual(receipt.confirmedAt)
    expect(work.paused).toBe(true)
    await work.resume()
    expect(calls.slice(4)).toEqual(['subscribe', 'run', 'resume'])
    expect(work.paused).toBe(false)
    expect(work.ids()).toMatchObject({
      handled: [[0, 4, 4, expect.any(Number), expect.any(Number)]],
      processed: { 0: '5' },
      committed: { 0: '5' },
    })
    expect(work.ids().pauses).toHaveLength(1)
  })

  test('waits for an in-flight handler before confirming, and refuses one that never goes idle', async () => {
    const { work } = consumer()
    await work.start()
    work.active = 1
    const pausing = work.pause()
    pausing.catch(() => {})
    await expect(pausing).rejects.toThrow('did not go idle')
  })

  test('offers the producer corpus only in its fixed slots and never while paused', async () => {
    let now = 0
    let arrived
    const paused = new Promise((resolve) => {
      arrived = resolve
    })
    let resume
    const blocked = new Promise((resolve) => {
      resume = resolve
    })
    const send = jest.fn(async () => [])
    const work = new OrdinaryWork({
      kind: 'producer',
      client: { send },
      stem: STEM,
      topic: 't',
      ctl: 'c',
      clock: { now: () => now },
      sleep: async (ms) => {
        if (work.paused) {
          arrived()
          await blocked
        }
        now += ms
      },
      slots: [1000, 1010, 1020],
    })
    work.running = true
    work.paused = true
    work.corpus()
    await paused
    expect(send).not.toHaveBeenCalled()
    work.paused = false
    resume()
    await until(() => send.mock.calls.length === 3)
    await work.stop()
    expect(work.ids().offered).toHaveLength(300)
    expect(work.ids().resolved).toEqual(work.ids().offered)
    expect(send.mock.calls[0][0].messages[0]).toMatchObject({ key: recordId(STEM, 'producer', 0) })
    expect(send.mock.calls[2][0].messages.at(-1).key).toBe(recordId(STEM, 'producer', 299))
    expect(work.ids().late).toHaveLength(3)
  })

  test('sends core work to the control topic and describes the cluster for an admin', async () => {
    const send = jest.fn(async () => [])
    const producer = new OrdinaryWork({
      kind: 'producer',
      client: { send },
      stem: STEM,
      topic: 't',
      ctl: 'ctl',
      clock: { now: () => 0 },
    })
    await producer.core(3)
    expect(send).toHaveBeenCalledWith(expect.objectContaining({ topic: 'ctl', acks: -1 }))
    const describeCluster = jest.fn(async () => ({}))
    const admin = new OrdinaryWork({
      kind: 'admin',
      client: { describeCluster },
      stem: STEM,
      topic: 't',
      ctl: 'ctl',
      clock: { now: () => 0 },
    })
    await admin.core(1)
    expect(describeCluster).toHaveBeenCalledWith({ timeout: 3000 })
  })
})

describe('telemetry profile decoder', () => {
  test.each(['available', 'unavailable'])(
    'forwards bounded raw input and reduces receipts with %s decoder memory to replayable facts',
    async (capability) => {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'decoder-spec-'))
      try {
        const body = { parse: 'ok', metrics: [] }
        const memory = {
          source: 'proc-pid-status',
          units: 'bytes',
          capability,
          ...(capability === 'unavailable' && { reason: 'not recorded by the observer' }),
          intervalMs: capability === 'available' ? 20 : null,
          samples: capability === 'available' ? 2 : 0,
          peakHwmBytes: capability === 'available' ? 8192 : null,
          peakRssBytes: capability === 'available' ? 4096 : null,
          processes:
            capability === 'available' ? [{ pid: 123, hwmBytes: 8192, rssBytes: 4096 }] : [],
        }
        fs.writeFileSync(
          path.join(directory, 'decoder-123.json'),
          JSON.stringify({
            code: 0,
            signal: null,
            line: '4.3.1',
            rawSha256: 'ab'.repeat(32),
            stdout: `${JSON.stringify({ verified: true, line: '4.3.1' })}\n${JSON.stringify(body)}\n`,
            stderr: '',
            ...(capability === 'available' && { memory }),
          })
        )
        const observer = {
          directory,
          decodeRaw: jest.fn(async () => body),
          dispose: jest.fn(async () => {}),
          assertClean: jest.fn(),
        }
        const decoder = new DecoderCoordinator({
          directory,
          line: '4.3.1',
          clock: { now: () => 0 },
          make: () => observer,
        })
        await expect(
          decoder.decode('producer', Buffer.alloc(10, 1).toString('base64'))
        ).resolves.toBe(body)
        await expect(decoder.decode('producer', '')).rejects.toThrow('outside its bound')
        await expect(
          decoder.decode('admin', Buffer.alloc(4097).toString('base64'))
        ).rejects.toThrow('outside its bound')
        const closed = await decoder.close()
        expect(await decoder.close()).toBe(closed)
        expect(observer.dispose).toHaveBeenCalledTimes(1)
        expect(closed.calls).toEqual([
          { kind: 'producer', rawSha256: sha(Buffer.alloc(10, 1)), bytes: 10 },
        ])
        expect(closed.receipts).toEqual([
          {
            pid: 123,
            code: 0,
            signal: null,
            line: '4.3.1',
            rawSha256: 'ab'.repeat(32),
            identity: { verified: true, line: '4.3.1' },
            bodySha256: sha(JSON.stringify(body)),
            stderrBytes: 0,
            memory,
          },
        ])
      } finally {
        fs.rmSync(directory, { recursive: true, force: true })
      }
    }
  )
})

describe('telemetry terminal final proof', () => {
  const ID = 'producer-lc-6'
  const raw = Buffer.from('terminal metrics data')
  const final = {
    type: 'request',
    apiKey: 72,
    index: 2,
    requestKey: 'r2',
    socketId: 1,
    uuid: U,
    clientId: 's-producer',
    node: '1',
    terminating: true,
    compressionType: 0,
    metricsBytes: raw.length,
    metricsSha256: sha(raw),
    writeAt: 20,
    response: { errorCode: 0, responseAt: 25 },
  }
  const sinkRow = {
    node: '1',
    seq: 41,
    terminating: true,
    clientId: 's-producer',
    clientInstanceId: U,
    dataSha256: sha(raw),
    dataBytes: raw.length,
    parse: 'ok',
  }
  /** The successful Get whose selection is in force when the close begins. */
  const get = (requestedMetrics = ['org.apache.kafka.']) => ({
    type: 'request',
    apiKey: 71,
    index: 1,
    requestKey: 'r1',
    socketId: 1,
    uuid: U,
    writeAt: 5,
    response: { errorCode: 0, uuid: U, requestedMetrics, responseAt: 8 },
  })
  /** One sealed terminal segment whose bundle holds the final's bytes but not yet its proof. */
  const sealed = (records = [get(), final]) => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'terminal-'))
    fs.mkdirSync(path.join(directory, 'segments'))
    fs.mkdirSync(path.join(directory, 'decoder'))
    const transcript = JSON.stringify({
      owner: ID,
      records,
      closures: [{ at: 18, end: 30, pending: [] }],
    })
    const bundle = records.some((r) => r.apiKey === 72)
      ? JSON.stringify({
          t: 'push',
          key: 'r2',
          bytes: raw.length,
          sha256: sha(raw),
          b64: raw.toString('base64'),
        }) + '\n'
      : ''
    fs.writeFileSync(path.join(directory, 'segments', `${ID}.transcript.json`), transcript)
    fs.writeFileSync(path.join(directory, 'segments', `${ID}.bundle.jsonl`), bundle)
    const receipt = { rawSha256: sha(raw), code: 0, line: '4.3.1', stdout: '' }
    fs.writeFileSync(path.join(directory, 'decoder', 'decoder-7.json'), JSON.stringify(receipt))
    const pack = {
      id: ID,
      transcript: { name: `${ID}.transcript.json`, bytes: 0, sha256: sha(transcript) },
      bundle: { name: `${ID}.bundle.jsonl`, bytes: 0, sha256: sha(bundle) },
    }
    const body = { dataSha256: sha(raw), dataBytes: raw.length, metrics: [] }
    const observer = {
      directory: path.join(directory, 'decoder'),
      decodeRaw: jest.fn(async () => body),
    }
    return { directory, pack, observer, transcript, bundle, receipt, body }
  }
  const coordinator = (observer, rows) =>
    new DecoderCoordinator({
      directory: '/unused',
      line: '4.3.1',
      clock: { now: () => 0 },
      make: () => observer,
      exports: () => rows,
    })

  test('is a required proof the worker-side bundle alone cannot satisfy', () => {
    const { directory, transcript, bundle } = sealed()
    try {
      expect(() =>
        checkSegmentFiles({
          id: ID,
          transcript,
          bundle,
          proofs: TelemetryProofs.selection([final]),
        })
      ).toThrow(`Missing proof r2 in segment ${ID}`)
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test('decodes the sealed final in the parent and completes the same bundle with its proof', async () => {
    const { directory, pack, observer, transcript, receipt, body } = sealed()
    try {
      const decoder = coordinator(observer, [sinkRow])
      await decoder.terminal(directory, 'producer', pack)
      expect(observer.decodeRaw).toHaveBeenCalledWith(raw, { budgetMs: 6000 })
      expect(decoder.calls).toEqual([{ kind: 'producer', rawSha256: sha(raw), bytes: raw.length }])
      const bundle = fs.readFileSync(path.join(directory, 'segments', pack.bundle.name), 'utf8')
      expect(pack.bundle).toMatchObject({ bytes: Buffer.byteLength(bundle), sha256: sha(bundle) })
      expect(pack.terminalProofs).toEqual(['r2'])
      const checked = checkSegmentFiles({
        id: ID,
        transcript,
        bundle,
        proofs: TelemetryProofs.selection([final]),
      })
      const proof = checked.proofs.get('r2')
      expect(proof.receipts).toEqual([receipt])
      expect(proof.decoded).toMatchObject({
        requestKey: 'r2',
        uuid: U,
        rawSha256: sha(raw),
        transmittedSha256: sha(raw),
        decoded: body,
        exportIdentity: { node: '1', seq: 41 },
      })
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test.each([
    [
      'an altered sealed bundle',
      (t) => (t.pack.bundle.sha256 = 'f'.repeat(64)),
      [sinkRow],
      'changed before its final was decoded',
    ],
    [
      'an ambiguous sink join',
      () => {},
      [sinkRow, { ...sinkRow, seq: 42 }],
      'lacks a unique sink join',
    ],
    [
      'a decode of other bytes',
      (t) => (t.body.dataSha256 = 'e'.repeat(64)),
      [sinkRow],
      'Terminal decode differs from captured bytes',
    ],
    [
      'a missing original decoder receipt',
      (t) => fs.rmSync(path.join(t.observer.directory, 'decoder-7.json')),
      [sinkRow],
      'Missing original terminal decoder receipt',
    ],
  ])('refuses %s', async (_name, change, rows, message) => {
    const t = sealed()
    try {
      change(t)
      await expect(
        coordinator(t.observer, rows).terminal(t.directory, 'producer', t.pack)
      ).rejects.toThrow(message)
    } finally {
      fs.rmSync(t.directory, { recursive: true, force: true })
    }
  })

  test.each([
    ['no final', [get()]],
    ['a cancelled final', [get(), { ...final, response: undefined }]],
    ['a rejected final', [get(), { ...final, response: { errorCode: 42, responseAt: 25 } }]],
  ])('refuses a close under a nonempty selection with %s', async (_name, records) => {
    const t = sealed(records)
    try {
      await expect(
        coordinator(t.observer, [sinkRow]).terminal(t.directory, 'producer', t.pack)
      ).rejects.toThrow(`The terminal segment ${ID} owes one accepted final`)
      expect(t.observer.decodeRaw).not.toHaveBeenCalled()
    } finally {
      fs.rmSync(t.directory, { recursive: true, force: true })
    }
  })

  test('decodes nothing for a close under an empty selection that sent no final', async () => {
    const t = sealed([get([])])
    try {
      await coordinator(t.observer, []).terminal(t.directory, 'producer', t.pack)
      expect(t.pack.terminalProofs).toEqual([])
      expect(t.observer.decodeRaw).not.toHaveBeenCalled()
    } finally {
      fs.rmSync(t.directory, { recursive: true, force: true })
    }
  })
})

describe('telemetry profile natural exit', () => {
  const natural = () => {
    jest.useFakeTimers()
    const made = rig({ now: () => Date.now() })
    made.processes.start('producer')
    made.say('producer', { type: 'hello' })
    return { ...made, child: made.children[0] }
  }
  afterEach(() => jest.useRealTimers())

  test.each(['result', 'failure'])(
    'drains a large terminal %s before disconnecting IPC',
    async (type) => {
      jest.useRealTimers()
      const processes = new TelemetryProcesses({
        script: path.join(__dirname, 'fixtures/telemetryIpc.js'),
        args: [type],
      })
      const worker = processes.start('producer')
      try {
        const closed = await processes.settle(worker, 10000)
        expect(closed).toMatchObject({ event: 'close', code: 1, signal: null })
        expect(worker.stderr).toBe('')
        expect(worker.messages.map((message) => message.type)).toEqual(['hello', 'progress', type])
        expect(worker.messages.at(-1).facts.ledger).toHaveLength(2 * 1024 * 1024)
      } finally {
        if (!worker.exit) await processes.cancel('producer')
      }
    }
  )

  test('archives drained output, last message and IPC disconnect on an early exit', async () => {
    const made = natural()
    made.say('producer', { type: 'progress', row: { kind: 'verdict', ok: false } })
    made.child.emit('disconnect')
    made.child.emit('exit', 1, null)
    made.child.stdout.emit('data', 'resource verdict failed\n')
    made.child.stderr.emit('data', 'last stderr\n')
    made.child.emit('close', 1, null)
    const exit = await made.processes.join('producer')
    expect(exit).toMatchObject({
      code: 1,
      signal: null,
      stdoutTail: 'resource verdict failed\n',
      stderrTail: 'last stderr\n',
      disconnectedAt: expect.any(Number),
      exitedAt: expect.any(Number),
      lastMessage: { type: 'progress', rowKind: 'verdict' },
    })
    expect(exit.lastMessage).not.toHaveProperty('token')
  })

  test('keeps the output tail and actual byte count after rejecting overflow', () => {
    const made = natural()
    made.child.stderr.emit('data', 'x'.repeat(65536))
    made.child.stderr.emit('data', 'fatal at end\n')
    made.child.emit('close', 1, null)
    const facts = made.processes.facts(
      made.processes.workers.get('producer'),
      made.processes.workers.get('producer').exit
    )
    expect(facts.stderrBytes).toBe(65549)
    expect(Buffer.byteLength(facts.stderrTail)).toBeLessThanOrEqual(65536)
    expect(facts.stderrTail.endsWith('fatal at end\n')).toBe(true)
    expect(made.processes.workers.get('producer').error.message).toContain('stderr overflow')
  })

  test('joins children naturally and treats timeouts as failures', async () => {
    const clean = natural()
    clean.say('producer', { type: 'receipt' })
    const joined = clean.processes.join('producer')
    await jest.advanceTimersByTimeAsync(9000)
    clean.child.emit('close', 0, null)
    await expect(joined).resolves.toMatchObject({
      event: 'close',
      code: 0,
      signal: null,
      stderrBytes: 0,
      unhandled: 0,
      receipts: 1,
      killed: false,
      watchdogExpired: false,
      afterDisconnectMs: 9000,
    })

    const late = natural()
    late.say('producer', { type: 'receipt' })
    const lateJoin = late.processes.join('producer')
    await jest.advanceTimersByTimeAsync(10001)
    late.child.emit('close', 0, null)
    await expect(lateJoin).resolves.toMatchObject({ event: 'timeout', watchdogExpired: true })

    const noReceipt = natural()
    noReceipt.child.emit('close', 0, null)
    await expect(noReceipt.processes.join('producer')).resolves.toMatchObject({
      event: 'close',
      afterDisconnectMs: null,
      receipts: 0,
    })

    const killed = natural()
    killed.say('producer', { type: 'receipt' })
    killed.child.stderr.emit('data', 'boom')
    killed.say('producer', { type: 'unhandled' })
    const killedJoin = killed.processes.join('producer')
    killed.child.emit('close', null, 'SIGTERM')
    await expect(killedJoin).resolves.toMatchObject({
      code: null,
      signal: 'SIGTERM',
      stderrBytes: 4,
      unhandled: 1,
    })
  })

  test('kills a worker still alive after the abort and its natural-exit bound, then joins its close', async () => {
    const cancelled = natural()
    const joining = cancelled.processes.cancel('producer')
    let settled = false
    joining.then(() => {
      settled = true
    })
    expect(cancelled.child.send).toHaveBeenCalledWith({ type: 'abort' }, expect.any(Function))
    await jest.advanceTimersByTimeAsync(9999)
    expect(cancelled.child.kill).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(2)
    expect(cancelled.child.kill).toHaveBeenCalledTimes(1)
    expect(cancelled.child.kill).toHaveBeenCalledWith('SIGKILL')
    expect(settled).toBe(false)
    cancelled.child.emit('close', null, 'SIGKILL')
    await expect(joining).resolves.toMatchObject({
      event: 'close',
      code: null,
      signal: 'SIGKILL',
      watchdogExpired: true,
      killed: true,
      aborted: true,
    })
    expect(cancelled.processes.workers.get('producer').error).toBeNull()
    await expect(cancelled.processes.cancel('producer')).resolves.toMatchObject({
      event: 'close',
      killed: true,
      watchdogExpired: true,
    })
    expect(cancelled.child.send).toHaveBeenCalledTimes(1)
    expect(cancelled.child.kill).toHaveBeenCalledTimes(1)
  })

  test('reports a killed worker that never closes as a failed timeout row and still returns', async () => {
    const stuck = natural()
    stuck.say('producer', { type: 'receipt' })
    const joining = stuck.processes.cancel('producer')
    await jest.advanceTimersByTimeAsync(10001)
    expect(stuck.child.kill).toHaveBeenCalledWith('SIGKILL')
    await jest.advanceTimersByTimeAsync(KILL_JOIN_MS)
    await expect(joining).resolves.toMatchObject({
      event: 'timeout',
      closedAt: null,
      killed: true,
      watchdogExpired: true,
      receipts: 1,
    })
    await expect(stuck.processes.cancel('producer')).resolves.toMatchObject({
      event: 'timeout',
      killed: true,
    })
    expect(stuck.child.kill).toHaveBeenCalledTimes(1)
  })

  test('sends the abort only over a connected channel and discards a send to a closed one', async () => {
    const onFailure = jest.fn()
    const gone = natural()
    gone.processes.onFailure = onFailure
    gone.child.connected = false
    const quiet = gone.processes.cancel('producer')
    expect(gone.child.send).not.toHaveBeenCalled()
    gone.child.emit('close', 0, null)
    await expect(quiet).resolves.toMatchObject({ event: 'close', aborted: true, killed: false })

    const closing = natural()
    closing.processes.onFailure = onFailure
    closing.child.send.mockImplementation((message, callback) =>
      callback(Object.assign(new Error('Channel closed'), { code: 'ERR_IPC_CHANNEL_CLOSED' }))
    )
    const cancelled = closing.processes.cancel('producer')
    expect(closing.child.send).toHaveBeenCalledTimes(1)
    closing.child.emit('close', 0, null)
    await expect(cancelled).resolves.toMatchObject({ event: 'close', killed: false })
    expect(closing.processes.workers.get('producer').error).toBeNull()
    expect(onFailure).not.toHaveBeenCalled()
  })

  test('rejects a second explicit-disconnect receipt and output beyond its bound', async () => {
    const onFailure = jest.fn()
    const dup = rig({ onFailure })
    dup.processes.start('producer')
    dup.say('producer', { type: 'hello' })
    dup.say('producer', { type: 'receipt' })
    dup.say('producer', { type: 'receipt' })
    expect(onFailure).toHaveBeenCalledTimes(1)
    expect(onFailure.mock.calls[0][1].message).toContain('a duplicate receipt')
    const loud = rig({ onFailure })
    loud.processes.start('admin')
    loud.children[0].stderr.emit('data', 'x'.repeat(65537))
    expect(onFailure.mock.calls.at(-1)[1].message).toContain('stderr overflow')
  })
})

describe('telemetry profile worker', () => {
  const getRecord = (index, socketId, over = {}) => ({
    type: 'request',
    apiKey: 71,
    index,
    requestKey: `r${index}`,
    socketId,
    uuid: U,
    writeAt: index * 10,
    response: {
      errorCode: 0,
      uuid: U,
      subscriptionId: 7,
      pushIntervalMs: 1000,
      requestedMetrics: ['org.apache.kafka.'],
      responseAt: index * 10 + 5,
    },
    ...over,
  })
  const observerOf = (records) => ({
    records,
    readTranscript: ({ after = 0 } = {}) =>
      records.filter((r) => r.index > after).map((r) => ({ ...r })),
    checkpoint: () => records.length,
    normalRequests: ({ after = 0 } = {}) =>
      records.filter((r) => r.index > after && r.response && r.normal),
    socketsClosed: async () => {},
    decodeAccepted: jest.fn(async () => ({})),
  })
  const make = (over = {}) => {
    const log = []
    let now = 0
    const from = { id: 'producer-lc-1', observer: observerOf([]) }
    const to = { id: 'producer-lc-2', observer: observerOf([getRecord(1, 3)]) }
    const notes = over.notes ?? [
      { seq: 1, type: 'select', from: from.id, to: to.id, at: 1 },
      { seq: 2, type: 'disconnect-resolved', from: from.id, to: to.id, at: 2 },
      { seq: 3, type: 'connect-called', from: from.id, to: to.id, at: 3 },
    ]
    const segments = {
      current: () => over.current ?? to,
      cycle: jest.fn(() => {
        log.push('cycle')
        return { from, to, closed: Promise.resolve(), reopened: Promise.resolve() }
      }),
      journal: () => notes,
      discharge: jest.fn(async () => ({})),
      sealed: () => [],
    }
    const ordinary = {
      pause: jest.fn(async () => {
        log.push('pause')
        return { pausedAt: 1, idleAt: 2, confirmedAt: 3 }
      }),
      resume: jest.fn(async () => log.push('resume')),
      core: jest.fn(async () => log.push('core')),
    }
    const client = {
      clientInstanceId: jest.fn(async () => over.cached ?? U),
    }
    const limits = { warmupMs: 60000, measureMs: 300000 }
    const worker = new TelemetryWorker({
      kind: over.kind ?? 'producer',
      client,
      tap: over.tap ?? { telemetry: () => [] },
      segments,
      registry: over.registry ?? { replace: jest.fn(() => ({ tick: 0, at: 1 })), replacements: [] },
      clock: { now: () => (now += 100) },
      sleep: over.sleep ?? (async () => {}),
      ordinary,
      limits,
      inbox: { take: async () => ({}) },
      send: jest.fn(),
    })
    worker.uuid = U
    return { worker, log, from, to, segments, ordinary }
  }

  test.each(KINDS)(
    'keeps %s transition releases outside every active lifecycle window',
    async (kind) => {
      const { TelemetrySegments } = require('../../../testHelpers/telemetrySegments')
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'subscription-windows-'))
      let at = 0
      let active = true
      let selection = 'org.apache.kafka.'
      const windows = [{ start: at, end: null }]
      const releases = []
      const readbacks = []
      const clock = { now: () => ++at }
      const segments = TelemetrySegments.ordinary('telemetry-churn', kind, {
        directory,
        clock,
        observer: { line: '4.3.1' },
      })
      const { worker } = make({ kind })
      worker.segments = segments
      worker.clock = clock
      worker.client = {
        disconnect: async () => {
          expect(active).toBe(true)
          active = false
          windows.at(-1).end = ++at
        },
        connect: async () => {
          expect(active).toBe(false)
          expect(readbacks.at(-1).at).toBeLessThan(at + 1)
          expect(readbacks.at(-1).selection).toBe(selection)
          active = true
          windows.push({ start: ++at, end: null })
          const get = getRecord(1, 1, {
            writeAt: ++at,
            response: {
              ...getRecord(1, 1).response,
              requestedMetrics: selection ? [selection] : [],
              responseAt: ++at,
            },
          })
          const records = [get]
          if (selection)
            records.push({
              type: 'request',
              apiKey: 72,
              index: 2,
              requestKey: 'r2',
              socketId: 1,
              uuid: U,
              subscriptionId: 7,
              terminating: false,
              metricsBytes: 10,
              writeAt: ++at,
              response: { errorCode: 0, responseAt: ++at },
            })
          jest
            .spyOn(segments.current().observer, 'readTranscript')
            .mockImplementation(() => records)
        },
        clientInstanceId: async () => U,
      }
      worker.tap = { telemetry: () => [{ segment: segments.current().id, ordinal: 1 }] }
      worker.seal = async (segment) => {
        await segment.observer.dispose()
        segment.observer = null
        segment.state = 'sealed'
        return {}
      }
      try {
        const schedule = new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
        for (const tick of schedule.ticks.filter((t) => t.transitions.length)) {
          const command = {
            tick: tick.number,
            index: tick.index,
            transitions: tick.transitions,
            selection: schedule.selection(tick.index),
            stage: 'confirm',
          }
          await worker.handle({ type: 'confirm-fence', ...command })
          expect(active).toBe(false)
          expect(worker.send.mock.calls.at(-1)[0]).toMatchObject({ type: 'confirm-fenced' })
          for (const index of tick.transitions) {
            expect(active).toBe(false)
            selection = schedule.selection(index)
            releases.push({ index, selection, at: ++at })
            readbacks.push({ index, selection, at: ++at })
          }
          await worker.handle({ type: 'stage', ...command })
          expect(active).toBe(true)
          await worker.sealing.promise
        }
        expect(releases).toHaveLength(35)
        expect(windows).toHaveLength(6)
        expect(releases.map((r) => r.index)).toEqual(Array.from({ length: 35 }, (_, n) => n + 1))
        expect(readbacks.map((r) => [r.index, r.selection])).toEqual(
          releases.map((r) => [r.index, r.selection])
        )
        for (const { at: releasedAt } of [...releases, ...readbacks])
          expect(
            windows.some((w) => w.start <= releasedAt && (w.end === null || releasedAt <= w.end))
          ).toBe(false)
        expect(worker.actions.measured).toEqual({ subscription: 35, reopen: 5, retire: 0 })
      } finally {
        for (const segment of segments.undischarged()) await segment.observer?.dispose()
        fs.rmSync(directory, { recursive: true, force: true })
      }
    }
  )

  test('retains immediate reopen ordering and normal pushes', async () => {
    const { worker, log, from, segments } = make()
    const claim = await worker.reopen({ tick: 5 })
    expect(log).toEqual(['pause', 'cycle', 'resume'])
    expect(claim).toMatchObject({
      tick: 5,
      from: 'producer-lc-1',
      to: 'producer-lc-2',
      cachedUuid: U,
      get: 'r1',
    })
    expect(claim.pause).toEqual({ pausedAt: 1, idleAt: 2, confirmedAt: 3 })
    expect(worker.owed.has('reopen')).toBe(true)
    await worker.sealing.promise
    expect(segments.discharge).toHaveBeenCalledWith(from, { proofs: [] })
  })

  test.each([
    [
      'a connect that was not the next public action',
      {
        notes: [
          { seq: 1, type: 'select', from: 'producer-lc-1', to: 'producer-lc-2' },
          { seq: 2, type: 'disconnect-resolved', from: 'producer-lc-1', to: 'producer-lc-2' },
          { seq: 4, type: 'connect-called', from: 'producer-lc-1', to: 'producer-lc-2' },
        ],
      },
      'not the next public action',
    ],
    ['a changed cached UUID', { cached: 'other' }, 'instance ID changed across a public reopen'],
  ])('refuses %s on a public reopen', async (_name, over, message) => {
    const { worker } = make(over)
    await expect(worker.reopen({ tick: 5 })).rejects.toThrow(message)
  })

  test('refuses a reopen without a successful replacement Get', async () => {
    const { worker, to } = make()
    to.observer.records.length = 0
    await expect(worker.reopen({ tick: 5 })).rejects.toThrow('no replacement Get within 15000 ms')
  })

  test('faults one telemetry socket beside ordinary work and proves the recovery Get', async () => {
    const records = []
    const current = { id: 'producer-lc-2', observer: observerOf(records) }
    const fault = { segment: current.id, ordinal: 3, declaredAt: 10, hit: Promise.resolve(15) }
    const tap = { retire: jest.fn(() => fault), telemetry: () => [] }
    let slept = 0
    const { worker, ordinary } = make({
      current,
      tap,
      sleep: async () => {
        slept += 1
        if (slept === 1)
          records.push({
            type: 'request',
            apiKey: 1,
            index: 1,
            requestKey: 'r1',
            socketId: 2,
            normal: true,
            writeAt: 12,
            response: { errorCode: 0, responseAt: 14 },
          })
        if (slept === 2) records.push(getRecord(2, 4, { writeAt: 40 }))
      },
    })
    const claim = await worker.retire({ tick: 3 })
    expect(ordinary.core).toHaveBeenCalledTimes(1)
    expect(claim).toMatchObject({
      tick: 3,
      segment: current.id,
      faulted: 3,
      declaredAt: 10,
      faultedAt: 15,
      core: 'r1',
      get: 'r2',
    })
    expect(worker.owed.has('retire')).toBe(true)
  })

  test.each([new Error('core failed'), undefined, null, false, 0])(
    'preserves the exact core rejection %p beside a retirement',
    async (reason) => {
      const current = { id: 'producer-lc-2', observer: observerOf([]) }
      const tap = {
        retire: () => ({
          segment: current.id,
          ordinal: 3,
          declaredAt: 10,
          hit: Promise.resolve(15),
        }),
      }
      const { worker, ordinary } = make({ current, tap })
      ordinary.core.mockRejectedValueOnce(reason)
      await expect(worker.retire({ tick: 3 })).rejects.toBe(reason)
      expect(worker.claims.retirements).toEqual([])
    }
  )

  test('confirms the selection with a later Get and discharges the push owed on the current socket', async () => {
    const records = [getRecord(1, 3)]
    const current = { id: 'producer-lc-2', observer: observerOf(records) }
    let slept = 0
    const { worker } = make({
      current,
      tap: { telemetry: () => [{ segment: current.id, ordinal: 3 }] },
      sleep: async () => {
        slept += 1
        if (slept === 1)
          records.push(
            getRecord(2, 3, {
              writeAt: 1e9,
              response: {
                ...getRecord(2, 3).response,
                requestedMetrics: ['com.example.'],
                subscriptionId: 8,
              },
            })
          )
        if (slept === 2)
          records.push({
            type: 'request',
            apiKey: 72,
            index: 3,
            requestKey: 'r3',
            socketId: 3,
            uuid: U,
            subscriptionId: 8,
            terminating: false,
            metricsBytes: 10,
            writeAt: 1e9 + 10,
            response: { errorCode: 0, responseAt: 1e9 + 15 },
          })
      },
    })
    worker.owed.add('reopen')
    worker.fenceConfirmation({ tick: 1, index: 6, selection: 'com.example.' })
    const claim = await worker.confirm({ tick: 1, index: 6, selection: 'com.example.' })
    expect(claim).toMatchObject({
      tick: 1,
      selection: 'com.example.',
      get: 'r2',
      subscriptionId: 8,
      proof: { socketId: 3, get: 'r1', push: 'r3', owedBy: ['reopen'] },
    })
    expect(worker.owed.size).toBe(0)
    expect(worker.claims.proofs).toEqual([
      { tick: 1, segment: current.id, push: 'r3', owedBy: ['reopen'] },
    ])
  })

  test('accepts a matching Get written between readback and confirmation delivery', async () => {
    const records = [getRecord(1, 3)]
    const current = { id: 'producer-lc-2', observer: observerOf(records) }
    const { worker } = make({ current })
    const tick = worker.schedule.ticks.find((t) => t.actions.includes('subscription')).number
    const command = { tick, index: 2, stage: 'confirm', selection: '' }
    await worker.handle({ ...command, type: 'confirm-fence' })
    const fence = worker.send.mock.calls.at(-1)[0]
    expect(fence).toMatchObject({ type: 'confirm-fenced', tick, after: 1, segment: current.id })
    records.push(
      getRecord(2, 3, {
        writeAt: fence.receivedAt + 1,
        response: { ...getRecord(2, 3).response, requestedMetrics: [], subscriptionId: 9 },
      })
    )
    await worker.handle({ ...command, type: 'stage' })
    expect(worker.claims.transitions[0]).toMatchObject({
      receivedAt: fence.receivedAt,
      after: 1,
      get: 'r2',
      subscriptionId: 9,
    })
    expect(worker.claims.transitions[0].releasedAt).toBeGreaterThan(records[1].writeAt)
  })

  test('rejects stale Gets on either side of the confirmation cursor and timestamp fence', async () => {
    for (const stale of ['cursor', 'timestamp']) {
      const response = { ...getRecord(1, 3).response, requestedMetrics: [] }
      const records = [getRecord(1, 3, { writeAt: 1e9, response })]
      const current = { id: 'producer-lc-2', observer: observerOf(records) }
      const { worker } = make({ current })
      const tick = worker.schedule.ticks.find((t) => t.actions.includes('subscription')).number
      const command = { tick, index: 2, stage: 'confirm', selection: '' }
      await worker.handle({ ...command, type: 'confirm-fence' })
      if (stale === 'timestamp') records.push(getRecord(2, 3, { writeAt: 0, response }))
      await expect(worker.confirm(command)).rejects.toThrow('no selection Get within 8000 ms')
      expect(worker.claims.transitions).toEqual([])
    }
  })

  test('proves an empty selection by its Get alone and never owes a Push under it', async () => {
    const records = [getRecord(1, 3)]
    const current = { id: 'producer-lc-2', observer: observerOf(records) }
    let slept = 0
    const { worker } = make({
      current,
      tap: { telemetry: () => [{ segment: current.id, ordinal: 3 }] },
      sleep: async () => {
        slept += 1
        if (slept === 1)
          records.push(
            getRecord(2, 3, {
              writeAt: 1e9,
              response: { ...getRecord(2, 3).response, requestedMetrics: [], subscriptionId: 9 },
            })
          )
      },
    })
    worker.fenceConfirmation({ tick: 1, index: 2, selection: '' })
    const claim = await worker.confirm({ tick: 1, index: 2, selection: '' })
    expect(claim).toMatchObject({ selection: '', get: 'r2', proof: null })
    worker.owed.add('retire')
    const none = make({
      current,
      tap: { telemetry: () => [{ segment: current.id, ordinal: 3 }] },
      sleep: async () => {},
    })
    none.worker.owed.add('retire')
    none.worker.fenceConfirmation({ tick: 1, index: 2, selection: '' })
    await expect(none.worker.confirm({ tick: 1, index: 2, selection: '' })).rejects.toThrow(
      'no selection Get'
    )
  })

  test('refuses a stage the fixed table does not hold', async () => {
    const { worker } = make()
    await expect(worker.handle({ type: 'stage', tick: 0, stage: 'lifecycle' })).rejects.toThrow(
      'A stage the fixed table does not hold'
    )
  })

  test.each([
    ['the sampler join', 'sampler'],
    ['the public disconnect', 'close'],
  ])('keeps a failure of %s and still attempts every later release', async (_name, at) => {
    const { worker, segments, ordinary } = make({
      tap: { telemetry: () => [], restore: jest.fn() },
    })
    const order = []
    const error = new Error(at)
    worker.timing.phaseAt = 0
    worker.sampler = {
      endSampling: jest.fn(async () => {
        order.push('sampler')
        if (at === 'sampler') throw error
      }),
    }
    ordinary.stop = jest.fn(async () => order.push('stop'))
    worker.registry.restore = jest.fn(() => order.push('restore'))
    segments.close = jest.fn(async () => {
      order.push('close')
      if (at === 'close') throw error
    })
    segments.discharge = jest.fn(async (segment) => {
      order.push('seal')
      return { id: segment.id }
    })
    const errors = []
    await worker.cleanup(errors)
    expect(errors).toEqual([error])
    expect(order).toEqual(['sampler', 'stop', 'restore', 'close', 'seal'])
    expect(worker.tap.restore).toHaveBeenCalledTimes(1)
    const receipts = worker.send.mock.calls.filter(([m]) => m.type === 'receipt')
    expect(receipts).toHaveLength(at === 'close' ? 0 : 1)
    expect(worker.send).toHaveBeenCalledWith({ type: 'progress', sealed: { id: 'producer-lc-2' } })
  })

  test.each(['consumer', 'shareConsumer'])(
    'joins a failed %s without waiting for the unproduced corpus',
    async (kind) => {
      const { worker, segments } = make({
        kind,
        tap: { telemetry: () => [], restore: () => {} },
      })
      let now = 0
      worker.ordinary = new OrdinaryWork({
        kind,
        client: {},
        clock: { now: () => now },
        sleep: async (ms) => {
          now += ms
        },
      })
      worker.sampler = { endSampling: async () => {} }
      worker.registry.restore = () => {}
      segments.close = async () => {}
      const primary = new Error('No initial proving Push')
      const errors = [primary]
      await worker.cleanup(errors)
      expect(now).toBeLessThan(10000)
      expect(errors[0]).toBe(primary)
      expect(errors[1].message).toBe('The corpus was not fully handled before the close')
      expect(worker.send).toHaveBeenCalledWith({ type: 'receipt', at: expect.any(Number) })
    }
  )

  test.each(KINDS)(
    'closes the %s setup, reopen and terminal lifecycles with the same 3000 ms budget',
    async (kind) => {
      const budget = { timeout: 3000 }
      const records = [getRecord(1, 3)]
      const setup = make({
        kind,
        current: {
          id: 'producer-lc-0',
          observer: { ...observerOf(records), subscriptions: () => records },
        },
      })
      setup.worker.registry.fill = jest.fn()
      setup.worker.client.connect = jest.fn(async () => {})
      setup.segments.close = jest.fn(async () => {})
      setup.segments.advance = jest.fn()
      await setup.worker.prepare()
      expect(setup.segments.close).toHaveBeenCalledWith(setup.worker.client, budget)

      const lifecycle = make({ kind })
      await lifecycle.worker.reopen({ tick: 5 })
      await lifecycle.worker.sealing.promise
      expect(lifecycle.segments.cycle).toHaveBeenCalledWith(lifecycle.worker.client, budget)

      const terminal = make({ kind, tap: { telemetry: () => [], restore: jest.fn() } })
      terminal.worker.timing.phaseAt = 0
      terminal.worker.sampler = { endSampling: jest.fn(async () => {}) }
      terminal.ordinary.stop = jest.fn(async () => {})
      terminal.worker.registry.restore = jest.fn()
      terminal.segments.close = jest.fn(async () => {})
      const errors = []
      await terminal.worker.cleanup(errors)
      expect(errors).toEqual([])
      expect(terminal.segments.close).toHaveBeenCalledWith(terminal.worker.client, budget)
    }
  )

  test('binds every worker message to the handshake and answers decode requests by identity', async () => {
    const handlers = {}
    const proc = {
      send: jest.fn(),
      on: jest.fn((event, handler) => {
        handlers[event] = handler
      }),
    }
    const env = { [WORKER_TOKEN]: 'tok' }
    const process = new TelemetryWorkerProcess({ kind: 'admin', soak: {}, proc, env })
    process.send({ type: 'hello' })
    expect(proc.send).toHaveBeenCalledWith({ type: 'hello', kind: 'admin', token: 'tok' })
    process.listen()
    const ok = process.askDecode(Buffer.from('ab'))
    expect(proc.send).toHaveBeenLastCalledWith({
      type: 'decode',
      id: 1,
      rawB64: Buffer.from('ab').toString('base64'),
      kind: 'admin',
      token: 'tok',
    })
    handlers.message({ type: 'decoded', id: 1, body: { parse: 'ok' } })
    await expect(ok).resolves.toEqual({ parse: 'ok' })
    const bad = process.askDecode(Buffer.from('cd'))
    handlers.message({ type: 'decoded', id: 2, error: 'boom' })
    await expect(bad).rejects.toThrow('boom')
    for (let i = 0; i < 16; i++) process.askDecode(Buffer.from('x')).catch(() => {})
    await expect(process.askDecode(Buffer.from('y'))).rejects.toThrow('Too many decode requests')
    handlers.unhandledRejection()
    expect(proc.send).toHaveBeenLastCalledWith({ type: 'unhandled', kind: 'admin', token: 'tok' })
    await expect(
      new TelemetryWorkerProcess({ kind: 'admin', soak: {}, proc: {}, env }).run()
    ).rejects.toThrow('needs its parent channel')
  })
})

describe('telemetry profile parent', () => {
  /** A scripted supervisor: answers every join at once and records what the parent sent. */
  const scripted = (clock) => {
    const sent = []
    const workers = new Map(KINDS.map((kind) => [kind, { exit: null }]))
    const processes = {
      workers,
      sent,
      start: jest.fn(),
      send: jest.fn((kind, message) => sent.push({ kind, ...message, at: clock.now() })),
      message: jest.fn(async (kind, type) => ({ type, kind, parentAt: clock.now() })),
      until: jest.fn(async (kind, match) => {
        const last = sent
          .filter((m) => m.kind === kind && ['stage', 'confirm-fence'].includes(m.type))
          .at(-1)
        if (last.type === 'confirm-fence')
          return {
            type: 'confirm-fenced',
            tick: last.tick,
            kind,
            receivedAt: 1,
            after: 0,
            segment: `${kind}-lc-1`,
            parentAt: clock.now(),
          }
        return {
          type: 'stage-done',
          kind,
          tick: last.tick,
          stage: last.stage,
          startedAt: 1,
          doneAt: 2,
          parentAt: clock.now(),
        }
      }),
      join: jest.fn(async (kind) => ({ kind, event: 'close' })),
      cancel: jest.fn(async (kind) => ({ kind, event: 'close' })),
      taint: jest.fn(),
    }
    return processes
  }
  const make = (over = {}) => {
    let now = 1000
    const clock = { now: () => now }
    const calls = []
    const fixture = {
      open: jest.fn(async () => calls.push('open')),
      inResource: jest.fn(async (work) => {
        calls.push('resource')
        try {
          await work()
        } finally {
          calls.push('restore')
        }
      }),
      topic: (kind) => `t-${kind}`,
      group: (kind) => `g-${kind}`,
      ctl: 'ctl',
      rotate: jest.fn(async (index, selection) => ({
        index,
        selection,
        setAt: now,
        readbackAt: now,
        readback: selection,
      })),
      produce: jest.fn(async (kind, from, count) => {
        calls.push(['produce', kind, from, now])
        return Array.from({ length: count }, (_, i) => from + i)
      }),
      close: jest.fn(async () => calls.push('fixture-close')),
      ...over.fixture,
    }
    const decoder = { close: jest.fn(async () => ({ receipts: [], calls: [] })), ...over.decoder }
    const processes = over.processes ?? scripted(clock)
    const churn = new TelemetryChurn({
      fixture,
      processes,
      decoder,
      record: jest.fn(),
      clock,
      sleep: (ms) =>
        new Promise((resolve) =>
          setImmediate(() => {
            now += ms
            resolve()
          })
        ),
      limits: {
        warmupMs: 60000,
        measureMs: 300000,
        cleanupMs: 120000,
        caseMs: 540000,
        exitMs: 10000,
      },
      verdict: jest.fn(),
      brokers: ['b:1'],
      stem: STEM,
      line: '4.3.1',
      directory: '/evidence',
    })
    return { churn, fixture, decoder, processes, calls, advance: (ms) => (now += ms), clock }
  }

  test('runs every tick in joined stages, rotates before confirming and offers nothing in the warm-up', async () => {
    const { churn, fixture, processes, calls, clock } = make()
    const timeline = { t0: clock.now() }
    const schedule = new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    const results = await churn.measure(timeline, schedule)
    expect(results.map((r) => r.kind)).toEqual([...KINDS])
    const stages = processes.sent.filter((m) => m.type === 'stage')
    const sixty = schedule.ticks.find((t) => t.at === 60000).number
    expect(stages.filter((m) => m.tick === sixty).map((m) => `${m.kind}:${m.stage}`)).toEqual(
      KINDS.map((kind) => `${kind}:retire`)
    )
    expect(fixture.rotate).toHaveBeenCalledTimes(35)
    expect(fixture.rotate.mock.calls[0]).toEqual([1, 'com.example.', 9000])
    expect(fixture.rotate.mock.calls[1]).toEqual([2, '', 9000])
    const produce = calls.filter((c) => Array.isArray(c))
    expect(produce).toHaveLength(2 * 100)
    expect(produce[0][3] - timeline.epochAt).toBeGreaterThanOrEqual(60000)
    expect(churn.rotations.map((r) => [r.index, r.selection, r.readback])).toEqual(
      Array.from({ length: 35 }, (_, n) => [
        n + 1,
        schedule.selection(n + 1),
        schedule.selection(n + 1),
      ])
    )
    for (const tick of schedule.ticks.filter((t) => t.transitions.length)) {
      const batch = churn.rotations.filter((r) => r.tick === tick.number)
      const releases = stages.filter((m) => m.tick === tick.number && m.stage === 'confirm')
      expect(releases).toHaveLength(KINDS.length)
      for (const release of releases) {
        expect(release.selection).toBe(batch.at(-1).readback)
        expect(batch.every((r) => r.readbackAt <= release.at)).toBe(true)
      }
    }
    expect(churn.stages).toHaveLength(KINDS.length * (5 + 11))
    expect(timeline.setupMs).toBe(0)
    expect(processes.sent.filter((m) => m.type === 'corpus')).toHaveLength(KINDS.length)
    expect(processes.sent.filter((m) => m.type === 'finish')).toHaveLength(KINDS.length)
    expect(timeline.boundaryAt - timeline.epochAt).toBeGreaterThanOrEqual(360000)
    churn.judge([], { errors: [], exits: {}, oracle: { ok: true } }, timeline, schedule)
    const verdict = churn.record.mock.calls.map(([row]) => row).find((r) => r.kind === 'verdict')
    expect(verdict.measured).toEqual({ subscription: 35, retire: 10, reopen: 5 })
    expect(verdict.violations.filter((v) => v.startsWith('measured '))).toEqual([])
  })

  test('joins all confirmation fences before rotating the subscription', async () => {
    const { churn, fixture, processes, clock, advance } = make()
    let releaseFence
    let fenceRequested
    const held = new Promise((resolve) => (releaseFence = resolve))
    const requested = new Promise((resolve) => (fenceRequested = resolve))
    const original = processes.until
    let heldOnce = false
    processes.until = jest.fn(async (...args) => {
      const [kind] = args
      const last = processes.sent.filter((m) => m.kind === kind).at(-1)
      if (!heldOnce && kind === 'admin' && last.type === 'confirm-fence') {
        heldOnce = true
        fenceRequested()
        await held
      }
      return original(...args)
    })
    fixture.rotate.mockImplementation(async (index, selection) => {
      const fences = processes.sent.filter((m) => m.type === 'confirm-fence')
      expect(fences.slice(-KINDS.length).map((m) => m.kind)).toEqual([...KINDS])
      expect(processes.until.mock.calls.slice(-KINDS.length).map(([kind]) => kind)).toEqual([
        ...KINDS,
      ])
      return { index, selection, setAt: clock.now(), readbackAt: clock.now(), readback: selection }
    })
    const ticking = churn.ticks(
      clock.now(),
      new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    )
    await Promise.race([requested, ticking])
    expect(fixture.rotate).not.toHaveBeenCalled()
    advance(8500)
    releaseFence()
    await ticking
    expect(fixture.rotate.mock.calls[0]).toEqual([1, 'com.example.', 1500])
    expect(processes.sent.filter((m) => m.type === 'confirm-fence')).toHaveLength(5 * KINDS.length)
  })

  test('fails a measured tick that is more than its late bound behind and never catches it up', async () => {
    const { churn, processes, advance, fixture } = make()
    const original = processes.until
    processes.until = jest.fn(async (...args) => {
      if (processes.sent.filter((m) => m.type === 'stage').length === 4 * 12) advance(30000)
      return original(...args)
    })
    await expect(churn.run()).rejects.toThrow('The measured tick at')
    expect(churn.missed.length).toBe(1)
    expect(churn.missed[0].measured).toBe(true)
    expect(fixture.rotate.mock.calls.length).toBeLessThan(35)
    expect(() => churn.guard()).toThrow('The measured tick at')
    expect(churn.failureStage).toBe('measurement')
  })

  test('refuses a setup over its budget', async () => {
    const { churn, advance, clock } = make()
    const t0 = clock.now()
    advance(48001)
    await expect(
      churn.measure({ t0 }, new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 }))
    ).rejects.toThrow('The setup exceeded its budget')
    expect(churn.failureStage).toBe('setup')
  })

  test('labels a missing terminal result as cleanup after the full measured schedule', async () => {
    const { churn, processes } = make()
    const message = processes.message
    processes.message = jest.fn(async (kind, type, ms) => {
      if (type === 'result') throw new Error('terminal result missing')
      return message(kind, type, ms)
    })
    await churn.run().catch(() => {})
    expect(churn.cause.message).toBe('terminal result missing')
    expect(churn.failureStage).toBe('cleanup')
    expect(churn.lastResult.archive.failureStage).toBe('cleanup')
  })

  test('restores owned resources after every failure stage and keeps every failure', async () => {
    const stage = async (over, expected) => {
      const made = make(over)
      const failure = await made.churn.run().catch((e) => e)
      expect(made.fixture.close).toHaveBeenCalledTimes(1)
      expect(made.decoder.close).toHaveBeenCalledTimes(1)
      expect(failure).toBeInstanceOf(AggregateError)
      expect(failure.errors.map((e) => e.message)).toEqual(expected)
      expect(() => made.churn.guard()).toThrow(expected[0])
      return made
    }
    await stage(
      {
        fixture: { open: jest.fn(async () => Promise.reject(new Error('open'))) },
        decoder: { close: jest.fn(async () => Promise.reject(new Error('decoder'))) },
      },
      ['open', 'decoder']
    )
    await stage(
      {
        fixture: {
          open: jest.fn(async () => Promise.reject(new Error('open'))),
          close: jest.fn(async () => Promise.reject(new Error('fixture'))),
        },
      },
      ['open', 'fixture']
    )
    const config = await stage(
      {
        processes: Object.assign(scripted({ now: () => 0 }), {
          message: jest.fn(async () => Promise.reject(new Error('hello'))),
        }),
        fixture: { close: jest.fn(async () => Promise.reject(new Error('fixture'))) },
      },
      ['hello', 'fixture']
    )
    expect(config.calls).toEqual(['open', 'resource', 'restore'])
    for (const kind of KINDS) expect(config.processes.cancel).toHaveBeenCalledWith(kind)
    expect(config.processes.join).not.toHaveBeenCalled()
  })

  test.each([
    [
      'the subscription set and readback',
      (made, error) =>
        made.fixture.rotate.mockImplementationOnce(async () => Promise.reject(error)),
    ],
    [
      'a request of an active stage',
      (made, error) => {
        const original = made.processes.until
        let calls = 0
        made.processes.until = jest.fn(async (...args) => {
          if (++calls === 6) throw error
          return original(...args)
        })
      },
    ],
    [
      'a corpus offer',
      (made, error) => {
        let calls = 0
        const original = made.fixture.produce.getMockImplementation()
        made.fixture.produce.mockImplementation(async (...args) => {
          if (++calls === 3) throw error
          return original(...args)
        })
      },
    ],
    [
      'the resource restoration after the active work',
      (made, error) =>
        made.fixture.inResource.mockImplementation(async (work) => {
          made.calls.push('resource')
          await work()
          made.calls.push('restore')
          throw error
        }),
    ],
  ])(
    'stops at a failure of %s, joins every owned operation and releases',
    async (_name, inject) => {
      const made = make()
      const error = new Error('injected')
      inject(made, error)
      const sentAt = []
      const send = made.processes.send
      made.processes.send = jest.fn((kind, message) => {
        sentAt.push({ failed: made.churn.failed, type: message.type })
        return send(kind, message)
      })
      const failure = await made.churn.run().catch((e) => e)
      const errors = failure instanceof AggregateError ? failure.errors : [failure]
      expect(errors).toContain(error)
      expect(made.churn.cause).toBe(error)
      expect(made.churn.outstanding.size).toBe(0)
      expect(made.calls).toContain('restore')
      expect(made.fixture.close).toHaveBeenCalledTimes(1)
      expect(made.decoder.close).toHaveBeenCalledTimes(1)
      for (const kind of KINDS) expect(made.processes.cancel).toHaveBeenCalledWith(kind)
      expect(
        sentAt.filter((m) => m.failed && ['stage', 'corpus', 'config', 'go'].includes(m.type))
      ).toEqual([])
      expect(made.churn.lastResult.ok).toBe(false)
    }
  )

  test('preserves a falsy primary failure and stops later work after it', async () => {
    const made = make({
      // eslint-disable-next-line prefer-promise-reject-errors
      fixture: { open: jest.fn(async () => Promise.reject(undefined)) },
    })
    await expect(made.churn.run()).rejects.toBeUndefined()
    expect(made.fixture.close).toHaveBeenCalledTimes(1)
    expect(made.decoder.close).toHaveBeenCalledTimes(1)
    expect(made.churn.failed).toBe(true)
    expect(made.churn.cause).toBeUndefined()
    await expect(made.churn.until(1e9)).rejects.toBeUndefined()
    expect(made.fixture.produce).not.toHaveBeenCalled()
  })

  test('taints the run when a worker reports a failure while stages are in flight', async () => {
    const made = make()
    const error = new Error('worker failed')
    made.churn.fail(error)
    made.churn.fail(new Error('later'))
    expect(() => made.churn.guard()).toThrow(error)
    await expect(made.churn.abort).rejects.toBe(error)
  })
})

describe('telemetry churn verification of one worker', () => {
  const getRec = (index, socketId, over = {}) => ({
    type: 'request',
    apiKey: 71,
    index,
    requestKey: `r${index}`,
    socketId,
    uuid: U,
    writeAt: index * 10,
    response: {
      errorCode: 0,
      uuid: U,
      subscriptionId: 7,
      pushIntervalMs: 1000,
      requestedMetrics: ['org.apache.kafka.'],
      responseAt: index * 10 + 5,
    },
    ...over,
  })
  const pushRec = (index, socketId, over = {}) => ({
    type: 'request',
    apiKey: 72,
    index,
    requestKey: `r${index}`,
    socketId,
    uuid: U,
    subscriptionId: 7,
    terminating: false,
    metricsBytes: 10,
    writeAt: index * 10,
    response: { errorCode: 0, responseAt: index * 10 + 5 },
    ...over,
  })

  describe('reopen', () => {
    const base = () => ({
      claim: {
        tick: 5,
        from: 'a',
        to: 'b',
        observedAt: 90,
        cachedUuid: U,
        get: 'r2',
        pause: { pausedAt: 50, idleAt: 60, confirmedAt: 70 },
      },
      from: {
        records: [{ type: 'connect', socketId: 1, at: 5, index: 1 }],
        closures: [{ at: 100, end: 110, pending: [] }],
      },
      to: {
        records: [
          { type: 'connect', socketId: 1, at: 130, index: 1 },
          getRec(2, 1, { writeAt: 140 }),
        ],
        closures: [],
      },
      journal: [
        { seq: 1, type: 'select', from: 'a', to: 'b', at: 95 },
        { seq: 2, type: 'disconnect-resolved', from: 'a', to: 'b', at: 120 },
        { seq: 3, type: 'connect-called', from: 'a', to: 'b', at: 125 },
      ],
      uuid: U,
    })

    test('accepts the immediate same-object reopen with its replacement Get', () => {
      expect(() => verifyChurnReopen(base())).not.toThrow()
    })

    test('checks subscription release against raw close and reconnect times', () => {
      const input = base()
      input.claim.closedAt = 121
      input.releasedAt = 123
      expect(() => verifyChurnReopen(input)).not.toThrow()
      for (const releasedAt of [110, 126])
        expect(() => verifyChurnReopen({ ...input, releasedAt })).toThrow(
          'The subscription release is outside the disconnected lifecycle gap'
        )
    })

    test.each([
      [
        'a connect that was not next',
        (i) => (i.journal[2].seq = 4),
        'Connect was not the next public action',
      ],
      [
        'a segment selected after the close resolved',
        (i) => (i.journal[0].seq = 9),
        'The next segment was not selected before the close resolved',
      ],
      [
        'a Get during the final',
        (i) => i.from.records.push(getRec(3, 1, { writeAt: 105 })),
        'Get, connect or authentication during the final',
      ],
      [
        'a different cached UUID',
        (i) => (i.claim.cachedUuid = 'other'),
        'The cached timeout-zero UUID differs',
      ],
      [
        'a replacement Get before the reconnect',
        (i) => (i.to.records[1].writeAt = 124),
        'The replacement Get precedes the reconnect',
      ],
      [
        'a replacement socket before the reconnect',
        (i) => (i.to.records[0].at = 100),
        'The replacement socket precedes the reconnect',
      ],
      [
        'a replacement Get that did not carry the cached UUID',
        (i) => (i.to.records[1].uuid = '00000000-0000-0000-0000-000000000000'),
        'The replacement Get is not a successful Get carrying the cached UUID',
      ],
      [
        'undrained admission',
        (i) => (i.claim.pause.confirmedAt = 91),
        'Admission was not paused, drained and confirmed before the close',
      ],
      [
        'two public closes',
        (i) => i.from.closures.push({ at: 101, end: 102 }),
        'A lifecycle must have exactly one public close',
      ],
      [
        'boundary times out of order',
        (i) => (i.claim.observedAt = 99.5 + 1),
        'Boundary times are out of order',
      ],
    ])('rejects %s', (_name, change, message) => {
      const input = base()
      change(input)
      expect(() => verifyChurnReopen(input)).toThrow(message)
    })
  })

  describe('retirement', () => {
    const base = () => ({
      claim: { tick: 3, segment: 's', faulted: 1, declaredAt: 20, core: 'r4', get: 'r6' },
      transcript: {
        faults: [{ socketId: 1, at: 20 }],
        sockets: [
          { id: 1, kind: 'telemetry', closedAt: 30 },
          { id: 2, kind: 'normal', closedAt: 99 },
          { id: 3, kind: 'telemetry', closedAt: 99 },
        ],
        cancellations: [{ socketId: 1, requestKey: 'r5', disposition: 'faulted-transport' }],
        records: [
          {
            type: 'request',
            requestKey: 'r4',
            apiKey: 1,
            socketId: 2,
            writeAt: 21,
            index: 4,
            response: { errorCode: 0, responseAt: 25 },
          },
          getRec(6, 3, { writeAt: 40 }),
        ],
      },
      uuid: U,
    })

    test('accepts one declared fault with core work beside it and a recovery Get', () => {
      expect(() => verifyChurnRetirement(base())).not.toThrow()
    })

    test.each([
      ['an undeclared fault', (i) => (i.transcript.faults = []), 'Undeclared faulted socket'],
      [
        'a non-telemetry target',
        (i) => (i.transcript.sockets[0].kind = 'normal'),
        'Faulted a non-telemetry socket',
      ],
      [
        'no cut-off exchange',
        (i) => (i.transcript.cancellations = []),
        'Exactly one exchange must be cut off',
      ],
      [
        'a close disposition',
        (i) => (i.transcript.cancellations[0].disposition = 'close-in-flight'),
        'faulted-transport',
      ],
      [
        'core work on the telemetry socket',
        (i) => (i.transcript.records[0].socketId = 3),
        'The core work ran on a telemetry socket',
      ],
      [
        'core work before the declaration',
        (i) => (i.transcript.records[0].writeAt = 19),
        'The core work precedes the fault declaration',
      ],
      [
        'a recovery on the faulted socket',
        (i) => (i.transcript.records[1].socketId = 1),
        'The recovery Get is not a successful Get on a new socket',
      ],
    ])('rejects %s', (_name, change, message) => {
      const input = base()
      change(input)
      expect(() => verifyChurnRetirement(input)).toThrow(message)
    })
  })

  describe('transition', () => {
    const custom = (over = {}) =>
      getRec(2, 3, {
        writeAt: 60,
        response: {
          ...getRec(2, 3).response,
          requestedMetrics: ['com.example.'],
          subscriptionId: 8,
        },
        ...over,
      })
    const base = () => ({
      claim: {
        tick: 1,
        index: 6,
        selection: 'com.example.',
        segment: 's',
        receivedAt: 50,
        releasedAt: 65,
        after: 1,
        get: 'r2',
        subscriptionId: 8,
        proof: { socketId: 3, get: 'r1', push: 'r3', owedBy: ['reopen'] },
      },
      tick: { actions: ['reopen', 'subscription'] },
      rotation: { selection: 'com.example.', readback: 'com.example.', setAt: 40, readbackAt: 45 },
      transcript: {
        records: [getRec(1, 3), custom(), pushRec(3, 3, { subscriptionId: 8, writeAt: 70 })],
      },
      uuid: U,
      selection: 'com.example.',
      previous: 7,
    })

    test('accepts a confirmed transition with its owed current Push', () => {
      expect(() => verifyChurnTransition(base())).not.toThrow()
    })

    test.each([
      [
        'a readback that differs',
        (i) => (i.rotation.readback = ''),
        'The readback differs from the set',
      ],
      ['a missing parent set', (i) => (i.rotation = undefined), 'No parent set and readback'],
      [
        'a Get written before the confirmation',
        (i) => (i.claim.receivedAt = 61),
        'No successful Get proves the selection',
      ],
      [
        'the wrong requested list',
        (i) => (i.transcript.records[1].response.requestedMetrics = ['org.apache.kafka.']),
        'No successful Get proves the selection',
      ],
      ['an unchanged subscription', (i) => (i.previous = 8), 'The subscription did not change'],
      [
        'a missing proof',
        (i) => (i.claim.proof = null),
        'A lifecycle change without its current Push proof',
      ],
      [
        'a proof for another action',
        (i) => (i.claim.proof.owedBy = ['retire']),
        'Expected values to be strictly deep-equal',
      ],
      [
        'a Push at the Get response time',
        (i) => (i.transcript.records[2].writeAt = 1),
        'No proving Push after the Get r1',
      ],
      [
        'a proof under an empty selection',
        (i) => {
          i.claim.selection = ''
          i.selection = ''
          i.rotation.selection = ''
          i.rotation.readback = ''
          i.transcript.records[1].response.requestedMetrics = []
        },
        'A proof cannot be owed under an empty selection',
      ],
    ])('rejects %s', (_name, change, message) => {
      const input = base()
      change(input)
      expect(() => verifyChurnTransition(input)).toThrow(message)
    })

    test('requires no proof at a tick without a lifecycle change', () => {
      const input = base()
      input.tick = { actions: ['subscription'] }
      expect(() => verifyChurnTransition(input)).toThrow('Unexpected current Push proof')
      input.claim.proof = null
      expect(() => verifyChurnTransition(input)).not.toThrow()
    })
  })

  describe('custom identity fence', () => {
    const slot = 'com.example.soak.producer.000'
    const L1 = 'producer-lc-1'
    const L2 = 'producer-lc-2'
    const names = ['producer-lc-0', L1, L2]
    const point = (variant, asInt = '0') => ({
      attrs: [
        ['slot', 'string', '0'],
        ['variant', 'string', variant],
      ],
      asInt,
    })
    const proof = (...points) => ({
      decoded: { decoded: { metrics: [{ name: slot, points }] } },
    })
    const custom = (index) =>
      getRec(index, 1, {
        response: { ...getRec(index, 1).response, requestedMetrics: ['com.example.'] },
      })
    /** A replacement whose unregister and register calls returned at the given cursors. */
    const replace = (removedVariant, addedVariant, cursors) => ({
      type: 'replace',
      tick: 1,
      removedVariant,
      addedVariant,
      unregisteredAt: 1,
      registeredAt: 1,
      unregisterFence: { cursors },
      registerFence: { cursors },
      at: 1,
    })
    /** Two segments, each a custom Get then Pushes at the given record indexes, all decoded. */
    const archive = (journal, pushes) => {
      const transcripts = {}
      const bundles = {}
      for (const [id, list] of Object.entries(pushes)) {
        transcripts[id] = { records: [custom(1), ...list.map(([index]) => pushRec(index, 1))] }
        bundles[id] = { proofs: new Map(list.map(([index, p]) => [`r${index}`, p])) }
      }
      return { facts: { replacements: journal }, bundles, transcripts }
    }
    const verify = (a) => verifyChurnFence('producer', a.facts, a.bundles, a.transcripts, names)

    test('proves the newly registered identity on the first Push after the register fence', () => {
      const ok = archive([replace('a', 'b', { [L1]: 2 })], {
        [L1]: [
          [2, proof(point('a'))],
          [3, proof(point('b'))],
        ],
      })
      expect(verify(ok)).toBe(2)
    })

    test('refuses the removed identity on the very first Push after the unregister fence', () => {
      const stale = archive([replace('a', 'b', { [L1]: 2 })], {
        [L1]: [[3, proof(point('a'))]],
      })
      expect(() => verify(stale)).toThrow('A retired custom identity was exported after its fence')
    })

    test('fences across successive replacements and a later segment with no elapsed-time grace', () => {
      const journal = [replace('a', 'b', { [L1]: 2 }), replace('b', 'a', { [L1]: 4 })]
      const ok = archive(journal, {
        [L1]: [
          [3, proof(point('b'))],
          [5, proof(point('a'))],
        ],
        [L2]: [[2, proof(point('a'))]],
      })
      expect(verify(ok)).toBe(3)
      const late = archive(journal, {
        [L1]: [[3, proof(point('b'))]],
        [L2]: [[2, proof(point('b'))]],
      })
      expect(() => verify(late)).toThrow('A retired custom identity was exported after its fence')
    })

    test('refuses a duplicate point, a foreign attribute and a missing fence by name', () => {
      const duplicate = archive([replace('a', 'b', { [L1]: 2 })], {
        [L1]: [[3, proof(point('b'), point('b'))]],
      })
      expect(() => verify(duplicate)).toThrow('Custom point cardinality differs')
      const foreign = archive([], { [L1]: [[3, proof({ attrs: [['variant', 'string', 'a']] })]] })
      expect(() => verify(foreign)).toThrow('Custom point identity differs')
      const missing = archive([{ ...replace('a', 'b', {}), unregisterFence: null }], {
        [L1]: [[3, proof(point('b'))]],
      })
      expect(() => verify(missing)).toThrow('Missing custom registration fences')
      const swapped = archive([replace('b', 'a', { [L1]: 2 })], { [L1]: [[3, proof(point('a'))]] })
      expect(() => verify(swapped)).toThrow('Custom replacement identity differs')
    })

    test('allows exactly one frozen old value, first after a same-identity update', () => {
      const update = {
        type: 'update',
        variant: 'a',
        previousValue: '1',
        value: '2',
        fence: { cursors: { [L1]: 2 } },
      }
      const current = archive([update], { [L1]: [[3, proof(point('a', '2'))]] })
      expect(verify(current)).toBe(1)
      const frozen = archive([update], {
        [L1]: [
          [3, proof(point('a', '1'))],
          [4, proof(point('a', '2'))],
        ],
      })
      expect(verify(frozen)).toBe(2)
      const twice = archive([update], {
        [L1]: [
          [3, proof(point('a', '1'))],
          [4, proof(point('a', '1'))],
        ],
      })
      expect(() => verify(twice)).toThrow('A frozen old value exceeded the one update allowance')
      const acrossSegments = archive([update], {
        [L1]: [[3, proof(point('a', '1'))]],
        [L2]: [[2, proof(point('a', '1'))]],
      })
      expect(() => verify(acrossSegments)).toThrow(
        'A frozen old value exceeded the one update allowance'
      )
      const changed = archive([{ ...update, variant: 'b' }], {
        [L1]: [[3, proof(point('a', '2'))]],
      })
      expect(() => verify(changed)).toThrow('A custom update changed the identity')
    })
  })
})

describe('telemetry churn complete-stream accounting', () => {
  const ID = 'producer-lc-2'
  const request = (index, apiKey, over = {}) => ({
    type: 'request',
    apiKey,
    apiVersion: 0,
    index,
    requestKey: `r${index}`,
    socketId: 1,
    uuid: U,
    writeAt: index * 10,
    response: { errorCode: 0, uuid: U, responseAt: index * 10 + 5 },
    ...over,
  })
  const selected = (index, over = {}) =>
    request(index, 71, {
      response: {
        errorCode: 0,
        uuid: U,
        requestedMetrics: ['org.apache.kafka.'],
        responseAt: index * 10 + 5,
      },
      ...over,
    })
  /** A Get, an accepted Push, a faulted exchange on a retired socket and an answered final. */
  const segment = () => ({
    records: [
      selected(1, { socketId: 1 }),
      request(2, 72, { socketId: 1, metricsBytes: 10 }),
      request(3, 72, { socketId: 1, response: undefined, writeAt: 31 }),
      selected(4, { socketId: 2 }),
      request(5, 72, { socketId: 2, terminating: true, metricsBytes: 10 }),
    ],
    sockets: [
      { id: 1, kind: 'telemetry', closedAt: 40 },
      { id: 2, kind: 'telemetry', closedAt: 60 },
    ],
    faults: [{ socketId: 1, at: 30 }],
    cancellations: [
      {
        requestKey: 'r3',
        apiKey: 72,
        socketId: 1,
        writeAt: 31,
        at: 30,
        end: 40,
        disposition: 'faulted-transport',
      },
    ],
    closures: [{ at: 50, end: 55, pending: [] }],
  })
  const verify = (change = () => {}, faulted = { [ID]: [1] }) => {
    const transcript = segment()
    change(transcript)
    return () => verifyChurnStream({ [ID]: transcript }, faulted)
  }

  test('rejects an undeclared 117 after a subscription transition', () => {
    expect(
      verify((t) => {
        const push = t.records.find((r) => r.apiKey === 72 && !r.terminating && r.response)
        push.response.errorCode = 117
      })
    ).toThrow('Undeclared error 117')
  })

  test('accounts for every request, the faulted exchange and the one final', () => {
    expect(verify()).not.toThrow()
  })

  test.each([
    ['an unscheduled fault', {}],
    ['a scheduled fault the segment never declared', { [ID]: [1, 2] }],
    ['a fault on another socket than the scheduled one', { [ID]: [2] }],
  ])('refuses %s', (_name, faulted) => {
    expect(verify(() => {}, faulted)).toThrow(
      `Declared faults of ${ID} differ from its scheduled retirements`
    )
  })

  describe.each(['producer-lc-2', 'producer-lc-6'])('the final of %s', (id) => {
    const check = (change = () => {}) => {
      const transcript = segment()
      change(transcript)
      return () => verifyChurnStream({ [id]: transcript }, { [id]: [1] })
    }
    const drop = (t) => (t.records = t.records.filter((r) => r.requestKey !== 'r5'))
    const empty = (t) => (t.records[3].response.requestedMetrics = [])

    test('passes with one accepted final under a nonempty selection, and none under an empty one', () => {
      expect(check()).not.toThrow()
      expect(
        check((t) => {
          empty(t)
          drop(t)
        })
      ).not.toThrow()
    })

    test.each([
      ['a missing final', drop, `The close of ${id} owes exactly one final`],
      [
        'a cancelled final',
        (t) => {
          t.records[4].response = undefined
          t.cancellations.push({
            requestKey: 'r5',
            apiKey: 72,
            socketId: 2,
            writeAt: 50,
            at: 50,
            end: 60,
            disposition: 'close-final',
          })
        },
        `The final of ${id} was not answered with error 0`,
      ],
      [
        'a final written before its close',
        (t) => (t.records[4].writeAt = 49),
        `The final of ${id} was not written inside its close`,
      ],
      [
        'an empty final outside the full-proof selection',
        (t) => (t.records[4].metricsBytes = 0),
        `The final of ${id} is outside the full-proof selection`,
      ],
      ['a final under an empty selection', empty, `${id} sent a final under an empty selection`],
    ])('refuses %s', (_name, change, message) => {
      expect(check(change)).toThrow(message)
    })
  })

  test.each([
    [
      'an unanswered request without a cancellation',
      (t) => (t.records[1].response = undefined),
      `Unanswered telemetry request of ${ID}`,
    ],
    [
      'a cancellation invented for an answered request',
      (t) => t.cancellations.push({ ...t.cancellations[0], requestKey: 'r2' }),
      `Unanswered telemetry request of ${ID}`,
    ],
    [
      'an undeclared Push error',
      (t) => (t.records[1].response.errorCode = 76),
      `Undeclared error 76 for r2 of ${ID}`,
    ],
    ['a Get error', (t) => (t.records[3].response.errorCode = 42), 'Unexpected Get error'],
    [
      'a rejected final',
      (t) => (t.records[4].response.errorCode = 42),
      `The final of ${ID} was not answered with error 0`,
    ],
    [
      'a second final',
      (t) => t.records.push(request(6, 72, { socketId: 2, terminating: true, writeAt: 52 })),
      `The close of ${ID} owes exactly one final`,
    ],
    [
      'telemetry after the final',
      (t) => t.records.push(request(6, 71, { socketId: 2 })),
      'Telemetry work followed the final',
    ],
    [
      'a fault covering unrelated requests',
      (t) => {
        t.records.splice(3, 0, request(3.5, 72, { socketId: 1, writeAt: 35, response: undefined }))
        t.cancellations.push({ ...t.cancellations[0], requestKey: 'r3.5', writeAt: 35 })
      },
      'Fault covers unrelated requests',
    ],
  ])('refuses %s', (_name, change, message) => {
    expect(verify(change)).toThrow(message)
  })

  test('refuses a segment outside the fixed catalogue', () => {
    expect(() => verifyChurnStream({ 'producer-lc-99': segment() })).toThrow(
      'Segment producer-lc-99 is outside the catalogue'
    )
  })
})

describe('telemetry churn lifecycle identity and correction', () => {
  const names = ['producer-lc-0', 'producer-lc-1', 'producer-lc-2']
  const Z = '00000000-0000-0000-0000-000000000000'
  const get = (index, over = {}) => ({
    type: 'request',
    apiKey: 71,
    index,
    requestKey: `r${index}`,
    socketId: 1,
    uuid: U,
    writeAt: index * 10,
    response: {
      errorCode: 0,
      uuid: U,
      requestedMetrics: ['org.apache.kafka.'],
      responseAt: index * 10 + 5,
    },
    ...over,
  })
  const push = (index, over = {}) => ({
    type: 'request',
    apiKey: 72,
    index,
    requestKey: `r${index}`,
    socketId: 1,
    uuid: U,
    terminating: false,
    metricsBytes: 10,
    writeAt: index * 10,
    response: { errorCode: 0, responseAt: index * 10 + 5 },
    ...over,
  })
  /** Setup with a zero-ID first Get and a final; a reopened lifecycle with one corrective 42. */
  const lifecycles = () => ({
    'producer-lc-0': { records: [get(1, { uuid: Z }), push(2), push(3, { terminating: true })] },
    'producer-lc-1': {
      records: [
        get(1),
        push(2, { response: { errorCode: 42, responseAt: 25 } }),
        get(3),
        push(4),
        push(5, { terminating: true }),
      ],
    },
    'producer-lc-2': { records: [get(1), push(2)] },
  })
  const verify = (change = () => {}) => {
    const transcripts = lifecycles()
    change(transcripts)
    return () => verifyChurnLifecycles({ names, transcripts, uuid: U })
  }

  test('allows the setup zero ID and one correction after a sent final', () => {
    expect(verify()).not.toThrow()
  })

  test.each([
    [
      'a zero ID after the assignment',
      (t) => t['producer-lc-0'].records.push(get(4, { uuid: Z })),
      'A request rotated the accepted UUID',
    ],
    [
      'a rotated UUID in a later lifecycle',
      (t) => (t['producer-lc-2'].records[1].uuid = 'ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee'),
      'A request rotated the accepted UUID',
    ],
    [
      'a second correction refilled by a successful Get',
      (t) =>
        t['producer-lc-1'].records.splice(
          3,
          0,
          push(3.5, { response: { errorCode: 42, responseAt: 36 } })
        ),
      'Corrective Push budget exceeded',
    ],
    [
      'a correction after the first accepted Push',
      (t) =>
        t['producer-lc-2'].records.push(push(3, { response: { errorCode: 42, responseAt: 35 } })),
      'Corrective Push budget exceeded',
    ],
    [
      'a correction in a lifecycle that follows no final',
      (t) => {
        t['producer-lc-1'].records.pop()
        t['producer-lc-2'].records.splice(
          1,
          0,
          push(1.5, { response: { errorCode: 42, responseAt: 16 } })
        )
      },
      'Corrective Push budget exceeded',
    ],
    [
      'a correction with another error',
      (t) => (t['producer-lc-1'].records[1].response.errorCode = 76),
      'Corrective Push budget exceeded',
    ],
    [
      'a Push accepted under an empty selection',
      (t) => (t['producer-lc-2'].records[0].response.requestedMetrics = []),
      'A Push was accepted under an empty selection',
    ],
  ])('refuses %s', (_name, change, message) => {
    expect(verify(change)).toThrow(message)
  })
})

describe('telemetry churn coordinated stage replay', () => {
  const OFFSET = { producer: 100, consumer: 200, admin: 300, shareConsumer: 400 }
  const EPOCH = 1000
  /**
   * The complete archive of a run that met every tick on time: rows sent after the previous
   * stage's four receipts, sets inside their barrier, worker clocks offset by a constant per kind,
   * and every claim inside its own stage.
   */
  const archive = () => {
    const table = new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    const stages = []
    const tickStarts = []
    const rotations = []
    const workers = Object.fromEntries(
      KINDS.map((kind) => [
        kind,
        { stages: [], claims: { reopens: [], retirements: [], transitions: [] } },
      ])
    )
    let t = EPOCH
    for (const tick of table.ticks) {
      t = Math.max(t, EPOCH + tick.at)
      tickStarts.push({ tick: tick.number, at: tick.at, beganAt: t })
      for (const stage of table.stages(tick)) {
        const sentAt = t
        let releasedAt
        if (stage === 'confirm') {
          for (const index of tick.transitions)
            rotations.push({
              tick: tick.number,
              index,
              selection: table.selection(index),
              readback: table.selection(index),
              requestedAt: t + 3,
              setAt: t + 3,
              readbackAt: t + 3,
            })
          releasedAt = t + 6
        }
        for (const kind of KINDS) {
          const startedAt = sentAt + 1 - OFFSET[kind]
          const doneAt = sentAt + 8 - OFFSET[kind]
          stages.push({
            tick: tick.number,
            stage,
            kind,
            sentAt,
            ...(stage === 'confirm' && {
              releasedAt,
              fence: {
                parentAt: sentAt + 2,
                receivedAt: startedAt,
                after: 1,
                segment: `${kind}-lc-1`,
              },
            }),
            parentAt: sentAt + 9,
            workerStartedAt: startedAt,
            workerDoneAt: doneAt,
          })
          const { claims } = workers[kind]
          workers[kind].stages.push({ tick: tick.number, stage, startedAt, doneAt })
          if (stage === 'confirm')
            claims.reopens.push({ tick: tick.number, observedAt: startedAt, closedAt: startedAt })
          if (stage === 'retire')
            claims.retirements.push({ tick: tick.number, declaredAt: startedAt, faultedAt: doneAt })
          if (stage === 'confirm')
            claims.transitions.push({
              tick: tick.number,
              receivedAt: startedAt,
              releasedAt: releasedAt + 1 - OFFSET[kind],
              after: 1,
              segment: `${kind}-lc-1`,
            })
        }
        t = sentAt + 9
      }
    }
    const boundaryAt = EPOCH + 360000
    for (const kind of KINDS)
      workers[kind].timing = {
        phaseAt: EPOCH - 50 - OFFSET[kind],
        goAt: EPOCH + 1 - OFFSET[kind],
        boundaryAt: boundaryAt + 1 - OFFSET[kind],
        measurementMs: 360000 + 51 - 60000,
      }
    return {
      archive: { stages, tickStarts, rotations, missed: [], workers },
      timeline: { epochAt: EPOCH, boundaryAt },
      table,
    }
  }
  const replay = (change = () => {}) => {
    const input = archive()
    change(input.archive, input.table)
    return () => verifyChurnStages({ archive: input.archive, timeline: input.timeline })
  }
  const tickWith = (table, action) => table.ticks.find((t) => t.actions.includes(action)).number

  test('rejects a rotation before every worker fence acknowledgement', () => {
    expect(
      replay((a) => {
        const rotation = a.rotations[0]
        const rows = a.stages.filter((r) => r.tick === rotation.tick && r.stage === 'confirm')
        for (const row of rows) {
          row.fence = { parentAt: rotation.requestedAt + 1 }
        }
      })
    ).toThrow('The rotation preceded a worker confirmation fence')
  })

  test('derives the measured counts per kind from the joined receipts', () => {
    const input = archive()
    const counts = verifyChurnStages({ archive: input.archive, timeline: input.timeline })
    expect(Object.keys(counts)).toEqual([...KINDS])
    for (const kind of KINDS)
      expect(counts[kind]).toEqual({ subscription: 35, retire: 10, reopen: 5 })
  })

  test('accepts a polled readback only before every worker release', () => {
    const input = archive()
    const rotation = input.archive.rotations[0]
    rotation.readbackAt += 250
    rotation.readbackAttempts = 6
    for (const next of input.archive.rotations.filter(
      (r) => r.tick === rotation.tick && r.index > rotation.index
    )) {
      next.requestedAt += 250
      next.setAt += 250
      next.readbackAt += 250
    }
    for (const kind of KINDS) {
      const row = input.archive.stages.find((r) => r.tick === rotation.tick && r.kind === kind)
      row.releasedAt += 250
      row.parentAt += 250
      row.workerDoneAt += 250
      input.archive.workers[kind].stages.find(
        (r) => r.tick === rotation.tick && r.stage === 'confirm'
      ).doneAt += 250
      input.archive.workers[kind].claims.transitions[0].releasedAt += 250
    }
    expect(verifyChurnStages(input).producer.subscription).toBe(35)
    rotation.readbackAt += 4
    expect(() => verifyChurnStages(input)).toThrow(
      'The set and readback are outside their stage barrier'
    )
  })

  test.each([
    [
      'a missing receipt',
      (a) => a.stages.splice(5, 1),
      'Stage catalogue differs from the fixed table',
    ],
    [
      'a duplicate replacing an omitted receipt',
      (a) => (a.stages[5] = { ...a.stages[4] }),
      'Stage catalogue differs from the fixed table',
    ],
    [
      'stages reordered within a tick',
      (a, table) => {
        const tick = table.ticks.find((t) => t.actions.length === 3).number
        const rows = a.stages.filter((r) => r.tick === tick)
        const first = a.stages.indexOf(rows[0])
        a.stages.splice(first, 8, ...rows.slice(4, 8), ...rows.slice(0, 4))
      },
      'Stage catalogue differs from the fixed table',
    ],
    [
      'a tick begun before it was due',
      (a) => (a.tickStarts[3].beganAt = EPOCH + a.tickStarts[3].at - 1),
      'A tick began early',
    ],
    [
      'a tick caught up beyond its late bound',
      (a) => (a.tickStarts[7].beganAt = EPOCH + a.tickStarts[7].at + 5001),
      'A tick began beyond its late bound',
    ],
    [
      'a stage sent before the previous stage was joined',
      (a, table) => {
        const tick = tickWith(table, 'retire')
        for (const row of a.stages.filter((r) => r.tick === tick && r.stage === 'retire'))
          row.sentAt -= 3
      },
      'A stage was sent before the previous stage was joined',
    ],
    [
      'a missed stage deadline',
      (a) => (a.stages[0].parentAt = a.stages[0].sentAt + 10001),
      'A stage missed its deadline',
    ],
    [
      'a set outside its barrier',
      (a) => (a.rotations[2].setAt = a.rotations[2].requestedAt - 1),
      'The set and readback are outside their stage barrier',
    ],
    [
      'a foreign worker receipt',
      (a) => (a.workers.consumer.stages[4].doneAt += 1),
      "consumer stage journal differs from the parent's receipts",
    ],
    [
      'a worker clock no single offset maps',
      (a) => {
        const row = a.stages.find((r) => r.kind === 'admin' && r.tick === 9)
        const own = a.workers.admin.stages.find((s) => s.tick === 9 && s.stage === row.stage)
        row.workerStartedAt += 10
        row.workerDoneAt += 10
        own.startedAt += 10
        own.doneAt += 10
      },
      'admin clock cannot be mapped onto the parent epoch',
    ],
    [
      'a claim outside its own stage',
      (a) => (a.workers.producer.claims.reopens[0].closedAt += 10),
      'lies outside its confirm stage',
    ],
    [
      'a transition while a lifecycle is still active',
      (a) => {
        const facts = a.workers.producer
        const reopen = facts.claims.reopens[0]
        const stage = facts.stages.find((s) => s.tick === reopen.tick && s.stage === 'confirm')
        reopen.closedAt = stage.startedAt + 5
      },
      'A subscription transition was released inside an active lifecycle',
    ],
    [
      'a transition without a disconnected lifecycle boundary',
      (a) => a.workers.producer.claims.reopens.shift(),
      'The subscription transition has no disconnected lifecycle boundary',
    ],
    [
      'an advertised measurement interval',
      (a) => (a.workers.shareConsumer.timing.measurementMs += 1),
      'shareConsumer measurement interval differs from its stamps',
    ],
    [
      'a missed measured tick',
      (a, table) => a.missed.push({ tick: table.ticks.find((t) => t.at >= 60000).number }),
      'A measured tick was missed',
    ],
  ])('refuses %s', (_name, change, message) => {
    expect(replay(change)).toThrow(message)
  })
})

describe('telemetry profile evidence after an expired bound', () => {
  test('keeps every received progress row, sealed receipt, failure fact and exit without a verdict', () => {
    const made = rig()
    const source = { churn: { file: 'c', sha256: 'a' }, driver: { file: 'd', sha256: 'b' } }
    const churn = new TelemetryChurn({
      fixture: {},
      processes: made.processes,
      decoder: {},
      record: jest.fn(),
      clock: { now: () => 0 },
      limits: {},
      verdict: jest.fn(),
      brokers: [],
      stem: STEM,
      line: '4.3.1',
      directory: '/evidence',
      source,
    })
    made.processes.start('producer')
    made.processes.start('consumer')
    made.say('producer', { type: 'hello' })
    made.say('producer', { type: 'progress', row: { kind: 'sample', n: 1 } })
    made.say('producer', { type: 'progress', sealed: { id: 'producer-lc-0' } })
    made.say('consumer', { type: 'hello' })
    made.say('consumer', {
      type: 'failure',
      message: 'boom',
      facts: { sealed: [{ id: 'consumer-lc-0' }] },
      failures: [{ name: 'Error', message: 'boom' }],
    })
    made.children[1].emit('close', 1, null)
    const result = churn.evidence()
    expect(result).toMatchObject({ ok: false, violations: ['the run ended before its verdict'] })
    expect(result.workers.map((w) => w.kind)).toEqual([...KINDS])
    const [producer, consumer, admin] = result.workers
    expect(producer).toEqual({
      kind: 'producer',
      facts: null,
      sealed: [{ id: 'producer-lc-0' }],
      rows: [{ kind: 'sample', n: 1 }],
      failures: [],
      exit: null,
    })
    expect(consumer).toMatchObject({
      facts: { sealed: [{ id: 'consumer-lc-0' }] },
      sealed: [{ id: 'consumer-lc-0' }],
      failures: [{ name: 'Error', message: 'boom' }],
      exit: { kind: 'consumer', event: 'close', code: 1, killed: false, watchdogExpired: false },
    })
    expect(admin).toMatchObject({ sealed: [], rows: [], exit: null })
    expect(result.archive.workers.producer).toBe(producer)
    expect(result.archive.processes.consumer).toMatchObject({
      code: 1,
      lastMessage: { type: 'failure', message: 'boom' },
    })
    expect(result.archive.source).toBe(source)
    const judgedFailure = churn.judge(
      [],
      { errors: [], exits: { consumer: consumer.exit }, oracle: null },
      { t0: 0, epochAt: 1, boundaryAt: 2, cleanupEndAt: 3 },
      new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    )
    expect(judgedFailure.archive.workers.producer).toMatchObject({
      rows: [{ kind: 'sample', n: 1 }],
      sealed: [{ id: 'producer-lc-0' }],
    })
    expect(judgedFailure.archive.processes.consumer).toMatchObject({ code: 1 })
    expect(
      churn.record.mock.calls.map(([row]) => row).filter((r) => r.kind === 'natural-exit')
    ).toHaveLength(4)
    const judged = { ok: true, marker: 'verdict' }
    churn.lastResult = judged
    expect(churn.evidence()).toBe(judged)
  })
})

describe('telemetry churn worker identity', () => {
  const stub = { registerMetricForSubscription() {}, unregisterMetricFromSubscription() {} }
  const digestOf = (kind, uuid, count) => {
    const registry = new Registry(stub, kind, count)
    registry.fill()
    return registry.digest(uuid)
  }
  const config = { topic: `${STEM}-producer`, ctl: `${STEM}-ctl` }

  describe('reference', () => {
    /** The producer worker's baseline, teardown, control and enabled replay of that control. */
    const row = () => {
      const digest = digestOf('producer', U)
      const manifest = controlManifest('producer', config)
      return {
        kind: 'producer',
        uuid: U,
        stem: STEM,
        input: {
          baseline: { registryDigest: digest, journals: { byteLength: 4096, cursor: 0 } },
          teardown: { registryDigest: digest, journals: { byteLength: 4096, cursor: 0 } },
          reference: {
            control: { kind: 'producer', manifest },
            foreground: manifest.operations.map((o) => ({ n: o.n, status: 'fulfilled' })),
          },
        },
      }
    }
    const verify = (change = () => {}) => {
      const input = row()
      change(input)
      return () => verifyChurnReference(input)
    }

    test('binds the population, the control catalogue and its replay to the worker', () => {
      expect(labelOf('producer')).toBe('producer')
      expect(row().input.reference.foreground.length).toBeGreaterThan(0)
      expect(verify()).not.toThrow()
    })

    test.each([
      [
        'a manifest of another kind',
        (r) => (r.input.reference.control.manifest = controlManifest('admin', config)),
        'The control ran another frozen catalogue',
      ],
      [
        'a manifest of another topic',
        (r) =>
          (r.input.reference.control.manifest = controlManifest('producer', {
            ...config,
            topic: `${STEM}-consumer`,
          })),
        'The control ran another frozen catalogue',
      ],
      [
        'a control of another kind',
        (r) => (r.input.reference.control.kind = 'admin'),
        'The control is of another kind',
      ],
      [
        'a digest of another UUID',
        (r) => (r.input.baseline.registryDigest = digestOf('producer', U.replace('a', 'f'))),
        'The retained registry and UUID population differs from the worker',
      ],
      [
        'a digest of another population',
        (r) => (r.input.baseline.registryDigest = digestOf('producer', U, 64)),
        'The retained registry and UUID population differs from the worker',
      ],
      [
        'a teardown of another population',
        (r) => (r.input.teardown.registryDigest = digestOf('consumer', U)),
        'The teardown population differs',
      ],
      [
        'a missing enabled replay',
        (r) => delete r.input.reference.foreground,
        'The enabled worker did not replay the control catalogue',
      ],
      [
        'a short enabled replay',
        (r) => r.input.reference.foreground.pop(),
        'The enabled worker did not replay the control catalogue',
      ],
      [
        'a replay with a failed operation',
        (r) => (r.input.reference.foreground[0].status = 'rejected'),
        'The enabled worker did not replay the control catalogue',
      ],
    ])('refuses %s', (_name, change, message) => {
      expect(verify(change)).toThrow(message)
    })
  })

  describe('handshakes', () => {
    /** Four hellos of one build and flags, all carrying the source the parent recorded. */
    const run = () => {
      const source = sourceStamp(path.resolve(__dirname, '../soak.js'))
      return {
        source,
        handshakes: KINDS.map((kind, i) => ({
          kind,
          origin: 1000 + i,
          pid: 100 + i,
          node: 'v26.0.0',
          v8: '13.0',
          platform: 'linux',
          arch: 'x64',
          execArgv: ['--expose-gc', '--min-semi-space-size=1', '--max-semi-space-size=1'],
          modules: 300,
          codecs: { gzip: true, zstd: true },
          source,
          parentAt: 5 + i,
        })),
      }
    }
    const verify = (change = () => {}) => {
      const input = run()
      change(input)
      return () => verifyChurnHandshakes(input.handshakes, input.source)
    }
    const foreign = (source) => ({
      ...source,
      driver: { ...source.driver, sha256: 'f'.repeat(64) },
    })

    test('joins four distinct workers to the driver source of this tree', () => {
      expect(run().source.driver.file).toBe('scripts/soak/soak.js')
      expect(run().source.churn.file).toBe('scripts/soak/telemetryChurn.js')
      expect(verify()).not.toThrow()
    })

    test('requires the exact five-minute flags even when all workers agree', () => {
      for (const flags of [
        ['--expose-gc'],
        ['--expose-gc', '--no-flush-bytecode'],
        ['--expose-gc', '--min-semi-space-size=1'],
        ['--expose-gc', '--max-semi-space-size=1'],
        ['--expose-gc', '--min-semi-space-size=1', '--max-semi-space-size=2'],
        ['--expose-gc', '--min-semi-space-size=1', '--max-semi-space-size=1', '--inspect'],
      ])
        expect(verify((r) => r.handshakes.forEach((hello) => (hello.execArgv = flags)))).toThrow(
          'Workers ran with unexpected five-minute flags'
        )
    })

    test.each([
      ['a missing kind', (r) => r.handshakes.pop(), 'Worker handshakes differ from the four kinds'],
      [
        'a duplicate pid',
        (r) => (r.handshakes[1].pid = r.handshakes[0].pid),
        'Two workers share one process',
      ],
      [
        'a differing node',
        (r) => (r.handshakes[1].node = 'v24.0.0'),
        'Worker consumer node differs',
      ],
      [
        'differing flags',
        (r) => (r.handshakes[2].execArgv = ['--expose-gc', '--inspect']),
        'Worker admin execArgv differs',
      ],
      [
        'no exposed collection',
        (r) => r.handshakes.forEach((h) => (h.execArgv = [])),
        'Workers ran without real collection',
      ],
      [
        'one worker of another source',
        (r) => (r.handshakes[3].source = foreign(r.source)),
        'Worker shareConsumer source differs from the driver',
      ],
      [
        'four consistent workers of a foreign driver',
        (r) => {
          r.source = foreign(r.source)
          r.handshakes.forEach((h) => (h.source = r.source))
        },
        'The recorded driver source differs from the verified tree',
      ],
      [
        'a driver at another path',
        (r) => {
          r.source = { ...r.source, driver: { ...r.source.driver, file: 'scripts/soak/other.js' } }
          r.handshakes.forEach((h) => (h.source = r.source))
        },
        'The recorded driver source differs from the verified tree',
      ],
    ])('refuses %s', (_name, change, message) => {
      expect(verify(change)).toThrow(message)
    })
  })
})

describe('telemetry churn decoder memory', () => {
  /** A labelled peak as a decoder child that was sampled reports it. */
  const available = () => ({
    source: 'proc-pid-status',
    units: 'bytes',
    capability: 'available',
    samples: 3,
    peakHwmBytes: 8192,
    peakRssBytes: 4096,
    processes: [],
  })

  test('accepts a labelled peak and an explicit unavailable fact', () => {
    expect(() => verifyDecoderMemory(available())).not.toThrow()
    expect(() =>
      verifyDecoderMemory(DecoderMemory.unavailable('no process status could be read'))
    ).not.toThrow()
  })

  test.each([
    ['a missing fact', () => undefined],
    ['another source', (m) => ({ ...m, source: 'ps' })],
    ['other units', (m) => ({ ...m, units: 'kB' })],
    ['an unknown capability', (m) => ({ ...m, capability: 'partial' })],
  ])('refuses %s as unlabelled', (_name, change) => {
    expect(() => verifyDecoderMemory(change(available()))).toThrow(
      'A decoder child memory fact is missing or unlabelled'
    )
  })

  test.each([
    ['a null high-water peak', { peakHwmBytes: null }],
    ['a fractional resident peak', { peakRssBytes: 1.5 }],
    ['a negative resident peak', { peakRssBytes: -1 }],
    ['zero samples', { samples: 0 }],
  ])('refuses an available fact with %s', (_name, over) => {
    expect(() => verifyDecoderMemory({ ...available(), ...over })).toThrow(
      'A decoder child memory fact is malformed'
    )
  })
})

describe('telemetry churn archive replay', () => {
  const churnModule = require('../telemetryChurn')
  const LINE = '4.3.1'
  const ROOT = 'case-telemetry-churn-s'
  const NODE = '1'
  const NONE = { subscription: 0, retire: 0, reopen: 0 }
  const VERDICT = { ok: true, violations: [], inconclusive: [] }
  const MEMORY = {
    source: 'proc-pid-status',
    units: 'bytes',
    capability: 'available',
    samples: 3,
    peakHwmBytes: 8192,
    peakRssBytes: 4096,
    processes: [],
  }
  const TIMELINE = { t0: 0, epochAt: 1000, boundaryAt: 361000, cleanupEndAt: 362000 }
  const CODECS = {
    0: (raw) => raw,
    1: (raw) => zlib.gzipSync(raw),
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- the verifier's own zstd codec
    4: (raw) => zlib.zstdCompressSync(raw),
  }
  let directory

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'churn-replay-'))
    jest.spyOn(churnModule, 'TelemetrySchedule').mockImplementation(() => ({
      ticks: [],
      warmupMs: 60000,
      required: () => ({ ...NONE }),
    }))
    jest.replaceProperty(churnModule, 'MINIMUMS', NONE)
    jest
      .spyOn(churnModule.OrdinaryOracle, 'check')
      .mockImplementation(({ kind }) => ({ violations: [], hashes: `${kind}-sets` }))
    jest
      .spyOn(churnModule.ShareOracle, 'check')
      .mockReturnValue({ violations: [], hashes: 'share-sets' })
    jest.spyOn(TelemetryResourceVerdict, 'evaluate').mockReturnValue(VERDICT)
  })

  afterEach(() => {
    jest.restoreAllMocks()
    fs.rmSync(directory, { recursive: true, force: true })
  })

  /**
   * A complete churn archive with an empty tick table, so no stage, reopen, retirement or
   * transition is claimed and the resource, oracle and stage arithmetic is stubbed: per kind, seven
   * lifecycles (a setup Get; the initial custom Get, its proving Push and an empty readback; plain
   * Gets after), with the sink rows, decoder receipts, worker facts, handshakes and parent rows that
   * join them. The producer's initial lifecycle also writes two gzip and one zstd Push, each resolved
   * by its native journal; the share capture is archived once and named by hash.
   * @returns {{record:object, archive:object, segments:object[], transcripts:object,
   *   bundles:object, sink:object[]}}
   */
  const world = () => {
    const sink = []
    const segments = []
    const transcripts = {}
    const bundles = {}
    const calls = []
    const receipts = []
    const workers = {}
    const resources = []
    const source = sourceStamp(path.resolve(__dirname, '../soak.js'))
    KINDS.forEach((kind, k) => {
      const uuid = `${'abcd'[k].repeat(8)}-1111-4111-8111-111111111111`
      const clientId = `${STEM}-${kind}`
      const name = (n) => `${kind}-lc-${n}`
      const get = (index, requestedMetrics, at) => ({
        type: 'request',
        apiKey: 71,
        apiVersion: 0,
        index,
        requestKey: `r${index}`,
        socketId: 1,
        uuid,
        clientId,
        writeAt: at,
        response: {
          errorCode: 0,
          uuid,
          subscriptionId: requestedMetrics.length ? 7 : 8,
          pushIntervalMs: 1000,
          telemetryMaxBytes: 4096,
          requestedMetrics,
          responseAt: at + 5,
        },
      })
      for (let n = 0; n < 7; n++) {
        const id = name(n)
        const records = [get(1, n === 1 ? ['com.example.'] : [], 10)]
        const lines = []
        const types = n !== 1 ? [] : kind === 'producer' ? [0, 1, 1, 4] : [0]
        types.forEach((type, i) => {
          const index = i + 2
          const raw = Buffer.from(`${kind} metrics ${index}`)
          const blob = CODECS[type](raw)
          sink.push({
            v: 1,
            node: NODE,
            seq: sink.length + 1,
            clientInstanceId: uuid,
            clientId,
            dataSha256: sha(raw),
            dataBytes: raw.length,
            terminating: false,
            pushIntervalMs: 1000,
            parse: 'ok',
          })
          records.push({
            type: 'request',
            apiKey: 72,
            apiVersion: 0,
            index,
            requestKey: `r${index}`,
            socketId: 1,
            uuid,
            clientId,
            node: NODE,
            writeAt: index * 10,
            subscriptionId: 7,
            terminating: false,
            compressionType: type,
            metricsBytes: blob.length,
            metricsSha256: sha(blob),
            response: { errorCode: 0, responseAt: index * 10 + 5 },
          })
          lines.push({
            t: 'push',
            key: `r${index}`,
            b64: blob.toString('base64'),
            bytes: blob.length,
            sha256: sha(blob),
          })
          if (index > 2) return
          const body = {
            dataSha256: sha(raw),
            dataBytes: raw.length,
            parse: 'ok',
            truncated: false,
            metrics: [
              {
                name: `com.example.soak.${kind}.000`,
                points: [
                  {
                    attrs: [
                      ['slot', 'string', '0'],
                      ['variant', 'string', 'a'],
                    ],
                    asInt: 1,
                  },
                ],
              },
            ],
          }
          lines.push({
            t: 'proof',
            key: 'r2',
            rawB64: raw.toString('base64'),
            rawSha256: sha(raw),
            receipts: [],
            decoded: {
              requestKey: 'r2',
              uuid,
              node: NODE,
              compressionType: 0,
              transmittedSha256: sha(blob),
              transmittedBytes: blob.length,
              rawSha256: sha(raw),
              rawBytes: raw.length,
              decoded: body,
              exportIdentity: { node: NODE, seq: sink.length },
            },
          })
          calls.push({ kind, rawSha256: sha(raw) })
          receipts.push({
            rawSha256: sha(raw),
            bodySha256: sha(JSON.stringify(body)),
            line: LINE,
            code: 0,
            signal: null,
            identity: { verified: true, line: LINE },
            memory: MEMORY,
          })
        })
        if (types.length) records.push(get(types.length + 2, [], 60))
        transcripts[id] = {
          owner: id,
          records,
          sockets: [{ id: 1, kind: 'telemetry', closedAt: 100 }],
          faults: [],
          cancellations: [],
          failures: [],
          cleanup: { children: 0, timers: 0, stillOpen: [], attached: 0 },
          closures: [{ at: 90, end: 95, pending: [] }],
        }
        bundles[id] = lines
        segments.push({
          id,
          kind,
          role: 'ordinary',
          transcript: `${ROOT}/segments/${id}.transcript.json`,
          bundle: `${ROOT}/segments/${id}.bundle.jsonl`,
        })
      }
      workers[kind] = {
        kind,
        uuid,
        requirements: { ok: true, violations: [] },
        claims: {
          setup: { segment: name(0), uuid, get: 'r1' },
          initial: { segment: name(1), cachedUuid: uuid, get: 'r1', push: 'r2' },
          reopens: [],
          retirements: [],
          transitions: [],
          proofs: [],
        },
        journal: Array.from({ length: 6 }, (_, n) => ({
          seq: n + 1,
          type: 'select',
          from: name(n),
          to: name(n + 1),
        })),
        replacements: [],
        stages: [],
        timing: { phaseAt: 0, goAt: 0, boundaryAt: 360000, measurementMs: 300000, receiptAt: 95 },
        ordinary: { handled: [`${kind} handled`], offered: [`${kind} offered`] },
        share: null,
      }
      const registry = new Registry(
        { registerMetricForSubscription() {}, unregisterMetricFromSubscription() {} },
        kind
      )
      registry.fill()
      const digest = registry.digest(uuid)
      const manifest = controlManifest(kind, {
        topic: `${STEM}-${labelOf(kind)}`,
        ctl: `${STEM}-ctl`,
      })
      const native = (codec) => ({ codec, outcome: 'resolved' })
      resources.push({
        kind: 'resource-verdict',
        clientKind: kind,
        clientId,
        uuid,
        result: VERDICT,
        input: {
          clientCount: 1,
          baseline: { registryDigest: digest, journals: { byteLength: 4096, cursor: 0 } },
          teardown: { registryDigest: digest, journals: { byteLength: 4096, cursor: 0 } },
          reference: {
            control: { kind, manifest },
            foreground: manifest.operations.map((o) => ({ n: o.n, status: 'fulfilled' })),
            native: {
              journal: {
                rows: kind === 'producer' ? ['gzip', 'gzip', 'zstd'].map(native) : [],
              },
            },
          },
        },
      })
    })
    const exchanges = [
      { n: 1, acknowledged: 2 },
      { n: 2, acknowledged: 1 },
    ]
    const share = {
      group: `${STEM}-share-group`,
      exchanges,
      exchangesSha256: sha(JSON.stringify(exchanges)),
      settled: [1, 2],
    }
    workers.shareConsumer.share = share
    const archive = {
      stem: STEM,
      workers,
      handshakes: KINDS.map((kind, i) => ({
        kind,
        origin: 1000 + i,
        pid: 100 + i,
        node: 'v26.0.0',
        v8: '13.0',
        platform: 'linux',
        arch: 'x64',
        execArgv: ['--expose-gc', '--min-semi-space-size=1', '--max-semi-space-size=1'],
        codecs: { gzip: true, zstd: true },
        source,
        parentAt: 5 + i,
      })),
      source,
      inputs: {
        producer: { offered: workers.producer.ordinary.offered },
        consumer: { handled: workers.consumer.ordinary.handled },
        shareConsumer: {
          group: share.group,
          closures: Array.from({ length: 7 }, () => ({ at: 90, end: 95 })),
          handled: workers.shareConsumer.ordinary.handled,
          exchangesSha256: share.exchangesSha256,
          settled: share.settled,
        },
        parentOffered: {
          consumer: [...Array(10000).keys()],
          shareConsumer: [...Array(10000).keys()],
        },
      },
      decoder: { receipts, calls },
      missed: [],
      stages: [],
      tickStarts: [],
      rotations: [],
    }
    const exits = KINDS.map((kind) => ({
      kind: 'natural-exit',
      clientKind: kind,
      event: 'close',
      code: 0,
      signal: null,
      stderrBytes: 0,
      unhandled: 0,
      receipts: 1,
      killed: false,
      watchdogExpired: false,
      receiptAt: 2000,
      closedAt: 2100,
      afterDisconnectMs: 100,
    }))
    const oracle = {
      kind: 'oracle',
      ok: true,
      independentSetHashes: {
        producer: 'producer-sets',
        consumer: 'consumer-sets',
        shareConsumer: 'share-sets',
      },
    }
    const verdict = {
      kind: 'verdict',
      ok: true,
      violations: [],
      timeline: TIMELINE,
      measured: NONE,
      setupMs: 1000,
      cleanupMs: 1000,
      totalMs: 362000,
    }
    const record = {
      run: { line: LINE },
      results: { rows: [...resources, ...exits, oracle, verdict] },
    }
    return { record, archive, segments, transcripts, bundles, sink }
  }

  /**
   * Writes the index, segment files, sink and oracle sidecar of one archive under the run directory.
   * @param {object} made a `world()` @returns {object} its sidecar manifest
   */
  const write = ({ archive, segments, transcripts, bundles, sink }) => {
    const put = (relative, text) => {
      const file = path.join(directory, relative)
      fs.mkdirSync(path.dirname(file), { recursive: true })
      fs.writeFileSync(file, text)
    }
    const jsonl = (rows) => rows.map((row) => JSON.stringify(row) + '\n').join('')
    put(
      `${ROOT}/index.json`,
      JSON.stringify({ format: SEGMENT_FORMAT, scenario: 'telemetry-churn', segments })
    )
    for (const entry of segments) {
      put(entry.transcript, JSON.stringify(transcripts[entry.id]))
      put(entry.bundle, jsonl(bundles[entry.id]))
    }
    put(`${ROOT}/exports-${NODE}.jsonl`, jsonl(sink))
    put(`${ROOT}/oracles.json`, JSON.stringify({ churn: archive }))
    return {
      root: ROOT,
      segments,
      files: [`${ROOT}/index.json`, ...segments.flatMap((s) => [s.transcript, s.bundle])].map(
        (file) => ({ path: file })
      ),
    }
  }

  const verify = (change = () => {}) => {
    const made = world()
    change(made)
    const manifest = write(made)
    const read = (file) => JSON.parse(fs.readFileSync(path.join(directory, file), 'utf8'))
    return () => verifyChurn(made.record, directory, manifest, read)
  }
  const journalOf = (made) =>
    made.record.results.rows.find((r) => r.clientKind === 'producer' && r.input).input.reference
      .native.journal.rows

  test('replays an archive whose journal resolves every compressed Push and whose share capture is kept once', () => {
    const made = world()
    expect(
      made.transcripts['producer-lc-1'].records
        .filter((r) => r.apiKey === 72)
        .map((r) => r.compressionType)
    ).toEqual([0, 1, 1, 4])
    expect(journalOf(made).map((r) => r.codec)).toEqual(['gzip', 'gzip', 'zstd'])
    expect(Object.hasOwn(made.archive.inputs.shareConsumer, 'exchanges')).toBe(false)
    expect(verify()).not.toThrow()
    expect(churnModule.ShareOracle.check).toHaveBeenCalledTimes(1)
    expect(churnModule.ShareOracle.check).toHaveBeenCalledWith(
      expect.objectContaining({ exchanges: made.archive.workers.shareConsumer.share.exchanges })
    )
  })

  test.each([
    [
      'fewer resolved gzip rows than gzip Pushes',
      (rows) => rows.splice(rows.indexOf(rows.find((r) => r.codec === 'gzip')), 1),
      'gzip',
    ],
    [
      'a zstd Push whose only native row was rejected',
      (rows) => (rows.find((r) => r.codec === 'zstd').outcome = 'rejected'),
      'zstd',
    ],
  ])('refuses a native journal with %s', (_name, change, codec) => {
    expect(verify((made) => change(journalOf(made)))).toThrow(
      `The producer native journal lacks the ${codec} compression of its Pushes`
    )
  })

  test.each(['missing', 'duplicate', 'reordered', 'wrong-timestamp'])(
    'rejects %s share journal facts through the production archive verifier',
    (fault) => {
      const journalArchive = (made) => {
        const captured = made.archive.workers.shareConsumer.share
        const ledger = new churnModule.ShareLedger({
          group: captured.group,
          clock: { now: () => 0 },
        })
        for (const row of captured.exchanges) ledger.exchanges.push({ ...row, at: row.n + 0.125 })
        const original = ledger.facts()
        captured.exchanges = original.exchanges
        captured.exchangesSha256 = original.exchangesSha256
        made.archive.inputs.shareConsumer.exchangesSha256 = original.exchangesSha256
      }
      expect(verify(journalArchive)).not.toThrow()
      expect(
        verify((made) => {
          journalArchive(made)
          const captured = made.archive.workers.shareConsumer.share
          if (fault === 'missing') captured.exchanges.pop()
          if (fault === 'duplicate') captured.exchanges.push(captured.exchanges[0])
          if (fault === 'reordered') captured.exchanges.reverse()
          if (fault === 'wrong-timestamp') captured.exchanges[0].at += 0.125
        })
      ).toThrow('Share exchange facts differ from their capture hash')
    }
  )

  test.each([
    [
      'a second copy of the exchanges in the oracle inputs',
      (a) => (a.inputs.shareConsumer.exchanges = a.workers.shareConsumer.share.exchanges),
      'Share exchanges are archived twice',
    ],
    [
      'captured exchanges that differ from their capture hash',
      (a) => (a.workers.shareConsumer.share.exchanges[1].acknowledged = 3),
      'Share exchange facts differ from their capture hash',
    ],
    [
      'oracle inputs that name another capture',
      (a) => (a.inputs.shareConsumer.exchangesSha256 = sha('other exchanges')),
      'The share oracle inputs name other exchanges',
    ],
  ])('refuses %s', (_name, change, message) => {
    expect(verify((made) => change(made.archive))).toThrow(message)
  })
})

describe('telemetry control warm-up', () => {
  test.each(['producer', 'admin', 'consumer', 'shareConsumer'])(
    'warms the %s catalogue and idle window before c0 without changing the measured replay',
    async (kind) => {
      const events = []
      let at = 0
      const client = {
        connect: async () => events.push('connect'),
        subscribe: async () => {},
        run: async () => {},
        disconnect: async () => events.push('disconnect'),
        send: async (request) => events.push(request.messages[0].key),
        describeCluster: async () => events.push('describeCluster'),
      }
      const config = { stem: 's', topic: 's-topic', ctl: 's-ctl', group: 's-group' }
      const { ReferenceControl } = require('../telemetryChurn')
      const result = await new ReferenceControl({
        kind,
        config,
        Kafka: function Kafka() {
          return { [kind]: () => client }
        },
        socketFactory: () => {},
        logLevel: { NOTHING: 0 },
        resources: { as: (_role, work) => work(), live: new Map() },
        clock: { now: () => ++at },
        sleep: async (ms) => {
          at += ms
          if (ms === 2000 || ms === 10000) events.push('idle')
        },
        gc: () => {},
        engine: { record: (reading) => events.push(reading.phase) },
      }).run()
      const count = { producer: 20, admin: 10, consumer: 0, shareConsumer: 0 }[kind]
      const catalogue = (prefix) =>
        Array.from({ length: count }, (_, n) =>
          kind === 'producer' ? `s:${prefix}:${n}` : 'describeCluster'
        )
      expect(events).toEqual([
        'connect',
        ...catalogue('ctl-warmup'),
        'idle',
        'c0',
        ...catalogue('ctl-off'),
        'idle',
        'c1',
        'disconnect',
        'control-released',
      ])
      expect(result.warmup).toBeDefined()
      expect(result.warmup.outcomes).toHaveLength(count)
      expect(result.warmup.outcomes.every((o, n) => o.n === n && o.status === 'fulfilled')).toBe(
        true
      )
      expect(result.warmup.completedAt - result.warmup.idleStartedAt).toBeGreaterThanOrEqual(
        kind === 'shareConsumer' ? 10000 : 2000
      )
      expect(result.warmup.completedAt - result.warmup.idleStartedAt).toBeLessThan(
        (kind === 'shareConsumer' ? 10000 : 2000) + 10
      )
      expect(result.warmup.completedAt).toBeLessThanOrEqual(result.c0.gc.admittedAt)
      expect(result.outcomes).toHaveLength(result.manifest.operations.length)
      expect(result.outcomes.every((o) => o.at > result.c0.at && o.at < result.c1.at)).toBe(true)
      expect(Object.keys(result.c0).sort()).toEqual(Object.keys(result.c1).sort())
    }
  )

  test('tops up an idle timer that fires early so the warm-up idle lasts the full window', async () => {
    for (const kind of ['consumer', 'shareConsumer']) {
      let at = 0
      const client = {
        connect: async () => {},
        subscribe: async () => {},
        run: async () => {},
        disconnect: async () => {},
        describeCluster: async () => {},
      }
      const { ReferenceControl, CONTROL } = require('../telemetryChurn')
      const result = await new ReferenceControl({
        kind,
        config: { stem: 's', topic: 's-topic', ctl: 's-ctl', group: 's-group' },
        Kafka: function Kafka() {
          return { [kind]: () => client }
        },
        socketFactory: () => {},
        logLevel: { NOTHING: 0 },
        resources: { as: (_role, work) => work(), live: new Map() },
        clock: { now: () => at },
        sleep: async (ms) => {
          at += ms >= CONTROL.idleMs ? ms - 0.35 : ms
        },
        gc: () => {},
        engine: { record: () => {} },
      }).run()
      expect(result.warmup.completedAt - result.warmup.idleStartedAt).toBeGreaterThanOrEqual(
        CONTROL.idleMs * (kind === 'shareConsumer' ? 5 : 1)
      )
      expect(result.warmup.completedAt).toBeLessThanOrEqual(result.c0.gc.admittedAt)
    }
  })

  test('preserves the frozen long-window control catalogue and three readings', async () => {
    const previous = process.env.TELEMETRY_LONG_WINDOW
    let Control
    try {
      process.env.TELEMETRY_LONG_WINDOW = '1'
      jest.isolateModules(() => {
        Control = require('../telemetryChurn').ReferenceControl
      })
    } finally {
      if (previous === undefined) delete process.env.TELEMETRY_LONG_WINDOW
      else process.env.TELEMETRY_LONG_WINDOW = previous
    }
    for (const kind of ['admin', 'shareConsumer']) {
      const events = []
      let at = 0
      const result = await new Control({
        kind,
        config: { stem: 's', topic: 't', ctl: 'ctl', group: 'g' },
        Kafka: function Kafka() {
          return {
            [kind]: () => ({
              subscribe: async () => {},
              run: async () => {},
              connect: async () => {},
              disconnect: async () => {},
              describeCluster: async () => events.push('operation'),
            }),
          }
        },
        socketFactory: () => {},
        logLevel: { NOTHING: 0 },
        resources: { as: (_role, work) => work(), live: new Map() },
        clock: { now: () => ++at },
        sleep: async (ms) => {
          at += ms
          events.push(ms)
        },
        gc: () => {},
        engine: { record: (reading) => events.push(reading.phase) },
      }).run()
      expect(events).toEqual([
        2000,
        100,
        'c0',
        ...Array(kind === 'admin' ? 10 : 0).fill('operation'),
        2000,
        100,
        'c1',
        100,
        'control-released',
      ])
      expect(result).not.toHaveProperty('warmup')
      expect(result.outcomes).toHaveLength(kind === 'admin' ? 10 : 0)
    }
  })
})
