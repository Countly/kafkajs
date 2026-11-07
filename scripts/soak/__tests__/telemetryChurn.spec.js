const { EventEmitter } = require('node:events')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const {
  CORPUS,
  DecoderCoordinator,
  KINDS,
  OrdinaryOracle,
  OrdinaryWork,
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
  logInputs,
  recordId,
  valueOf,
} = require('../telemetryChurn')
const { PROFILES, buildTelemetryChurn, instrument, main, parseArgs } = require('../soak')
const {
  verifyChurnFence,
  verifyChurnReopen,
  verifyChurnRetirement,
  verifyChurnTransition,
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

  test('holds the fixed measured action counts, the serialization order and the corpus slots', () => {
    const schedule = new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    expect(schedule.required()).toEqual({ subscription: 30, retire: 10, reopen: 5 })
    expect(schedule.ticks).toHaveLength(35)
    schedule.ticks.forEach((tick, number) => {
      expect(tick.number).toBe(number)
      expect(tick.index).toBe(tick.at / 10000)
    })
    const sixty = schedule.ticks.find((tick) => tick.at === 60000)
    expect(sixty.actions).toEqual(['reopen', 'retire', 'subscription'])
    expect(schedule.stages(sixty)).toEqual(['lifecycle', 'retire', 'confirm'])
    expect(schedule.stages(schedule.ticks[0])).toEqual(['confirm'])
    expect(schedule.ticks.at(-1).at).toBe(350000)
    expect(
      schedule.ticks.filter((tick) => !schedule.measured(tick)).flatMap((t) => t.actions)
    ).toEqual(expect.arrayContaining(['retire', 'subscription']))
    expect([1, 2, 3, 4, 5, 6].map((i) => schedule.selection(i))).toEqual([
      'com.example.',
      '',
      'org.apache.kafka.',
      'com.example.',
      '',
      'org.apache.kafka.',
    ])
    for (const tick of schedule.ticks.filter((t) => t.actions.length > 1))
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
    const registry = new Registry(client, 'producer', 128, { now: () => (now += 1) })
    registry.fill()
    expect(registry.registered).toHaveLength(128)
    const canonical = registry.digest('uuid')
    const entry = registry.replace(7)
    expect(entry).toEqual({ tick: 7, removedVariant: 'a', addedVariant: 'b', at: 101 })
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
    expect(TelemetryProofs.selection(records, { terminal: false })).toEqual([
      'r2',
      'r5',
      'r8',
      'r9',
    ])
    expect(TelemetryProofs.selection(records, { terminal: true })).toEqual(['r2', 'r5', 'r8'])
    expect(TelemetryProofs.selection([get(1)], { terminal: false })).toEqual([])
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
    const range = (partition, count, over = {}) => ({
      topicId: 'T',
      partition,
      firstOffset: '0',
      lastOffset: String(count - 1),
      ...over,
    })
    const build = () => {
      const log = inputsOf('shareConsumer')
      const counts = Object.entries(log.partitions).map(([p, rows]) => [Number(p), rows.length])
      return {
        stem: STEM,
        log,
        topicId: 'T',
        handled: handledOf(log),
        acquisitions: counts.map(([p, n]) =>
          range(p, n, { deliveryCount: 1, at: 10, lockMs: 15000 })
        ),
        settlements: counts.map(([p, n]) => range(p, n, { type: 1, errorCode: 0, at: 20 })),
        settled: counts.map(([p, n]) =>
          range(p, n, { topic: 'share', type: 'accept', error: null, at: 21 })
        ),
        closures: [],
      }
    }
    const check = (change) => {
      const input = build()
      change(input)
      return ShareOracle.check(input)
    }

    test('accepts every offset once at the broker and publicly with no duplicate delivery', () => {
      expect(ShareOracle.check(build())).toMatchObject({ ok: true, violations: [] })
    })

    test.each([
      [
        'a second accept',
        (i) => i.settlements.push(range(0, 1, { type: 1, errorCode: 0, at: 30 })),
        'an offset lacks exactly one broker accept',
      ],
      [
        'a settlement after the accept',
        (i) => i.settlements.push(range(0, 1, { type: 2, errorCode: 0, at: 30 })),
        'an offset was settled again after its accept',
      ],
      [
        'a failed acknowledgement counted as a settlement',
        (i) => (i.settlements[0].errorCode = 9),
        'a failed acknowledgement counted as a settlement',
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
          i.settlements.push(range(0, 1, { type: 2, errorCode: 0, at: 15 }))
          i.settled.push(range(0, 1, { topic: 'share', type: 'release', error: null, at: 16 }))
        },
        'deliveries differ from one plus the releases',
      ],
      [
        'a foreign topic',
        (i) => (i.acquisitions[0].topicId = 'X'),
        'an acquisition names a foreign topic',
      ],
    ])('rejects %s', (_name, change, label) => {
      expect(check(change).violations).toContain(label)
    })

    test('requires the close to accept the handled prefix and release the rest', () => {
      const closed = (change = () => {}) => {
        const input = build()
        input.closures = [{ at: 15, end: 18 }]
        input.settlements.forEach((s) => (s.at = 16))
        change(input)
        return ShareOracle.check(input)
      }
      expect(closed()).toMatchObject({ ok: true })
      expect(
        closed((i) => {
          i.handled.forEach((e) => (e[4] = null))
        }).violations
      ).toContain('the close did not accept the handled prefix and release the rest')
      expect(closed((i) => i.settlements.forEach((s) => (s.at = 40))).violations).toContain(
        'an offset was unsettled when its close ended'
      )
    })
  })
})

describe('telemetry profile ordinary work', () => {
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
    const send = jest.fn(async () => [])
    const work = new OrdinaryWork({
      kind: 'producer',
      client: { send },
      stem: STEM,
      topic: 't',
      ctl: 'c',
      clock: { now: () => now },
      sleep: (ms) =>
        new Promise((resolve) =>
          setImmediate(() => {
            now += ms
            resolve()
          })
        ),
      slots: [1000, 1010, 1020],
    })
    work.running = true
    work.paused = true
    work.corpus()
    for (let i = 0; i < 20; i++) await turn()
    expect(send).not.toHaveBeenCalled()
    work.paused = false
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
  test('forwards bounded raw input and reduces the real receipts to replayable facts', async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'decoder-spec-'))
    try {
      const body = { parse: 'ok', metrics: [] }
      fs.writeFileSync(
        path.join(directory, 'decoder-123.json'),
        JSON.stringify({
          code: 0,
          signal: null,
          line: '4.3.1',
          rawSha256: 'ab'.repeat(32),
          stdout: `${JSON.stringify({ verified: true, line: '4.3.1' })}\n${JSON.stringify(body)}\n`,
          stderr: '',
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
      await expect(decoder.decode('admin', Buffer.alloc(4097).toString('base64'))).rejects.toThrow(
        'outside its bound'
      )
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
        },
      ])
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
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

    const tainted = natural()
    tainted.say('producer', { type: 'receipt' })
    tainted.processes.taint('producer')
    expect(tainted.child.kill).toHaveBeenCalledWith('SIGKILL')
    tainted.child.emit('close', null, 'SIGKILL')
    await expect(tainted.processes.join('producer')).resolves.toMatchObject({ killed: true })
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
      kind: 'producer',
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

  test('fails the stage when ordinary work beside the fault fails', async () => {
    const current = { id: 'producer-lc-2', observer: observerOf([]) }
    const tap = {
      retire: () => ({ segment: current.id, ordinal: 3, declaredAt: 10, hit: Promise.resolve(15) }),
    }
    const { worker, ordinary } = make({ current, tap })
    ordinary.core.mockRejectedValueOnce(new Error('core failed'))
    await expect(worker.retire({ tick: 3 })).rejects.toThrow('core failed')
  })

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
    const claim = await worker.confirm({ tick: 1, index: 2, selection: '' })
    expect(claim).toMatchObject({ selection: '', get: 'r2', proof: null })
    worker.owed.add('retire')
    const none = make({
      current,
      tap: { telemetry: () => [{ segment: current.id, ordinal: 3 }] },
      sleep: async () => {},
    })
    none.worker.owed.add('retire')
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
        const last = sent.filter((m) => m.kind === kind && m.type === 'stage').at(-1)
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
      ['lifecycle', 'retire', 'confirm'].flatMap((stage) => KINDS.map((kind) => `${kind}:${stage}`))
    )
    expect(fixture.rotate).toHaveBeenCalledTimes(35)
    expect(fixture.rotate.mock.calls[0]).toEqual([1, 'com.example.'])
    expect(fixture.rotate.mock.calls[1]).toEqual([2, ''])
    const produce = calls.filter((c) => Array.isArray(c))
    expect(produce).toHaveLength(2 * 100)
    expect(produce[0][3] - timeline.epochAt).toBeGreaterThanOrEqual(60000)
    expect(churn.rotations).toHaveLength(35)
    expect(churn.stages).toHaveLength(KINDS.length * (35 + 11 + 5))
    expect(timeline.setupMs).toBe(0)
    expect(processes.sent.filter((m) => m.type === 'corpus')).toHaveLength(KINDS.length)
    expect(processes.sent.filter((m) => m.type === 'finish')).toHaveLength(KINDS.length)
    expect(timeline.boundaryAt - timeline.epochAt).toBeGreaterThanOrEqual(360000)
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
  })

  test('refuses a setup over its budget', async () => {
    const { churn, advance, clock } = make()
    const t0 = clock.now()
    advance(48001)
    await expect(
      churn.measure({ t0 }, new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 }))
    ).rejects.toThrow('The setup exceeded its budget')
  })

  test('restores owned resources after every failure stage and keeps every failure', async () => {
    const stage = async (over, expected) => {
      const made = make(over)
      const failure = await made.churn.run().catch((e) => e)
      expect(made.fixture.close).toHaveBeenCalledTimes(1)
      expect(made.decoder.close).toHaveBeenCalledTimes(1)
      expect(failure).toBeInstanceOf(AggregateError)
      expect(failure.errors.map((e) => e.message)).toEqual(expected)
      expect(() => made.churn.guard()).toThrow()
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
    for (const kind of KINDS) expect(config.processes.join).toHaveBeenCalledWith(kind)
  })

  test('preserves a falsy primary failure and stops later work after it', async () => {
    const made = make({
      // eslint-disable-next-line prefer-promise-reject-errors
      fixture: { open: jest.fn(async () => Promise.reject(undefined)) },
    })
    await expect(made.churn.run()).rejects.toBeUndefined()
    expect(made.fixture.close).toHaveBeenCalledTimes(1)
    expect(made.decoder.close).toHaveBeenCalledTimes(1)
    expect(() => made.churn.guard()).toThrow()
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
      expect(() => verifyChurnTransition(input)).toThrow()
      input.claim.proof = null
      expect(() => verifyChurnTransition(input)).not.toThrow()
    })
  })

  describe('custom identity fence', () => {
    const slot = 'com.example.soak.producer.000'
    const proof = (variant) => ({
      decoded: {
        decoded: {
          metrics: [{ name: slot, points: [{ attrs: [['variant', 'string', variant]] }] }],
        },
      },
    })
    const base = (variant, writeAt) => ({
      facts: { replacements: [{ at: 1000, addedVariant: 'b' }] },
      bundles: { 'producer-lc-1': { proofs: new Map([['r2', proof(variant)]]) } },
      transcripts: {
        'producer-lc-1': {
          records: [
            getRec(1, 1, {
              response: { ...getRec(1, 1).response, requestedMetrics: ['com.example.'] },
            }),
            pushRec(2, 1, { writeAt }),
          ],
        },
      },
    })

    test('requires the current variant once the fence has settled', () => {
      const ok = base('b', 5000)
      expect(verifyChurnFence('producer', ok.facts, ok.bundles, ok.transcripts)).toBe(1)
      const stale = base('a', 5000)
      expect(() =>
        verifyChurnFence('producer', stale.facts, stale.bundles, stale.transcripts)
      ).toThrow('A retired custom identity was exported after its fence')
    })

    test('allows one frozen old sample inside the fence window and treats the original variant before any swap', () => {
      const frozen = base('a', 2400)
      expect(verifyChurnFence('producer', frozen.facts, frozen.bundles, frozen.transcripts)).toBe(1)
      const before = base('a', 500)
      expect(verifyChurnFence('producer', before.facts, before.bundles, before.transcripts)).toBe(1)
    })
  })
})
