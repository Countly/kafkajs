const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { EventEmitter } = require('node:events')
const { AsyncLocalStorage } = require('node:async_hooks')
const { buildConfig, Supervisor, workloadProblems } = require('../finalSoak')
const {
  MonotonicClock,
  planSchedule,
  Scheduler,
  harnessWait,
  harnessBound,
  clearHarnessTimeout,
  liveHarnessTimers,
  realSpan,
} = require('../finalSoakScheduler')
const {
  sha256,
  ChildRuntime,
  ClientRig,
  ConsumerWorker,
  ShareWorker,
  ReaderWorker,
  DecoderWorker,
  WireLog,
  ClientLog,
  createWorker,
  child,
} = require('../finalSoakWorkers')

const matrix = JSON.parse(fs.readFileSync(path.join(__dirname, '../finalSoakMatrix.json'), 'utf8'))
const TREE = 'b'.repeat(40)
const RUN = '0123456789ab'
const IDS = {
  A: 'AAAAAAAAAAAAAAAAAAAAAA',
  B: 'BBBBBBBBBBBBBBBBBBBBBB',
  C: 'CCCCCCCCCCCCCCCCCCCCCC',
}
const config = buildConfig(matrix, { mode: 'smoke', runId: RUN, treeSha: TREE, brokers: ['k:1'] })
const tick = () => new Promise(setImmediate)
/** @returns {Promise<unknown>} the promise's value, or `{ hung: true }` after `ms` */
const within = (promise, ms) =>
  Promise.race([promise, new Promise((resolve) => setTimeout(resolve, ms, { hung: true }))])

/** A clock that jumps to the time it is asked to sleep until. */
const jumpClock = (start = 0) => {
  const clock = {
    t: start,
    now: () => clock.t,
    sleepUntil: async (at) => {
      clock.t = Math.max(clock.t, at)
    },
  }
  return clock
}

/**
 * The real monotonic clock, except that a sleep on an aborted signal is counted and, past fifty,
 * throws: a loop that spins on one would otherwise starve the event loop and hang the test run.
 */
const boundedClock = () => {
  const clock = new MonotonicClock()
  const sleep = clock.sleepUntil.bind(clock)
  clock.abortedSleeps = 0
  clock.sleepUntil = (at, signal) => {
    if (signal?.aborted && ++clock.abortedSleeps > 50)
      throw new Error('the loop spins on an aborted sleep')
    return sleep(at, signal)
  }
  return clock
}

/** @returns {object} the runtime surface a worker of one config client slot reads and writes */
const workerRuntime = (slotId, clock = new MonotonicClock(), observer = {}) => {
  observer = { forget() {}, ...observer }
  const slot = config.clients.find((c) => c.slot === slotId)
  const slots = config.clients.filter((c) => c.process === slot.process)
  const runtime = {
    slot: slot.process,
    config,
    matrix,
    process: config.processes.find((p) => p.slot === slot.process),
    slots,
    clock,
    context: new AsyncLocalStorage(),
    commits: new AsyncLocalStorage(),
    topicIds: new Map([
      [config.topics.main, IDS.A],
      [config.topics.transactions, IDS.B],
    ]),
    ids: 0,
    records: [],
    problems: [],
    observer,
    nextId: () => `${slot.process}.${runtime.ids++}`,
    emit: async (record) => runtime.records.push(record) && true,
    diagnose: async (message) => runtime.records.push({ kind: 'diagnostic', message }),
    problem: async (message) => runtime.problems.push(message),
    failed: (call, error) => ChildRuntime.prototype.failed.call(runtime, call, error),
    forget: (client) => observer.forget(client),
    request: async () => {},
  }
  return runtime
}

/** A client whose public calls are scripted; every ClientRig built after this returns it. */
const useClient = (object) =>
  jest.spyOn(ClientRig.prototype, 'construct').mockImplementation(async function () {
    this.object = object
    return object
  })

const started = []
afterEach(async () => {
  jest.restoreAllMocks()
  for (const worker of started.splice(0)) await worker.disconnect().catch(() => {})
})

describe('final soak producer shutdown', () => {
  test.each([
    ['c08', 'idempotent'],
    ['c09', 'transactional'],
    ['c11', 'telemetry'],
  ])(
    'the %s (%s) producer loop ends when the producer closes, and the close never waits on the loop',
    async (slotId) => {
      const clock = boundedClock()
      const events = []
      useClient({
        connect: async () => {},
        disconnect: async () => {
          events.push('close-start')
          await tick()
          events.push('close-end')
        },
        send: async () => [{ baseOffset: '1' }],
        transaction: async () => ({ send: async () => [{ baseOffset: '1' }] }),
      })
      const runtime = workerRuntime(slotId, clock)
      const worker = createWorker(runtime)
      started.push(worker)
      await worker.begin()
      await new Promise((resolve) => setTimeout(resolve, 20))
      await worker.disconnect()
      expect(clock.abortedSleeps).toBeLessThanOrEqual(1)
      expect(events).toEqual(['close-start', 'close-end'])
      expect(runtime.problems).toEqual([])
      expect(runtime.records.filter((r) => r.kind === 'diagnostic')).toEqual([])
    }
  )

  test('a send in flight when the producer closes settles and is journaled before the close starts', async () => {
    let release
    const events = []
    useClient({
      connect: async () => {},
      disconnect: async () => events.push('close'),
      send: () =>
        release
          ? Promise.resolve([{ baseOffset: '8' }])
          : new Promise((resolve) => (release = () => resolve([{ baseOffset: '7' }]))),
    })
    const clock = boundedClock()
    const runtime = workerRuntime('c08', clock)
    const worker = createWorker(runtime)
    started.push(worker)
    worker.active = true
    await worker.begin()
    await tick()
    const closing = worker.disconnect()
    await tick()
    expect(events).toEqual([])
    release()
    await closing
    expect(clock.abortedSleeps).toBeLessThanOrEqual(1)
    const acks = runtime.records.filter((r) => r.stage === 'ack')
    expect(acks).toEqual([expect.objectContaining({ outcome: 'acknowledged', offset: '7' })])
    expect(events).toEqual(['close'])
  })

  test('a producer loop that fails is reported as a problem of the run, not only a note', async () => {
    useClient({
      connect: async () => {},
      disconnect: async () => {},
      send: async () => {
        throw new Error('loop broke')
      },
    })
    const runtime = workerRuntime('c08', boundedClock())
    runtime.topicIds = null
    const worker = createWorker(runtime)
    started.push(worker)
    worker.active = true
    await worker.begin()
    await new Promise((resolve) => setTimeout(resolve, 20))
    await worker.disconnect()
    expect(runtime.problems).toEqual([expect.stringContaining('producer loop failed')])
  })

  test('a disconnect that fails is a problem of the run', async () => {
    useClient({
      connect: async () => {},
      disconnect: async () => {
        throw new Error('close failed')
      },
    })
    const runtime = workerRuntime('c08', boundedClock())
    const worker = createWorker(runtime)
    started.push(worker)
    await worker.begin()
    await worker.disconnect()
    expect(runtime.problems).toEqual([expect.stringContaining('close failed')])
  })

  test('probe sends stop at the execution deadline and the reply says so', async () => {
    const sent = []
    useClient({
      connect: async () => {},
      send: async ({ topic }) => {
        sent.push(topic)
        t += 400
        return [{ baseOffset: '1' }]
      },
    })
    const clock = boundedClock()
    let t = 0
    clock.now = () => t
    const runtime = workerRuntime('c08', clock)
    const worker = createWorker(runtime)
    started.push(worker)
    await worker.begin()
    const reply = await worker.onProbe({
      id: 'p',
      execution: 'h00t0',
      topic: config.topics.main,
      partitions: 1,
      count: 10,
      deadline: 1000,
    })
    expect(sent).toHaveLength(3)
    expect(reply).toEqual({ offered: 3, acknowledged: 3, stopped: true })
    await worker.disconnect()
  })

  test('a producer whose offers were closed at the core checker capacity makes no offer and journals why', async () => {
    const sent = []
    useClient({ connect: async () => {}, send: async () => sent.push(1) && [{ baseOffset: '1' }] })
    const runtime = workerRuntime('c08', boundedClock())
    const worker = createWorker(runtime)
    started.push(worker)
    await worker.begin()
    await worker.perform({ type: 'close-offers', id: 'close-offers' })
    const reply = await worker.onProbe({
      id: 'p',
      execution: 'h00t0',
      topic: config.topics.main,
      partitions: 1,
      count: 2,
    })
    expect(reply.acknowledged).toBe(0)
    expect(sent).toEqual([])
    expect(runtime.records.filter((r) => r.stage === 'offered')).toEqual([])
    expect(runtime.records.filter((r) => r.kind === 'diagnostic').map((r) => r.message)).toEqual([
      `offer to ${config.topics.main} not made: offers are closed at the core checker's capacity`,
      `offer to ${config.topics.main} not made: offers are closed at the core checker's capacity`,
    ])
    await worker.disconnect()
  })

  test('a probe whose deadline passes while its offered record is journaled is never sent, is recorded rejected, and the probe stops', async () => {
    const sent = []
    useClient({
      connect: async () => {},
      send: async () => sent.push(1) && [{ baseOffset: '1' }],
    })
    const clock = boundedClock()
    let t = 0
    clock.now = () => t
    const runtime = workerRuntime('c08', clock)
    const emit = runtime.emit
    runtime.emit = async (record) => {
      if (record.stage === 'offered') t = 1000
      return emit(record)
    }
    const worker = createWorker(runtime)
    started.push(worker)
    await worker.begin()
    const reply = await worker.onProbe({
      id: 'p',
      execution: 'h00t0',
      topic: config.topics.main,
      partitions: 1,
      count: 3,
      deadline: 1000,
    })
    expect(sent).toEqual([])
    expect(reply).toEqual({ offered: 1, acknowledged: 0, stopped: true })
    expect(runtime.records.filter((r) => r.stage === 'ack')).toEqual([
      expect.objectContaining({
        outcome: 'rejected',
        offset: null,
        errorType: 'ExecutionDeadline',
        execution: 'h00t0',
      }),
    ])
    await worker.disconnect()
  })

  test('a probe send still in flight at the execution deadline is recorded unknown, the probe answers by the deadline, and the late settlement is awaited before idle', async () => {
    let release = null
    useClient({
      connect: async () => {},
      disconnect: async () => {},
      send: () => new Promise((resolve) => (release = () => resolve([{ baseOffset: '9' }]))),
    })
    const clock = new MonotonicClock()
    const runtime = workerRuntime('c08', clock)
    const worker = createWorker(runtime)
    started.push(worker)
    await worker.begin()
    const deadline = clock.now() + 100
    const reply = await within(
      worker.onProbe({
        id: 'p',
        execution: 'h00t0',
        topic: config.topics.main,
        partitions: 1,
        count: 3,
        deadline,
      }),
      2000
    )
    expect(reply).toEqual({ offered: 1, acknowledged: 0, stopped: true })
    expect(clock.now()).toBeLessThan(deadline + 1000)
    const acks = () => runtime.records.filter((r) => r.stage === 'ack')
    expect(acks()).toEqual([
      expect.objectContaining({
        outcome: 'unknown',
        offset: null,
        errorType: 'ExecutionDeadline',
        execution: 'h00t0',
      }),
    ])
    let idle = false
    const settled = worker.onIdle().then(() => (idle = true))
    await tick()
    expect(idle).toBe(false)
    release()
    await settled
    expect(acks()).toHaveLength(1)
    expect(runtime.records).toContainEqual({
      kind: 'diagnostic',
      message: expect.stringMatching(/^probe send c08\.\d+ settled after the h00t0 deadline: ok$/),
    })
  })

  test('a recovery send counts only when the broker acknowledged it', async () => {
    let outcome = 'offset'
    useClient({
      connect: async () => {},
      send: async () => (outcome === 'offset' ? [{ baseOffset: '4' }] : [{}]),
    })
    const runtime = workerRuntime('c08', boundedClock())
    const worker = createWorker(runtime)
    started.push(worker)
    await worker.begin()
    expect(await worker.onRecovered({ id: 'h00o' })).toEqual({ ok: true })
    outcome = 'none'
    expect(await worker.onRecovered({ id: 'h00o' })).toEqual({ ok: false })
    await worker.disconnect()
  })
})

/**
 * @param {{ mode?: string }} [options]
 * @returns {{ s: Supervisor, out: string, remove: () => void }} a supervisor with no cluster
 */
const supervisor = ({ mode = 'smoke' } = {}) => {
  const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-run-'))
  const s = new Supervisor({
    mode,
    matrix: 'scripts/soak/finalSoakMatrix.json',
    expectSha: TREE,
    brokers: ['k:1'],
    out,
  })
  s.runtime = { ids: 0, nextId: () => `p15.${s.runtime.ids++}` }
  s.emit = async () => 'p15.1'
  s.collector = { note: async () => true, failures: [], records: 0 }
  return { s, out, remove: () => fs.rmSync(out, { recursive: true, force: true }) }
}

/** A child process that records what it is sent. */
const fakeChild = () => {
  const child = new EventEmitter()
  child.connected = true
  child.sent = []
  child.exitCode = null
  child.signalCode = null
  child.send = (message, callback) => {
    child.sent.push(message)
    callback?.(null)
    return true
  }
  child.kill = jest.fn()
  child.on('exit', () => setImmediate(() => child.emit('close')))
  return child
}

describe('final soak supervisor commands', () => {
  test('a direct admin call of the supervisor that never answers fails readiness and the settled ends by its bound', async () => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      const hung = () => new Promise(() => {})
      s.admin = { fetchTopicMetadata: hung, fetchTopicOffsets: hung }
      const main = config.topics.main
      await expect(within(s.ready(main, IDS.A, 1, 1000), 2000)).rejects.toThrow(
        `${main} has no ready metadata for ${IDS.A} by the deadline`
      )
      await expect(within(s.settledEnds(1000, [main]), 2000)).rejects.toThrow(
        `fetchTopicOffsets of ${main} did not answer within 30000 ms`
      )
    } finally {
      remove()
    }
  })

  test('offers stop at the core checker capacity: the supervisor journals the error, aborts and closes offers in every process once', () => {
    const { s, remove } = supervisor()
    try {
      const capacity = matrix.traffic.maxOffers.value
      expect(s.offerCapacity).toBe(capacity)
      s.offerCapacity = 3
      const sent = []
      s.children = new Map([
        ['p01', fakeChild()],
        ['p02', fakeChild()],
      ])
      s.command = async (slot, step) => {
        sent.push([slot, step.type])
        return { reply: null, error: null }
      }
      const offered = { kind: 'identity', stage: 'offered' }
      s.count(offered)
      s.count({ kind: 'identity', stage: 'ack' })
      s.count(offered)
      expect(s.aborted).toBeNull()
      expect(sent).toEqual([])
      s.count(offered)
      const reason =
        "offers reached the core checker's capacity of 3 (traffic.maxOffers); no process makes another"
      expect(s.problems).toEqual([reason])
      expect(s.aborted).toBe(reason)
      expect(sent).toEqual([
        ['p01', 'close-offers'],
        ['p02', 'close-offers'],
      ])
      s.count(offered)
      expect(sent).toHaveLength(2)
      expect(s.problems).toEqual([reason])
    } finally {
      remove()
    }
  })

  test('a command whose child exits after receiving it settles with an error and fails the run', async () => {
    const { s, remove } = supervisor()
    try {
      const child = fakeChild()
      s.children.set('p03', child)
      s.watch('p03', child)
      const pending = s.command('p03', { type: 'periodic', id: 'k1', slot: 1 })
      child.emit('exit', 1, null)
      await expect(pending).resolves.toMatchObject({
        error: expect.stringContaining('p03'),
        reply: null,
      })
      expect(s.waiting.size).toBe(0)
      expect(s.problems).toEqual([expect.stringContaining('p03')])
      expect(s.aborted).toContain('p03')
    } finally {
      remove()
    }
  })

  test('a command no child answers settles at its budget and fails the run', async () => {
    const { s, remove } = supervisor()
    try {
      const child = fakeChild()
      s.children.set('p03', child)
      s.budget = () => 30
      const reply = await within(s.command('p03', { type: 'periodic', id: 'k1', slot: 1 }), 2000)
      expect(reply).toMatchObject({ error: expect.stringContaining('did not answer'), reply: null })
      expect(s.waiting.size).toBe(0)
      expect(s.problems).toEqual([expect.stringContaining('p03')])
      expect(s.aborted).toContain('p03')
    } finally {
      remove()
    }
  })

  test('an expected exit after the disconnect command is not a failure', async () => {
    const { s, remove } = supervisor()
    try {
      const child = fakeChild()
      s.children.set('p03', child)
      s.watch('p03', child)
      const pending = s.command('p03', { type: 'disconnect', id: 'disconnect' })
      s.receive('p03', { type: 'done', id: s.commands, error: null, reply: null })
      await pending
      child.emit('disconnect')
      child.emit('exit', 0, null)
      expect(s.problems).toEqual([])
      expect(s.aborted).toBeNull()
    } finally {
      remove()
    }
  })

  test('a child that answers the disconnect but never exits is killed after the teardown allowance and the run says so', async () => {
    const { s, remove } = supervisor()
    try {
      const child = fakeChild()
      child.kill = jest.fn(() => child.emit('exit', null, 'SIGKILL'))
      s.children.set('p03', child)
      s.watch('p03', child)
      s.exited = new Promise((resolve) => (s.allExited = resolve))
      s.teardownAllowanceMs = () => 30
      await s.untilExited()
      expect(child.kill).toHaveBeenCalledWith('SIGKILL')
      expect(s.forced).toEqual(['p03'])
      expect(s.problems).toEqual([expect.stringContaining('p03')])
    } finally {
      remove()
    }
  })

  test('children that exit within the teardown allowance are left alone', async () => {
    const { s, remove } = supervisor()
    try {
      const child = fakeChild()
      s.children.set('p03', child)
      s.watch('p03', child)
      s.exited = new Promise((resolve) => (s.allExited = resolve))
      s.teardownAllowanceMs = () => 5000
      s.leaving.add('p03')
      setImmediate(() => child.emit('exit', 0, null))
      await s.untilExited()
      expect(child.kill).not.toHaveBeenCalled()
      expect(s.forced).toEqual([])
    } finally {
      remove()
    }
  })

  test('a child whose spawn failed reaches its terminal state at close without an exit record, and the teardown does not wait for it', async () => {
    const { s, remove } = supervisor()
    try {
      const records = []
      s.emit = async (record) => records.push(record) && 'p15.1'
      const child = fakeChild()
      child.connected = false
      s.children.set('p03', child)
      s.exited = new Promise((resolve) => (s.allExited = resolve))
      s.watch('p03', child)
      child.emit('error', new Error('spawn /usr/bin/node EAGAIN'))
      child.emit('close', -11, null)
      expect(await within(s.untilExited(), 2000)).toBeUndefined()
      expect(records.filter((r) => r.kind === 'process')).toEqual([])
      expect(s.problems).toEqual(['process p03 spawn /usr/bin/node EAGAIN before its disconnect'])
      expect(s.forced).toEqual([])
    } finally {
      remove()
    }
  })

  test('a child that does not end after its kill is named and the teardown still ends', async () => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      const child = fakeChild()
      child.kill = jest.fn(() => {
        throw new Error('EPERM')
      })
      s.children.set('p03', child)
      s.watch('p03', child)
      s.exited = new Promise((resolve) => (s.allExited = resolve))
      s.teardownAllowanceMs = () => 30
      expect(await within(s.untilExited(), 2000)).toBeUndefined()
      expect(s.forced).toEqual(['p03'])
      expect(s.problems).toEqual([
        'process p03 did not exit within 30 ms of its disconnect; killed',
        'process p03 could not be killed: EPERM',
        'process p03 did not end 10000 ms after its kill',
      ])
      expect(s.incomplete).toBe('process p03 did not end')
    } finally {
      remove()
    }
  })

  /** A child process run against a fake channel and runtime. */
  const childProcess = ({ start = async () => {} } = {}) => {
    const proc = new EventEmitter()
    proc.connected = true
    proc.sent = []
    proc.send = (message) => proc.sent.push(message)
    proc.disconnect = () => {
      proc.connected = false
      proc.emit('disconnect')
    }
    const runtimes = []
    class Runtime {
      constructor(options) {
        Object.assign(this, options, { topicIds: new Map(), orphans: 0 })
        this.channel = { pendingBytes: 0 }
        runtimes.push(this)
      }

      start() {
        return start()
      }

      async orphaned() {
        this.orphans++
      }

      async handle({ id }) {
        this.notify({ type: 'done', id, error: null, reply: null })
      }
    }
    child({ child: 'p03', out: '/nonexistent' }, { proc, Runtime })
    proc.emit('message', { type: 'init', config, matrix, originMs: 0, topicIds: [] })
    return { proc, runtimes }
  }

  test('when the supervisor goes away, a handler waiting on a readback and records awaiting their acknowledgement are settled', async () => {
    const { proc, runtimes } = childProcess()
    await tick()
    const [runtime] = runtimes
    const readback = runtime.request({ type: 'readback', key: {} })
    let refused = null
    runtime.send({ type: 'record', record: { seq: 0 } }, (r) => (refused = r))
    expect(proc.sent.map((m) => m.type)).toEqual(['readback', 'record'])
    proc.disconnect()
    expect(await within(readback, 2000)).toBeUndefined()
    expect(refused).toBe(true)
    await tick()
    expect(runtime.orphans).toBe(1)
    expect(await within(runtime.request({ type: 'readback' }), 2000)).toBeUndefined()
    expect(proc.sent).toHaveLength(2)
  })

  test('a child whose runtime failed to start answers each command with the failure', async () => {
    const { proc } = childProcess({ start: async () => Promise.reject(new Error('no broker')) })
    proc.emit('message', { type: 'command', id: 4, step: { type: 'active', id: 'h00a' } })
    await tick()
    await tick()
    expect(proc.sent).toEqual([
      { type: 'done', id: 4, error: 'start failed: no broker', reply: null },
    ])
  })

  test('the day does not start when any process failed to start, and the failure is recorded', async () => {
    const { s, remove } = supervisor()
    try {
      for (const slot of ['p05', 'p06']) s.children.set(slot, fakeChild())
      s.command = async (slot) => ({ error: slot === 'p05' ? 'no assignment' : null, reply: null })
      expect(await s.start()).toBe(false)
      expect(s.problems).toEqual([expect.stringContaining('p05')])
      expect(s.aborted).toContain('readiness')
    } finally {
      remove()
    }
  })

  test('a late or failed step, a reported problem and a refused record each become a recorded problem', async () => {
    const { s, remove } = supervisor()
    try {
      const child = fakeChild()
      s.children.set('p08', child)
      s.stepOutcome({ id: 'h00r0', type: 'execution', outcome: 'ok', error: null })
      expect(s.problems).toEqual([])
      s.stepOutcome({ id: 'h00r0', type: 'execution', outcome: 'late', error: null })
      expect(s.aborted).toBeNull()
      s.stepOutcome({ id: 'h00t0', type: 'execution', outcome: 'error', error: 'boom' })
      expect(s.aborted).toContain('boom')
      s.receive('p08', { type: 'problem', message: 'producer loop failed: x' })
      expect(s.problems).toEqual([
        expect.stringContaining('h00r0'),
        expect.stringContaining('boom'),
        expect.stringContaining('producer loop failed'),
      ])
      s.collector.append = async () => false
      s.receive('p08', { type: 'record', record: { seq: 0, kind: 'diagnostic' } })
      await tick()
      expect(child.sent.at(-1)).toEqual({ type: 'ack', seq: 0, refused: true })
      expect(s.result()).toMatchObject({
        problems: s.problems,
        forced: [],
        incomplete: null,
        failures: [],
      })
    } finally {
      remove()
    }
  })
})

describe('final soak scheduled work', () => {
  const outageStep = { id: 'h00o', node: 1, stop: 'clean', downMs: 2000, deadline: 20000 }
  const brokers = [1, 2, 3].map((nodeId) => ({ nodeId }))
  const partitions = (isr) =>
    config.topics.recreate.reduce(
      (list, topic) => [
        ...list,
        {
          name: topic,
          topicId: IDS.A,
          partitions: [0, 1].map((id) => ({ partitionId: id, leader: 1, isr })),
        },
      ],
      [
        {
          name: config.topics.main,
          topicId: IDS.A,
          partitions: [{ partitionId: 0, leader: 1, isr }],
        },
        {
          name: config.topics.transactions,
          topicId: IDS.B,
          partitions: [{ partitionId: 0, leader: 1, isr }],
        },
      ]
    )

  /** An outage with scripted docker, metadata and recovery witnesses. */
  const outage = async ({ failing = null, isr = [1, 2, 3], topics = true, witness = true }) => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      let running = true
      s.docker = async (args) => {
        if (args[1] === failing) throw new Error(`docker ${failing} failed`)
        if (['stop', 'kill'].includes(args[1])) running = false
        if (args[1] === 'start') running = true
        if (args[0] === 'inspect') return running ? '4242' : '0'
        return 'container-1'
      }
      s.admin = {
        describeCluster: async () => ({ brokers }),
        fetchTopicMetadata: async () => ({ topics: topics ? partitions(isr) : [] }),
      }
      s.send = async () => []
      s.command = async () => ({ reply: { ok: witness }, error: null })
      await s.outage(outageStep, ['p01'])
      return s.problems
    } finally {
      remove()
    }
  }

  test('an outage with every stop, start, ISR and recovery confirmed succeeds', async () => {
    await expect(outage({})).resolves.toEqual([])
  })

  test("an outage records the configured down interval and the container's own stop and start instants on the run clock", async () => {
    const { s, remove } = supervisor()
    const WALL = Date.parse('2026-11-18T16:00:00.000Z')
    try {
      s.clock = jumpClock(500000)
      jest.spyOn(Date, 'now').mockImplementation(() => WALL + s.clock.t)
      const iso = (t) => new Date(WALL + t).toISOString().replace('Z', '123456Z')
      let pid = 4242
      let startedAt = iso(1000)
      let finishedAt = '0001-01-01T00:00:00Z'
      s.docker = async (args) => {
        if (['stop', 'kill'].includes(args[1])) {
          s.clock.t += 3000
          pid = 0
          finishedAt = iso(s.clock.t - 400)
        }
        if (args[1] === 'start') {
          pid = 5151
          startedAt = iso(s.clock.t + 250)
          s.clock.t += 300
        }
        if (args[0] === 'inspect') return `${pid} ${startedAt} ${finishedAt}`
        return 'container-1'
      }
      s.admin = {
        describeCluster: async () => ({ brokers }),
        fetchTopicMetadata: async () => ({ topics: partitions([1, 2, 3]) }),
      }
      s.send = async () => []
      s.command = async () => ({ reply: { ok: true }, error: null })
      const records = []
      s.emit = async (record) => records.push(record)
      await s.outage(outageStep, ['p01'])
      const states = records.filter((r) => r.kind === 'broker-state')
      expect(states.map((r) => [r.event, r.pid, r.downMs, r.processAt])).toEqual([
        ['down', 0, 2000, 502600],
        ['ready', 5151, 2000, 505250],
        ['isr', 5151, 2000, 505250],
      ])
      expect(states[0].processTime).toBe(iso(502600))
      expect(states[1].processTime).toBe(iso(505250))
      expect(states[1].processAt - states[0].processAt).toBeGreaterThanOrEqual(outageStep.downMs)
    } finally {
      remove()
    }
  })

  test("an outage journals each run partition's leader and in-sync replicas in its own record citing the broker state", async () => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      let running = true
      s.docker = async (args) => {
        if (['stop', 'kill'].includes(args[1])) running = false
        if (args[1] === 'start') running = true
        if (args[0] === 'inspect') return running ? '4242' : '0'
        return 'container-1'
      }
      s.admin = {
        describeCluster: async () => ({ brokers }),
        fetchTopicMetadata: async () => ({ topics: partitions([1, 2, 3]) }),
      }
      s.send = async () => []
      s.command = async () => ({ reply: { ok: true }, error: null })
      const records = []
      s.emit = async (record) => {
        const recordId = `p15.${records.length}`
        records.push({ recordId, ...record })
        return recordId
      }
      await s.outage(outageStep, ['p01'])
      const expected = partitions([1, 2, 3]).flatMap((t) =>
        t.partitions.map((p) => ({
          key: { topic: t.name, topicId: t.topicId, partition: p.partitionId },
          leader: 1,
          isr: [1, 2, 3],
        }))
      )
      const states = records.filter((r) => r.kind === 'broker-state')
      expect(states.map((r) => [r.event, r.partitions])).toEqual([
        ['down', 6],
        ['ready', 6],
        ['isr', 6],
      ])
      for (const state of states)
        expect(
          records
            .filter((r) => r.kind === 'partition-state' && r.cause === state.recordId)
            .map(({ execution, key, leader, isr }) => ({ execution, key, leader, isr }))
        ).toEqual(expected.map((p) => ({ execution: outageStep.id, ...p })))
    } finally {
      remove()
    }
  })

  test('an outage whose container cannot tell its stop and start instants records them as unknown', async () => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      s.docker = async (args) => (args[0] === 'inspect' ? 'garbage' : 'container-1')
      s.admin = {
        describeCluster: async () => ({ brokers }),
        fetchTopicMetadata: async () => ({ topics: partitions([1, 2, 3]) }),
      }
      s.send = async () => []
      s.command = async () => ({ reply: { ok: true }, error: null })
      const records = []
      s.emit = async (record) => records.push(record)
      await expect(s.outage(outageStep, ['p01'])).rejects.toThrow(/no running process/)
      const states = records.filter((r) => r.kind === 'broker-state')
      expect(states.map((r) => [r.pid, r.processAt, r.processTime])).toEqual([
        [0, null, null],
        [0, null, null],
        [0, null, null],
      ])
    } finally {
      remove()
    }
  })

  test.each([
    ['a failed docker stop', { failing: 'stop' }, /broker-stop/],
    ['a failed docker start', { failing: 'start' }, /broker-start/],
    ['metadata that never lists the run partitions', { topics: false }, /in sync/],
    ['a partition that stays out of sync', { isr: [1, 2] }, /in sync/],
    ['a client without a recovery witness', { witness: false }, /recovery witness/],
  ])('an outage with %s fails its step', async (_, options, message) => {
    await expect(outage(options)).rejects.toThrow(message)
  })

  test('an execution that fails still ends its shape, so the stream never holds an execution that never ends', async () => {
    const { s, remove } = supervisor()
    try {
      const shapes = []
      s.shape = async (step, phase) => shapes.push(`${step.id} ${phase}`)
      s.outage = async () => {
        throw new Error('outage failed')
      }
      s.scheduler = { aborted: null }
      const step = {
        id: 'h00o0',
        type: 'execution',
        shape: 'broker-outage',
        participants: ['producer-idempotent'],
      }
      await expect(s.perform(step)).rejects.toThrow('outage failed')
      expect(shapes).toEqual(['h00o0 start', 'h00o0 end'])
    } finally {
      remove()
    }
  })

  test('a membership wave in which no process saw a paused partition transfer fails its step', async () => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      s.waves = new Map([['h00m0', [{ id: 'h00m0w0', deadline: 90000 }]]])
      s.send = async (slots, step) =>
        step.phase === 'leave'
          ? [{ reply: { transfers: 0 } }, { reply: { transfers: 1 } }, { reply: null }]
          : []
      s.sampleAll = async () => {}
      s.shape = async () => 'x'
      await expect(s.churnMembers({ id: 'h00m0' }, ['p01', 'p02', 'p07'])).rejects.toThrow(
        /paused partition.*p01/
      )
    } finally {
      remove()
    }
  })

  test('a topic execution starts no stage that is past its deadline', async () => {
    const { s, remove } = supervisor()
    try {
      const clock = jumpClock(0)
      const stages = []
      s.clock = clock
      s.createTopic = async () => stages.push('create') && IDS.A
      s.ready = async () => {}
      s.deleteTopic = async () => stages.push('delete')
      s.seal = async () => stages.push('seal')
      s.send = async (slots, step) => {
        stages.push(step.type)
        if (step.type === 'probe') clock.t += 200000
        return [{ reply: { offered: 10, acknowledged: 10 } }]
      }
      const step = {
        id: 'h00r0',
        type: 'execution',
        shape: 'regex-churn',
        deadline: 120000,
        participants: [],
      }
      await expect(s.churnTopic(step, ['p01'])).rejects.toThrow(/deadline/)
      expect(stages).toEqual(['create', 'execution', 'probe'])
      expect(clock.t).toBeLessThanOrEqual(120000 + 200000)
    } finally {
      remove()
    }
  })

  test("a topic execution drains its recipients to the topic's settled ends and fails before the delete when one falls short", async () => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      const stages = []
      const topic = `${s.config.topics.regex}-h00r0`
      s.createTopic = async () => stages.push('create') && IDS.A
      s.ready = async () => {}
      s.deleteTopic = async () => stages.push('delete')
      s.seal = async () => stages.push('seal')
      s.settledEnds = async (deadline, topics) => {
        stages.push(`ends ${topics}`)
        return { [`${topic}|0`]: '5', [`${topic}|1`]: '5' }
      }
      const drains = []
      s.send = async (slots, step) => {
        stages.push(step.type)
        if (step.type === 'drain') drains.push(step.ends)
        if (step.type === 'probe') return [{ reply: { offered: 10, acknowledged: 10 } }]
        return slots.map((slot) => ({
          reply: step.type === 'drain' ? { reached: slot !== 'p02' } : {},
        }))
      }
      const step = { id: 'h00r0', type: 'execution', shape: 'regex-churn', deadline: 120000 }
      await expect(s.churnTopic(step, ['p01', 'p02'])).rejects.toThrow(
        `h00r0: p02 did not drain ${topic} to its end`
      )
      expect(drains).toEqual([{ [`${topic}|0`]: '5', [`${topic}|1`]: '5' }])
      expect(stages).not.toContain('seal')
      expect(stages).not.toContain('delete')
    } finally {
      remove()
    }
  })

  /**
   * A topic execution whose admin, metadata and children are scripted. `probe` is the probe reply,
   * `replies` maps a stage type to each participant's reply, `admin` overrides admin calls (or is
   * a function of the recreated topic's name that returns them).
   */
  const topicExecution = async ({
    shape = 'topic-recreation',
    probe,
    replies = {},
    admin = {},
  }) => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      s.runtime = { ...s.runtime, topicIds: new Map() }
      const topic = s.config.topics.recreate[0]
      s.topicIds = [[topic, IDS.A]]
      const stages = []
      s.rig = { id: 'p15.1' }
      s.readback = async () => {}
      s.publish = async (name, id) => stages.push(`publish ${id}`)
      s.unpublish = async () => stages.push('unpublish')
      s.seal = async () => stages.push('seal')
      s.settledEnds = async () => ({})
      s.admin = {
        createTopics: async () => true,
        deleteTopics: async () => {},
        listTopics: async () => [],
        fetchTopicMetadata: async () => ({
          topics: [
            {
              topicId: IDS.B,
              partitions: [0, 1].map((partitionId) => ({ partitionId, leader: 1, isr: [1, 2] })),
            },
          ],
        }),
        ...(typeof admin === 'function' ? admin(topic) : admin),
      }
      s.send = async (slots, step) => {
        stages.push(step.type)
        if (step.type === 'probe') return [{ reply: probe ?? { offered: 10, acknowledged: 10 } }]
        return slots.map((_, i) => ({ reply: replies[step.type]?.[i] ?? {} }))
      }
      const step = { id: 'h00x0', type: 'execution', shape, topic: 0, deadline: 120000 }
      const error = await s.churnTopic(step, ['p01', 'p02']).then(
        () => null,
        (e) => e.message
      )
      return { error: error?.replace(s.config.runId, RUN) ?? null, stages }
    } finally {
      remove()
    }
  }

  test('a recreation creates the new incarnation, waits until every recipient is assigned it, then probes it', async () => {
    const { error, stages } = await topicExecution({})
    expect(error).toBeNull()
    expect(stages).toEqual([
      'probe',
      'drain',
      'seal',
      'execution',
      `publish ${IDS.B}`,
      'recreated',
      'probe',
    ])
  })

  test('a recreation whose creation gives back the old topic id fails before anything is offered to it', async () => {
    const same = {
      fetchTopicMetadata: async () => ({ topics: [{ topicId: IDS.A, partitions: [] }] }),
    }
    const { error, stages } = await topicExecution({ admin: same })
    expect(error).toBe(`creating ${config.topics.recreate[0]} gave its old topic id (error code 0)`)
    expect(stages.filter((st) => st === 'probe')).toHaveLength(1)
  })

  test('a deletion the broker refuses or never confirms fails the execution before the name is created again', async () => {
    const refused = await topicExecution({
      admin: {
        deleteTopics: async () => Promise.reject(Object.assign(new Error('no'), { code: 41 })),
      },
    })
    expect(refused.error).toBe(`deleting ${config.topics.recreate[0]} failed (error code 41)`)
    expect(refused.stages).not.toContain(`publish ${IDS.B}`)
    const listed = await topicExecution({
      admin: (topic) => ({ listTopics: async () => [topic] }),
    })
    expect(listed.error).toBe(`deleting ${config.topics.recreate[0]} left it listed (error code 0)`)
  })

  test.each([
    ['ten unacknowledged offers', { offered: 10, acknowledged: 0, stopped: false }, '0 of 10'],
    ['one fast failed send', { offered: 10, acknowledged: 9, stopped: false }, '9 of 10'],
  ])('a probe with %s fails its execution', async (_, probe, count) => {
    const { error, stages } = await topicExecution({ probe })
    expect(error).toBe(
      `h00x0: ${count} probe offers to ${config.topics.recreate[0]} were acknowledged`
    )
    expect(stages).toEqual(['probe'])
  })

  test('a recipient that is not assigned the new incarnation fails the recreation before the probe', async () => {
    const { error, stages } = await topicExecution({
      replies: { recreated: [{ assigned: true }, { assigned: false }] },
    })
    expect(error).toBe('h00x0: recreated failed in p02')
    expect(stages.at(-1)).toBe('recreated')
  })

  test('a regex execution whose recipient never accepts the new topic fails before the probe, and a deleted name is forgotten', async () => {
    const refused = await topicExecution({
      shape: 'regex-churn',
      replies: { execution: [{ accepted: false }, { accepted: true }] },
    })
    expect(refused.error).toBe('h00x0: accept failed in p01')
    expect(refused.stages).not.toContain('probe')
    const done = await topicExecution({ shape: 'regex-churn' })
    expect(done.error).toBeNull()
    expect(done.stages.slice(-2)).toEqual(['retired', 'unpublish'])
  })

  test('a membership wave works to its scheduled deadline, not to a deadline counted from a late start', async () => {
    const clock = jumpClock(1000000)
    const standing = { id: 'c01.1', object: { pause() {}, resume() {} }, slot: config.clients[0] }
    const observer = {
      ordinarySections: (client) => ({
        sections: {
          assignment:
            client === 'c01.1' ? [{ key: { topic: 't', topicId: IDS.A, partition: 0 } }] : [],
        },
      }),
    }
    const runtime = workerRuntime('c01', clock, observer)
    jest.spyOn(ConsumerWorker.prototype, 'join').mockImplementation(async function (slot) {
      return {
        id: 'c02.1',
        slot,
        object: { disconnect: async () => {} },
        call: async (event, work) => work(),
        retire: async () => {},
      }
    })
    const worker = new ConsumerWorker(runtime)
    worker.standing.push(standing)
    const step = { execution: 'h00m0w0', wave: 1, phase: 'leave', waveDeadline: 1030000 }
    const reply = await worker.onMembershipChurn(step)
    expect(reply).toEqual({ transfers: 0, returned: true })
    expect(clock.t).toBeLessThanOrEqual(1030000)
  })

  /**
   * A static-membership worker (c02 and its static extra c14) whose observer reports the standing
   * member's keys from `held({ joined, retired })`, and whose extras are scripted rigs that hold
   * every key and record how they retire.
   */
  const staticWave = (clock, held) => {
    const keys = [0, 1].map((partition) => ({ topic: 't', topicId: IDS.A, partition }))
    const standing = {
      id: 'c02.1',
      object: { pause() {}, resume() {} },
      slot: config.clients.find((c) => c.slot === 'c02'),
    }
    const runtime = workerRuntime('c02', clock, {
      ordinarySections: (client) => ({
        sections: {
          assignment: (client === 'c02.1' ? held({ joined: n, retired }) : keys).map((key) => ({
            key,
          })),
        },
      }),
    })
    const retired = []
    let n = 0
    jest.spyOn(ConsumerWorker.prototype, 'join').mockImplementation(async function (slot) {
      const id = `c14.${++n}`
      return {
        id,
        slot,
        object: { disconnect: async (options) => retired.push({ id, restart: options }) },
        retire: async (execution, options) => retired.push({ id, options }),
      }
    })
    jest
      .spyOn(ConsumerWorker.prototype, 'operation')
      .mockImplementation(async (rig, op, k, e, work) => work())
    const worker = new ConsumerWorker(runtime)
    worker.standing.push(standing)
    return { worker, runtime, keys, retired }
  }

  test("a static extra leaves its group explicitly at its final retirement but not at its restart, and the wave waits for the standing member's partitions", async () => {
    const clock = jumpClock(0)
    const wave = staticWave(clock, ({ joined, retired }) =>
      joined === 0 || (retired.length === 2 && clock.t >= 1000) ? wave.keys : wave.keys.slice(0, 1)
    )
    const step = {
      execution: 'h00m0w0',
      wave: 0,
      phase: 'leave',
      restart: ['classic-eager-static'],
    }
    const reply = await wave.worker.onMembershipChurn({ ...step, waveDeadline: 90000 })
    expect(wave.retired).toEqual([
      { id: 'c14.1', restart: { leaveGroup: false } },
      { id: 'c14.2', options: { leaveGroup: true } },
    ])
    expect(reply).toEqual({ transfers: 1, returned: true })
    expect(clock.t).toBeGreaterThanOrEqual(1000)
    expect(wave.runtime.records).toContainEqual({
      kind: 'diagnostic',
      message: 'h00m0w0 retires c14.2 with an explicit leave',
    })
  })

  test('a wave journals which paused keys moved to which extra and when they returned to the standing member', async () => {
    const clock = jumpClock(0)
    const wave = staticWave(clock, ({ joined, retired }) =>
      joined === 0 || (retired.length === 1 && clock.t >= 1000) ? wave.keys : []
    )
    await wave.worker.onMembershipChurn({
      execution: 'h00m0w1',
      wave: 1,
      phase: 'leave',
      waveDeadline: 90000,
    })
    const records = wave.runtime.records.filter((r) => ['wave', 'wave-key'].includes(r.kind))
    const summary = (event, member) => ({
      kind: 'wave',
      recordId: expect.any(String),
      execution: 'h00m0w1',
      event,
      client: 'c02.1',
      member,
      paused: 2,
      held: 2,
      observed: true,
    })
    const keys = (summary) =>
      wave.keys.map((key) => ({
        kind: 'wave-key',
        recordId: expect.any(String),
        execution: 'h00m0w1',
        cause: summary.recordId,
        key,
        held: true,
      }))
    expect(records).toEqual([
      summary('transfer', 'c14.1'),
      ...keys(records[0]),
      summary('return', 'c02.1'),
      ...keys(records[3]),
    ])
  })

  test('a wave whose paused keys never all returned journals what the standing member held at the deadline', async () => {
    const clock = jumpClock(0)
    const wave = staticWave(clock, ({ joined }) =>
      joined === 0 ? wave.keys : wave.keys.slice(0, 1)
    )
    await wave.worker.onMembershipChurn({
      execution: 'h00m0w1',
      wave: 1,
      phase: 'leave',
      waveDeadline: 90000,
    })
    const records = wave.runtime.records.filter((r) => r.kind === 'wave')
    expect(records.map((r) => [r.event, r.member, r.paused, r.held, r.observed])).toEqual([
      ['transfer', 'c14.1', 2, 2, true],
      ['return', 'c02.1', 2, 1, false],
    ])
    const returned = wave.runtime.records.filter(
      (r) => r.kind === 'wave-key' && r.cause === records[1].recordId
    )
    expect(returned.map((r) => [r.key, r.held])).toEqual([
      [wave.keys[0], true],
      [wave.keys[1], false],
    ])
  })

  test('a wave whose paused partitions never return to the standing member says so in its reply', async () => {
    const clock = jumpClock(0)
    const wave = staticWave(clock, ({ joined }) =>
      joined === 0 ? wave.keys : wave.keys.slice(0, 1)
    )
    const reply = await wave.worker.onMembershipChurn({
      execution: 'h00m0w1',
      wave: 1,
      phase: 'leave',
      waveDeadline: 90000,
    })
    expect(reply).toEqual({ transfers: 1, returned: false })
    expect(clock.t).toBeLessThanOrEqual(85000)
    expect(wave.runtime.records).toContainEqual({
      kind: 'diagnostic',
      message: 'h00m0w1 saw the paused partitions not return to c02.1',
    })
  })

  test('a wave admitted before its scheduled start works no longer than its budget from its actual start', async () => {
    const clock = jumpClock(1000000)
    const wave = staticWave(clock, ({ joined }) => (joined === 0 ? wave.keys : []))
    const waveMs = matrix.schedule.executions.find((e) => e.shape === 'membership-churn').waveMaxMs
    await wave.worker.onMembershipChurn({
      execution: 'h00m0w1',
      wave: 1,
      phase: 'leave',
      waveDeadline: 1000000 + 2 * waveMs,
    })
    expect(clock.t).toBeLessThanOrEqual(1000000 + waveMs)
  })

  test('a membership wave in which a paused partition did not return fails its step', async () => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      s.waves = new Map([['h00m0', [{ id: 'h00m0w0', deadline: 90000 }]]])
      s.send = async (slots, step) =>
        step.phase === 'leave'
          ? [
              { reply: { transfers: 1, returned: true } },
              { reply: { transfers: 1, returned: false } },
            ]
          : []
      s.sampleAll = async () => {}
      s.shape = async () => 'x'
      await expect(s.churnMembers({ id: 'h00m0' }, ['p02', 'p04'])).rejects.toThrow(
        'h00m0w0 saw paused partitions not return in p04'
      )
    } finally {
      remove()
    }
  })
})

describe('final soak work admission and quiet samples', () => {
  test('a work step still running past its grace is reported failed then, and stays in flight until it settles', async () => {
    const clock = jumpClock(0)
    let release = null
    const outcomes = []
    const scheduler = new Scheduler({
      clock,
      graceMs: 500,
      perform: () => new Promise((resolve) => (release = resolve)),
      onOutcome: (o) => outcomes.push(o),
    })
    await scheduler.run([{ id: 'h00r0', lane: 'work', type: 'execution', at: 0, deadline: 1000 }])
    expect(outcomes).toEqual([
      expect.objectContaining({
        id: 'h00r0',
        outcome: 'error',
        error: 'h00r0 still runs 500 ms after its deadline',
      }),
    ])
    expect(scheduler.quiescent()).toBe(false)
    release()
    await tick()
    expect(scheduler.quiescent()).toBe(true)
  })

  test('a work step due while a quiet sample runs starts only once the sample settled', async () => {
    const clock = jumpClock(0)
    const order = []
    let release = null
    const scheduler = new Scheduler({
      clock,
      perform: async (step) => {
        order.push(`${step.id} start`)
        if (step.type === 'quiet') await new Promise((resolve) => (release = resolve))
        order.push(`${step.id} end`)
      },
    })
    const running = scheduler.run([
      { id: 'h00q2', lane: 'timer', type: 'quiet', at: 0, deadline: 100 },
      { id: 'disconnect', lane: 'work', type: 'disconnect', at: 10 },
    ])
    await tick()
    await tick()
    expect(order).toEqual(['h00q2 start'])
    release()
    await running
    expect(order).toEqual(['h00q2 start', 'h00q2 end', 'disconnect start', 'disconnect end'])
  })

  test('an idle window ends only after the quiet sample running in it settled', async () => {
    const { s, remove } = supervisor()
    try {
      s.clock = jumpClock(0)
      s.runEndAt = 1000
      let settle = null
      const quiet = new Promise((resolve) => (settle = resolve))
      s.scheduler = new Scheduler({ clock: s.clock, perform: async () => {} })
      s.scheduler.quieting.add(quiet)
      quiet.then(() => s.scheduler.quieting.delete(quiet))
      const sent = []
      s.send = async (slots, step) => sent.push([step.type, s.quiesced])
      s.shape = async () => null
      const idle = s.perform({ id: 'h00i', type: 'idle', shape: 'idle', until: 500 })
      await tick()
      await tick()
      expect(s.quiesced).toBe('h00i')
      settle()
      await idle
      expect(sent).toEqual([
        ['idle', null],
        ['active', null],
      ])
      expect(s.quiesced).toBeNull()
    } finally {
      remove()
    }
  })

  /** A supervisor whose sampling is scripted; returns what each quiet step did. */
  const quiet = async ({ quiesced = 'h00i', inflight = [], aborted = null, during = () => {} }) => {
    const { s, remove } = supervisor()
    try {
      s.scheduler = { inflight: new Set(inflight) }
      s.quiesced = quiesced
      s.aborted = aborted
      const sampled = []
      s.sampleAll = async (slots, step) => {
        sampled.push(step.id)
        during(s)
      }
      const step = { id: 'h00q0', type: 'quiet', boundary: 'h00', reading: 0 }
      const error = await s.perform(step).then(
        () => null,
        (e) => e.message
      )
      return { error, sampled }
    } finally {
      remove()
    }
  }

  test('a quiet sample in an idle window with no work running is taken', async () => {
    expect(await quiet({})).toEqual({ error: null, sampled: ['h00q0'] })
  })

  test.each([
    [
      'an admin or docker operation of an earlier step is still in flight',
      { inflight: ['h00o0'] },
      'h00q0 is not a quiet sample: h00o0 is still running',
    ],
    [
      'the processes have not confirmed the idle window',
      { quiesced: null },
      'h00q0 is not a quiet sample: the processes have not confirmed idle',
    ],
    [
      'a timed-out command aborted the run',
      { aborted: 'process p03 did not answer' },
      'h00q0 is not a quiet sample: the run is aborted',
    ],
  ])('a quiet sample is refused when %s', async (_, state, message) => {
    expect(await quiet(state)).toEqual({ error: message, sampled: [] })
  })

  test('a quiet sample during which a work step began is refused after it was taken', async () => {
    const result = await quiet({ during: (s) => s.scheduler.inflight.add('h00d') })
    expect(result).toEqual({
      error: 'h00q0 is not a quiet sample: h00d is still running',
      sampled: ['h00q0'],
    })
  })

  test('a boundary whose last quiet reading starts past its deadline has a skipped heap snapshot receipt for each snapshot process and sends no snapshot command', async () => {
    const { s, remove } = supervisor()
    try {
      const notes = []
      s.collector.note = async (message, extra) => notes.push([message, extra.snapshot])
      const commands = []
      s.command = async (slot, step) => commands.push([slot, step.type])
      s.sampleAll = async (slots, step) => commands.push(['sample', step.id])
      s.quiesced = 'h00i'
      const steps = [0, 1, 2].map((reading) => ({
        id: `h00q${reading}`,
        type: 'quiet',
        lane: 'timer',
        boundary: 'h00',
        reading,
        at: reading * 10,
        deadline: 50,
      }))
      const clock = jumpClock(0)
      const outcomes = []
      const recorded = []
      s.scheduler = new Scheduler({
        clock,
        perform: async (step) => {
          await s.perform(step)
          // The first two readings run so long that the last one starts past the deadline.
          clock.t += 30
        },
        onOutcome: (o) => {
          outcomes.push(`${o.id} ${o.outcome}`)
          recorded.push(s.missedSnapshot(steps[o.id.at(-1)], o.outcome))
        },
      })
      await s.scheduler.run(steps)
      await Promise.all(recorded)
      expect(outcomes).toEqual(['h00q0 ok', 'h00q1 late', 'h00q2 late'])
      expect(commands).toEqual([
        ['sample', 'h00q0'],
        ['sample', 'h00q1'],
      ])
      const receipts = s.config.heapSnapshots.map((slot) => ({
        slot,
        boundary: 'h00',
        file: `heap/${slot}.h00.heapsnapshot`,
        at: null,
        ms: null,
        bytes: null,
        error: 'not taken: step h00q2 late',
      }))
      expect(s.heapSnapshots).toEqual(receipts)
      expect(notes).toEqual(receipts.map((r) => [`heap snapshot ${r.slot} h00 skipped`, r]))
    } finally {
      remove()
    }
  })

  test.each(['smoke', 'rehearsal', 'acceptance'])(
    'in %s mode the last quiet reading of a boundary has every subject, the seed, the reader and the decoder write a heap snapshot while work is held, and gets no skipped receipt',
    async (mode) => {
      const { s, remove } = supervisor({ mode })
      try {
        s.collector.note = async () => {}
        const commands = []
        s.command = async (slot, step) => {
          commands.push([slot, step.type, step.boundary])
          return { error: null, reply: { at: 1, ms: 2, bytes: 3, error: null } }
        }
        s.sampleAll = async () => {}
        s.quiesced = 'h00i'
        const held = []
        s.scheduler = { inflight: new Set(), hold: (work) => held.push(work) }
        const step = { id: 'h00q2', type: 'quiet', boundary: 'h00', reading: 2 }
        await s.perform(step)
        expect(held).toHaveLength(1)
        await Promise.all(held)
        const slots = s.config.processes
          .filter((p) => ['subject', 'seed', 'reader', 'decoder'].includes(p.role))
          .map((p) => p.slot)
        expect(slots).toHaveLength(15)
        expect(commands).toEqual(slots.map((slot) => [slot, 'heap-snapshot', 'h00']))
        expect(s.missedSnapshot(step, 'late')).toBeUndefined()
        expect(s.heapSnapshots.map((r) => [r.slot, r.error])).toEqual(
          slots.map((slot) => [slot, null])
        )
      } finally {
        remove()
      }
    }
  )

  test('a docker command that does not end in time is killed and fails', async () => {
    const { s, remove } = supervisor()
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-docker-'))
    const PATH = process.env.PATH
    try {
      fs.writeFileSync(path.join(bin, 'docker'), '#!/bin/sh\nexec sleep 30\n', { mode: 0o755 })
      process.env.PATH = `${bin}:${PATH}`
      s.dockerMs = 200
      const started = Date.now()
      await expect(s.docker(['compose', 'stop', 'kafka1'])).rejects.toMatchObject({
        killed: true,
        signal: 'SIGKILL',
      })
      expect(Date.now() - started).toBeLessThan(5000)
    } finally {
      process.env.PATH = PATH
      fs.rmSync(bin, { recursive: true, force: true })
      remove()
    }
  })

  test('a harness wait and a harness bound of one length arm the same delay at the real rate; a faster harness clock shortens only the wait; both count as harness timers', () => {
    const spy = jest.spyOn(global, 'setTimeout')
    const before = liveHarnessTimers()
    const armed = (rate) => {
      MonotonicClock.rate = rate
      const timers = [harnessWait(() => {}, 6000), harnessBound(() => {}, 6000)]
      const delays = spy.mock.calls.slice(-2).map((call) => call[1])
      const live = liveHarnessTimers() - before
      timers.forEach(clearHarnessTimeout)
      return { delays, live, span: realSpan(6000) }
    }
    try {
      expect([armed(1), armed(60), liveHarnessTimers() - before]).toEqual([
        { delays: [6000, 6000], live: 2, span: 6000 },
        { delays: [100, 6000], live: 2, span: 360000 },
        0,
      ])
    } finally {
      MonotonicClock.rate = 1
      spy.mockRestore()
    }
  })

  test('a docker command arms its time bound as a harness timer, released once the command ends', async () => {
    const { s, remove } = supervisor()
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-docker-'))
    const PATH = process.env.PATH
    const timeouts = () =>
      // eslint-disable-next-line n/no-unsupported-features/node-builtins -- the sampler's own resource source
      process.getActiveResourcesInfo().filter((t) => t === 'Timeout').length
    try {
      fs.writeFileSync(path.join(bin, 'docker'), '#!/bin/sh\nsleep 0.3\necho done\n', {
        mode: 0o755,
      })
      process.env.PATH = `${bin}:${PATH}`
      s.dockerMs = 10000
      const harness = liveHarnessTimers()
      const client = timeouts() - harness
      const running = s.docker(['compose', 'ps'])
      expect(liveHarnessTimers()).toBe(harness + 1)
      expect(timeouts() - liveHarnessTimers()).toBe(client)
      await expect(running).resolves.toBe('done')
      expect(liveHarnessTimers()).toBe(harness)
    } finally {
      process.env.PATH = PATH
      fs.rmSync(bin, { recursive: true, force: true })
      remove()
    }
  })

  test('a membership execution starts no wave after a failed one, and no wave before its planned time', async () => {
    const { s, remove } = supervisor()
    try {
      const clock = jumpClock(0)
      s.clock = clock
      const starts = []
      s.waves = new Map([
        [
          'h00m0',
          [
            { id: 'h00m0w0', at: 0, deadline: 90000 },
            { id: 'h00m0w1', at: 90000, deadline: 180000 },
            { id: 'h00m0w2', at: 180000, deadline: 270000 },
          ],
        ],
      ])
      s.shape = async (wave, phase) => phase === 'start' && starts.push([wave.id, clock.t])
      s.sampleAll = async () => {}
      let leaves = 0
      s.send = async (slots, step) =>
        step.phase === 'leave'
          ? [{ reply: { transfers: ++leaves === 2 ? 0 : 1, returned: true } }]
          : []
      await expect(s.churnMembers({ id: 'h00m0' }, ['p01'])).rejects.toThrow(/h00m0w1/)
      expect(starts).toEqual([
        ['h00m0w0', 0],
        ['h00m0w1', 90000],
      ])
    } finally {
      remove()
    }
  })
})

describe('final soak retained harness state', () => {
  /** The per-client and per-incarnation state of one consumer process, without its clients. */
  const retaining = () => {
    const runtime = workerRuntime('c01')
    const worker = new ConsumerWorker(runtime)
    const forgotten = []
    const child = {
      observer: { forget: (client) => forgotten.push(client) },
      lifetimes: new Map(),
      revisions: new Map(),
      topicIds: new Map(),
      worker,
    }
    return { runtime, worker, child, forgotten }
  }
  const forget = (child, client) => ChildRuntime.prototype.forget.call(child, client)
  const setTopicId = (child, topic, id) => ChildRuntime.prototype.setTopicId.call(child, topic, id)

  test("a retired client's subscription lifetimes, revision, exchanges and boundaries are dropped", () => {
    const { worker, child, forgotten } = retaining()
    child.lifetimes.set('c01.2/main', 'r/A').set('c01.20/main', 'r/A')
    child.revisions.set('c01.2', 3).set('c01.20', 1)
    worker.lastBoundary.set(`c01.2/${IDS.A}/0`, 'x').set(`c01.20/${IDS.A}/0`, 'y')
    forget(child, 'c01.2')
    expect([...child.lifetimes.keys()]).toEqual(['c01.20/main'])
    expect([...child.revisions.keys()]).toEqual(['c01.20'])
    expect([...worker.lastBoundary.keys()]).toEqual([`c01.20/${IDS.A}/0`])
    expect(forgotten).toEqual(['c01.2'])
  })

  test('a replaced or deleted incarnation drops its progress, boundaries and owed counts, and keeps the others', () => {
    const { worker, child } = retaining()
    const rig = {
      id: 'c01.1',
      uncommitted: new Map([
        [`${IDS.A}|0`, 4],
        [`${IDS.B}|0`, 2],
      ]),
    }
    worker.standing.push(rig)
    worker.progress.set(`g/${IDS.A}/0`, {}).set(`g/${IDS.B}/0`, {})
    worker.lastBoundary.set(`c01.1/${IDS.A}/0`, 'x')
    setTopicId(child, 'r', IDS.A)
    setTopicId(child, 'r', IDS.C)
    expect([...worker.progress.keys()]).toEqual([`g/${IDS.B}/0`])
    expect(worker.lastBoundary.size).toBe(0)
    expect([...rig.uncommitted.keys()]).toEqual([`${IDS.B}|0`])
    setTopicId(child, 'r', null)
    expect(child.topicIds.has('r')).toBe(false)
  })

  test("a share member forgets a retired incarnation's entries, activity, fetch times and unconfirmed accepts, and still counts the accepts left unresolved", () => {
    const share = new ShareWorker(workerRuntime('c07'))
    const rig = { id: 'c07.1', slot: { group: 'g' } }
    for (const [topicId, topic] of [
      [IDS.A, config.topics.main],
      [IDS.B, config.topics.transactions],
    ]) {
      share.runtime.topicIds.set(topic, topicId)
      share.delivered(`${topicId}|0`, '0')
      share.onOutcome(rig, {
        topic,
        partition: 0,
        firstOffset: '0',
        lastOffset: '0',
        type: 'accept',
        error: Object.assign(new Error('lost'), { reason: 'session-lost' }),
      })
      share.onFetch(rig, {
        node: 'h:1',
        sentAt: 0,
        session: { epoch: 0, partitions: [{ topicId, partition: 0 }], forgotten: [] },
        decoded: { errorCode: 0, acquisitionLockTimeoutMs: 30000, topics: [] },
      })
    }
    share.forgetTopic(IDS.A)
    for (const map of [
      share.entered,
      share.activity,
      share.unconfirmed,
      ...share.sessions.values(),
    ])
      expect([...map.keys()]).toEqual([`${IDS.B}|0`])
    expect(share.evidence()).toEqual({
      unconfirmedAccepts: 2,
      unresolvedOffsets: '2',
      untracked: 0,
    })
  })

  test("a day's membership waves and regex topics leave the retained state at the live set", () => {
    const { worker, child } = retaining()
    const rig = { id: 'c01.1', uncommitted: new Map() }
    worker.standing.push(rig)
    for (let wave = 0; wave < 288; wave++) {
      const extra = `c01.${wave + 2}`
      child.lifetimes.set(`${extra}/main`, 'r/A')
      child.revisions.set(extra, 1)
      worker.lastBoundary.set(`${extra}/${IDS.A}/0`, 'x')
      forget(child, extra)
    }
    for (let n = 0; n < 96; n++) {
      const id = `R${String(n).padStart(21, '0')}`
      setTopicId(child, `regex-${n}`, id)
      worker.progress.set(`g/${id}/0`, {})
      worker.lastBoundary.set(`c01.1/${id}/1`, 'x')
      rig.uncommitted.set(`${id}|0`, 1)
      setTopicId(child, `regex-${n}`, null)
    }
    expect([child.lifetimes.size, child.revisions.size, child.topicIds.size]).toEqual([0, 0, 0])
    expect([worker.progress.size, worker.lastBoundary.size, rig.uncommitted.size]).toEqual([
      0, 0, 0,
    ])
  })

  /** A reader whose raw broker answers `fetches` fetches of one record each. */
  const reader = (fetches, dir) => {
    fs.mkdirSync(path.join(dir, 'raw'))
    const runtime = { ...workerRuntime('c21'), out: dir }
    const worker = new ReaderWorker(runtime)
    worker.standing.push({ id: 'p14.1' })
    worker.leaderOf = async () => ({ leader: { host: 'h', port: 1 } })
    let position = 0
    worker.openBroker = async (address, onExchange) => {
      const exchange = (apiKey) =>
        onExchange({
          apiKey,
          request: Buffer.from([apiKey]),
          response: Buffer.from('x'.repeat(64)),
        })
      return {
        disconnect: async () => {},
        listOffsets: async () => {
          exchange(2)
          return { responses: [{ partitions: [{ offset: String(fetches) }] }] }
        },
        fetch: async () => {
          exchange(1)
          const offset = String(position++)
          return {
            responses: [
              {
                partitions: [
                  {
                    messages: [
                      {
                        offset,
                        key: null,
                        value: Buffer.from(`${RUN}|c08.${offset}||`),
                        headers: {},
                        batchContext: { isControlBatch: false, inTransaction: false },
                      },
                    ],
                    abortedTransactions: [],
                  },
                ],
              },
            ],
          }
        },
      }
    }
    return { runtime, worker }
  }
  const scanStep = { id: 's', topic: config.topics.main, topicId: IDS.A, partition: 0, from: '0' }

  test('a scan over a long missed interval journals each fetch as it arrives and digests the raw file without reading it back', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-scan-'))
    try {
      const { runtime, worker } = reader(500, dir)
      const read = jest.spyOn(fs, 'readFileSync')
      const reply = await worker.onScan(scanStep)
      expect(read.mock.calls.filter(([f]) => String(f).includes('raw'))).toEqual([])
      expect(reply.to).toBe('500')
      expect(runtime.records.filter((r) => r.stage === 'log')).toHaveLength(500)
      const [scan] = runtime.records.filter((r) => r.kind === 'scan')
      const bytes = fs.readFileSync(path.join(dir, scan.rawFile))
      expect(scan).toMatchObject({ records: 500, rawSha256: sha256(bytes) })
      expect(worker.rawBytes).toBe(bytes.length)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test('a scan that would pass the raw byte budget of all scans fails', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-scan-'))
    try {
      const { runtime, worker } = reader(5, dir)
      worker.rawBytes = 8 * 1024 ** 3 - 400
      await expect(worker.onScan(scanStep)).rejects.toThrow(/raw scan budget of 8589934592 bytes/)
      expect(runtime.records.filter((r) => r.kind === 'scan')).toEqual([])
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test('raw share wire files stop at their byte budget, count what they dropped and report it once', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-wire-'))
    try {
      const full = []
      const wire = new WireLog({
        dir,
        prefix: 'p05.share',
        budget: 300,
        onFull: (m) => full.push(m),
      })
      const exchange = { apiKey: 78, request: Buffer.alloc(40), response: Buffer.alloc(40) }
      const written = [1, 2, 3, 4].map(() => wire.append(exchange))
      expect(written.map((w) => w !== null)).toEqual([true, false, false, false])
      expect(wire.dropped).toBe(3)
      expect(full).toEqual(['raw wire budget of 300 bytes reached'])
      expect(fs.statSync(path.join(dir, 'raw/p05.share.0.ndjson')).size).toBeLessThanOrEqual(300)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test('a share exchange past the wire budget is journaled as missing its wire, never as a record citing no file', () => {
    const records = []
    const runtime = {
      nextId: () => 'p07.0',
      emit: (r) => records.push(r),
      diagnose: (message) => records.push({ kind: 'diagnostic', message }),
      slotOf: () => ({ group: 'g' }),
    }
    const event = {
      type: 'share-acquire',
      acquisition: 'c07.1.a1',
      client: 'c07.1',
      key: { topic: config.topics.main, topicId: IDS.A, partition: 0 },
      acquired: [{ from: '0', to: '0', deliveryCount: 1 }],
      wire: { file: null, sha256: null },
    }
    ChildRuntime.prototype.observed.call(runtime, event)
    expect(records).toEqual([
      { kind: 'diagnostic', message: 'missing-source share wire for c07.1.a1' },
    ])
  })

  test('the telemetry decoder reads a large reporter suffix in bounded polls', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-decoder-'))
    const sink = path.join(dir, 'sink')
    fs.mkdirSync(sink)
    try {
      const runtime = {
        out: dir,
        slotOf: () => undefined,
        nextId: () => 'p16.0',
        emit: async () => true,
        diagnose: async () => true,
      }
      const decoder = new DecoderWorker(runtime)
      decoder.dir = sink
      fs.writeFileSync(path.join(sink, 'reporter-1.json'), JSON.stringify({ loadedAtMs: 100 }))
      const line = (seq) =>
        JSON.stringify({ node: '1', seq, clientId: 'x', parse: 'ok', pad: 'p'.repeat(1000) }) + '\n'
      const lines = Array.from({ length: 9000 }, (_, i) => line(i + 1)).join('')
      fs.writeFileSync(path.join(sink, 'exports-1.jsonl'), lines)
      const alloc = jest.spyOn(Buffer, 'alloc')
      expect(await decoder.onTick()).toEqual({ abort: null })
      expect(Math.max(...alloc.mock.calls.map(([n]) => n))).toBeLessThanOrEqual(8 * 1024 * 1024)
      expect(decoder.offsets.get('exports-1.jsonl')).toBeLessThan(lines.length)
      expect(await decoder.onTick()).toEqual({ abort: null })
      expect(decoder.offsets.get('exports-1.jsonl')).toBe(lines.length)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })
})

describe('final soak final drain', () => {
  const main = config.topics.main
  const key = `${main}|0`

  /**
   * A consumer worker with one running member that holds main partition 0 of the run's current
   * incarnation. Its public `position()` answers `state.position`, `commitOffsets` stores the offset
   * the group's `committed()` then reads back, and `state.held` says whether it holds the partition.
   */
  const consumer = (commitOffsets = null) => {
    const group = { acquired: new Map([[key, { acquiredAt: 7 }]]) }
    const state = { position: '0', committed: null, held: true, groupTopicId: IDS.A }
    const runtime = workerRuntime('c01', new MonotonicClock(), {
      mine: () => [group],
      currentAuthority: (g, k) => ({ run: 'now', acquiredAt: g.acquired.get(k)?.acquiredAt }),
      topicIdOf: () => state.groupTopicId,
      batchAuthority: () => ({ client: 'c01.1', run: 'r1', token: 1, acquiredAt: 7 }),
    })
    const worker = new ConsumerWorker(runtime)
    const store = jest.fn(async ([{ offset }]) => {
      state.committed = offset
    })
    const rig = {
      id: 'c01.1',
      slot: config.clients[0],
      object: {
        commitOffsets: commitOffsets
          ? jest.fn(async (entries) => {
              await commitOffsets(entries)
              await store(entries)
            })
          : store,
        assignment: () => (state.held ? [{ topic: main, partitions: [0] }] : []),
        position: jest.fn(async () => state.position),
        committed: jest.fn(async ([{ topic, partition }]) => [
          { topic, partition, offset: state.committed, metadata: null },
        ]),
      },
      uncommitted: new Map(),
      call: async (event, work) => work(),
    }
    worker.standing.push(rig)
    return { worker, rig, commitOffsets: rig.object.commitOffsets, runtime, group, state }
  }
  const commitsOf = (runtime) =>
    runtime.records.filter((r) => r.stage === 'commit' && r.phase === 'end')
  const message = (offset) => ({ offset: String(offset), key: null, value: null, headers: {} })

  test("the drain waits until the member's live position reaches the log end, then commits that position through the member and reads it back", async () => {
    const { worker, rig, commitOffsets, state } = consumer()
    state.position = '248'
    setTimeout(() => (state.position = '252'), 80)
    const reply = await worker.onDrain({ ends: { [key]: '252' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true, confirmed: 1 })
    expect(commitOffsets).toHaveBeenCalledTimes(1)
    expect(commitOffsets).toHaveBeenCalledWith([{ topic: main, partition: 0, offset: '252' }])
    expect(rig.object.committed).toHaveBeenCalledWith([{ topic: main, partition: 0 }])
  })

  test("a drain commits the current acquisition's live position, never the offset an earlier acquisition's handler resolved", async () => {
    const { worker, rig, commitOffsets, runtime, group, state } = consumer()
    await worker.handle(rig, { topic: main, partition: 0, message: message(41) })
    group.acquired.set(key, { acquiredAt: 8 })
    state.position = '30'
    const behind = await worker.onDrain({
      ends: { [key]: '42' },
      deadline: runtime.clock.now() + 120,
    })
    expect(behind).toEqual({ reached: false, confirmed: 0 })
    expect(commitOffsets).not.toHaveBeenCalled()
    state.position = '42'
    const caught = await worker.onDrain({ ends: { [key]: '42' }, deadline: Infinity })
    expect(caught).toEqual({ reached: true, confirmed: 1 })
    expect(commitsOf(runtime)).toEqual([
      expect.objectContaining({ next: '42', authority: { run: 'now', acquiredAt: 8 } }),
    ])
  })

  test('a drain commits nothing while the group still reads an earlier incarnation of the same name', async () => {
    const { worker, commitOffsets, runtime, state } = consumer()
    runtime.topicIds.set(main, IDS.B)
    state.position = '260'
    state.committed = '260'
    const stale = await worker.onDrain({
      ends: { [key]: '260' },
      deadline: runtime.clock.now() + 120,
    })
    expect(stale).toEqual({ reached: false, confirmed: 0 })
    expect(commitOffsets).not.toHaveBeenCalled()
    state.groupTopicId = IDS.B
    state.committed = null
    const fresh = await worker.onDrain({ ends: { [key]: '260' }, deadline: Infinity })
    expect(fresh).toEqual({ reached: true, confirmed: 1 })
    expect(commitsOf(runtime)[0].key).toEqual({ topic: main, topicId: IDS.B, partition: 0 })
  })

  test('a tail the client resolved with only control or aborted records is committed from the live position without any handler', async () => {
    const { worker, commitOffsets, runtime, state } = consumer()
    state.position = '260'
    const reply = await worker.onDrain({ ends: { [key]: '260' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true, confirmed: 1 })
    expect(commitOffsets).toHaveBeenCalledWith([{ topic: main, partition: 0, offset: '260' }])
    expect(commitsOf(runtime)).toEqual([
      expect.objectContaining({
        next: '260',
        outcome: 'ok',
        key: { topic: main, topicId: IDS.A, partition: 0 },
        authority: { run: 'now', acquiredAt: 7 },
      }),
    ])
  })

  test('a position read while the member lost and regained the partition is not committed, and the drain commits the new acquisition once it is stable', async () => {
    const { worker, rig, commitOffsets, runtime, group, state } = consumer()
    state.position = '260'
    let reads = 0
    rig.object.position.mockImplementation(async () => {
      if (++reads === 1) group.acquired.set(key, { acquiredAt: 9 })
      return state.position
    })
    const reply = await worker.onDrain({ ends: { [key]: '260' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true, confirmed: 1 })
    expect(commitOffsets).toHaveBeenCalledTimes(1)
    expect(commitsOf(runtime)[0].authority).toEqual({ run: 'now', acquiredAt: 9 })
  })

  /**
   * Holds the journal's acknowledgement of the first commit begin record until `release`, so a test
   * can move the member's live view between that record and the public commit call.
   */
  const holdBegin = (runtime) => {
    const emit = runtime.emit
    let release = null
    const reached = new Promise((resolve) => {
      runtime.emit = (record) => {
        if (release || record.stage !== 'commit' || record.phase !== 'begin') return emit(record)
        emit(record)
        return new Promise((ack) => {
          release = () => ack(true)
          resolve()
        })
      }
    })
    return { reached, release: () => release() }
  }

  test('a recreation or a restart between the begin record and the public commit call never sends the earlier offset, and the drain commits the new view', async () => {
    for (const [move, now] of [
      [
        ({ runtime, state }) => {
          runtime.topicIds.set(main, IDS.C)
          state.groupTopicId = IDS.C
        },
        {
          key: { topic: main, topicId: IDS.C, partition: 0 },
          authority: { run: 'now', acquiredAt: 7 },
        },
      ],
      [
        ({ group }) => group.acquired.set(key, { acquiredAt: 8 }),
        {
          key: { topic: main, topicId: IDS.A, partition: 0 },
          authority: { run: 'now', acquiredAt: 8 },
        },
      ],
    ]) {
      const ctx = consumer()
      ctx.state.position = '260'
      const held = holdBegin(ctx.runtime)
      const drain = ctx.worker.onDrain({ deadline: ctx.runtime.clock.now() + 5000 })
      await held.reached
      move(ctx)
      ctx.state.position = '5'
      held.release()
      expect(await drain).toEqual({ reached: true, confirmed: 1 })
      expect(ctx.commitOffsets.mock.calls).toEqual([[[{ topic: main, partition: 0, offset: '5' }]]])
      expect(commitsOf(ctx.runtime)).toEqual([
        expect.objectContaining({ next: '260', outcome: 'refused', errorType: 'DrainViewMoved' }),
        expect.objectContaining({ next: '5', outcome: 'ok', ...now }),
      ])
    }
  })

  test('a readback is accepted only while the group reads the topic id the commit was made under, on the already-at-end path too', async () => {
    const recreateDuringRead = ({ rig, runtime, state }) =>
      rig.object.committed.mockImplementationOnce(async ([{ topic, partition }]) => {
        const offset = state.committed
        runtime.topicIds.set(main, IDS.C)
        state.groupTopicId = IDS.C
        state.committed = null
        return [{ topic, partition, offset, metadata: null }]
      })
    const atEnd = consumer()
    atEnd.state.committed = '260'
    recreateDuringRead(atEnd)
    const reply = await atEnd.worker.onDrain({
      ends: { [key]: '260' },
      deadline: atEnd.runtime.clock.now() + 120,
    })
    expect(reply).toEqual({ reached: false, confirmed: 0 })
    const settled = consumer()
    settled.state.position = '260'
    settled.state.committed = '260'
    recreateDuringRead(settled)
    expect(await settled.worker.settle(settled.rig, main, 0)).toEqual({
      position: '260',
      confirmed: false,
    })
  })

  test('a commit whose readback does not return its offset is not confirmed, and the drain stays unreached', async () => {
    const { worker, rig, runtime, state } = consumer()
    state.position = '260'
    rig.object.committed.mockResolvedValue([
      { topic: main, partition: 0, offset: '200', metadata: null },
    ])
    const reply = await worker.onDrain({
      ends: { [key]: '260' },
      deadline: runtime.clock.now() + 120,
    })
    expect(reply).toEqual({ reached: false, confirmed: 0 })
    expect(runtime.records).toContainEqual({
      kind: 'diagnostic',
      message: `drain: ${key} not committed to 260`,
    })
  })

  test('the final commit waits until the member holds the key again', async () => {
    const held = []
    const { worker, state } = consumer(async () => held.push(state.held))
    state.position = '260'
    state.held = false
    setTimeout(() => (state.held = true), 80)
    const reply = await worker.onDrain({ ends: { [key]: '260' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true, confirmed: 1 })
    expect(held).toEqual([true])
  })

  test('a final commit the client refuses is sent again, and the drain is reached only once a commit succeeded', async () => {
    const refused = () =>
      Object.assign(new Error('rebalancing'), { name: 'KafkaJSCommitFailedError' })
    const once = jest.fn().mockRejectedValueOnce(refused()).mockResolvedValue(undefined)
    const retried = consumer(once)
    retried.state.position = '260'
    const reply = await retried.worker.onDrain({ ends: { [key]: '260' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true, confirmed: 1 })
    expect(once).toHaveBeenCalledTimes(2)
    const never = consumer(jest.fn(async () => Promise.reject(refused())))
    never.state.position = '260'
    const failed = await never.worker.onDrain({
      ends: { [key]: '260' },
      deadline: never.worker.runtime.clock.now() + 200,
    })
    expect(failed).toEqual({ reached: false, confirmed: 0 })
  })

  test('a key whose group readback already shows the log end for the current incarnation needs no new commit', async () => {
    const { worker, commitOffsets, state } = consumer()
    state.committed = '260'
    state.position = '200'
    const reply = await worker.onDrain({ ends: { [key]: '260' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true, confirmed: 0 })
    expect(commitOffsets).not.toHaveBeenCalled()
  })

  test('a member is not asked to reach a log it does not read', async () => {
    const { worker, commitOffsets, rig } = consumer()
    worker.seeded = true
    const txn = `${config.topics.transactions}|0`
    const reply = await worker.onDrain({ ends: { [txn]: '50' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true, confirmed: 0 })
    expect(commitOffsets).not.toHaveBeenCalled()
    expect(rig.object.position).not.toHaveBeenCalled()
  })

  test('a drain without ends commits and reads back the live position of every held partition, and leaves a partition the member lost to its new owner', async () => {
    const { worker, rig, commitOffsets, state } = consumer()
    state.position = '62'
    expect(await worker.onDrain()).toEqual({ reached: true, confirmed: 1 })
    expect(commitOffsets).toHaveBeenCalledWith([{ topic: main, partition: 0, offset: '62' }])
    rig.object.position.mockImplementation(async () => {
      state.held = false
      return '70'
    })
    expect(await worker.onDrain()).toEqual({ reached: true, confirmed: 0 })
    expect(commitOffsets).toHaveBeenCalledTimes(1)
  })

  test('a failed commit at the threshold leaves the offsets owed, and a successful one settles them', async () => {
    const commitOffsets = jest
      .fn()
      .mockRejectedValueOnce(
        Object.assign(new Error('refused'), { name: 'KafkaJSNumberOfRetriesExceeded' })
      )
      .mockResolvedValue(undefined)
    const { worker, rig } = consumer(commitOffsets)
    const captured = {
      key: { topic: main, topicId: IDS.A, partition: 0 },
      authority: { run: 'r1' },
    }
    rig.uncommitted.set(`${IDS.A}|0`, 100)
    expect(await worker.commit(rig, captured, '100')).toBe('unknown')
    expect(rig.uncommitted.get(`${IDS.A}|0`)).toBe(100)
    expect(await worker.commit(rig, captured, '100')).toBe('ok')
    expect(rig.uncommitted.has(`${IDS.A}|0`)).toBe(false)
  })

  test('the recovery witness needs a handler after the recovery and a drain commit that was read back', async () => {
    const { worker, rig, runtime, state } = consumer()
    state.position = '5'
    setTimeout(() => worker.handle(rig, { topic: main, partition: 0, message: message(4) }), 40)
    expect(await worker.onRecovered({})).toEqual({ ok: true })
    rig.object.committed.mockResolvedValue([
      { topic: main, partition: 0, offset: null, metadata: null },
    ])
    runtime.clock = jumpClock(0)
    setTimeout(() => worker.handle(rig, { topic: main, partition: 0, message: message(5) }), 10)
    expect(await worker.onRecovered({})).toEqual({ ok: false })
  })

  /**
   * A share worker on a jumping clock. `take` hands one record of a topic id to a handler now and
   * settles its accept with `errorCode`; `fetch` is one answered share fetch of the member whose
   * session holds partition 0 of the given topic ids; `onTick` runs after every sleep, so a member
   * can keep fetching while a drain waits. The fetches report a lock duration of `lockMs`.
   */
  const shareDrain = ({ lockMs = 30000 } = {}) => {
    const ctx = { onTick: () => {} }
    const clock = jumpClock(0)
    clock.sleepUntil = async (at) => {
      clock.t = Math.max(clock.t, at)
      ctx.onTick(clock.t)
    }
    const runtime = { ...workerRuntime('c07', clock), slotOf: () => ({ group: 'g' }) }
    const worker = new ShareWorker(runtime)
    runtime.worker = worker
    const rig = { id: 'c07.1', slot: { group: 'g' } }
    worker.standing.push(rig)
    const take = (topicId, offset, errorCode = 0) => {
      worker.delivered(`${topicId}|0`, String(offset))
      ChildRuntime.prototype.observed.call(runtime, {
        type: 'share-settle',
        acquisition: 'c07.1.a1',
        client: 'c07.1',
        key: { topic: main, topicId, partition: 0 },
        offsets: [{ offset: String(offset), automatic: false, type: 'accept' }],
        result: 'ok',
        errorCode,
        wire: { file: 'wire', sha256: 'sha' },
      })
    }
    const fetch = (topicIds = [IDS.A], { errorCode = 0, acquired = [], epoch = 0 } = {}) =>
      worker.onFetch(rig, {
        node: 'h:1',
        sentAt: clock.now(),
        session: {
          epoch,
          partitions: topicIds.map((topicId) => ({ topicId, partition: 0 })),
          forgotten: [],
        },
        decoded: {
          errorCode,
          acquisitionLockTimeoutMs: lockMs,
          topics: topicIds.map((topicId) => ({
            topicId,
            partitions: [{ partitionIndex: 0, fetchErrorCode: 0, acquired }],
          })),
        },
      })
    /** Makes the member fetch once a second from now on. */
    const keepFetching = (topicIds = [IDS.A]) => {
      let last = -Infinity
      ctx.onTick = (t) => {
        if (t - last >= 1000) fetch(topicIds, { epoch: 1 }) && (last = t)
      }
    }
    fetch()
    return { ctx, clock, runtime, worker, rig, take, fetch, keepFetching }
  }
  const drainRecord = (runtime) =>
    runtime.records.filter((r) => r.kind === 'diagnostic' && r.shareDrain).at(-1)?.shareDrain

  test('a share drain is reached once every offset below the end had a handler entry and, while the member kept fetching, nothing was delivered for the lock duration reported on the wire and a margin', async () => {
    const { clock, runtime, worker, take, keepFetching } = shareDrain({ lockMs: 10000 })
    for (let offset = 0; offset < 10; offset++) take(IDS.A, offset)
    keepFetching()
    const reply = await worker.onDrain({ ends: { [key]: '10' }, deadline: 120000 })
    expect(reply).toEqual({ reached: true })
    expect(clock.now()).toBeGreaterThanOrEqual(15000)
    expect(clock.now()).toBeLessThan(17000)
    expect(drainRecord(runtime)).toEqual({
      reached: true,
      unconfirmedAccepts: 0,
      unresolvedOffsets: '0',
      untracked: 0,
    })
  })

  test('an accept whose reply was lost early in a long run does not fail the drain however many settlements follow, and stays in the drain record as an unresolved accept', async () => {
    const { runtime, worker, rig, keepFetching } = shareDrain()
    const k = `${IDS.A}|0`
    worker.delivered(k, '0')
    worker.onOutcome(rig, {
      topic: main,
      partition: 0,
      firstOffset: '0',
      lastOffset: '0',
      type: 'accept',
      error: Object.assign(new Error('Acknowledgement was not confirmed'), {
        name: 'KafkaJSShareAcknowledgementError',
        reason: 'session-lost',
      }),
    })
    for (let offset = 1; offset <= 150001; offset++) {
      worker.delivered(k, String(offset))
      worker.onShareSettled({
        key: { topicId: IDS.A, partition: 0 },
        offsets: [{ offset: String(offset), type: 'accept' }],
        result: 'ok',
        errorCode: 0,
      })
    }
    keepFetching()
    const reply = await worker.onDrain({ ends: { [key]: '150002' }, deadline: 120000 })
    expect(reply).toEqual({ reached: true })
    expect(runtime.records).toContainEqual({
      kind: 'identity',
      recordId: expect.any(String),
      stage: 'share-outcome',
      client: 'c07.1',
      group: 'g',
      key: { topic: main, topicId: IDS.A, partition: 0 },
      from: '0',
      to: '0',
      type: 'accept',
      reason: 'session-lost',
      errorType: 'KafkaJSShareAcknowledgementError',
    })
    expect(drainRecord(runtime)).toEqual({
      reached: true,
      unconfirmedAccepts: 1,
      unresolvedOffsets: '1',
      untracked: 0,
    })
    expect(runtime.problems).toEqual([])
  })

  test('an accept reported unconfirmed and later redelivered and accepted on the wire is journaled as resolved and leaves no unresolved offset', async () => {
    const { runtime, worker, rig, take, keepFetching } = shareDrain()
    for (let offset = 0; offset < 10; offset++) worker.delivered(`${IDS.A}|0`, String(offset))
    worker.onOutcome(rig, {
      topic: main,
      partition: 0,
      firstOffset: '0',
      lastOffset: '9',
      type: 'accept',
      error: Object.assign(new Error('lost'), {
        name: 'KafkaJSShareAcknowledgementError',
        reason: 'session-lost',
      }),
    })
    for (let offset = 0; offset < 10; offset++) take(IDS.A, offset)
    keepFetching()
    expect(await worker.onDrain({ ends: { [key]: '10' }, deadline: 120000 })).toEqual({
      reached: true,
    })
    expect(runtime.records).toContainEqual({
      kind: 'diagnostic',
      message: `share accept without wire confirmation resolved: ${IDS.A}|0 offsets 0 to 9 (session-lost, KafkaJSShareAcknowledgementError) were redelivered and accepted`,
    })
    expect(drainRecord(runtime)).toEqual({
      reached: true,
      unconfirmedAccepts: 1,
      unresolvedOffsets: '0',
      untracked: 0,
    })
  })

  test('a share drain fails on a missing handler entry and names the offset, even when every later offset was delivered', async () => {
    const { runtime, worker, take, keepFetching } = shareDrain()
    for (let offset = 0; offset < 150001; offset++)
      if (offset !== 5) worker.delivered(`${IDS.A}|0`, String(offset))
    take(IDS.A, 150001)
    keepFetching()
    const step = { ends: { [key]: '150002' }, deadline: 120000 }
    expect(await worker.onDrain(step)).toEqual({ reached: false })
    expect(runtime.records).toContainEqual({
      kind: 'diagnostic',
      message: `share drain: ${key} of topic id ${IDS.A} has no handler entry at offset 5`,
    })
    expect(worker.entered.get(`${IDS.A}|0`).ranges).toHaveLength(1)
    take(IDS.A, 5)
    expect(await worker.onDrain({ ...step, deadline: 300000 })).toEqual({ reached: true })
  })

  test('a record redelivered after its lock expired is a new delivery and extends the quiet period', async () => {
    const { ctx, clock, worker, take, fetch } = shareDrain()
    for (let offset = 0; offset < 10; offset++) take(IDS.A, offset)
    let redelivered = false
    ctx.onTick = (t) => {
      if (!redelivered && t >= 30000) {
        redelivered = true
        fetch([IDS.A], {
          epoch: 1,
          acquired: [{ firstOffset: '9', lastOffset: '9', deliveryCount: 2 }],
        })
        take(IDS.A, 9)
        return
      }
      fetch([IDS.A], { epoch: 1 })
    }
    expect(await worker.onDrain({ ends: { [key]: '10' }, deadline: 120000 })).toEqual({
      reached: true,
    })
    expect(redelivered).toBe(true)
    expect(clock.now()).toBeGreaterThanOrEqual(30000 + 35000)
  })

  test('a member that stopped fetching, lost its membership or dropped the partition from its session cannot satisfy the quiet period', async () => {
    for (const fetching of [
      () => {},
      (fetch) => fetch([IDS.A], { epoch: 1, errorCode: 25 }),
      (fetch) => fetch([IDS.B], { epoch: 0 }),
    ]) {
      const { ctx, runtime, worker, take, fetch } = shareDrain()
      for (let offset = 0; offset < 10; offset++) take(IDS.A, offset)
      ctx.onTick = () => fetching(fetch)
      expect(await worker.onDrain({ ends: { [key]: '10' }, deadline: 120000 })).toEqual({
        reached: false,
      })
      expect(runtime.records).toContainEqual({
        kind: 'diagnostic',
        message: `share drain: ${key} of topic id ${IDS.A} has no share fetch answered without error, by a session holding the partition, sent at or after 35000 (the end of the quiet period)`,
      })
    }
  })

  /** @returns {void} one answered share fetch of the member with an explicit session change and partition answer */
  const sessionFetch = (h, session, partitions = []) =>
    h.worker.onFetch(h.rig, {
      node: 'h:1',
      sentAt: h.clock.now(),
      session: { partitions: [], forgotten: [], ...session },
      decoded: {
        errorCode: 0,
        acquisitionLockTimeoutMs: 30000,
        topics: [{ topicId: IDS.A, partitions }],
      },
    })

  test.each([
    ['a top-level error ends its session', (h) => h.fetch([IDS.A], { epoch: 1, errorCode: 25 })],
    ['its session closes', (h) => h.fetch([IDS.A], { epoch: -1 })],
    ['its session is reset without the partition', (h) => h.fetch([IDS.B], { epoch: 0 })],
    [
      'its session forgets the partition',
      (h) => sessionFetch(h, { epoch: 2, forgotten: [{ topicId: IDS.A, partition: 0 }] }),
    ],
    [
      'the partition fails in a later answer of the same session',
      (h) =>
        sessionFetch(h, { epoch: 2 }, [{ partitionIndex: 0, fetchErrorCode: 3, acquired: [] }]),
    ],
    [
      'the member retires',
      (h) =>
        ChildRuntime.prototype.forget.call(
          {
            observer: { forget() {} },
            lifetimes: new Map(),
            revisions: new Map(),
            worker: h.worker,
          },
          h.rig.id
        ),
    ],
  ])(
    'a quiet fetch witness earned and then lost when %s certifies no drain until a new qualifying fetch after recovery',
    async (_, lose) => {
      const h = shareDrain()
      for (let offset = 0; offset < 10; offset++) h.take(IDS.A, offset)
      const step = () => ({ ends: { [key]: '10' }, deadline: h.clock.now() })
      h.clock.t = 35000
      h.fetch([IDS.A], { epoch: 1 })
      expect(h.worker.shortOf(IDS.A, 0, '10')).toBeNull()
      h.clock.t = 35001
      lose(h)
      expect(await h.worker.onDrain(step())).toEqual({ reached: false })
      expect(drainRecord(h.runtime).reached).toBe(false)
      h.clock.t = 36000
      h.fetch([IDS.A], { epoch: 0 })
      expect(await h.worker.onDrain(step())).toEqual({ reached: true })
    }
  )

  test('a share drain of one topic incarnation is reached while deliveries of another topic never pause', async () => {
    const { ctx, clock, worker, take, fetch } = shareDrain({ lockMs: 1000 })
    for (let offset = 0; offset < 10; offset++) take(IDS.A, offset)
    let other = 0
    ctx.onTick = () => {
      take(IDS.B, other++)
      fetch([IDS.A, IDS.B], { epoch: 1 })
    }
    const reply = await worker.onDrain({ ends: { [key]: '10' }, deadline: 120000 })
    expect(reply).toEqual({ reached: true })
    expect(clock.now()).toBeLessThan(10000)
  })

  test('a share member is ready only once a share fetch of main partition 0 was answered without error', async () => {
    const answers = []
    const runtime = workerRuntime('c07', jumpClock(0), {
      wireFor: (client, apiKey, test) =>
        answers.find((a) => client === 'c07.1' && apiKey === 78 && test(a)) ?? null,
    })
    const worker = new ShareWorker(runtime)
    const rig = { id: 'c07.1' }
    const answer = (errorCode, topicId, fetchErrorCode) => ({
      errorCode,
      topics: [{ topicId, partitions: [{ partitionIndex: 0, fetchErrorCode }] }],
    })
    answers.push(answer(0, IDS.B, 0), answer(0, IDS.A, 3), answer(58, IDS.A, 0))
    expect(worker.fetchAnswered(rig)).toBe(false)
    answers.push(answer(0, IDS.A, 0))
    expect(worker.fetchAnswered(rig)).toBe(true)
  })

  test('a share member that never has a fetch answered fails its start after 60 s', async () => {
    const clock = jumpClock(0)
    const runtime = workerRuntime('c07', clock, { wireFor: () => null })
    jest
      .spyOn(ShareWorker.prototype, 'join')
      .mockImplementation(async (slot) => ({ id: 'c07.1', slot }))
    const worker = new ShareWorker(runtime)
    await expect(worker.begin()).rejects.toThrow(
      'c07.1 had no share fetch of the main log answered within 60000 ms'
    )
    expect(clock.t).toBeGreaterThanOrEqual(60000)
  })
})

describe('final soak final drain ends', () => {
  test('the final drain reads the ends once the committed and uncommitted ends agree, and drains again when they moved during the drain', async () => {
    const { s, remove } = supervisor()
    try {
      const { main, transactions } = s.config.topics
      const reads = [
        { [main]: ['10', '10'], [transactions]: ['40', '41'] },
        { [main]: ['10', '10'], [transactions]: ['42', '42'] },
        { [main]: ['10', '10'], [transactions]: ['43', '43'] },
        { [main]: ['10', '10'], [transactions]: ['43', '43'] },
      ]
      let read = -1
      s.admin = {
        fetchTopicOffsets: jest.fn(async (topic, { isolationLevel }) => {
          if (topic === main && isolationLevel === 'read_committed') read++
          const [committed, uncommitted] = reads[read][topic]
          const offset = isolationLevel === 'read_committed' ? committed : uncommitted
          return [{ partition: 0, offset, high: offset, low: '0' }]
        }),
      }
      const drains = []
      s.closeTransactions = async () => {}
      s.command = jest.fn(async (slot, step) => {
        drains.push(step.ends[`${transactions}|0`])
        return { reply: { reached: true } }
      })
      await s.finalDrain()
      const slots = new Set(s.command.mock.calls.map(([slot]) => slot)).size
      expect(drains).toEqual([...Array(slots).fill('42'), ...Array(slots).fill('43')])
      expect(s.problems).toEqual([])
    } finally {
      remove()
    }
  })
})

describe('final soak closing record', () => {
  test('the final drain first ends the transactions log with one acknowledged visible record, offered once no transaction is open on it', async () => {
    const { s, remove } = supervisor()
    try {
      const { transactions } = s.config.topics
      const order = []
      s.settledEnds = jest.fn(async (deadline, topics = ['both']) => {
        order.push(`ends ${topics.join(',')}`)
        return {}
      })
      s.command = jest.fn(async (slot, step) => {
        order.push(`${step.type} ${slot}`)
        return { reply: step.type === 'probe' ? { acknowledged: 1 } : { reached: true } }
      })
      await s.finalDrain()
      const producer = s.processOf('producer-idempotent')
      expect(order.slice(0, 2)).toEqual([`ends ${transactions}`, `probe ${producer}`])
      expect(order[2]).toBe('ends both')
      expect(s.command.mock.calls[0][1]).toMatchObject({
        type: 'probe',
        id: 'final-close',
        execution: null,
        topic: transactions,
        partitions: 1,
        count: 1,
      })
      expect(s.problems).toEqual([])
      s.command = jest.fn(async (slot, step) => ({
        reply: step.type === 'probe' ? { acknowledged: 0, offered: 1 } : { reached: true },
      }))
      await s.finalDrain()
      expect(s.problems).toEqual([
        `final drain: the closing record of ${transactions} was not acknowledged`,
      ])
    } finally {
      remove()
    }
  })
})

describe('final soak transaction outcomes', () => {
  /** A transactional worker whose producer hands out the scripted transactions in turn. */
  const transactional = (...txns) => {
    const runtime = workerRuntime('c09', jumpClock(0))
    const worker = createWorker(runtime)
    worker.seq = 0
    const rig = { id: 'c09.1', slot: config.clients.find((c) => c.slot === 'c09') }
    rig.object = { transaction: jest.fn(async () => txns.shift()) }
    return { worker, rig, runtime }
  }
  /** A transaction whose sends acknowledge, except the `failAt`-th, which rejects. */
  const scripted = ({ failAt = 0, commit = async () => {}, abort = async () => {} } = {}) => {
    let n = 0
    return {
      send: jest.fn(async () => {
        if (++n === failAt)
          throw Object.assign(new Error('timed out'), { name: 'KafkaJSRequestTimeoutError' })
        return [{ baseOffset: String(n) }]
      }),
      commit: jest.fn(commit),
      abort: jest.fn(abort),
      isActive: () => false,
    }
  }
  const outcomes = (runtime) =>
    runtime.records
      .filter((r) => r.stage === 'transaction')
      .map(({ decision, outcome, errorType }) => ({ decision, outcome, errorType }))

  test('a send that fails inside a transaction stops its sends and aborts it, journaled as the confirmed abort, and the next transaction commits', async () => {
    const failing = scripted({ failAt: 3 })
    const next = scripted()
    const { worker, rig, runtime } = transactional(failing, next)
    await worker.transact(rig, 1)
    await worker.transact(rig, 3)
    expect(failing.send).toHaveBeenCalledTimes(3)
    expect(failing.commit).not.toHaveBeenCalled()
    expect(failing.abort).toHaveBeenCalledTimes(1)
    expect(next.commit).toHaveBeenCalledTimes(1)
    expect(outcomes(runtime)).toEqual([
      { decision: 'abort', outcome: 'aborted', errorType: null },
      { decision: 'commit', outcome: 'committed', errorType: null },
    ])
  })

  test('a commit the client refuses is followed by an abort, journaled as the confirmed abort with the refusal', async () => {
    const refused = Object.assign(new Error('abortable'), {
      name: 'KafkaJSTransactionAbortableError',
      retriable: false,
    })
    const txn = scripted({
      commit: async () => {
        throw refused
      },
    })
    const { worker, rig, runtime } = transactional(txn)
    await worker.transact(rig, 1)
    expect(txn.abort).toHaveBeenCalledTimes(1)
    expect(outcomes(runtime)).toEqual([
      { decision: 'abort', outcome: 'aborted', errorType: 'KafkaJSTransactionAbortableError' },
    ])
  })

  test('a commit still unconfirmed when an idle window begins stops retrying and is aborted, so the idle answers', async () => {
    let attempts = 0
    const txn = scripted({
      commit: async () => {
        if (++attempts <= 50)
          throw Object.assign(new Error('unknown'), {
            name: 'KafkaJSProtocolError',
            retriable: true,
          })
      },
    })
    const { worker, rig, runtime } = transactional(txn)
    const transacting = worker.transact(rig, 1)
    await worker.onIdle()
    await transacting
    expect(attempts).toBe(1)
    expect(txn.abort).toHaveBeenCalledTimes(1)
    expect(outcomes(runtime)).toEqual([
      { decision: 'abort', outcome: 'aborted', errorType: 'KafkaJSProtocolError' },
    ])
  })

  test('a commit of unknown outcome is sent again until the client confirms it', async () => {
    let calls = 0
    const txn = scripted({
      commit: async () => {
        if (++calls < 3)
          throw Object.assign(new Error('unknown'), { name: 'KafkaJSError', retriable: true })
      },
    })
    const { worker, rig, runtime } = transactional(txn)
    await worker.transact(rig, 1)
    expect(txn.commit).toHaveBeenCalledTimes(3)
    expect(txn.abort).not.toHaveBeenCalled()
    expect(outcomes(runtime)).toEqual([
      { decision: 'commit', outcome: 'committed', errorType: null },
    ])
  })

  test('an abort the client cannot confirm leaves the outcome unknown', async () => {
    const txn = scripted({
      failAt: 1,
      abort: async () => {
        throw Object.assign(new Error('fenced'), { name: 'KafkaJSApplicationRecoverableError' })
      },
    })
    const { worker, rig, runtime } = transactional(txn)
    await worker.transact(rig, 1)
    expect(outcomes(runtime)).toEqual([
      { decision: 'abort', outcome: 'unknown', errorType: 'KafkaJSApplicationRecoverableError' },
    ])
  })

  test('a send the broker confirmed as a duplicate is journaled acknowledged with no offset, never offset -1', async () => {
    useClient({ connect: async () => {}, send: async () => [{ baseOffset: '-1' }] })
    const runtime = workerRuntime('c08', boundedClock())
    const worker = createWorker(runtime)
    started.push(worker)
    await worker.begin()
    expect(await worker.onRecovered({ id: 'h00o' })).toEqual({ ok: true })
    expect(runtime.records.filter((r) => r.stage === 'ack')).toEqual([
      expect.objectContaining({ outcome: 'acknowledged', offset: null, errorType: null }),
    ])
  })
})

describe('final soak failure evidence', () => {
  test('a failed public call keeps the error name, type, code, retriable flag, message and cause chain', async () => {
    const runtime = workerRuntime('c01')
    const rig = new ClientRig({ runtime, slot: config.clients[0], incarnation: 1 })
    const root = Object.assign(new Error('socket hang up'), { name: 'KafkaJSConnectionError' })
    const mid = Object.assign(new Error('Request timed out', { cause: root }), {
      name: 'KafkaJSRequestTimeoutError',
    })
    const error = Object.assign(new Error('x'.repeat(300), { cause: mid }), {
      name: 'KafkaJSError',
      type: 'CONSUMER_STOPPING',
      code: 7,
      retriable: false,
    })
    await expect(
      rig.call('disconnect', async () => {
        throw error
      })
    ).rejects.toBe(error)
    const failure = runtime.records.find((r) => r.kind === 'diagnostic')
    expect(failure).toEqual({
      kind: 'diagnostic',
      recordId: expect.any(String),
      message: 'public call c01.1.k1 failed',
      failedCall: 'c01.1.k1',
      error: {
        name: 'KafkaJSError',
        type: 'CONSUMER_STOPPING',
        code: 7,
        retriable: false,
        message: `${'x'.repeat(98)}… (300 characters)`,
        causes: [
          { name: 'KafkaJSRequestTimeoutError', message: 'Request timed out' },
          { name: 'KafkaJSConnectionError', message: 'socket hang up' },
        ],
      },
    })
  })

  test('a failure record fits the line cap whatever the error holds, and a cause cycle ends', async () => {
    const runtime = workerRuntime('c01')
    const error = Object.assign(new Error('m'.repeat(5000)), { name: 'n'.repeat(40) })
    error.cause = error
    await runtime.failed('c18.4.m9385', error)
    const [record] = runtime.records
    expect(record.error.causes).toHaveLength(2)
    const line = JSON.stringify({ ...record, source: 'p06', seq: 1e9, at: 1e9, t: 1e9 })
    expect(Buffer.byteLength(line)).toBeLessThanOrEqual(matrix.host.maxEventLineBytes.value)
  })

  test('a commit that outlived the membership it began under is journaled as an expected refusal, and a lost membership still as a failed call', async () => {
    const { KafkaJSCommitFailedError } = require('../../../src/errors')
    const runtime = workerRuntime('c18')
    const worker = new ConsumerWorker(runtime)
    const refusals = [
      new KafkaJSCommitFailedError('The commit outlived its assignment', {
        reason: 'stale-assignment',
      }),
      new KafkaJSCommitFailedError('The consumer no longer owns this membership', {
        reason: 'membership-lost',
        cause: new Error('member lost'),
      }),
    ]
    const rig = {
      id: 'c18.4',
      slot: config.clients.find((c) => c.slot === 'c18'),
      object: {
        commitOffsets: async () => {
          throw refusals.shift()
        },
      },
      uncommitted: new Map(),
    }
    const key = { topic: config.topics.main, topicId: IDS.A, partition: 0 }
    expect(await worker.commit(rig, { key, authority: {} }, '7400')).toBe('refused')
    const [stale] = runtime.records.filter((r) => r.stage === 'commit' && r.phase === 'end')
    expect(stale).toMatchObject({ outcome: 'refused', errorType: 'KafkaJSCommitFailedError' })
    expect(runtime.records.filter((r) => r.kind === 'diagnostic')).toEqual([
      {
        kind: 'diagnostic',
        message: `expected refusal: commit ${stale.commit} outlived the membership it began under`,
      },
    ])
    expect(await worker.commit(rig, { key, authority: {} }, '7400')).toBe('refused')
    expect(runtime.records.filter((r) => r.failedCall)).toEqual([
      expect.objectContaining({
        message: expect.stringMatching(/^public call c18\.4\.m\d+ failed$/),
        error: expect.objectContaining({ name: 'KafkaJSCommitFailedError' }),
      }),
    ])
  })

  test('a commit the client refuses because the run is still starting is journaled as an expected refusal, not a failed call', async () => {
    const { KafkaJSNonRetriableError } = require('../../../src/errors')
    const runtime = workerRuntime('c18')
    const worker = new ConsumerWorker(runtime)
    const k = `${IDS.A}|0`
    const rig = {
      id: 'c18.4',
      slot: config.clients.find((c) => c.slot === 'c18'),
      object: {
        commitOffsets: async () => {
          throw new KafkaJSNonRetriableError(
            'Consumer is not running, wait for consumer#run to resolve first'
          )
        },
      },
      uncommitted: new Map([[k, 100]]),
    }
    const key = { topic: config.topics.main, topicId: IDS.A, partition: 0 }
    expect(await worker.commit(rig, { key, authority: {} }, '4300')).toBe('refused')
    const end = runtime.records.find((r) => r.stage === 'commit' && r.phase === 'end')
    expect(end).toMatchObject({ outcome: 'refused', errorType: 'KafkaJSNonRetriableError' })
    expect(runtime.records.filter((r) => r.kind === 'diagnostic')).toEqual([
      {
        kind: 'diagnostic',
        message: `expected refusal: commit ${end.commit} not sent, the run is starting`,
      },
    ])
    expect(rig.uncommitted.get(k)).toBe(100)
  })

  test('a commit whose public call failed is journaled with what the client returned', async () => {
    const runtime = workerRuntime('c18')
    const worker = new ConsumerWorker(runtime)
    const stopping = Object.assign(new Error('The consumer is stopping'), {
      name: 'KafkaJSError',
      type: 'CONSUMER_STOPPING',
      retriable: false,
    })
    const k = `${IDS.A}|0`
    const rig = {
      id: 'c18.4',
      slot: config.clients.find((c) => c.slot === 'c18'),
      object: {
        commitOffsets: async () => {
          throw stopping
        },
      },
      uncommitted: new Map([[k, 100]]),
    }
    const key = { topic: config.topics.main, topicId: IDS.A, partition: 0 }
    await worker.commit(rig, { key, authority: {} }, '4300')
    expect(runtime.records.find((r) => r.stage === 'commit' && r.phase === 'end')).toMatchObject({
      outcome: 'unknown',
      errorType: 'KafkaJSError',
    })
    expect(runtime.records.find((r) => r.failedCall)).toMatchObject({
      failedCall: expect.stringMatching(/^c18\.4\.m\d+$/),
      error: { name: 'KafkaJSError', type: 'CONSUMER_STOPPING', retriable: false, causes: [] },
    })
  })
})

describe('final soak telemetry teardown', () => {
  test("the telemetry decoder disconnects last, after it read the disconnected telemetry clients' terminating pushes", async () => {
    const { s, remove } = supervisor()
    try {
      s.children = new Map(
        s.config.processes.filter((p) => p.slot !== 'p15').map((p) => [p.slot, {}])
      )
      const sent = []
      s.send = async (slots, step) => {
        sent.push([step.type, slots.includes('p16') ? slots.length : 0, step.clients ?? null])
        return slots.map(() => ({
          reply: step.type === 'drain-telemetry' ? { missing: [] } : null,
        }))
      }
      s.finalDrain = async () => {}
      s.seal = async () => {}
      s.shape = async () => {}
      s.gate = { idle: async () => {} }
      s.clock = jumpClock(0)
      await s.teardown({ id: 'disconnect', type: 'disconnect' })
      expect(sent).toEqual([
        ['idle', 15, null],
        ['disconnect', 0, null],
        ['drain-telemetry', 1, ['c11', 'c12']],
        ['disconnect', 1, null],
      ])
      expect(s.problems).toEqual([])
    } finally {
      remove()
    }
  })

  test('the decoder reads the reporters until each telemetry client has a terminating push, or notes the ones it did not read', async () => {
    const clock = jumpClock(0)
    const decoder = createWorker({ ...workerRuntime('c21', clock), process: { role: 'decoder' } })
    let ticks = 0
    decoder.onTick = async () => {
      if (++ticks === 3) decoder.terminated.add('c11.1')
      return { abort: null }
    }
    expect(await decoder.onDrainTelemetry({ clients: ['c11'], deadline: 15000 })).toEqual({
      abort: null,
      missing: [],
    })
    expect(ticks).toBe(3)
    expect(
      await decoder.onDrainTelemetry({ clients: ['c11', 'c12'], deadline: clock.now() + 2000 })
    ).toEqual({ abort: null, missing: ['c12'] })
    expect(clock.t).toBeLessThanOrEqual(15000)
  })
})

describe('final soak smoke prerequisites', () => {
  test('the smoke aborts its early transaction and commits the ones around it', async () => {
    const decisions = []
    const txn = { send: async () => [{ baseOffset: '1' }], isActive: () => true }
    txn.commit = async () => decisions.push('commit')
    txn.abort = async () => decisions.push('abort')
    const runtime = workerRuntime('c09', jumpClock(0))
    const worker = createWorker(runtime)
    worker.seq = 0
    const rig = { id: 'c09.1', slot: config.clients.find((c) => c.slot === 'c09') }
    rig.object = { transaction: async () => txn }
    for (let n = 1; n <= 3; n++) await worker.transact(rig, n)
    expect(matrix.schedule.smoke.abortAtTransaction).toBe(2)
    expect(decisions).toEqual(['commit', 'abort', 'commit'])
    expect(runtime.records.filter((r) => r.stage === 'transaction').map((r) => r.outcome)).toEqual([
      'committed',
      'aborted',
      'committed',
    ])
  })

  test('a smoke whose planned outage did not end ok is a problem of the run', () => {
    const { s, remove } = supervisor()
    try {
      s.requireOutages(
        new Map([
          ['h00o0', 'ok'],
          ['h00o1', 'aborted'],
          ['h00o2', null],
        ])
      )
      expect(s.problems).toEqual([
        'smoke outage h00o1 has no receipt (aborted)',
        'smoke outage h00o2 has no receipt (not run)',
      ])
    } finally {
      remove()
    }
  })

  test('after an abort the seed clear still runs, before the disconnect, and other steps do not', async () => {
    const performed = []
    const scheduler = new Scheduler({
      clock: jumpClock(0),
      perform: async (step) => performed.push(step.id),
    })
    scheduler.abort('wave failed')
    const steps = ['h00o0', 'seed-clear', 'disconnect'].map((id, order) => ({
      id,
      type: id === 'h00o0' ? 'execution' : id,
      lane: 'work',
      at: 1000,
      order,
    }))
    const summary = await scheduler.run(steps)
    expect(performed).toEqual(['seed-clear', 'disconnect'])
    expect(summary).toMatchObject({ ok: 2, aborted: 1 })
  })

  test('the share group reads back share.auto.offset.reset=earliest through the public admin calls', async () => {
    const { s, remove } = supervisor()
    try {
      const configs = new Map()
      const calls = []
      s.admin = {
        incrementalAlterConfigs: async ({ resources }) => {
          calls.push(['alter', resources])
          for (const r of resources)
            for (const e of r.configEntries) configs.set(`${r.name}/${e.name}`, e.value)
          return { resources: resources.map(() => ({ errorCode: 0, errorMessage: null })) }
        },
        describeConfigs: async ({ resources }) => {
          calls.push(['describe', resources])
          return {
            resources: resources.map((r) => ({
              resourceType: r.type,
              resourceName: r.name,
              configEntries: r.configNames.map((configName) => ({
                configName,
                configValue: configs.get(`${r.name}/${configName}`) ?? null,
                configSource: 8,
              })),
            })),
          }
        },
      }
      const notes = []
      s.collector = { note: async (message) => notes.push(message) }
      await s.configureShareGroup()
      const group = s.config.clients.find((c) => c.slot === 'c07').group
      expect(calls).toEqual([
        [
          'alter',
          [
            {
              type: 32,
              name: group,
              configEntries: [{ name: 'share.auto.offset.reset', operation: 0, value: 'earliest' }],
            },
          ],
        ],
        ['describe', [{ type: 32, name: group, configNames: ['share.auto.offset.reset'] }]],
      ])
      expect(notes).toEqual([`share group ${group} reads back share.auto.offset.reset=earliest`])
    } finally {
      remove()
    }
  })

  test("an aborted supervisor's receipt is not a green workload, and a clean one is", () => {
    const { s, remove } = supervisor()
    try {
      expect(workloadProblems(s.result())).toEqual([])
      s.abort('step h00m0 failed: h00m0w1 saw no paused partition transfer in p02')
      s.problem('step h00m0 error')
      expect(workloadProblems(JSON.parse(JSON.stringify(s.result())))).toEqual([
        'problems: ["step h00m0 error"]',
        'aborted: "step h00m0 failed: h00m0w1 saw no paused partition transfer in p02"',
      ])
      expect(workloadProblems(null)).toEqual(['supervisor.json is missing or unreadable'])
      expect(
        workloadProblems({ ...s.result(), problems: [], aborted: null, forced: ['p01'] })
      ).toEqual(['forced: ["p01"]'])
    } finally {
      remove()
    }
  })
})

describe('final soak smoke schedule', () => {
  const smoke = matrix.schedule.smoke
  const plan = planSchedule(matrix, 'smoke')
  const outages = plan.steps.filter((s) => s.shape === 'broker-outage')

  test('the smoke holds two outages whose down times alternate two and twelve seconds', () => {
    expect(outages.map((o) => o.downMs)).toEqual([2000, 12000])
    expect(outages[0].deadline).toBeLessThanOrEqual(outages[1].at)
    expect(outages.at(-1).deadline).toBeLessThanOrEqual(smoke.recoveredByMs)
    expect(outages[0].at).toBe(smoke.outageAtMs)
  })

  test('the smoke leaves its outages clear of the tenth transaction, so one commit and one abort can be confirmed', () => {
    const every = matrix.traffic.transactionEveryMs.value
    const abortAt = matrix.traffic.abortEvery.value * every
    expect(smoke.activeMs.to).toBeGreaterThan(abortAt + every / 2)
    for (const o of outages) expect(abortAt < o.at || abortAt > o.deadline).toBe(true)
    expect(every).toBeLessThan(outages[0].at)
  })

  test('the matrix rejects a smoke abort that would not come before the first outage', () => {
    const { validateMatrix } = require('../finalSoakSchema')
    const late = JSON.parse(JSON.stringify(matrix))
    late.schedule.smoke.abortAtTransaction =
      smoke.outageAtMs / matrix.traffic.transactionEveryMs.value + 1
    expect(validateMatrix(matrix)).toEqual([])
    expect(validateMatrix(late)).toEqual([
      'matrix.schedule.smoke.abortAtTransaction does not abort before the first outage',
    ])
  })

  test('every work step of the smoke ends before the next starts and before the drain', () => {
    const work = plan.steps.filter((s) => s.lane === 'work' && s.shape && s.shape !== 'traffic')
    for (let i = 1; i < work.length; i++)
      expect(work[i - 1].deadline).toBeLessThanOrEqual(work[i].at)
  })

  test('the acceptance run orders its six outages two seconds, twelve seconds, two seconds, twelve seconds, each node stopped once clean and once hard', () => {
    const day = planSchedule(matrix, 'acceptance').steps.filter((s) => s.shape === 'broker-outage')
    expect(day).toHaveLength(6)
    expect(day.map((o) => o.downMs)).toEqual(
      Array.from({ length: 6 }, (_, i) => (i % 2 === 0 ? 2000 : 12000))
    )
    expect(day.map((o) => o.node)).toEqual(Array.from({ length: 6 }, (_, i) => (i % 3) + 1))
    expect(day.map((o) => o.stop)).toEqual(
      Array.from({ length: 6 }, (_, i) => (i % 2 === 0 ? 'clean' : 'hard'))
    )
    for (const node of [1, 2, 3])
      expect(
        day
          .filter((o) => o.node === node)
          .map((o) => o.stop)
          .sort()
      ).toEqual(['clean', 'hard'])
  })
})

describe('final soak record timestamps and client log', () => {
  const { SourceChannel } = require('../finalSoakCollector')
  const { StreamValidator } = require('../finalSoakSchema')

  test("a wave's records keep nondecreasing at within their source when another record of the source is emitted between two wave-key writes", async () => {
    let t = 0
    const clock = { now: () => ++t, sleepUntil: async () => {} }
    const sent = []
    const channel = new SourceChannel({
      source: 'p01',
      clock,
      send: ({ record }, done) => {
        sent.push({ ...record, t: clock.now() })
        done()
      },
    })
    let ids = 0
    let interleaved = false
    const runtime = {
      clock,
      nextId: () => `p01.${ids++}`,
      emit: async (record) => {
        const ok = await channel.emit(record)
        if (record.kind === 'wave-key' && !interleaved) {
          interleaved = true
          await channel.emit({ kind: 'diagnostic', recordId: runtime.nextId(), message: 'between' })
        }
        return ok
      },
    }
    const worker = Object.create(ConsumerWorker.prototype)
    worker.runtime = runtime
    const keys = [0, 1, 2].map((partition) => ({ topic: 'main', topicId: IDS.A, partition }))
    await worker.waveRecord(
      'h00m0w0',
      'transfer',
      { id: 'c02.1' },
      { id: 'c14.1' },
      keys,
      () => keys,
      true
    )
    expect(sent.map((r) => r.kind)).toEqual([
      'wave',
      'wave-key',
      'diagnostic',
      'wave-key',
      'wave-key',
    ])
    const header = { runId: config.runId, mode: config.mode, window: { observedEndAt: Infinity } }
    const stream = new StreamValidator({ matrix, config, header })
    for (const record of sent) stream.envelope(record)
    expect(stream.problems).toEqual([])
  })

  /** @returns {object} a client log entry at `level` with `message` */
  const entry = (message, level = 2) => ({
    namespace: 'Consumer',
    level,
    log: { message, groupId: 'g1', timestamp: '2026-11-19T00:00:00.000Z' },
  })
  const readLog = (file) =>
    fs
      .readFileSync(file, 'utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))

  test('a client log past its byte budget counts every line, keeps the newest dropped ones and writes the counts when it closes', () => {
    const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-client-log-'))
    try {
      const file = path.join(out, 'log.ndjson')
      const log = new ClientLog({ file, maxBytes: 64 * 1024 + 2000 })
      const add = log.creator()(2)
      for (let n = 1; n <= 5001; n++) add(entry(`warning ${n}`))
      expect(log.close()).toEqual({
        total: 5001,
        written: log.written,
        dropped: 5001 - log.written,
        failed: null,
      })
      expect(fs.statSync(file).size).toBeLessThanOrEqual(64 * 1024 + 2000)
      const lines = readLog(file)
      const summary = lines.at(-1)
      expect(summary).toMatchObject({ summary: true, total: 5001, failed: null })
      expect(summary.written).toBe(lines.length - 1)
      expect(summary.tail.map((l) => l.message).at(-1)).toBe('warning 5001')
      expect(summary.tail).toHaveLength(20)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test('a huge client log message is clipped to a fixed size', () => {
    const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-client-log-'))
    try {
      const file = path.join(out, 'log.ndjson')
      const log = new ClientLog({ file })
      log.creator()(1)(entry('x'.repeat(5 * 1024 * 1024), 1))
      log.close()
      const [line] = readLog(file)
      expect(Buffer.byteLength(JSON.stringify(line.message))).toBeLessThanOrEqual(1000)
      expect(line.message).toMatch(/… \(5242880 characters\)$/)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test('a client log write that fails is recorded and never reaches the logging client', () => {
    const files = {
      openSync: () => 7,
      writeSync: () => {
        throw new Error('EIO')
      },
      closeSync: () => {},
    }
    const log = new ClientLog({ file: 'unused', files })
    const add = log.creator()(2)
    expect(() => add(entry('one'))).not.toThrow()
    add(entry('two'))
    expect(log.close()).toEqual({ total: 2, written: 0, dropped: 2, failed: 'EIO' })
  })

  test('a client log write that takes no bytes ends the file instead of spinning, and is recorded', () => {
    let calls = 0
    const files = {
      openSync: () => 7,
      writeSync: () => {
        if (++calls > 1000) throw new Error('spinning')
        return 0
      },
      closeSync: () => {},
    }
    const log = new ClientLog({ file: 'unused', files })
    log.creator()(2)(entry('one'))
    expect(calls).toBe(1)
    expect(log.close()).toEqual({
      total: 1,
      written: 0,
      dropped: 1,
      failed: 'the client log file accepted no bytes',
    })
  })

  test('the raw wire budget counts the UTF-8 bytes of a line, not its characters', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-wire-bytes-'))
    try {
      const full = []
      const wire = new WireLog({ dir, prefix: 'p07.utf8', budget: 60, onFull: (m) => full.push(m) })
      expect(wire.note({ pad: 'é'.repeat(30) })).toBeNull()
      expect(full).toEqual(['raw wire budget of 60 bytes reached'])
      expect(wire.note({ pad: 'e'.repeat(30) })).not.toBeNull()
      expect(wire.total).toBe(fs.statSync(path.join(dir, 'raw/p07.utf8.0.ndjson')).size)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test("a process's client log is closed at its stop and its counts are journaled", async () => {
    const records = []
    const runtime = {
      slot: 'p01',
      clientLog: { close: () => ({ total: 9, written: 7, dropped: 2, failed: null }) },
      diagnose: async (message) => records.push(message),
    }
    await ChildRuntime.prototype.closeClientLog.call(runtime)
    expect(records).toEqual([
      'client log client-log.p01.ndjson: 9 WARN or ERROR lines, 7 written, 2 dropped',
    ])
  })

  test("a client's WARN and ERROR log lines reach a bounded file of its process in the artifact and lower levels do not", async () => {
    const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-client-log-'))
    try {
      const runtime = {
        slot: 'p01',
        out,
        config,
        context: new AsyncLocalStorage(),
        sockets: { factory: () => undefined },
        nextId: () => 'p01.0',
        emit: async () => true,
        clientLogCreator: ChildRuntime.prototype.clientLogCreator,
      }
      const slot = config.clients.find((c) => c.process === 'p01')
      const rig = new ClientRig({ runtime, slot, incarnation: 1 })
      await rig.construct((kafka) => kafka)
      const logger = rig.kafka.logger()
      logger.info('quiet line')
      logger.warn('Rebalance is still running', { groupId: 'g1' })
      logger.error('broken line', { groupId: 'g1' })
      runtime.clientLog?.close()
      const lines = fs
        .readFileSync(path.join(out, 'client-log.p01.ndjson'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      expect(lines.map((l) => [l.message, l.group])).toEqual([
        ['Rebalance is still running', 'g1'],
        ['broken line', 'g1'],
        [undefined, undefined],
      ])
      expect(lines[2]).toMatchObject({ summary: true, total: 2, written: 2, dropped: 0 })
      expect(runtime.clientLog.maxBytes).toBe(8 * 1024 * 1024)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })
})

describe('final soak share request capture', () => {
  const { SocketRegistry } = require('../finalSoakObserver')
  const { shareRequestSummary } = require('../shareProtocol')
  const shareAcknowledgeV2 = require('../../../src/protocol/requests/shareAcknowledge/v2/request')
  const UUID = '41414141-4141-4141-4141-414141414141'

  /** @returns {Promise<Buffer>} a framed ShareAcknowledge v2 request accepting `[from, to]` */
  const ackFrame = async (correlationId, shareSessionEpoch, from, to) => {
    const body = (
      await shareAcknowledgeV2({
        groupId: 'g',
        memberId: 'm',
        shareSessionEpoch,
        topics: [
          {
            topicId: UUID,
            partitions: [
              {
                partitionIndex: 0,
                acknowledgementBatches: [
                  { firstOffset: from, lastOffset: to, acknowledgeTypes: [1] },
                ],
              },
            ],
          },
        ],
      }).encode()
    ).buffer
    const header = Buffer.alloc(13)
    header.writeInt16BE(79, 0)
    header.writeInt16BE(2, 2)
    header.writeInt32BE(correlationId, 4)
    header.writeInt16BE(2, 8)
    header.write('c7', 10)
    header.writeUInt8(0, 12)
    const size = Buffer.alloc(4)
    size.writeInt32BE(header.length + body.length)
    return Buffer.concat([size, header, body])
  }

  test('a capturing socket reports each share request when it is written and, at close, each one never answered', async () => {
    const socket = new EventEmitter()
    socket.write = () => true
    const registry = new SocketRegistry({ base: () => () => socket })
    const seen = { sent: [], unanswered: [], exchanges: [] }
    registry.factory('c07.1', {
      apiKeys: [78, 79],
      onExchange: (e) => seen.exchanges.push(e.request.readInt32BE(8)),
      onSent: (r) => seen.sent.push([r.node, r.apiKey, r.correlationId]),
      onUnanswered: (r) => seen.unanswered.push(r),
    })({ host: 'h', port: 1 })
    const answered = await ackFrame(7, 3, '1', '1')
    const lost = await ackFrame(8, 4, '2', '2')
    socket.write(answered)
    socket.write(lost)
    expect(seen.sent).toEqual([
      ['h:1', 79, 7],
      ['h:1', 79, 8],
    ])
    const response = Buffer.alloc(9)
    response.writeInt32BE(5, 0)
    response.writeInt32BE(7, 4)
    socket.emit('data', response)
    socket.emit('close')
    expect(seen.exchanges).toEqual([7])
    expect(seen.unanswered).toHaveLength(1)
    expect(seen.unanswered[0]).toMatchObject({ node: 'h:1', apiKey: 79, correlationId: 8 })
    expect(seen.unanswered[0].request.equals(lost)).toBe(true)
  })

  test('a capture hook that throws never stops the client write: the socket still writes, the failure is reported, and each request carries its socket generation and send time', async () => {
    const socket = new EventEmitter()
    const written = []
    socket.write = (chunk) => written.push(chunk) > 0
    let now = 41
    const registry = new SocketRegistry({ base: () => () => socket, now: () => ++now })
    const errors = []
    const sent = []
    registry.factory('c07.1', {
      apiKeys: [79],
      onExchange: () => {
        throw new Error('exchange sink full')
      },
      onSent: (r) => {
        sent.push(r)
        throw new Error('sent sink full')
      },
      onError: (stage, e) => errors.push([stage, e.message]),
    })({ host: 'h', port: 1 })
    const frame = await ackFrame(7, 3, '1', '1')
    socket.write(frame)
    expect(written).toEqual([frame])
    expect(sent).toEqual([
      expect.objectContaining({ node: 'h:1', socket: 1, sentAt: 42, apiKey: 79, correlationId: 7 }),
    ])
    const response = Buffer.alloc(9)
    response.writeInt32BE(5, 0)
    response.writeInt32BE(7, 4)
    socket.emit('data', response)
    expect(errors).toEqual([
      ['sent', 'sent sink full'],
      ['exchange', 'exchange sink full'],
    ])
  })

  test('a share member whose wire sink fails at a request reports a problem of the run and its client still writes', async () => {
    let capture = null
    jest.spyOn(ClientRig.prototype, 'construct').mockImplementation(async function (make, c) {
      capture = c
      this.object = {
        events: { ACKNOWLEDGE: 'share.acknowledge' },
        on: () => {},
        connect: async () => {},
        subscribe: async () => {},
        run: async () => {},
      }
      return this.object
    })
    const runtime = workerRuntime('c07')
    runtime.wire = {
      note: () => {
        throw new Error('ENOSPC')
      },
    }
    const worker = new ShareWorker(runtime)
    const rig = await worker.join(runtime.slots.find((c) => c.purpose === 'subject'))
    const socket = new EventEmitter()
    const written = []
    socket.write = (chunk) => written.push(chunk) > 0
    new SocketRegistry({ base: () => () => socket }).factory(
      rig.id,
      capture
    )({ host: 'h', port: 1 })
    const frame = await ackFrame(8, 4, '15760', '15760')
    socket.write(frame)
    expect(written).toEqual([frame])
    expect(runtime.problems).toEqual([
      `share wire capture of ${rig.id} lost its sent evidence: ENOSPC`,
    ])
  })

  test('a share request summary names its version, session epoch and acknowledged ranges, and a frame it cannot read yields nulls and the reason', async () => {
    expect(shareRequestSummary(await ackFrame(8, 4, '15760', '15760'))).toEqual({
      apiVersion: 2,
      sessionEpoch: 4,
      acknowledged: [{ topicId: UUID, partition: 0, from: '15760', to: '15760', types: [1] }],
      error: null,
    })
    const truncated = (await ackFrame(8, 4, '1', '1')).subarray(0, 20)
    truncated.writeInt32BE(16, 0)
    expect(shareRequestSummary(truncated)).toEqual({
      apiVersion: null,
      sessionEpoch: null,
      acknowledged: null,
      error: expect.any(String),
    })
  })

  test('a share member writes a short wire line for each request it sends and one with the full bytes for each request its socket closed without an answer, inside the wire budget', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-sent-'))
    try {
      let capture = null
      jest.spyOn(ClientRig.prototype, 'construct').mockImplementation(async function (make, c) {
        capture = c
        this.object = {
          events: { ACKNOWLEDGE: 'share.acknowledge' },
          on: () => {},
          connect: async () => {},
          subscribe: async () => {},
          run: async () => {},
          disconnect: async () => {},
        }
        return this.object
      })
      const runtime = workerRuntime('c07')
      const full = []
      runtime.wire = new WireLog({ dir, prefix: 'p07.share', onFull: (m) => full.push(m) })
      const worker = new ShareWorker(runtime)
      const rig = await worker.join(runtime.slots.find((c) => c.purpose === 'subject'))
      const lost = await ackFrame(8, 4, '15760', '15760')
      capture.onSent({ node: 'h:1', apiKey: 79, correlationId: 8, request: lost })
      capture.onUnanswered({ node: 'h:1', apiKey: 79, correlationId: 8, request: lost })
      const lines = fs
        .readFileSync(path.join(dir, 'raw/p07.share.0.ndjson'), 'utf8')
        .trim()
        .split('\n')
        .map((l) => JSON.parse(l))
      expect(lines).toEqual([
        {
          client: rig.id,
          event: 'sent',
          node: 'h:1',
          apiKey: 79,
          correlationId: 8,
          apiVersion: 2,
          sessionEpoch: 4,
          acknowledged: [{ topicId: UUID, partition: 0, from: '15760', to: '15760', types: [1] }],
          error: null,
        },
        {
          client: rig.id,
          event: 'unanswered',
          node: 'h:1',
          apiKey: 79,
          correlationId: 8,
          request: lost.toString('hex'),
        },
      ])
      const small = new WireLog({
        dir,
        prefix: 'p07.small',
        budget: 50,
        onFull: (m) => full.push(m),
      })
      expect(small.note({ client: rig.id, event: 'sent', pad: 'x'.repeat(60) })).toBeNull()
      expect(small.dropped).toBe(1)
      expect(full).toEqual(['raw wire budget of 50 bytes reached'])
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })
})
