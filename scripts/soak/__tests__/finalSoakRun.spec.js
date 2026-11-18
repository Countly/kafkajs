const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { EventEmitter } = require('node:events')
const { AsyncLocalStorage } = require('node:async_hooks')
const { buildConfig, Supervisor } = require('../finalSoak')
const { MonotonicClock, planSchedule } = require('../finalSoakScheduler')
const { ClientRig, ConsumerWorker, ShareWorker, createWorker } = require('../finalSoakWorkers')

const matrix = JSON.parse(fs.readFileSync(path.join(__dirname, '../finalSoakMatrix.json'), 'utf8'))
const TREE = 'b'.repeat(40)
const RUN = '0123456789ab'
const IDS = { A: 'AAAAAAAAAAAAAAAAAAAAAA', B: 'BBBBBBBBBBBBBBBBBBBBBB' }
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
    expect(reply).toEqual({ offered: 3, stopped: true })
    await worker.disconnect()
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

/** @returns {{ s: Supervisor, out: string, remove: () => void }} a supervisor with no cluster */
const supervisor = () => {
  const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-run-'))
  const s = new Supervisor({
    mode: 'smoke',
    matrix: 'scripts/soak/finalSoakMatrix.json',
    expectSha: TREE,
    brokers: ['k:1'],
    out,
  })
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
  return child
}

describe('final soak supervisor commands', () => {
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
      s.stepOutcome({ id: 'h00t0', type: 'execution', outcome: 'error', error: 'boom' })
      s.receive('p08', { type: 'problem', message: 'producer loop failed: x' })
      expect(s.problems).toEqual([
        expect.stringContaining('h00r0'),
        expect.stringContaining('boom'),
        expect.stringContaining('producer loop failed'),
      ])
      expect(s.aborted).toContain('producer loop failed')
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

  test.each([
    ['a failed docker stop', { failing: 'stop' }, /broker-stop/],
    ['a failed docker start', { failing: 'start' }, /broker-start/],
    ['metadata that never lists the run partitions', { topics: false }, /in sync/],
    ['a partition that stays out of sync', { isr: [1, 2] }, /in sync/],
    ['a client without a recovery witness', { witness: false }, /recovery witness/],
  ])('an outage with %s fails its step', async (_, options, message) => {
    await expect(outage(options)).rejects.toThrow(message)
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
        /p01.*paused partition/
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
      s.deleteTopic = async () => stages.push('delete')
      s.seal = async () => stages.push('seal')
      s.send = async (slots, step) => {
        stages.push(step.type)
        if (step.type === 'probe') clock.t += 200000
        return [{ reply: { offered: 10 } }]
      }
      const step = { id: 'h00r0', shape: 'regex-churn', deadline: 120000, participants: [] }
      await expect(s.churnTopic(step, ['p01'])).rejects.toThrow(/deadline/)
      expect(stages).toEqual(['create', 'regex-churn', 'probe'])
      expect(clock.t).toBeLessThanOrEqual(120000 + 200000)
    } finally {
      remove()
    }
  })

  test('a membership wave works to its scheduled deadline, not to a deadline counted from a late start', async () => {
    const clock = jumpClock(1000000)
    const standing = { id: 'c01.1', object: { pause() {}, resume() {} }, slot: config.clients[0] }
    const observer = {
      ordinarySections: () => ({
        sections: { assignment: [{ key: { topic: 't', topicId: IDS.A, partition: 0 } }] },
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
    expect(reply).toEqual({ transfers: 0 })
    expect(clock.t).toBeLessThanOrEqual(1030000)
  })
})

describe('final soak final drain', () => {
  const main = config.topics.main
  const key = `${main}|0`

  /** A consumer worker with one running member and a scripted commit. */
  const consumer = (commitOffsets = jest.fn(async () => {})) => {
    const runtime = workerRuntime('c01', new MonotonicClock(), {
      mine: () => [],
      currentAuthority: () => null,
    })
    const worker = new ConsumerWorker(runtime)
    const rig = {
      id: 'c01.1',
      slot: config.clients[0],
      object: { commitOffsets },
      processed: new Map(),
      uncommitted: new Map(),
      committed: new Map(),
      authority: new Map([
        [key, { key: { topic: main, topicId: IDS.A, partition: 0 }, authority: { run: 'r' } }],
      ]),
      call: async (event, work) => work(),
    }
    worker.standing.push(rig)
    return { worker, rig, commitOffsets, runtime }
  }
  const batchEnd = (worker, rig, lastOffset, batchSize = 0) =>
    worker.onBatchEnd(rig, { topic: main, partition: 0, batchSize, lastOffset })

  test('the drain waits until the member has resolved the log end, then commits that offset through the member', async () => {
    const { worker, rig, commitOffsets } = consumer()
    rig.processed.set(key, '248')
    setTimeout(() => batchEnd(worker, rig, '251'), 80)
    const reply = await worker.onDrain({ ends: { [key]: '252' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true })
    expect(commitOffsets).toHaveBeenCalledWith([{ topic: main, partition: 0, offset: '252' }])
  })

  test('a trailing aborted suffix and commit marker are resolved by filtered batches and committed', async () => {
    const { worker, rig, commitOffsets } = consumer()
    rig.processed.set(key, '250')
    batchEnd(worker, rig, '258')
    batchEnd(worker, rig, '259')
    const reply = await worker.onDrain({ ends: { [key]: '260' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true })
    expect(commitOffsets).toHaveBeenCalledTimes(1)
    expect(commitOffsets).toHaveBeenCalledWith([{ topic: main, partition: 0, offset: '260' }])
  })

  test('a batch that carried messages does not move the resolved offset beyond what its handler resolved', () => {
    const { worker, rig } = consumer()
    rig.processed.set(key, '100')
    batchEnd(worker, rig, '149', 50)
    expect(rig.processed.get(key)).toBe('100')
  })

  test('a failed commit at the threshold leaves the offsets owed, and the drain commits them again', async () => {
    const commitOffsets = jest
      .fn()
      .mockRejectedValueOnce(
        Object.assign(new Error('refused'), { name: 'KafkaJSNumberOfRetriesExceeded' })
      )
      .mockResolvedValue(undefined)
    const { worker, rig } = consumer(commitOffsets)
    rig.processed.set(key, '100')
    rig.uncommitted.set(key, 100)
    await worker.commit(rig, key)
    expect(rig.uncommitted.get(key)).toBe(100)
    expect(rig.committed.get(key)).toBeUndefined()
    const reply = await worker.onDrain({ ends: { [key]: '100' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true })
    expect(commitOffsets).toHaveBeenCalledTimes(2)
    expect(rig.committed.get(key)).toBe('100')
  })

  test('a member that never reaches the end by the deadline reports it and commits only what it resolved', async () => {
    const { worker, rig, commitOffsets } = consumer()
    rig.processed.set(key, '90')
    const reply = await worker.onDrain({
      ends: { [key]: '100' },
      deadline: worker.runtime.clock.now() + 40,
    })
    expect(reply).toEqual({ reached: false })
    expect(commitOffsets).toHaveBeenCalledWith([{ topic: main, partition: 0, offset: '90' }])
  })

  test('a member is not asked to reach a log it does not read', async () => {
    const { worker, rig, commitOffsets } = consumer()
    worker.seeded = true
    const txn = `${config.topics.transactions}|0`
    const reply = await worker.onDrain({ ends: { [txn]: '50' }, deadline: Infinity })
    expect(reply).toEqual({ reached: true })
    expect(commitOffsets).not.toHaveBeenCalled()
    expect(rig.processed.size).toBe(0)
  })

  test('a share member waits until the last offset of the log is delivered and the deliveries go quiet', async () => {
    const runtime = workerRuntime('c07')
    const worker = new ShareWorker(runtime)
    worker.standing.push({ id: 'c07.1', slot: config.clients.find((c) => c.slot === 'c07') })
    worker.delivered(`${main}|0`, '98')
    setTimeout(() => worker.delivered(`${main}|0`, '99'), 60)
    const reply = await worker.onDrain({
      ends: { [key]: '100' },
      deadline: runtime.clock.now() + 5000,
      quietMs: 40,
    })
    expect(reply).toEqual({ reached: true })
    const late = await worker.onDrain({
      ends: { [key]: '120' },
      deadline: runtime.clock.now() + 40,
      quietMs: 10,
    })
    expect(late).toEqual({ reached: false })
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

  test('every work step of the smoke ends before the next starts and before the drain', () => {
    const work = plan.steps.filter((s) => s.lane === 'work' && s.shape && s.shape !== 'traffic')
    for (let i = 1; i < work.length; i++)
      expect(work[i - 1].deadline).toBeLessThanOrEqual(work[i].at)
  })

  test('the day orders its twelve outages two seconds, twelve seconds, two seconds, twelve seconds', () => {
    const day = planSchedule(matrix, 'acceptance').steps.filter((s) => s.shape === 'broker-outage')
    expect(day).toHaveLength(12)
    expect(day.map((o) => o.downMs)).toEqual(
      Array.from({ length: 12 }, (_, i) => (i % 2 === 0 ? 2000 : 12000))
    )
    expect(day.map((o) => o.node)).toEqual(Array.from({ length: 12 }, (_, i) => (i % 3) + 1))
    expect(day.map((o) => o.stop)).toEqual(
      Array.from({ length: 12 }, (_, i) => (i % 2 === 0 ? 'clean' : 'hard'))
    )
  })
})
