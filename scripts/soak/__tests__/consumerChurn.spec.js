const { EventEmitter } = require('node:events')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const v8 = require('node:v8')
const vm = require('node:vm')
const { KafkaJSCommitFailedError } = require('../../../index')
const {
  CONSUMER_CHURN,
  ChurnClientLog,
  ChurnDeadline,
  ChurnGroups,
  ChurnJournal,
  ConsumerChurnOracle,
  FrameTally,
  GapSampler,
  OfferScheduler,
  PublicConsumerProbe,
  acknowledgedViolations,
  consumerChurnPlan,
  offsetStateViolations,
  consumerControls,
  interningControl,
  judgeQuietHeap,
  quietViolations,
  runControls,
  shapeHeapGrowth,
} = require('../consumerChurn')
const {
  ChurnLogReader,
  ChurnMember,
  ConsumerChurnRun,
  OffsetStateSampler,
} = require('../consumerChurnRun')
const {
  PROFILES,
  WORKLOADS,
  buildConsumerChurn,
  main,
  offsetStateReader,
  parseArgs,
} = require('../soak')
const { evidencePath } = require('../../../testHelpers/e2eEvidence')

const MIB = 1024 * 1024

/** @returns {{ now: () => number, advance: (ms: number) => void, sleep: Function }} a manual clock */
const manualClock = () => {
  const clock = { at: 0 }
  return {
    now: () => clock.at,
    advance: (ms) => {
      clock.at += ms
    },
    sleep: async (ms) => {
      clock.at += ms
    },
  }
}

const frame = (...ints) => {
  const body = Buffer.alloc(ints.length * 4)
  ints.forEach((value, index) => body.writeInt32BE(value, index * 4))
  const size = Buffer.alloc(4)
  size.writeInt32BE(body.length)
  return Buffer.concat([size, body])
}

describe('Consumer churn profile > plan and registry', () => {
  test('registers four full five-minute phases and a three-minute warm-up for acceptance', () => {
    expect(PROFILES['consumer-churn'].map(({ name, ms }) => [name, ms])).toEqual([
      ['member-churn', 300000],
      ['regex-topics', 300000],
      ['pause-recreate', 300000],
      ['mostly-idle', 300000],
    ])
    const plan = consumerChurnPlan()
    expect(plan).toMatchObject({ acceptance: true, warmupMs: 180000 })
    expect(plan.warmups.map(({ name, workload }) => [name, workload])).toEqual([
      ['warm-member-churn', 'memberChurn'],
      ['warm-regex-topics', 'regexTopics'],
      ['warm-pause-recreate', 'pauseRecreate'],
      ['warm-mostly-idle', 'mostlyIdle'],
    ])
    expect(plan.warmups.map(({ workload }) => workload)).toEqual(
      plan.phases.map(({ workload }) => workload)
    )
    expect(plan.warmups.reduce((total, { ms }) => total + ms, 0)).toBe(plan.warmupMs)
    expect(plan.warmups.every(({ ms }) => ms >= 20000)).toBe(true)
    expect(Math.min(...plan.warmups.map(({ ms }) => ms))).toBe(
      plan.warmups.find(({ workload }) => workload === 'mostlyIdle').ms
    )
    expect(plan.phases.map(({ ms }) => ms)).toEqual([300000, 300000, 300000, 300000])
    expect(parseArgs(['--profile', 'consumer-churn'])).toEqual({
      profile: 'consumer-churn',
      phaseSeconds: null,
    })
  })

  test('each phase workload runs the phase of its own name through the owner', async () => {
    const owner = { runPhase: jest.fn(async () => {}) }
    for (const { name, workload } of PROFILES['consumer-churn']) {
      await WORKLOADS[workload]({ consumerChurn: owner }, 7)
      expect(owner.runPhase).toHaveBeenLastCalledWith(name, 7)
    }
  })

  test('a shortened run is never acceptance and takes no warm-up', () => {
    expect(consumerChurnPlan(5)).toMatchObject({ acceptance: false, warmupMs: 0, warmups: [] })
    expect(consumerChurnPlan(5).phases.every(({ ms }) => ms === 5000)).toBe(true)
  })

  test('the declared budget leaves the whole child inside 1800000 ms', () => {
    const { warmupMs, setupAndWarmupMs, phaseMs, transitionMs, finalMs, childMs } = CONSUMER_CHURN
    expect(setupAndWarmupMs + 4 * phaseMs + 3 * transitionMs + finalMs).toBe(childMs)
    expect(warmupMs).toBeLessThan(setupAndWarmupMs)
  })

  test('main selects the profile before it creates a topic or an internal probe', async () => {
    const exposed = globalThis.gc
    globalThis.gc = () => {}
    const owner = {
      run: jest.fn(async () => ({ ok: true, acceptance: false })),
      close: jest.fn(async () => {}),
    }
    const build = jest.fn(() => owner)
    const created = ['.journal', '.clientlog']
      .map((ext) => evidencePath('consumer-churn').replace(/\.jsonl$/, ext))
      .filter((file) => !fs.existsSync(file))
    try {
      const verdict = await main('consumer-churn', {
        brokers: ['unreachable.invalid:1'],
        phaseSeconds: 5,
        consumerChurn: build,
      })
      expect(verdict).toEqual({ ok: true, acceptance: false })
    } finally {
      globalThis.gc = exposed
      for (const file of created) fs.rmSync(file, { force: true })
    }
    expect(build).toHaveBeenCalledWith(expect.objectContaining({ phaseSeconds: 5 }))
    expect(owner.close).toHaveBeenCalledTimes(1)
  })
})

describe('Consumer churn profile > deadline and offers', () => {
  test('a stage that cannot fit the remaining budget is refused, not shortened', () => {
    const clock = manualClock()
    const deadline = new ChurnDeadline(clock, 1000)
    deadline.require(1000, 'the whole plan')
    clock.advance(600)
    expect(deadline.remaining()).toBe(400)
    expect(() => deadline.require(500, 'a phase')).toThrow('a phase needs 500 ms, 400 left')
    clock.advance(900)
    expect(deadline.remaining()).toBe(0)
  })

  test('offers on a deadline schedule, makes up a late tick and records the slip', async () => {
    const clock = manualClock()
    const scheduler = new OfferScheduler({ rate: 100, clock, sleep: clock.sleep })
    const offered = []
    await scheduler.run(1000, (index) => {
      offered.push(index)
      return Promise.resolve()
    })
    expect(offered).toHaveLength(100)
    expect(scheduler.stats).toMatchObject({
      offered: 100,
      settled: 100,
      failed: 0,
      backpressure: 0,
    })
  })

  test('never has more than the in-flight limit unsettled and counts backpressure', async () => {
    const clock = manualClock()
    const settle = []
    let sleeps = 0
    const sleep = async (ms) => {
      clock.advance(ms)
      if (++sleeps % 10 === 0) settle.splice(0).forEach((resolve) => resolve())
    }
    const scheduler = new OfferScheduler({ rate: 1000, maxInFlight: 4, clock, sleep })
    let high = 0
    await scheduler.run(50, () => {
      const pending = new Promise((resolve) => settle.push(resolve))
      high = Math.max(high, scheduler.inFlight + 1)
      return pending
    })
    expect(high).toBeLessThanOrEqual(4)
    expect(scheduler.stats.backpressure).toBeGreaterThan(0)
    expect(scheduler.inFlight).toBe(0)
  })

  test('counts a failed offer apart from a settled one', async () => {
    const clock = manualClock()
    const scheduler = new OfferScheduler({ rate: 10, clock, sleep: clock.sleep })
    await scheduler.run(500, (index) =>
      index % 2 === 0 ? Promise.resolve() : Promise.reject(new Error('refused'))
    )
    expect(scheduler.stats.failed).toBeGreaterThan(0)
    expect(scheduler.stats.settled + scheduler.stats.failed).toBe(scheduler.stats.offered)
  })
})

describe('Consumer churn profile > handler entry gaps', () => {
  test('measures consecutive gaps per group and phase and holds them to the rate and sample floor', () => {
    const gaps = new GapSampler({ capacity: 64, minimum: 3 })
    gaps.prepare('g', 'phase')
    for (let n = 0; n < 5; n++) gaps.enter('g', 'phase', n * 5)
    const { summary, violations } = gaps.judge(1000)
    expect(summary[0]).toMatchObject({ key: 'g|phase', count: 4, p50: 5, p90: 5, p99: 5 })
    expect(violations).toEqual([expect.stringContaining('below 200')].concat([]).slice(0, 1))
  })

  test('a negative gap is a clock anomaly and too few gaps are a failure, never a pass', () => {
    const gaps = new GapSampler({ capacity: 8, minimum: 5 })
    gaps.prepare('g', 'p')
    gaps.enter('g', 'p', 10)
    gaps.enter('g', 'p', 4)
    const { violations } = gaps.judge(1000)
    expect(violations).toEqual(
      expect.arrayContaining([
        expect.stringContaining('1 clock anomalies'),
        expect.stringContaining('fewer than 5'),
      ])
    )
  })

  test('refuses an entry for a series that was never prepared', () => {
    expect(() => new GapSampler().enter('g', 'p', 1)).toThrow('No gap series for g in p')
  })

  test('keeps storage fixed however many entries arrive', () => {
    const gaps = new GapSampler({ capacity: 16, minimum: 1 })
    gaps.prepare('g', 'p')
    const storage = gaps.series.get('g|p').gaps
    for (let n = 0; n < 1000; n++) gaps.enter('g', 'p', n)
    expect(gaps.series.get('g|p').gaps).toBe(storage)
    expect(storage.length).toBe(16)
  })
})

describe('Consumer churn profile > journal on disk', () => {
  let dir
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'churn-journal-'))
  })
  afterEach(() => fs.rmSync(dir, { recursive: true, force: true }))

  test('writes full segments as they fill and replays only what is new, in order', async () => {
    const journal = new ChurnJournal({ file: path.join(dir, 'j.journal'), segment: 3 })
    for (let n = 0; n < 7; n++) journal.append({ n })
    expect(journal.written).toBe(6)
    const batches = []
    expect(await journal.replay(async (records) => batches.push(records.map(({ n }) => n)))).toBe(7)
    expect(batches).toEqual([[0, 1, 2], [3, 4, 5], [6]])
    journal.append({ n: 7 })
    const later = []
    expect(await journal.replay(async (records) => later.push(...records.map(({ n }) => n)))).toBe(
      1
    )
    expect(later).toEqual([7])
    journal.close()
    journal.close()
  })

  test('holds no more than one segment in memory while the run goes on', () => {
    const journal = new ChurnJournal({ file: path.join(dir, 'j.journal'), segment: 4096 })
    for (let n = 0; n < 10000; n++) journal.append({ n })
    expect(journal.staged.length).toBeLessThan(4096)
    journal.close()
  })
})

describe('Consumer churn profile > public probe', () => {
  const fakeSocket = () =>
    Object.assign(new EventEmitter(), { write: jest.fn(() => true), destroyed: false })
  const probed = () => {
    const sockets = []
    const probe = new PublicConsumerProbe({ activeResources: () => ({ Timeout: 0 }) })
    const factory = probe.wrap(() => {
      const socket = fakeSocket()
      sockets.push(socket)
      return socket
    })
    return { probe, factory, sockets }
  }

  test('splits fragmented and coalesced frames into correlated requests and answers', () => {
    const request = new FrameTally(4)
    const first = frame(0x00120007, 41, 0)
    expect(request.push(first.subarray(0, 5))).toEqual([])
    expect(request.push(first.subarray(5))).toEqual([41])
    const both = Buffer.concat([frame(1, 42), frame(1, 43)])
    expect(request.push(both)).toEqual([42, 43])
    const answer = new FrameTally(0)
    expect(answer.push(Buffer.concat([frame(41, 9), frame(42, 9).subarray(0, 6)]))).toEqual([41])
  })

  test('counts a request outstanding until its answer, and abandons it only when the socket closes', async () => {
    const { probe, factory, sockets } = probed()
    const socket = factory({})
    socket.write(frame(0x00010002, 7))
    socket.write(frame(0x00010002, 8))
    socket.emit('data', frame(7, 0))
    expect((await probe.snapshot()).requests).toMatchObject({ outstanding: 1, abandoned: 0 })
    sockets[0].emit('close')
    expect(await probe.snapshot()).toMatchObject({
      sockets: { created: 1, destroyed: 1, open: 0 },
      requests: { outstanding: 0, abandoned: 1 },
    })
  })

  test('counts the listeners left on live sockets and on a socket as it closes', async () => {
    const { probe, factory } = probed()
    const socket = factory({})
    socket.on('error', () => {})
    expect((await probe.snapshot()).listeners.open).toBeGreaterThan(1)
    socket.emit('close')
    const closed = await probe.snapshot()
    expect(closed.listeners.open).toBe(0)
    expect(closed.listeners.closed).toBeGreaterThan(0)
  })

  describe('listeners left on closed sockets', () => {
    /** The product keeps one inert `error` listener on a closed socket; `extra` adds another event. */
    const closing = (probe, count, extra = []) => {
      const factory = probe.wrap(() => {
        const socket = fakeSocket()
        socket.on('error', () => {})
        for (const event of extra) socket.on(event, () => {})
        return socket
      })
      for (let n = 0; n < count; n++) factory({}).emit('close')
    }

    test('reads the same after 38 closes as after 500, with the product leaving one listener per socket', async () => {
      const probe = new PublicConsumerProbe({ activeResources: () => ({}) })
      closing(probe, 38)
      const baseline = await probe.snapshot()
      closing(probe, 500)
      const later = await probe.snapshot()
      expect(later.listeners.closed).toBe(baseline.listeners.closed)
      expect(quietViolations('after phase', later, baseline)).toEqual([])
    })

    test('names the one socket that kept more than the baseline, however many closed cleanly around it', async () => {
      const probe = new PublicConsumerProbe({ activeResources: () => ({}) })
      closing(probe, 38)
      const baseline = await probe.snapshot()
      closing(probe, 100)
      closing(probe, 1, ['data'])
      closing(probe, 100)
      const violations = quietViolations('after phase', await probe.snapshot(), baseline)
      expect(violations).toEqual([
        'after phase: a closed socket kept 1 more listener than the baseline',
      ])
    })

    test('keeps no closed socket alive', async () => {
      v8.setFlagsFromString('--expose-gc')
      const gc = vm.runInNewContext('gc')
      const probe = new PublicConsumerProbe({ activeResources: () => ({}) })
      const refs = []
      const factory = probe.wrap(() => {
        const socket = fakeSocket()
        refs.push(new WeakRef(socket))
        return socket
      })
      for (let n = 0; n < 10; n++) factory({}).emit('close')
      await new Promise((resolve) => setImmediate(resolve))
      gc()
      expect(refs.filter((ref) => ref.deref() !== undefined)).toHaveLength(0)
    })
  })

  test('tracks and removes the registrations made on a client', async () => {
    const { probe, factory } = probed()
    factory({})
    const events = new EventEmitter()
    const client = {
      events: { REQUEST_QUEUE_SIZE: 'queue' },
      on: (name, handler) => {
        events.on(name, handler)
        return () => events.off(name, handler)
      },
    }
    const remove = probe.track(client)
    events.emit('queue', { payload: { queueSize: 3 } })
    expect((await probe.snapshot()).listeners.registrations).toBe(1)
    expect((await probe.snapshot()).requests.queueSize).toBe(3)
    remove()
    expect((await probe.snapshot()).listeners.registrations).toBe(0)
    expect(events.listenerCount('queue')).toBe(0)
  })

  test('refuses a reading when no socket was ever observed', async () => {
    await expect(
      new PublicConsumerProbe({ activeResources: () => ({}) }).snapshot()
    ).rejects.toThrow('observed no socket')
  })

  describe('quiet comparison', () => {
    const reading = (overrides = {}) => ({
      sockets: { created: 2, destroyed: 2, open: 0 },
      requests: { outstanding: 0, abandoned: 0, queueSize: 0 },
      listeners: { open: 0, closed: 0, registrations: 0 },
      activeResources: { Timeout: 1, TTYWrap: 1 },
      ...overrides,
    })
    test('accepts a reading equal to the baseline', () => {
      expect(quietViolations('after phase', reading(), reading())).toEqual([])
    })
    test.each([
      [
        'a socket still open',
        { sockets: { created: 2, destroyed: 1, open: 1 } },
        'sockets still open',
      ],
      [
        'a request outstanding',
        { requests: { outstanding: 2, abandoned: 0, queueSize: 0 } },
        'requests still outstanding',
      ],
      [
        'a request a closed socket abandoned',
        { requests: { outstanding: 0, abandoned: 1, queueSize: 0 } },
        'abandoned',
      ],
      [
        'a listener left on a socket',
        { listeners: { open: 0, closed: 3, registrations: 0 } },
        'a closed socket kept 3 more listeners than the baseline',
      ],
      [
        'a registration left',
        { listeners: { open: 0, closed: 0, registrations: 1 } },
        'registrations listeners left',
      ],
      [
        'a resource type outside the usual two',
        { activeResources: { Timeout: 1, TTYWrap: 1, FSReqCallback: 2 } },
        'FSReqCallback 2, baseline 0',
      ],
    ])('names %s', (_name, overrides, text) => {
      expect(quietViolations('after phase', reading(overrides), reading()).join('\n')).toContain(
        text
      )
    })
  })
})

describe('Consumer churn profile > heap screen and controls', () => {
  const series = (heaps, perBoundary = 3, boundaries = ['warm', 'a', 'b']) =>
    boundaries.flatMap((boundary, b) =>
      Array.from({ length: perBoundary }, (_, n) => ({
        boundary,
        atMs: (b * perBoundary + n) * 30000,
        heapUsed: heaps(b * perBoundary + n),
      }))
    )
  const bounds = { boundaries: ['warm', 'a', 'b'] }

  test('accepts a flat series', () => {
    expect(
      judgeQuietHeap(
        series(() => 100 * MIB),
        bounds
      ).violations
    ).toEqual([])
  })
  test('refuses endpoint growth past two MiB', () => {
    const { violations } = judgeQuietHeap(
      series((n) => 100 * MIB + (n >= 6 ? 3 * MIB : 0)),
      bounds
    )
    expect(violations.join()).toContain('heap endpoint grew')
  })
  test('refuses a slope past 256 KiB a minute even inside the endpoint bound', () => {
    const { violations, slopeBytesPerMinute } = judgeQuietHeap(
      series((n) => 100 * MIB + n * 200 * 1024),
      bounds
    )
    expect(slopeBytesPerMinute).toBeGreaterThan(256 * 1024)
    expect(violations.join()).toContain('heap slope')
  })
  test('needs the minimum matched readings at every boundary', () => {
    const { violations } = judgeQuietHeap(
      series(() => 1, 2),
      bounds
    )
    expect(violations.join()).toContain('fewer than 3')
  })

  test('the interning control sees growth from distinct retained objects and fails an observer that does not', async () => {
    const heaps = (growth) => {
      const values = [0, growth, 0]
      return jest.fn(async () => values.shift() ?? 0)
    }
    expect(await interningControl({ heap: heaps(10 * MIB), count: 8 })).toMatchObject({ ok: true })
    expect(await interningControl({ heap: heaps(MIB), count: 8 })).toMatchObject({ ok: false })
  })

  test('every control is rejected by the oracle for its own invariant, never for another reason', async () => {
    const results = await runControls(['g'])
    expect(results).toHaveLength(consumerControls().length)
    expect(results.filter(({ ok }) => !ok)).toEqual([])
    expect(new Set(results.map(({ received }) => received)).size).toBeGreaterThan(5)
  })
})

describe('Consumer churn profile > a member', () => {
  const setup = ({ positions = { 0: '0' }, refuse = false, known = async () => true } = {}) => {
    const topics = { t: 't.1' }
    const appended = []
    const handlers = {}
    const consumer = {
      connect: jest.fn(async () => {}),
      subscribe: jest.fn(async () => {}),
      run: jest.fn(async (config) =>
        Object.assign(handlers, config.rebalanceListener, { each: config.eachMessage })
      ),
      commitOffsets: jest.fn(async () => {
        if (refuse) throw new KafkaJSCommitFailedError('fenced', { reason: 'membership-lost' })
      }),
      disconnect: jest.fn(async () => {}),
    }
    const committed = { ...positions }
    const admin = {
      fetchOffsets: jest.fn(async () => [
        {
          topic: 't',
          partitions: Object.entries(committed).map(([partition, offset]) => ({
            partition: Number(partition),
            offset,
          })),
        },
      ]),
    }
    consumer.commitOffsets.mockImplementation(async ([{ partition, offset }]) => {
      if (refuse) throw new KafkaJSCommitFailedError('fenced', { reason: 'membership-lost' })
      committed[partition] = offset
    })
    const gaps = { enter: jest.fn() }
    const member = new ChurnMember({
      consumer,
      admin,
      group: 'g',
      label: 'm',
      journal: { append: (record) => appended.push(record) },
      gaps,
      phase: 'p',
      clock: { now: () => 5 },
      handled: new Map(),
      incarnationOf: (topic) => topics[topic],
      acknowledged: known,
    })
    return { member, appended, handlers, consumer, gaps, committed, topics }
  }
  const message = (offset) => ({
    topic: 't',
    partition: 0,
    message: { offset: String(offset), value: Buffer.from(`t.1|s${offset}|   `) },
  })

  test('journals ownership, the broker position it acquired at, deliveries and a broker-read commit', async () => {
    const { member, appended, handlers, gaps } = setup({ positions: { 0: '-1' } })
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await handlers.each(message(0))
    await member.commitAll()
    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'handler', 'commit'])
    expect(appended[1]).toMatchObject({ next: '0' })
    expect(appended[2]).toMatchObject({
      owner: 'm-1',
      incarnation: 't.1',
      offset: '0',
      sequence: 's0',
    })
    expect(appended[3]).toMatchObject({ next: '1', brokerNext: '1', owner: 'm-1' })
    expect(gaps.enter).toHaveBeenCalledWith('g', 'p', 5)
  })

  test('journals a commit in flight before a revoke and does not repeat it after', async () => {
    const { member, appended, handlers, consumer, committed } = setup()
    let release
    const gate = new Promise((resolve) => (release = resolve))
    consumer.commitOffsets.mockImplementation(async ([{ partition, offset }]) => {
      await gate
      committed[partition] = offset
    })
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    for (let offset = 0; offset < 24; offset++) await handlers.each(message(offset))
    const delivering = handlers.each(message(24))
    const revoking = handlers.onPartitionsRevoked({ partitions: [{ topic: 't', partitions: [0] }] })
    release()
    await Promise.all([delivering, revoking])
    const kinds = appended.map(({ kind }) => kind).filter((kind) => kind !== 'handler')
    expect(kinds).toEqual(['acquire', 'position', 'commit', 'revoke'])
    expect(consumer.commitOffsets).toHaveBeenCalledTimes(1)
  })

  test('commits before it reports a revoke, and never commits for a lost partition', async () => {
    const revoked = setup()
    await revoked.member.start(['t'])
    await revoked.handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await revoked.handlers.each(message(0))
    await revoked.handlers.onPartitionsRevoked({ partitions: [{ topic: 't', partitions: [0] }] })
    expect(revoked.appended.map(({ kind }) => kind)).toEqual([
      'acquire',
      'position',
      'handler',
      'commit',
      'revoke',
    ])

    const lost = setup()
    await lost.member.start(['t'])
    await lost.handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await lost.handlers.each(message(0))
    await lost.handlers.onPartitionsLost({ partitions: [{ topic: 't', partitions: [0] }] })
    await lost.member.commitAll()
    expect(lost.appended.map(({ kind }) => kind)).toEqual([
      'acquire',
      'position',
      'handler',
      'loss',
    ])
    expect(lost.consumer.commitOffsets).not.toHaveBeenCalled()
  })

  /** The producer and log-reader observations of record `t.1|s<offset>`, as the oracle wants them first. */
  const acknowledgedAndRead = (offset) =>
    ['produce', 'read'].map((kind) => ({
      kind,
      incarnation: 't.1',
      topic: 't',
      partition: 0,
      offset: String(offset),
      sequence: `s${offset}`,
    }))
  const deleteRow = { kind: 'delete', incarnation: 't.1', topic: 't' }

  test('a revoke after its topic was deleted ends the ownership without a journaled boundary', async () => {
    const { member, appended, handlers, topics } = setup()
    await member.start([/^t/])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await handlers.each(message(0))
    await member.commitAll()
    const drained = appended.length
    delete topics.t
    member.noteJoin()
    expect(member.settledWithout(new Set(['t']))).toBe(false)

    await handlers.onPartitionsRevoked({ partitions: [{ topic: 't', partitions: [0] }] })
    member.noteJoin()

    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'handler', 'commit'])
    expect(member.settledWithout(new Set(['t']))).toBe(true)
    const rows = [...acknowledgedAndRead(0), ...appended.slice(0, drained), deleteRow]
    await expect(new ConsumerChurnOracle([], ['g']).feed(rows)).resolves.toBeUndefined()
  })

  test('a delivery after its topic was deleted is journaled, so the oracle rejects the duplicate', async () => {
    const { member, appended, handlers, topics } = setup()
    await member.start([/^t/])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await handlers.each(message(0))
    await member.commitAll()
    const drained = appended.length
    delete topics.t

    await handlers.each(message(0))

    expect(appended.slice(drained)).toEqual([
      expect.objectContaining({ kind: 'handler', incarnation: 't.1', offset: '0', sequence: 's0' }),
    ])
    const before = [...acknowledgedAndRead(0), ...appended.slice(0, drained), deleteRow]
    await expect(
      new ConsumerChurnOracle([], ['g']).feed([...before, ...appended.slice(drained)])
    ).rejects.toThrow('Consumer journal: unexpected handler record')
  })

  test('a commit the client refuses is neither an error nor an observation', async () => {
    const { member, appended, handlers } = setup({ refuse: true })
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await handlers.each(message(0))
    await expect(member.commitAll()).resolves.toBeUndefined()
    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'handler'])
  })

  test('a commit the client sends nothing for, because the run is stopping or still starting, stays owed and is no observation', async () => {
    const { KafkaJSError, KafkaJSNonRetriableError } = require('../../../index')
    const { member, appended, handlers, consumer } = setup()
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await handlers.each(message(0))
    const sendCommit = consumer.commitOffsets.getMockImplementation()
    const stopping = Object.assign(new KafkaJSError('The consumer is stopping'), {
      type: 'CONSUMER_STOPPING',
    })
    const starting = new KafkaJSNonRetriableError(
      'Consumer is not running, wait for consumer#run to resolve first'
    )
    for (const refusal of [stopping, starting]) {
      consumer.commitOffsets.mockImplementationOnce(async () => {
        throw refusal
      })
      await expect(member.commitAll()).resolves.toBeUndefined()
    }
    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'handler'])

    consumer.commitOffsets.mockImplementationOnce(sendCommit)
    await member.commitAll()
    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'handler', 'commit'])
    expect(appended[3]).toMatchObject({ next: '1', brokerNext: '1' })
  })

  test('a stop error after an attempt that may have been sent, with or without a cause, stays owed and is sent again', async () => {
    const { KafkaJSError } = require('../../../index')
    const { member, appended, handlers, consumer, committed } = setup()
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await handlers.each(message(0))
    const sendCommit = consumer.commitOffsets.getMockImplementation()
    const stopping = (cause) =>
      Object.assign(new KafkaJSError('The consumer is stopping', { cause }), {
        type: 'CONSUMER_STOPPING',
      })
    // A forced close or a call deadline rejects with no cause after the request reached the broker.
    for (const error of [stopping(undefined), stopping(new Error('lookup failed'))]) {
      consumer.commitOffsets.mockImplementationOnce(async (offsets) => {
        await sendCommit(offsets)
        throw error
      })
      await expect(member.commitAll()).resolves.toBeUndefined()
    }
    expect(committed[0]).toBe('1')
    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'handler'])

    await member.commitAll()
    expect(consumer.commitOffsets).toHaveBeenCalledTimes(3)
    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'handler', 'commit'])
    expect(appended[3]).toMatchObject({ next: '1', brokerNext: '1' })
    const rows = [...acknowledgedAndRead(0), ...appended]
    await expect(new ConsumerChurnOracle([], ['g']).feed(rows)).resolves.toBeUndefined()
  })

  describe('a commit that fails inside the handler', () => {
    const owned = async (options) => {
      const harness = setup(options)
      await harness.member.start(['t'])
      await harness.handlers.onPartitionsAssigned({
        partitions: [{ topic: 't', partitions: [0] }],
      })
      for (let offset = 0; offset < 24; offset++) await harness.handlers.each(message(offset))
      return harness
    }
    const produced = (offset) => ({
      kind: 'produce',
      incarnation: 't.1',
      topic: 't',
      partition: 0,
      offset: String(offset),
      sequence: `s${offset}`,
    })

    test('declares the failed record a retry, which the real oracle spends on its redelivery', async () => {
      const { appended, handlers, consumer } = await owned()
      consumer.commitOffsets.mockRejectedValueOnce(new Error('illegal generation'))
      await expect(handlers.each(message(24))).rejects.toThrow('illegal generation')
      expect(appended.slice(-2)).toMatchObject([
        { kind: 'handler', offset: '24' },
        { kind: 'replay', reason: 'retry', owner: 'm-1', from: '24', to: '25' },
      ])
      await handlers.each(message(24))
      const offsets = Array.from({ length: 25 }, (_, offset) => offset)
      const rows = [...offsets.map(produced), ...appended]
      await expect(new ConsumerChurnOracle([], ['g']).feed(rows)).resolves.toBeUndefined()
      await expect(
        new ConsumerChurnOracle([], ['g']).feed([...rows, ...appended.slice(-1)])
      ).rejects.toThrow('undeclared handler replay')
    })

    test('declares nothing once the ownership was lost while the commit ran', async () => {
      const { appended, handlers, consumer } = await owned()
      let fail
      consumer.commitOffsets.mockImplementationOnce(
        () => new Promise((resolve, reject) => (fail = reject))
      )
      const delivering = handlers.each(message(24))
      await new Promise(setImmediate)
      expect(consumer.commitOffsets).toHaveBeenCalledTimes(1)
      await handlers.onPartitionsLost({ partitions: [{ topic: 't', partitions: [0] }] })
      fail(new Error('illegal generation'))
      await expect(delivering).rejects.toThrow('illegal generation')
      expect(appended.map(({ kind }) => kind)).not.toContain('replay')
    })

    test('declares nothing for a record that was never journaled', async () => {
      const { appended, handlers, consumer } = await owned({ known: async () => false })
      consumer.commitOffsets.mockRejectedValueOnce(new Error('illegal generation'))
      await expect(handlers.each(message(24))).rejects.toThrow('illegal generation')
      expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position'])
    })
  })

  test('drops a delivery for a partition it does not own', async () => {
    const { member, appended, handlers } = setup()
    await member.start(['t'])
    await handlers.each(message(0))
    expect(appended).toEqual([])
  })

  test('acquires, positions and delivers under one incarnation, so the real oracle accepts them', async () => {
    const { member, appended, handlers } = setup()
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await handlers.each(message(0))
    expect(appended.slice(0, 3).map(({ kind, incarnation }) => [kind, incarnation])).toEqual([
      ['acquire', 't.1'],
      ['position', 't.1'],
      ['handler', 't.1'],
    ])
    const oracle = new ConsumerChurnOracle([], ['g'])
    await oracle.feed([
      {
        kind: 'produce',
        incarnation: 't.1',
        topic: 't',
        partition: 0,
        offset: '0',
        sequence: 's0',
      },
      ...appended,
    ])
  })

  test('ignores a topic that has no incarnation', async () => {
    const { member, appended, handlers } = setup()
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 'other', partitions: [0] }] })
    expect(appended).toEqual([])
  })

  test('a recreated topic is re-acquired under the new incarnation, from offset 0, before anything else', async () => {
    const { member, appended, handlers, topics, committed } = setup({ positions: { 0: '9' } })
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    topics.t = 't.2'
    await handlers.each({
      topic: 't',
      partition: 0,
      message: { offset: '0', value: Buffer.from('t.2|s0|   ') },
    })
    await member.commitAll()
    const kinds = appended
      .slice(2)
      .map(({ kind, incarnation, owner, next }) => [kind, incarnation, owner, next])
    expect(kinds).toEqual([
      ['acquire', 't.2', 'm-2', undefined],
      ['position', 't.2', 'm-2', '0'],
      ['handler', 't.2', 'm-2', undefined],
      ['commit', 't.2', 'm-2', '1'],
    ])
    expect(committed[0]).toBe('1')
  })

  test('ends a revoke under the incarnation it holds when the topic was recreated meanwhile', async () => {
    const { member, appended, handlers, topics } = setup()
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    topics.t = 't.2'
    await handlers.onPartitionsRevoked({ partitions: [{ topic: 't', partitions: [0] }] })
    expect(appended.slice(2).map(({ kind, incarnation }) => [kind, incarnation])).toEqual([
      ['acquire', 't.2'],
      ['position', 't.2'],
      ['revoke', 't.2'],
    ])
  })

  test('stop journals the end of every ownership before it disconnects, and nothing after', async () => {
    const { member, appended, handlers, consumer } = setup()
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    consumer.disconnect.mockImplementation(async () => {
      expect(appended.at(-1)).toMatchObject({ kind: 'revoke', owner: 'm-1' })
      await handlers.each(message(0))
    })
    await member.stop()
    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'revoke'])
  })

  test('journals a delivery only after the record is known acknowledged, and skips one that never is', async () => {
    let settle
    const waiting = new Promise((resolve) => {
      settle = resolve
    })
    const first = setup({ known: () => waiting })
    await first.member.start(['t'])
    await first.handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    const delivery = first.handlers.each(message(0))
    await new Promise(setImmediate)
    expect(first.appended.map(({ kind }) => kind)).toEqual(['acquire', 'position'])
    settle(true)
    await delivery
    expect(first.appended.at(-1)).toMatchObject({ kind: 'handler' })

    const never = setup({ known: async () => false })
    await never.member.start(['t'])
    await never.handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    await never.handlers.each(message(0))
    expect(never.appended.map(({ kind }) => kind)).toEqual(['acquire', 'position'])
    expect(never.gaps.enter).not.toHaveBeenCalled()
    await never.member.commitAll()
    expect(never.appended.at(-1)).toMatchObject({ kind: 'commit', next: '1' })
  })

  test('drops a delivery whose ownership ended while it waited for the acknowledgement', async () => {
    let settle
    const waiting = new Promise((resolve) => {
      settle = resolve
    })
    const { member, appended, handlers } = setup({ known: () => waiting })
    await member.start(['t'])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 't', partitions: [0] }] })
    const delivery = handlers.each(message(0))
    await handlers.onPartitionsLost({ partitions: [{ topic: 't', partitions: [0] }] })
    settle(true)
    await delivery
    expect(appended.map(({ kind }) => kind)).toEqual(['acquire', 'position', 'loss'])
  })
})

describe('Consumer churn profile > the run', () => {
  const record = (kind, fields = {}) => ({
    kind,
    group: 'g',
    owner: 'o',
    incarnation: 'i',
    topic: 't',
    partition: 0,
    offset: '0',
    sequence: 's0',
    ...fields,
  })
  const corpus = (journal) => {
    for (const entry of [
      record('produce'),
      record('acquire'),
      record('read'),
      record('handler'),
      record('commit', { next: '1', brokerNext: '1' }),
      record('revoke'),
      record('delete'),
    ])
      journal.append(entry)
  }
  const quietReading = () => ({
    sockets: { created: 1, destroyed: 1, open: 0 },
    requests: { outstanding: 0, abandoned: 0, queueSize: 0 },
    listeners: { open: 0, closed: 0, registrations: 0 },
    activeResources: { Timeout: 0 },
    heapUsed: 100 * MIB,
  })
  let dir
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'churn-run-'))
  })
  afterEach(() => fs.rmSync(dir, { recursive: true, force: true }))

  /** A plan with a one-pass-each warm-up of tiny lengths. */
  const warmPlan = () => ({
    ...consumerChurnPlan(1),
    warmupMs: 10,
    warmups: [
      { name: 'warm-member-churn', workload: 'memberChurn', ms: 2 },
      { name: 'warm-regex-topics', workload: 'regexTopics', ms: 3 },
      { name: 'warm-pause-recreate', workload: 'pauseRecreate', ms: 4 },
      { name: 'warm-mostly-idle', workload: 'mostlyIdle', ms: 1 },
    ],
  })

  const build = ({
    plan = consumerChurnPlan(1),
    workloads,
    heap = async () => 100 * MIB,
    clock = manualClock(),
    journalFeed = corpus,
    clientLog,
    snapshot,
  } = {}) => {
    const order = []
    const lines = []
    const journal = new ChurnJournal({ file: path.join(dir, 'run.journal') })
    const gaps = new GapSampler({ capacity: 8, minimum: 1 })
    const names = ['memberChurn', 'regexTopics', 'pauseRecreate', 'mostlyIdle']
    const real = {
      setup: async () => order.push('setup'),
      drain: async (_, phase) => order.push(`drain ${phase}`),
      close: async () => order.push('close'),
      ...Object.fromEntries(
        names.map((name) => [
          name,
          async (context) => {
            order.push(name)
            if (name === 'regexTopics') {
              context.record({ kind: 'regex-rotation', phase: context.phase(), deletions: 0 })
              for (const group of context.groups) context.record(offsetRow(context.phase(), group))
            }
            if (!context.measured()) return
            for (const group of context.groups) {
              context.gaps.enter(group, context.phase(), 0)
              context.gaps.enter(group, context.phase(), 1)
            }
            context.stats.set(context.phase(), { acked: 1000 })
            if (name === 'memberChurn') journalFeed(context.journal)
          },
        ])
      ),
      ...workloads,
    }
    const run = new ConsumerChurnRun({
      workloads: real,
      probe: {
        openSockets: () => 0,
        snapshot: jest.fn(async () => {
          clock.advance(1000)
          return snapshot ? snapshot() : quietReading()
        }),
      },
      journal,
      clientLog,
      record: (line) => lines.push(line),
      plan,
      clock,
      sleep: clock.sleep,
      gaps,
      groups: ['g'],
      heap,
      progressMs: 10,
    })
    return { run, order, lines }
  }

  test('runs setup, the phases in order and the final verdict, and closes everything once', async () => {
    const { run, order, lines } = build()
    const verdict = await run.run()
    expect(order).toEqual([
      'setup',
      'memberChurn',
      'drain member-churn',
      'regexTopics',
      'drain regex-topics',
      'pauseRecreate',
      'drain pause-recreate',
      'mostlyIdle',
      'drain mostly-idle',
      'close',
    ])
    expect(verdict).toMatchObject({
      acceptance: false,
      counts: { acknowledged: 1, delivered: 1, commits: 1 },
    })
    expect(lines.filter(({ kind }) => kind === 'verdict')).toHaveLength(1)
    expect(lines.filter(({ kind }) => kind === 'quiet')).toHaveLength(15)
    expect(verdict.controls.every(({ ok }) => ok)).toBe(true)
  })

  test('records a lost round with the client log lines near it, and ignores a far or other-group line', async () => {
    const wall = { at: 100000 }
    const clientLog = new ChurnClientLog({
      file: path.join(dir, 'run.clientlog'),
      wall: () => wall.at,
    })
    const warn = (group, message, at) =>
      clientLog.add({
        namespace: 'Consumer',
        level: 2,
        log: { message, groupId: group, timestamp: new Date(at).toISOString() },
      })
    const { run, lines } = build({
      clientLog,
      workloads: {
        memberChurn: async (context) => {
          corpus(context.journal)
          warn('g', 'before', 98000)
          warn('other', 'elsewhere', 100000)
          warn('g', 'too early', 80000)
          clientLog.noteLoss({ group: 'g', member: 'm-1' })
          wall.at += 1000
          warn('g', 'after', wall.at)
          wall.at += 10000
        },
      },
    })
    await run.run()
    const rows = lines.filter(({ kind }) => kind === 'loss')
    expect(rows).toEqual([
      {
        kind: 'loss',
        group: 'g',
        member: 'm-1',
        at: 100000,
        clientLog: { count: 2, first: ['before', 'after'] },
      },
    ])
  })

  test('runs one pass of every shape before the baseline, each in a phase of its own', async () => {
    const { run, order, lines } = build({ plan: warmPlan() })
    const seen = []
    for (const name of ['memberChurn', 'regexTopics', 'pauseRecreate', 'mostlyIdle']) {
      const real = run.workloads[name]
      run.workloads[name] = async (context, ms) => {
        seen.push([name, context.phase(), context.measured(), ms])
        await real(context, ms)
      }
    }
    await run.run()
    expect(seen.slice(0, 4)).toEqual([
      ['memberChurn', 'warm-member-churn', false, 2],
      ['regexTopics', 'warm-regex-topics', false, 3],
      ['pauseRecreate', 'warm-pause-recreate', false, 4],
      ['mostlyIdle', 'warm-mostly-idle', false, 1],
    ])
    expect(seen.slice(4).map(([, phase, measured]) => [phase, measured])).toEqual([
      ['member-churn', true],
      ['regex-topics', true],
      ['pause-recreate', true],
      ['mostly-idle', true],
    ])
    expect(order.slice(0, 5)).toEqual([
      'setup',
      'memberChurn',
      'drain warm-member-churn',
      'regexTopics',
      'drain warm-regex-topics',
    ])
    const boundaries = lines.filter(({ kind }) => kind === 'quiet').map(({ boundary }) => boundary)
    expect(boundaries.indexOf('warm')).toBe(15)
    expect([...new Set(boundaries.slice(0, 15))]).toEqual([
      'warm-up',
      'warm-member-churn',
      'warm-regex-topics',
      'warm-pause-recreate',
      'warm-mostly-idle',
    ])
  })

  test('a plan with no warm-up passes takes no warm-up reading', async () => {
    const { run, order, lines } = build()
    await run.run()
    expect(order.slice(0, 2)).toEqual(['setup', 'memberChurn'])
    expect(lines.filter(({ kind }) => kind === 'quiet')).toHaveLength(15)
    expect(run.warmQuiets).toEqual([])
  })

  test('the run verdict fails on an excessive or a missing offset-state row in a warm-up or a measured regex pass', async () => {
    const excessive = (phase) => [
      { kind: 'regex-rotation', phase, deletions: 0 },
      offsetRow(phase, 'g', { offsetState: presentState(LIVE_NAMES, { topics: 9, cells: 1 }) }),
    ]
    const periodicMissing = (phase) => [
      { kind: 'regex-rotation', phase, deletions: 5 },
      offsetRow(phase, 'g', { deletions: 5 }),
    ]
    for (const [pass, rows, violation] of [
      [
        'warm-regex-topics',
        excessive,
        'offset state of g in warm-regex-topics (final): 9 stamp topics for 4 live topics',
      ],
      [
        'warm-regex-topics',
        periodicMissing,
        'offset state of g in warm-regex-topics: no periodic sample at 5 deletions',
      ],
      [
        'regex-topics',
        excessive,
        'offset state of g in regex-topics (final): 9 stamp topics for 4 live topics',
      ],
      [
        'regex-topics',
        periodicMissing,
        'offset state of g in regex-topics: no periodic sample at 5 deletions',
      ],
    ]) {
      const { run, lines } = build({ plan: warmPlan() })
      const fake = run.workloads.regexTopics
      run.workloads.regexTopics = async (context, ms) => {
        if (context.phase() !== pass) return fake(context, ms)
        for (const row of rows(pass)) context.record(row)
      }

      const verdict = await run.run()

      expect(verdict).toMatchObject({ ok: false, violations: [violation] })
      expect(lines.filter(({ kind }) => kind === 'verdict')).toEqual([
        expect.objectContaining({ kind: 'verdict', ok: false, violations: [violation] }),
      ])
    }
  })

  test('the warm-up readings stay out of the heap bound and the leftovers check', async () => {
    let call = 0
    const { run } = build({
      plan: warmPlan(),
      snapshot: () => ({ ...quietReading(), heapUsed: (call++ < 15 ? 10 : 100) * MIB }),
    })
    const verdict = await run.run()
    expect(verdict.violations).toEqual([])
    expect(verdict.heap.endpointBytes).toBe(0)
  })

  test('a shape that keeps growing in its second pass is shown in the verdict, not failed', async () => {
    let call = 0
    const { run, lines } = build({
      plan: warmPlan(),
      snapshot: () => {
        call += 1
        const boundary = Math.floor((call - 1) / 3)
        const bytes = [10, 11, 13, 13, 14, 14, 15, 18, 18, 18][boundary] * 1000
        return { ...quietReading(), heapUsed: bytes }
      },
    })
    const verdict = await run.run()
    expect(verdict.shapeHeap).toEqual([
      { shape: 'member-churn', firstPassBytes: 1000, secondPassBytes: 1000 },
      { shape: 'regex-topics', firstPassBytes: 2000, secondPassBytes: 3000 },
      { shape: 'pause-recreate', firstPassBytes: 0, secondPassBytes: 0 },
      { shape: 'mostly-idle', firstPassBytes: 1000, secondPassBytes: 0 },
    ])
    expect(lines.find(({ kind }) => kind === 'verdict').shapeHeap).toEqual(verdict.shapeHeap)
    expect(verdict.violations.join()).not.toContain('shape')
  })

  test('a journal that loses an acknowledged record fails the verdict through the oracle', async () => {
    const { run } = build({
      journalFeed: (journal) => {
        journal.append(record('produce'))
        journal.append(record('acquire'))
        journal.append(record('read'))
      },
    })
    const verdict = await run.run()
    expect(verdict.ok).toBe(false)
    expect(verdict.violations.join()).toContain('acknowledged handler loss')
  })

  test('verifies what every phase journaled, not only the first phase', async () => {
    const { run } = build({
      workloads: {
        regexTopics: async (context) => {
          context.gaps.enter('g', 'regex-topics', 0)
          context.gaps.enter('g', 'regex-topics', 1)
          context.stats.set('regex-topics', { acked: 1000 })
          context.journal.append(record('handler', { incarnation: 'late', owner: 'x' }))
        },
      },
    })
    await expect(run.run()).rejects.toThrow('unexpected handler record')
  })

  test('closes before it judges, so a cleanup failure is the only verdict', async () => {
    const { run, order, lines } = build({
      workloads: {
        close: async () => {
          order.push('close')
          throw new Error('client left open')
        },
      },
    })
    await expect(run.run()).rejects.toThrow('client left open')
    expect(order.filter((step) => step === 'close')).toHaveLength(1)
    expect(lines.filter(({ kind }) => kind === 'verdict')).toEqual([
      expect.objectContaining({ ok: false, violations: ['run failed: client left open'] }),
    ])
  })

  test('a run with no heap reader cannot judge memory and is not ok', async () => {
    const { run } = build({ heap: null })
    const verdict = await run.run()
    expect(verdict.ok).toBe(false)
    expect(verdict.violations.join()).toContain('no heap reader')
  })

  test('an acceptance plan that cannot fit the deadline is refused before any work', async () => {
    const clock = manualClock()
    clock.advance(0)
    const { run, order } = build({
      plan: consumerChurnPlan(),
      clock,
      workloads: {},
    })
    run.settings = { ...CONSUMER_CHURN, childMs: 1000 }
    await expect(run.run()).rejects.toThrow('Consumer churn budget: the whole plan')
    expect(order).toEqual(['close'])
  })

  test('a failing phase records a failed verdict and still closes, then rethrows the original error', async () => {
    const failure = new Error('phase broke')
    const { run, order, lines } = build({
      workloads: { regexTopics: async () => Promise.reject(failure) },
    })
    await expect(run.run()).rejects.toBe(failure)
    expect(order.at(-1)).toBe('close')
    expect(lines.at(-1)).toMatchObject({
      kind: 'verdict',
      ok: false,
      violations: ['run failed: phase broke'],
    })
  })

  test('leaves no listener, timer or exit call behind', async () => {
    const exit = jest.spyOn(process, 'exit').mockImplementation(() => {})
    const before = process.listenerCount('unhandledRejection')
    const { run } = build()
    await run.run()
    expect(process.listenerCount('unhandledRejection')).toBe(before)
    expect(run.timer._destroyed).toBe(true)
    expect(exit).not.toHaveBeenCalled()
    exit.mockRestore()
  })

  test('reports progress at the configured interval while it works', async () => {
    jest.useFakeTimers({ doNotFake: ['setImmediate'] })
    try {
      const { run, lines } = build({
        workloads: {
          memberChurn: async (context) => {
            await jest.advanceTimersByTimeAsync(35)
            corpus(context.journal)
            for (const group of context.groups) {
              context.gaps.enter(group, 'member-churn', 0)
              context.gaps.enter(group, 'member-churn', 1)
            }
            context.stats.set('member-churn', { acked: 1 })
          },
        },
      })
      await run.run()
      expect(lines.filter(({ kind }) => kind === 'progress').length).toBeGreaterThanOrEqual(3)
    } finally {
      jest.useRealTimers()
    }
  })
})

describe('Consumer churn profile > shape heap growth', () => {
  const points = { warm: ['w0', 'w1', 'w2'], measured: ['m0', 'm1', 'm2'], shapes: ['a', 'b'] }
  const at = (boundary, ...heaps) => heaps.map((heapUsed) => ({ boundary, heapUsed }))

  test('pairs each shape step in the warm-up with its step in the measured pass, by medians', () => {
    const readings = [
      ...at('w0', 100, 5000, 100),
      ...at('w1', 150, 150, 9000),
      ...at('w2', 400, 400, 400),
      ...at('m0', 400, 400, 400),
      ...at('m1', 410, 410, 410),
      ...at('m2', 200, 200, 200),
    ]
    expect(shapeHeapGrowth(readings, points)).toEqual([
      { shape: 'a', firstPassBytes: 50, secondPassBytes: 10 },
      { shape: 'b', firstPassBytes: 250, secondPassBytes: -210 },
    ])
  })

  test('is null for a step with a missing or heapless point, never a number made of nothing', () => {
    const readings = [
      ...at('w0', 1, 1, 1),
      ...at('w1', 2, 2, 2),
      ...at('m0', 5, 5, 5),
      ...at('m1', undefined),
    ]
    expect(shapeHeapGrowth(readings, points)).toEqual([
      { shape: 'a', firstPassBytes: 1, secondPassBytes: null },
      { shape: 'b', firstPassBytes: null, secondPassBytes: null },
    ])
  })
})

describe('Consumer churn profile > oracle sensitivity', () => {
  test('the oracle named by the profile is the one the run uses', () => {
    expect(new ConsumerChurnOracle([], ['g']).retained()).toEqual({ acknowledged: 0, scopes: 0 })
  })
})

describe('Consumer churn profile > group identities', () => {
  test('one mapping scopes every phase and every warm-up pass to groups of its own', () => {
    const groups = new ChurnGroups('tg')
    expect(groups.forPhase('member-churn')).toEqual([
      'member-churn-cooperative-tg',
      'member-churn-consumer-tg',
    ])
    expect(groups.id('x', 'warm-member-churn')).toBe('x-tg-wm')
    expect(groups.id('x', 'warm-regex-topics')).toBe('x-tg-wrx')
    expect(groups.id('x', 'warm-pause-recreate')).toBe('x-tg-wpause')
    expect(groups.id('x', 'warm-mostly-idle')).toBe('x-tg-widle')
    const phases = [...CONSUMER_CHURN.warmups, ...CONSUMER_CHURN.phases].map(({ name }) => name)
    const ids = phases.map((phase) => groups.id('x', phase))
    expect(new Set(ids).size).toBe(phases.length)
    expect(groups.id('x', 'regex-topics')).toBe('x-tg-rx')
    expect(groups.id('x', 'pause-recreate')).toBe('x-tg-pause')
    expect(groups.id('x', 'mostly-idle')).toBe('x-tg-idle')
    expect(() => groups.id('x', 'final')).toThrow('No consumer churn groups for phase final')
  })

  test('the run builds its oracle from the shared mapping, not from the base groups', async () => {
    const groups = new ChurnGroups('tg', ['b'])
    const run = new ConsumerChurnRun({
      workloads: {},
      scope: groups,
      probe: {},
      journal: {},
      record: () => {},
      plan: consumerChurnPlan(1),
      clock: manualClock(),
      sleep: async () => {},
      gaps: new GapSampler(),
      groups: ['b'],
    })
    expect(run.oracle.groups.sort()).toEqual(['b-tg', 'b-tg-idle', 'b-tg-pause', 'b-tg-rx'].sort())
    const produce = {
      kind: 'produce',
      incarnation: 'i',
      topic: 't',
      partition: 0,
      offset: '0',
      sequence: 's0',
      groups: ['b-tg-rx'],
    }
    await run.oracle.feed([produce])
    for (const group of ['b-tg-pause', 'b']) {
      await expect(
        run.oracle.feed([
          { ...produce, kind: 'acquire', group, owner: 'o' },
          { ...produce, kind: 'handler', group, owner: 'o' },
        ])
      ).rejects.toThrow('unexpected group')
      run.oracle.active.clear()
    }
  })

  test('the owner built for the profile joins the run and the workloads on one mapping', () => {
    const created = ['.journal', '.clientlog']
      .map((ext) => evidencePath('consumer-churn').replace(/\.jsonl$/, ext))
      .filter((file) => !fs.existsSync(file))
    const owner = buildConsumerChurn({ brokers: ['localhost:1'], phaseSeconds: 4 })
    try {
      expect(owner).toBeInstanceOf(ConsumerChurnRun)
      expect(owner.scope).toBe(owner.workloads.groups)
      expect(owner.oracle.groups).toEqual(
        expect.arrayContaining(owner.workloads.groups.forPhase('regex-topics'))
      )
    } finally {
      owner.journal.close()
      owner.clientLog.close()
      for (const file of created) fs.rmSync(file, { force: true })
    }
  })

  test('an acknowledged record is owed only to the groups it names', async () => {
    const base = { incarnation: 'i', topic: 't', partition: 0, offset: '0', sequence: 's0' }
    const journal = [
      { ...base, kind: 'produce', groups: ['a'] },
      { ...base, kind: 'acquire', group: 'a', owner: 'o' },
      { ...base, kind: 'read' },
      { ...base, kind: 'handler', group: 'a', owner: 'o' },
      { ...base, kind: 'revoke', group: 'a', owner: 'o' },
      { ...base, kind: 'delete' },
    ]
    await expect(new ConsumerChurnOracle(journal, ['a', 'b']).verify()).resolves.toMatchObject({
      delivered: 1,
    })
    const lost = journal.filter(({ kind }) => !['handler', 'revoke', 'delete'].includes(kind))
    await expect(new ConsumerChurnOracle(lost, ['a', 'b']).verify()).rejects.toThrow(
      'acknowledged handler loss'
    )
  })
})

describe('Consumer churn profile > the log reader', () => {
  const topic = (acks) => ({ name: 't', incarnation: 't.1', acks: new Map(acks) })
  const build = (deliver, { timeoutMs = 1000 } = {}) => {
    const appended = []
    const clock = manualClock()
    const consumer = {
      connect: jest.fn(async () => {}),
      subscribe: jest.fn(async () => {}),
      run: jest.fn(async (config) => deliver(config.eachMessage)),
    }
    const reader = new ChurnLogReader({
      consumer,
      journal: { append: (record) => appended.push(record) },
      clock,
      sleep: clock.sleep,
      timeoutMs,
    })
    return { reader, appended, consumer }
  }
  const record = (partition, offset, value) => ({
    topic: 't',
    partition,
    message: { offset: String(offset), value: Buffer.from(value) },
  })

  test('journals one read per acknowledged record, from the start, committing nothing', async () => {
    const { reader, appended, consumer } = build(async (each) => {
      await each(record(0, 0, 't.1|s0|'))
      await each(record(0, 0, 't.1|s0|'))
      await each(record(1, 5, 't.1|s1|'))
    })
    await reader.read([
      topic([
        ['0|0', 's0'],
        ['1|5', 's1'],
      ]),
    ])
    expect(appended).toEqual([
      { kind: 'read', incarnation: 't.1', topic: 't', partition: 0, offset: '0', sequence: 's0' },
      { kind: 'read', incarnation: 't.1', topic: 't', partition: 1, offset: '5', sequence: 's1' },
    ])
    expect(consumer.subscribe).toHaveBeenCalledWith({ topic: 't', fromBeginning: true })
    expect(consumer.run).toHaveBeenCalledWith(expect.objectContaining({ autoCommit: false }))
  })

  test('does not count a record of another incarnation, another sequence or no acknowledgement', async () => {
    const { reader, appended } = build(async (each) => {
      await each(record(0, 0, 't.0|s0|'))
      await each(record(0, 0, 't.1|s9|'))
      await each(record(0, 7, 't.1|s0|'))
    })
    await expect(reader.read([topic([['0|0', 's0']])])).rejects.toThrow(
      '1 acknowledged records were not read back within 1000 ms'
    )
    expect(appended).toEqual([])
  })
})

describe('Consumer churn profile > the quiet point', () => {
  test('waits a bounded time for closing sockets, then reads', async () => {
    const clock = manualClock()
    let open = 2
    const probe = {
      openSockets: jest.fn(() => (open > 0 ? open-- : 0)),
      snapshot: jest.fn(async () => ({
        sockets: { created: 1, destroyed: 1, open: 0 },
        requests: { outstanding: 0, abandoned: 0, queueSize: 0 },
        listeners: { open: 0, closed: 0, registrations: 0 },
        activeResources: {},
        heapUsed: 1,
      })),
    }
    const run = new ConsumerChurnRun({
      workloads: {},
      probe,
      journal: {},
      record: () => {},
      plan: consumerChurnPlan(1),
      clock,
      sleep: clock.sleep,
      gaps: new GapSampler(),
    })
    await run.quiet('warm')
    expect(clock.now()).toBe(200)
    expect(probe.snapshot).toHaveBeenCalledTimes(3)

    const stuck = { ...probe, openSockets: () => 1 }
    run.probe = stuck
    const before = clock.now()
    await run.quiet('after')
    expect(clock.now() - before).toBe(CONSUMER_CHURN.socketGraceMs)
  })

  test('the probe counts open sockets and drops its own data listener when one closes', () => {
    const sockets = []
    const probe = new PublicConsumerProbe({ activeResources: () => ({}) })
    const socket = Object.assign(new EventEmitter(), { write: () => true })
    sockets.push(socket)
    probe.wrap(() => socket)({})
    expect(probe.openSockets()).toBe(1)
    expect(socket.listenerCount('data')).toBe(1)
    socket.emit('close')
    expect(probe.openSockets()).toBe(0)
    expect(socket.listenerCount('data')).toBe(0)
  })
})

describe('Consumer churn profile > client log', () => {
  let dir
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'churn-clientlog-'))
  })
  afterEach(() => fs.rmSync(dir, { recursive: true, force: true }))

  const read = (file) =>
    fs
      .readFileSync(file, 'utf8')
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line))

  test('a WARN from a churn member lands with member and group; INFO and DEBUG do not', () => {
    const file = path.join(dir, 'c.clientlog')
    const clientLog = new ChurnClientLog({ file })
    const log = clientLog.creator()(2)
    const base = {
      namespace: 'ConsumerGroup',
      log: { groupId: 'g1', memberId: 'm-9', generationId: 4 },
    }
    log({
      ...base,
      level: 2,
      log: { ...base.log, message: 'stale', error: { name: 'KafkaJSProtocolError', code: 22 } },
    })
    log({ ...base, level: 1, log: { ...base.log, message: 'boom', error: 'text' } })
    log({ ...base, level: 4, log: { ...base.log, message: 'info' } })
    log({ ...base, level: 5, log: { ...base.log, message: 'debug' } })
    clientLog.close()
    expect(read(file)).toMatchObject([
      {
        member: 'm-9',
        group: 'g1',
        generation: 4,
        namespace: 'ConsumerGroup',
        message: 'stale',
        errorType: 'KafkaJSProtocolError',
        errorCode: 22,
      },
      { message: 'boom', error: 'text' },
    ])
  })

  test('a loss names the lines of no group, which carry the broker answer, beside its own', () => {
    const wall = { at: 100000 }
    const clientLog = new ChurnClientLog({
      file: path.join(dir, 'n.clientlog'),
      wall: () => wall.at,
    })
    const warn = (log, at) =>
      clientLog.add({
        namespace: 'x',
        level: 2,
        log: { ...log, timestamp: new Date(at).toISOString() },
      })
    warn(
      { message: 'Response OffsetCommit', error: 'Specified group generation id is not valid' },
      99990
    )
    warn({ message: 'Response Heartbeat', error: 'The group is rebalancing' }, 80000)
    warn({ message: 'elsewhere', groupId: 'other' }, 99995)
    clientLog.noteLoss({ group: 'g', member: 'm' })
    clientLog.close()
    expect(clientLog.settle(true)).toEqual([
      {
        group: 'g',
        member: 'm',
        at: 100000,
        count: 1,
        first: ['Response OffsetCommit: Specified group generation id is not valid'],
      },
    ])
  })

  test('the file and memory stay bounded', () => {
    const clientLog = new ChurnClientLog({ file: path.join(dir, 'b.clientlog'), maxLines: 3 })
    for (let index = 0; index < 1200; index++)
      clientLog.add({ namespace: 'n', level: 2, log: { message: `m${index}`, groupId: 'g' } })
    clientLog.close()
    expect(read(path.join(dir, 'b.clientlog'))).toHaveLength(3)
    expect(clientLog.dropped).toBe(1197)
    expect(clientLog.recent.length).toBeLessThanOrEqual(500)
  })

  test('a member tells the log about its loss and not about a revoke', async () => {
    const onLoss = jest.fn()
    const handlers = {}
    const member = new ChurnMember({
      consumer: {
        connect: async () => {},
        subscribe: async () => {},
        run: async (config) => Object.assign(handlers, config.rebalanceListener),
        commitOffsets: async () => {},
        disconnect: async () => {},
      },
      admin: {},
      group: 'g',
      label: 'm',
      journal: { append: () => {} },
      gaps: null,
      phase: 'p',
      clock: { now: () => 0 },
      handled: new Map(),
      onLoss,
      incarnationOf: () => 't.1',
      acknowledged: async () => true,
    })
    await member.start(['t'])
    const partitions = [{ topic: 't', partitions: [0] }]
    await handlers.onPartitionsRevoked({ partitions })
    await handlers.onPartitionsLost({ partitions })
    expect(onLoss.mock.calls).toEqual([[{ group: 'g', member: 'm' }]])
  })
})

describe('Consumer churn profile > acknowledged records', () => {
  const phases = [
    { name: 'a', ms: 300000 },
    { name: 'b', ms: 300000 },
  ]

  test('are held to the rate over the five planned minutes, not the drain after them', () => {
    const stats = new Map([
      ['a', { acked: 66000, measuredMs: 363000 }],
      ['b', { acked: 59999, measuredMs: 300000 }],
    ])
    expect(acknowledgedViolations(stats, phases, 200)).toEqual([
      'b: 59999 acknowledged records, fewer than 200/s',
    ])
  })

  test('a phase that recorded nothing acknowledged nothing', () => {
    expect(acknowledgedViolations(new Map([['a', { acked: 60000 }]]), phases, 200)).toEqual([
      'b: 0 acknowledged records, fewer than 200/s',
    ])
  })
})

const LIVE_NAMES = ['a', 'b', 'c', 'control']

/** A present store whose cache keys, subscription and claims all hold `names`, plus `counts`. */
function presentState(names = LIVE_NAMES, counts = {}) {
  const sorted = [...names].sort()
  return {
    present: true,
    topics: names.length,
    cells: 15,
    cache: names.length,
    cacheKeys: sorted,
    subscribed: sorted,
    claimed: sorted,
    unapplied: [],
    ...counts,
  }
}

/** One well-formed offset-state row: four live topics with fifteen partitions, within bounds. */
function offsetRow(phase, group, overrides = {}) {
  return {
    kind: 'offset-state',
    phase,
    group,
    checkpoint: 'final',
    deletions: 0,
    atMs: 0,
    waitedMs: 0,
    quiescent: true,
    live: { topics: 4, partitions: 15, names: LIVE_NAMES },
    offsetState: presentState(),
    ...overrides,
  }
}

describe('Consumer churn profile > offset state', () => {
  const sampler = ({ read, clock = manualClock(), quietMs = 1000, onSleep = () => {} } = {}) => {
    const rows = []
    const sleeps = []
    const instance = new OffsetStateSampler({
      read,
      clock,
      sleep: async (...args) => {
        sleeps.push(args)
        await clock.sleep(args[0])
        onSleep(sleeps.length)
      },
      record: (row) => rows.push(row),
      quietMs,
      pollMs: 100,
    })
    return { instance, rows, sleeps, clock }
  }
  const live = [
    { name: 'control', partitions: 6 },
    { name: 'a', partitions: 3 },
    { name: 'b', partitions: 3 },
    { name: 'c', partitions: 3 },
  ]
  /** A present store whose cache, subscription and claims all hold `names`. */
  const readState = (names, { claimed = names, subscribed = names } = {}) => ({
    present: true,
    topics: names.length,
    cells: 15,
    cache: names.length,
    cacheKeys: [...names].sort(),
    subscribed: [...subscribed].sort(),
    claimed: [...claimed].sort(),
    open: 0,
  })
  const liveNames = ['a', 'b', 'c', 'control']
  const finalOnly = { passes: ['regex-topics'], groupsOf: () => ['g'], deletionsOf: () => 1 }
  const stale =
    'offset state of g in regex-topics (final): cache keys outside the live and claimed topics: '
  /** A real member that held `old`, then saw its revoke and the join that followed. */
  const revokedMember = async () => {
    const handlers = {}
    const member = new ChurnMember({
      consumer: {
        connect: jest.fn(async () => {}),
        subscribe: jest.fn(async () => {}),
        run: jest.fn(async (config) => Object.assign(handlers, config.rebalanceListener)),
        commitOffsets: jest.fn(async () => {}),
        disconnect: jest.fn(async () => {}),
      },
      admin: { fetchOffsets: jest.fn(async () => [{ topic: 'old', partitions: [] }]) },
      group: 'g',
      label: 'm',
      journal: { append: () => {} },
      gaps: null,
      phase: 'p',
      clock: { now: () => 0 },
      handled: new Map(),
      incarnationOf: () => 'old.1',
      acknowledged: async () => true,
    })
    await member.start([/^o/])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 'old', partitions: [0] }] })
    member.noteJoin()
    await handlers.onPartitionsRevoked({ partitions: [{ topic: 'old', partitions: [0] }] })
    member.noteJoin()
    return member
  }

  test('a revoke and a join that precede the subscription application are not sampled as excess', async () => {
    const member = await revokedMember()
    let applied = false
    const before = [...liveNames, 'old']
    const { instance, rows } = sampler({
      read: () => (applied ? readState(liveNames) : readState(before)),
      onSleep: (n) => {
        if (n === 3) applied = true
      },
    })

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [member],
      live,
      deleted: new Set(['old']),
    })

    expect(rows).toEqual([
      expect.objectContaining({
        deletions: 1,
        atMs: 300,
        quiescent: true,
        offsetState: expect.objectContaining({ cache: 4, unapplied: [] }),
      }),
    ])
    expect(offsetStateViolations(rows, finalOnly)).toEqual([])
    await member.stop()
  })

  test('a cache key left after the subscription was applied and its claims released fails and names the topic', async () => {
    const member = await revokedMember()
    const { instance, rows } = sampler({
      read: () => ({
        ...readState([...liveNames, 'old'], { claimed: liveNames, subscribed: liveNames }),
        topics: 4,
      }),
    })

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [member],
      live,
      deleted: new Set(['old']),
    })

    expect(rows).toEqual([expect.objectContaining({ atMs: 0, quiescent: true })])
    expect(offsetStateViolations(rows, finalOnly)).toEqual([
      'offset state of g in regex-topics (final): cache keys outside the live and claimed topics: old',
    ])
    await member.stop()
  })

  test('a subscription that still claims a deleted topic at the cutoff fails as not applied, not as excess', async () => {
    const member = await revokedMember()
    const { instance, rows } = sampler({
      read: () => ({ ...readState([...liveNames, 'old']), topics: 4 }),
      quietMs: 1000,
    })

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [member],
      live,
      deleted: new Set(['old']),
    })

    expect(rows).toEqual([
      expect.objectContaining({
        waitedMs: 1000,
        quiescent: false,
        offsetState: expect.objectContaining({
          cacheKeys: ['a', 'b', 'c', 'control', 'old'],
          subscribed: ['a', 'b', 'c', 'control', 'old'],
          unapplied: ['old'],
        }),
      }),
    ])
    expect(offsetStateViolations(rows, finalOnly)).toEqual([
      'offset state of g in regex-topics (final): subscription not applied within 1.0 s: still subscribes old',
    ])
    await member.stop()
  })

  /** Samples one settled member over `state` at a final checkpoint after `deleted`. */
  const sampleOnce = async (state, deleted, quietMs = 1000) => {
    const { instance, rows } = sampler({ read: () => state, quietMs })
    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [{ group: 'g', settledWithout: () => true }],
      live,
      deleted: new Set(deleted),
    })
    return rows
  }

  test('a stale cache key is named even when a live topic has no key yet', async () => {
    const keys = ['a', 'b', 'control', 'old']
    const rows = await sampleOnce(
      { ...readState(keys, { claimed: liveNames, subscribed: liveNames }), topics: 4 },
      ['old']
    )

    expect(rows).toEqual([
      expect.objectContaining({
        atMs: 0,
        quiescent: true,
        offsetState: expect.objectContaining({ cache: 4, cacheKeys: keys, claimed: liveNames }),
      }),
    ])
    expect(offsetStateViolations(rows, finalOnly)).toEqual([`${stale}old`])
  })

  test('a live topic with no cache key yet is not a failure', async () => {
    const keys = ['a', 'b', 'control']
    const rows = await sampleOnce(
      { ...readState(keys, { claimed: liveNames, subscribed: liveNames }), cells: 12 },
      ['old']
    )

    expect(rows).toEqual([expect.objectContaining({ quiescent: true })])
    expect(offsetStateViolations(rows, finalOnly)).toEqual([])
  })

  test('a deleted topic only a claim holds at the cutoff fails as unreleased, not as an unapplied subscription', async () => {
    const rows = await sampleOnce(
      {
        ...readState([...liveNames, 'old'], { subscribed: liveNames }),
        topics: 4,
      },
      ['old']
    )

    expect(rows).toEqual([
      expect.objectContaining({
        quiescent: false,
        offsetState: expect.objectContaining({ subscribed: liveNames, unapplied: ['old'] }),
      }),
    ])
    expect(offsetStateViolations(rows, finalOnly)).toEqual([
      'offset state of g in regex-topics (final): claims not released within 1.0 s: still claims old',
    ])
  })

  test('a claim timeout still names a separate unclaimed stale cache key', async () => {
    const rows = await sampleOnce(
      {
        ...readState([...liveNames, 'old-a', 'old-b'], {
          claimed: [...liveNames, 'old-a'],
          subscribed: liveNames,
        }),
        topics: 4,
      },
      ['old-a', 'old-b']
    )

    expect(rows).toEqual([
      expect.objectContaining({
        quiescent: false,
        offsetState: expect.objectContaining({ unapplied: ['old-a'] }),
      }),
    ])
    expect(offsetStateViolations(rows, { ...finalOnly, deletionsOf: () => 2 })).toEqual([
      'offset state of g in regex-topics (final): claims not released within 1.0 s: still claims old-a',
      `${stale}old-b`,
    ])
  })

  test('a store read without claims is never quiescent', async () => {
    const { claimed, ...withoutClaims } = readState(liveNames)
    const { instance, rows } = sampler({ read: () => withoutClaims, quietMs: 200 })

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [{ group: 'g', settledWithout: () => true }],
      live,
      deleted: new Set(),
    })

    expect(claimed).toEqual(liveNames)
    expect(rows).toEqual([expect.objectContaining({ waitedMs: 200, quiescent: false })])
  })

  test('consumer-churn samples offset-state cardinalities after quiescence', async () => {
    let applied = false
    const member = { group: 'g-rx', settledWithout: () => applied }
    let open = 2
    const keys = ['a', 'b', 'c', 'control']
    const state = () =>
      applied
        ? { ...readState(keys), topics: 4, cells: 12, open }
        : { ...readState([...keys, 'd']), topics: 5, cells: 18 }
    const { instance, rows } = sampler({
      read: state,
      onSleep: (n) => {
        if (n === 3) applied = true
        if (n === 5) open = 0
      },
    })

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'periodic',
      members: [member],
      live,
      deleted: new Set(['d', 'e', 'f', 'g', 'h']),
    })

    expect(rows).toEqual([
      {
        kind: 'offset-state',
        phase: 'regex-topics',
        group: 'g-rx',
        checkpoint: 'periodic',
        deletions: 5,
        atMs: 500,
        waitedMs: 500,
        quiescent: true,
        live: { topics: 4, partitions: 15, names: ['a', 'b', 'c', 'control'] },
        offsetState: {
          present: true,
          topics: 4,
          cells: 12,
          cache: 4,
          cacheKeys: ['a', 'b', 'c', 'control'],
          subscribed: ['a', 'b', 'c', 'control'],
          claimed: ['a', 'b', 'c', 'control'],
          unapplied: [],
        },
      },
    ])
    expect(
      offsetStateViolations(rows, {
        passes: ['regex-topics'],
        groupsOf: () => ['g-rx'],
        deletionsOf: () => 5,
      })
    ).toEqual(['offset state of g-rx in regex-topics: no final sample'])
  })

  test('the sampler waits for the post-revoke apply before it samples', async () => {
    const setupMember = () => {
      const handlers = {}
      const consumer = {
        connect: jest.fn(async () => {}),
        subscribe: jest.fn(async () => {}),
        run: jest.fn(async (config) => Object.assign(handlers, config.rebalanceListener)),
        commitOffsets: jest.fn(async () => {}),
        disconnect: jest.fn(async () => {}),
      }
      const member = new ChurnMember({
        consumer,
        admin: { fetchOffsets: jest.fn(async () => [{ topic: 'old', partitions: [] }]) },
        group: 'g',
        label: 'm',
        journal: { append: () => {} },
        gaps: null,
        phase: 'p',
        clock: { now: () => 0 },
        handled: new Map(),
        incarnationOf: () => 'old.1',
        acknowledged: async () => true,
      })
      return { member, handlers }
    }
    const { member, handlers } = setupMember()
    await member.start([/^o/])
    await handlers.onPartitionsAssigned({ partitions: [{ topic: 'old', partitions: [0] }] })
    member.noteJoin()
    const deleted = new Set(['old'])
    expect(member.settledWithout(deleted)).toBe(false)

    await handlers.onPartitionsRevoked({ partitions: [{ topic: 'old', partitions: [0] }] })
    expect(member.settledWithout(deleted)).toBe(false)
    member.noteJoin()
    expect(member.settledWithout(deleted)).toBe(true)

    await member.stop()
    expect(member.settledWithout(deleted)).toBe(false)
  })

  test('a sample waits without the fault signal and records a timeout as not quiescent', async () => {
    const member = { group: 'g', settledWithout: () => false }
    const { instance, rows, sleeps } = sampler({
      read: () => ({ present: true, topics: 1, cells: 1, cache: 1, open: 0, claimed: [] }),
      quietMs: 1000,
    })

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [member],
      live,
      deleted: new Set(),
      deadlineAt: 600,
    })

    expect(sleeps.every((args) => args.length === 1)).toBe(true)
    expect(rows).toEqual([expect.objectContaining({ quiescent: false, waitedMs: 600 })])
    const absent = sampler({ read: undefined, quietMs: 200 })
    await absent.instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [{ group: 'g', settledWithout: () => true }],
      live,
      deleted: new Set(),
    })
    expect(absent.rows[0]).toMatchObject({
      quiescent: false,
      offsetState: { present: false },
    })
  })

  test('a member that settles only after the cutoff is not quiescent, whatever the poll interval', async () => {
    const clock = manualClock()
    const member = { group: 'g', settledWithout: () => clock.now() >= 200 }
    const { instance, rows, sleeps } = sampler({
      read: () => ({ present: true, topics: 1, cells: 1, cache: 1, open: 0, claimed: [] }),
      clock,
      quietMs: 1000,
    })
    instance.pollMs = 250

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [member],
      live,
      deleted: new Set(),
      deadlineAt: 100,
    })

    expect(sleeps).toEqual([[100]])
    expect(rows).toEqual([expect.objectContaining({ atMs: 100, waitedMs: 100, quiescent: false })])
  })

  test('an overdue wake that finds the member settled after the cutoff is not quiescent', async () => {
    const clock = manualClock()
    const member = { group: 'g', settledWithout: () => clock.now() >= 1000 }
    const late = sampler({
      read: () => ({ present: true, topics: 1, cells: 1, cache: 1, open: 0, claimed: [] }),
      clock,
      quietMs: 1000,
      onSleep: (n) => {
        if (n === 10) clock.advance(40)
      },
    })

    await late.instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [member],
      live,
      deleted: new Set(),
    })

    expect(late.rows).toEqual([
      expect.objectContaining({ atMs: 1040, waitedMs: 1040, quiescent: false }),
    ])
  })

  test('a settled member observed at or after an expired deadline is not quiescent past it', async () => {
    const clock = manualClock()
    clock.advance(500)
    const { instance, rows, sleeps } = sampler({
      read: () => ({ present: true, topics: 1, cells: 1, cache: 1, open: 0, claimed: [] }),
      clock,
    })

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [{ group: 'g', settledWithout: () => true }],
      live,
      deleted: new Set(),
      deadlineAt: 400,
    })
    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [{ group: 'h', settledWithout: () => true }],
      live,
      deleted: new Set(),
      deadlineAt: 500,
    })

    expect(sleeps).toEqual([])
    expect(rows).toEqual([
      expect.objectContaining({ group: 'g', atMs: 500, quiescent: false }),
      expect.objectContaining({ group: 'h', atMs: 500, quiescent: true }),
    ])
  })

  test('a later member reached only after the shared cutoff is recorded as not quiescent', async () => {
    const clock = manualClock()
    const { instance, rows } = sampler({
      read: () => ({ present: true, topics: 1, cells: 1, cache: 1, open: 0, claimed: [] }),
      clock,
      quietMs: 1000,
      onSleep: (n) => {
        if (n === 10) clock.advance(40)
      },
    })

    await instance.sample({
      phase: 'regex-topics',
      checkpoint: 'final',
      members: [
        { group: 'g1', settledWithout: () => false },
        { group: 'g2', settledWithout: () => true },
      ],
      live,
      deleted: new Set(),
    })

    expect(rows).toEqual([
      expect.objectContaining({ group: 'g1', atMs: 1040, quiescent: false }),
      expect.objectContaining({ group: 'g2', atMs: 1040, quiescent: false }),
    ])
  })

  test('the independent check fails when cardinality exceeds the live topic count', () => {
    const expected = { passes: ['regex-topics'], groupsOf: () => ['g'], deletionsOf: () => 0 }
    expect(offsetStateViolations([offsetRow('regex-topics', 'g')], expected)).toEqual([])
    for (const [field, value, message] of [
      ['topics', 5, '5 stamp topics for 4 live topics'],
      ['cells', 16, '16 stamp cells for 15 live partitions'],
    ]) {
      const row = offsetRow('regex-topics', 'g', {
        offsetState: presentState(LIVE_NAMES, { [field]: value }),
      })
      expect(offsetStateViolations([row], expected)).toEqual([
        `offset state of g in regex-topics (final): ${message}`,
      ])
    }
    const multi = offsetRow('regex-topics', 'g', {
      live: { topics: 2, partitions: 9, names: ['a', 'control'] },
      offsetState: presentState(['a', 'control'], { cells: 9 }),
    })
    expect(offsetStateViolations([multi], expected)).toEqual([])
  })

  test('the independent check rejects missing, unexpected and malformed offset-state rows', () => {
    const expected = {
      passes: ['warm-regex-topics', 'regex-topics'],
      groupsOf: (phase) => [`g1-${phase}`, `g2-${phase}`],
      deletionsOf: () => 0,
    }
    const finals = expected.passes.flatMap((phase) =>
      expected.groupsOf(phase).map((group) => offsetRow(phase, group))
    )
    expect(offsetStateViolations(finals, expected)).toEqual([])
    expect(offsetStateViolations(finals.slice(1), expected)).toEqual([
      'offset state of g1-warm-regex-topics in warm-regex-topics: no final sample',
    ])
    expect(offsetStateViolations([], expected)).toHaveLength(4)

    const bad = (overrides) => {
      const row = offsetRow(...overrides)
      const kept = finals.filter(({ phase, group }) => phase !== row.phase || group !== row.group)
      return offsetStateViolations([...kept, row], expected)
    }
    expect(bad(['regex-topics', 'other'])).toEqual([
      'offset state of other in regex-topics (final): unexpected group or phase',
    ])
    expect(bad(['member-churn', 'g1-regex-topics'])).toEqual([
      'offset state of g1-regex-topics in member-churn (final): unexpected group or phase',
    ])
    expect(bad(['regex-topics', 'g1-regex-topics', { checkpoint: 'later' }])).toEqual([
      'offset state of g1-regex-topics in regex-topics (later): unknown checkpoint',
      'offset state of g1-regex-topics in regex-topics: no final sample',
    ])
    expect(bad(['regex-topics', 'g1-regex-topics', { quiescent: 'yes' }])).toEqual([
      'offset state of g1-regex-topics in regex-topics (final): did not quiesce',
    ])
    expect(bad(['regex-topics', 'g1-regex-topics', { offsetState: { present: false } }])).toEqual([
      'offset state of g1-regex-topics in regex-topics (final): stores absent',
    ])
    for (const value of [undefined, Number.NaN, -1, 1.5, '3']) {
      expect(
        bad([
          'regex-topics',
          'g1-regex-topics',
          { offsetState: { present: true, topics: value, cells: 1, cache: 1 } },
        ])
      ).toEqual(['offset state of g1-regex-topics in regex-topics (final): malformed counts'])
    }
    for (const state of [
      presentState(LIVE_NAMES, { cacheKeys: undefined }),
      presentState(LIVE_NAMES, { cache: 3 }),
      presentState(LIVE_NAMES, { claimed: undefined }),
    ]) {
      expect(bad(['regex-topics', 'g1-regex-topics', { offsetState: state }])).toEqual([
        'offset state of g1-regex-topics in regex-topics (final): malformed names',
      ])
    }
    expect(
      bad([
        'regex-topics',
        'g1-regex-topics',
        { live: { topics: 4, partitions: 15, names: ['a'] } },
      ])
    ).toEqual(['offset state of g1-regex-topics in regex-topics (final): malformed names'])
    for (const live of [
      { topics: 0, partitions: 0 },
      { topics: 4, partitions: 3 },
      { topics: Number.NaN, partitions: 15 },
      undefined,
    ]) {
      expect(bad(['regex-topics', 'g1-regex-topics', { live }])).toEqual([
        'offset state of g1-regex-topics in regex-topics (final): live denominators do not fit the workload',
      ])
    }
  })

  test('the independent check requires every periodic sample the deletions made due, for each group', () => {
    const groups = ['g1', 'g2']
    const expected = { passes: ['regex-topics'], groupsOf: () => groups, deletionsOf: () => 12 }
    const periodic = (group, deletions) =>
      offsetRow('regex-topics', group, { checkpoint: 'periodic', deletions })
    const complete = groups.flatMap((group) => [
      periodic(group, 5),
      periodic(group, 10),
      offsetRow('regex-topics', group, { deletions: 12 }),
    ])
    expect(offsetStateViolations(complete, expected)).toEqual([])

    expect(
      offsetStateViolations(
        complete.filter((row) => row !== complete[4]),
        expected
      )
    ).toEqual(['offset state of g2 in regex-topics: no periodic sample at 10 deletions'])
    expect(
      offsetStateViolations(
        complete.filter(({ checkpoint }) => checkpoint === 'final'),
        expected
      )
    ).toEqual([
      'offset state of g1 in regex-topics: no periodic sample at 5 deletions',
      'offset state of g1 in regex-topics: no periodic sample at 10 deletions',
      'offset state of g2 in regex-topics: no periodic sample at 5 deletions',
      'offset state of g2 in regex-topics: no periodic sample at 10 deletions',
    ])
    expect(offsetStateViolations([...complete, periodic('g1', 5)], expected)).toEqual([
      'offset state of g1 in regex-topics (periodic): duplicate sample',
    ])
    for (const row of [
      periodic('g1', 15),
      periodic('g1', 7),
      periodic('g1', 0),
      offsetRow('regex-topics', 'g1', { deletions: 10 }),
      offsetRow('regex-topics', 'g1', { deletions: undefined }),
    ]) {
      expect(offsetStateViolations([...complete, row], expected)).toEqual([
        `offset state of g1 in regex-topics (${row.checkpoint}): unexpected checkpoint at ${row.deletions} deletions`,
      ])
    }
    expect(offsetStateViolations(complete, { ...expected, deletionsOf: () => undefined })).toEqual([
      'regex pass regex-topics: no rotation record',
    ])
  })

  test('the independent check holds the live denominators to the control topic and the rotation window', () => {
    const expected = { passes: ['regex-topics'], groupsOf: () => ['g'], deletionsOf: () => 5 }
    const rows = (overrides) => [
      offsetRow('regex-topics', 'g', { checkpoint: 'periodic', deletions: 5, ...overrides }),
      offsetRow('regex-topics', 'g', { deletions: 5 }),
    ]
    expect(offsetStateViolations(rows({}), expected)).toEqual([])
    for (const [topics, partitions] of [
      [1, 6],
      [2, 9],
      [3, 12],
    ]) {
      const names = ['control', ...LIVE_NAMES.slice(0, topics - 1)]
      const final = offsetRow('regex-topics', 'g', {
        deletions: 5,
        live: { topics, partitions, names },
        offsetState: presentState(names, { cells: partitions }),
      })
      expect(offsetStateViolations([rows({})[0], final], expected)).toEqual([])
    }

    for (const live of [
      { topics: 1000, partitions: 3000 },
      { topics: 5, partitions: 18 },
      { topics: 4, partitions: 16 },
      { topics: 3, partitions: 12 },
    ]) {
      const state = {
        present: true,
        topics: live.topics,
        cells: live.partitions,
        cache: live.topics,
      }
      expect(offsetStateViolations(rows({ live, offsetState: state }), expected)).toEqual([
        'offset state of g in regex-topics (periodic): live denominators do not fit the workload',
      ])
    }
    const inflated = offsetRow('regex-topics', 'g', {
      deletions: 5,
      live: { topics: 1000, partitions: 3000 },
      offsetState: { present: true, topics: 1000, cells: 3000, cache: 1000 },
    })
    expect(offsetStateViolations([rows({})[0], inflated], expected)).toEqual([
      'offset state of g in regex-topics (final): live denominators do not fit the workload',
    ])
  })

  test('the offset-state reader creates no registry entry, revision source or ticket', () => {
    const { Kafka, logLevel } = require('../../../index')
    const GroupRegistry = require('../../../src/cluster/groupRegistry')
    const CommittedOffsetRevisions = require('../../../src/cluster/committedOffsetRevisions')
    const kafka = new Kafka({ brokers: ['localhost:9092'], logLevel: logLevel.NOTHING })
    const read = offsetStateReader(kafka)
    const symbols = Object.getOwnPropertySymbols(kafka)
    const store = (name) =>
      kafka[symbols.find(({ description }) => description === `private:Kafka:${name}`)]

    expect(read('g')).toEqual({ present: false })
    expect(store('offsets').size).toBe(0)
    expect(store('operations').size).toBe(0)

    const holder = {}
    const entry = GroupRegistry.forStores(store('offsets'), store('operations')).acquire(
      'g',
      holder
    )
    entry.offsets.topic = { 0: '1' }
    const base = { cacheKeys: ['topic'], subscribed: [], claimed: [] }
    expect(read('g')).toEqual({ present: true, topics: 0, cells: 0, cache: 1, open: 0, ...base })
    expect(CommittedOffsetRevisions.peek(entry.offsets)).toBeUndefined()
    CommittedOffsetRevisions.forCache(entry.offsets).capture('topic', 0)
    const ticket = entry.ledger.begin([{ topic: 'topic', partition: 0 }])
    expect(read('g')).toEqual({
      present: true,
      topics: 1,
      cells: 1,
      cache: 1,
      open: 1,
      ...base,
      claimed: ['topic'],
    })
    entry.ledger.end(ticket)
    expect(entry.holders).toEqual(new Set([holder]))
    expect(entry.ledger.openTickets).toBe(0)
    expect(store('offsets').size).toBe(1)
  })

  test('the offset-state reader names the subscription and every claim of the attached consumers', () => {
    const { Kafka, logLevel } = require('../../../index')
    const GroupRegistry = require('../../../src/cluster/groupRegistry')
    const ResetObligations = require('../../../src/consumer/offsetManager/resetObligations')
    const kafka = new Kafka({ brokers: ['localhost:9092'], logLevel: logLevel.NOTHING })
    const read = offsetStateReader(kafka)
    const store = (name) =>
      kafka[
        Object.getOwnPropertySymbols(kafka).find(
          ({ description }) => description === `private:Kafka:${name}`
        )
      ]
    const entry = GroupRegistry.forStores(store('offsets'), store('operations')).acquire('g', {})
    const first = new ResetObligations()
    const second = new ResetObligations()
    first.attachTo(entry.ledger)
    second.attachTo(entry.ledger)
    first.claimSubscription(['s2', 's1'])
    first.claimAssignment(['assigned'])
    second.claimSubscription(['s1', 's3'])
    second.owe('owed', 'id-1')
    entry.offsets.s1 = {}
    entry.offsets.gone = {}

    expect(read('g')).toEqual({
      present: true,
      topics: 0,
      cells: 0,
      cache: 2,
      open: 0,
      cacheKeys: ['gone', 's1'],
      subscribed: ['s1', 's2', 's3'],
      claimed: ['assigned', 'owed', 's1', 's2', 's3'],
    })
    first.clear()
    second.clear()
  })
})
