const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const crypto = require('node:crypto')
const { AsyncLocalStorage } = require('node:async_hooks')
const ConsumerGroup = require('../../../src/consumer/consumerGroup')
const GroupOwnership = require('../../../src/consumer/groupOwnership')
const OffsetOperations = require('../../../src/consumer/offsetManager/offsetOperations')
const ResetObligations = require('../../../src/consumer/offsetManager/resetObligations')
const SeekOffsets = require('../../../src/consumer/seekOffsets')
const SubscriptionState = require('../../../src/consumer/subscriptionState')
const dictionary = require('../../../src/utils/dictionary')
const {
  Supervisor,
  buildConfig,
  digest,
  digestFile,
  finalize,
  recordOuterExit,
} = require('../finalSoak')
const { Collector, SourceChannel } = require('../finalSoakCollector')
const { PassiveObserver } = require('../finalSoakObserver')
const {
  ChildRuntime,
  ClientRig,
  ConsumerWorker,
  DecoderWorker,
  ShareWorker,
} = require('../finalSoakWorkers')
const { ContainerDumper, describeProblem } = require('../finalSoakDump')
const { validateRecord } = require('../finalSoakSchema')

const matrix = JSON.parse(fs.readFileSync(path.join(__dirname, '../finalSoakMatrix.json'), 'utf8'))
const TREE = 'b'.repeat(40)
const RUN = '0123456789ab'
const MIB = 1024 * 1024
const IDS = { A: 'AAAAAAAAAAAAAAAAAAAAAA', B: 'BBBBBBBBBBBBBBBBBBBBBB' }
const DASHED = '00000000-0000-4000-8000-000000000001'
const PRINTED = Buffer.from(DASHED.replace(/-/g, ''), 'hex').toString('base64url')
const config = buildConfig(matrix, { mode: 'smoke', runId: RUN, treeSha: TREE, brokers: ['k:1'] })
const tmp = (name) => fs.mkdtempSync(path.join(os.tmpdir(), `final-soak-${name}-`))
const tick = () => new Promise(setImmediate)
/** @returns {Promise<unknown>} the promise's value, or `{ hung: true }` after `ms` */
const within = (promise, ms) =>
  Promise.race([promise, new Promise((resolve) => setTimeout(resolve, ms, { hung: true }))])

describe('final soak collector writes', () => {
  const clock = { now: () => 0, sleepUntil: () => new Promise(() => {}) }
  const record = (seq) => ({
    kind: 'diagnostic',
    recordId: `p01.${seq}`,
    source: 'p01',
    seq,
    at: 0,
    execution: null,
    message: `note ${seq}`,
  })

  test('a short write is continued with the rest of the chunk until every byte is in the file', async () => {
    const written = []
    const sink = {
      write: async (buffer) => {
        const bytes = buffer.subarray(0, 57)
        written.push(bytes)
        return { bytesWritten: bytes.length }
      },
      sync: async () => {},
    }
    const collector = new Collector({ sink, clock, matrix })
    for (let seq = 0; seq < 6; seq++) expect(await collector.append(record(seq))).toBe(true)
    await collector.close()
    const text = Buffer.concat(written).toString()
    expect(text.split('\n').filter(Boolean)).toHaveLength(6)
    expect(text.endsWith('\n')).toBe(true)
    expect(Buffer.byteLength(text)).toBe(collector.streamBytes)
    expect(collector.failures).toEqual([])
  })

  test('a rejected write stops admission, is listed and reported at once, and never loses order', async () => {
    let calls = 0
    const sink = {
      write: async (buffer) => {
        if (++calls === 2) throw new Error('disk full')
        return { bytesWritten: buffer.length }
      },
      sync: async () => {},
    }
    const failed = []
    const collector = new Collector({ sink, clock, matrix })
    collector.onFailure = (problem) => failed.push(problem)
    expect(await collector.append(record(0))).toBe(true)
    await tick()
    expect(await collector.append(record(1))).toBe(false)
    expect(await collector.append(record(2))).toBe(false)
    await collector.close()
    expect(collector.failures).toEqual([expect.stringContaining('disk full')])
    expect(failed).toEqual([expect.stringContaining('disk full')])
  })

  test('an append is acknowledged only once its bytes are in the sink, not when it is queued', async () => {
    let open = null
    const lines = []
    const sink = {
      write: async (buffer) => {
        await new Promise((resolve) => (open = resolve))
        lines.push(buffer.toString())
        return { bytesWritten: buffer.length }
      },
      sync: async () => {},
    }
    const collector = new Collector({ sink, clock, matrix })
    let acked = null
    collector.append(record(0)).then((ok) => (acked = ok))
    await tick()
    expect(lines).toEqual([])
    expect(acked).toBeNull()
    open()
    await tick()
    expect(acked).toBe(true)
    expect(lines.join('')).toContain('"p01.0"')
  })

  test('a short write followed by an error refuses every record not wholly written, and later appends', async () => {
    let calls = 0
    const sink = {
      write: async (buffer) => {
        if (++calls === 1) return { bytesWritten: 10 }
        throw new Error('io error')
      },
      sync: async () => {},
    }
    const collector = new Collector({ sink, clock, matrix })
    const results = [collector.append(record(0)), collector.append(record(1))]
    expect(await Promise.all(results)).toEqual([false, false])
    expect(await collector.append(record(2))).toBe(false)
    expect(collector.failures).toEqual(['sink write failed: io error'])
  })

  test('a sink write that never settles stops the collection in bounded time and the close returns', async () => {
    const sink = { write: () => new Promise(() => {}), sync: async () => {} }
    const collector = new Collector({ sink, clock, matrix, writeTimeoutMs: 50 })
    expect(await within(collector.append(record(0)), 2000)).toBe(false)
    expect(await within(collector.close(), 2000)).toBeUndefined()
    expect(collector.failures).toEqual(['sink write failed: a write did not settle within 50 ms'])
  })

  test("an oversize supervisor record keeps its place before the abort note its failure writes through the supervisor's real abort", async () => {
    const lines = []
    const sink = { write: async (b) => lines.push(b.toString()), sync: async () => {} }
    const collector = new Collector({ sink, clock, matrix })
    const supervisor = Object.create(Supervisor.prototype)
    Object.assign(supervisor, { collector, aborted: null, problems: [] })
    collector.onFailure = (problem) => supervisor.abort(`collector: ${problem}`)
    const big = { ...record(0), source: 'p15', recordId: 'p15.0', message: 'x'.repeat(4000) }
    expect(await collector.append(big)).toBe(true)
    await collector.close()
    const stream = lines.join('').trim().split('\n').map(JSON.parse)
    expect(stream.map((r) => r.recordId)).toEqual(['p15.0', 'p15.c0'])
    expect(stream[1].message).toMatch(/^abort: collector: p15\.0 diagnostic line of \d+ bytes/)
  })

  test("a source's last records are acknowledged only after they are written, so its pending bytes reach zero only then", async () => {
    let open = null
    const sink = {
      write: async (buffer) => {
        await new Promise((resolve) => (open = resolve))
        return { bytesWritten: buffer.length }
      },
      sync: async () => {},
    }
    const collector = new Collector({ sink, clock, matrix })
    const channel = new SourceChannel({
      source: 'p01',
      clock,
      send: (message, done) => collector.append(message.record).then((ok) => done(!ok)),
    })
    await channel.emit({ kind: 'sample', recordId: 'p01.0', role: 'final' })
    await tick()
    expect(channel.pendingBytes).toBeGreaterThan(0)
    open()
    await tick()
    expect(channel.pendingBytes).toBe(0)
  })

  test('a write that accepts no bytes is a failure and not an endless loop', async () => {
    const sink = { write: async () => ({ bytesWritten: 0 }), sync: async () => {} }
    const collector = new Collector({ sink, clock, matrix })
    await collector.append(record(0))
    await collector.close()
    expect(collector.failures).toEqual([expect.stringContaining('no bytes')])
  })

  test('a source whose record the collector refused is incomplete and sends nothing more', async () => {
    const sent = []
    const channel = new SourceChannel({
      source: 'p01',
      clock,
      send: (message, done) => {
        sent.push(message)
        done(sent.length === 1)
      },
    })
    expect(await channel.emit({ kind: 'diagnostic', recordId: 'p01.0', message: 'a' })).toBe(true)
    expect(channel.incomplete).toEqual(expect.stringContaining('refused'))
    expect(await channel.emit({ kind: 'diagnostic', recordId: 'p01.1', message: 'b' })).toBe(false)
    expect(sent).toHaveLength(1)
    expect(channel.pendingBytes).toBe(0)
  })

  test('an oversize record is journalled as a diagnostic in its place, naming its kind and size, and its source still delivers its final sample', async () => {
    const lines = []
    const sink = { write: async (b) => lines.push(b.toString()), sync: async () => {} }
    const collector = new Collector({ sink, clock, matrix })
    const aborts = []
    collector.onFailure = (problem) => aborts.push(problem)
    const channel = new SourceChannel({
      source: 'p01',
      clock,
      send: (message, done) => collector.append(message.record).then((ok) => done(!ok)),
    })
    const wave = { kind: 'wave', recordId: 'p01.0', execution: 'h00m0w0', pad: 'x'.repeat(2000) }
    expect(await channel.emit(wave)).toBe(true)
    await tick()
    expect(await channel.emit({ kind: 'sample', recordId: 'p01.1', role: 'final' })).toBe(true)
    await tick()
    await collector.close()
    const stream = lines.join('').trim().split('\n').map(JSON.parse)
    expect(stream.map((r) => [r.recordId, r.source, r.seq, r.kind])).toEqual([
      ['p01.0', 'p01', 0, 'diagnostic'],
      ['p01.1', 'p01', 1, 'sample'],
    ])
    const bytes = /^p01\.0 wave line of (\d+) bytes exceeds 1024 bytes$/.exec(
      collector.failures[0]
    )?.[1]
    expect(Number(bytes)).toBeGreaterThan(2000)
    expect(collector.failures).toHaveLength(1)
    expect(aborts).toEqual(collector.failures)
    expect(stream[0]).toMatchObject({
      execution: 'h00m0w0',
      message: `oversize wave record p01.0 of ${bytes} bytes not written`,
    })
    expect(channel.incomplete).toBeNull()
  })
})

/** A consumer group made from the real prototype and collaborators, without the network. */
const realGroup = () => {
  const group = Object.create(ConsumerGroup.prototype)
  Object.assign(group, {
    groupId: 'g-1',
    runIdentity: {},
    topicsSubscribed: ['main'],
    topicConfigurations: {},
    preferredReadReplicasPerTopicPartition: dictionary(),
    seekOffset: new SeekOffsets(),
    acquired: new Map(),
    acquireClock: 0,
    haltedPartitions: new Map(),
    ownership: new GroupOwnership(),
    subscriptionState: new SubscriptionState(),
    operations: new OffsetOperations(),
    obligations: new ResetObligations(),
    assignmentEpoch: 0,
    groupMembership: {
      kind: 'classic',
      grownTopicNames: new Set(),
      memberId: 'm1',
      generationId: 5,
    },
    cluster: { topicId: () => DASHED, targetTopics: new Set() },
  })
  return group
}

describe('final soak observer evidence', () => {
  test('an inventory carries every topic id in its printed form, the observed list and the reset debt included', () => {
    const context = new AsyncLocalStorage()
    const observer = new PassiveObserver({
      owners: { ConsumerGroup, ResetObligations },
      watched: { ConsumerGroup: ['retainSubscription'], ResetObligations: ['owe'] },
      context,
    })
    observer.install()
    try {
      const group = realGroup()
      group.subscription = {
        applied: { revision: 1 },
        desired: { names: ['main'] },
        observed: new Map([
          ['main', DASHED],
          ['unresolved', null],
        ]),
      }
      context.run('c03.1', () => {
        group.retainSubscription(['main'])
        group.obligations.owe('main', DASHED)
      })
      const { sections } = observer.ordinarySections('c03.1')
      expect(sections.subscription[0].observed).toEqual([
        { topic: 'main', topicId: PRINTED },
        { topic: 'unresolved', topicId: null },
      ])
      expect(sections['reset-debt']).toEqual([
        { client: 'c03.1', topic: 'main', topicId: PRINTED, discharged: [] },
      ])
      for (const section of ['subscription', 'reset-debt'])
        expect(
          validateRecord({
            kind: 'inventory',
            recordId: 'p03.1',
            source: 'p03',
            seq: 1,
            at: 1,
            t: 1,
            execution: null,
            inventory: 'p03.i1',
            process: 'p03',
            client: 'c03.1',
            section,
            chunk: 0,
            chunks: 1,
            entries: sections[section],
          })
        ).toEqual([])
    } finally {
      observer.restore()
    }
  })

  test('a hook that throws is counted and named once, and the process reports it as a problem', async () => {
    class Owner {
      plain() {
        return 1
      }
    }
    const observer = new PassiveObserver({ owners: { Owner }, watched: { Owner: ['plain'] } })
    observer.hooks = () => ({
      'Owner#plain': {
        after: () => {
          throw new Error('boom')
        },
      },
    })
    observer.install()
    try {
      expect(new Owner().plain()).toBe(1)
      expect(observer.errors).toBe(1)
      const failures = observer.drainFailures()
      expect(failures).toEqual([expect.stringContaining('Owner#plain')])
      expect(failures[0]).toContain('boom')
      expect(observer.drainFailures()).toEqual([])
      const problems = []
      const self = {
        observer: { drainFailures: () => ['Owner#plain: boom'] },
        problem: async (m) => problems.push(m),
      }
      await ChildRuntime.prototype.reportObserverFailures.call(self)
      expect(problems).toEqual(['Owner#plain: boom'])
    } finally {
      observer.restore()
    }
  })

  test('a retired client drops what the observer kept for it, even when its disconnect fails, across hundreds of waves', async () => {
    const observer = new PassiveObserver({ owners: {}, watched: {} })
    const runtime = {
      observer,
      lifetimes: new Map(),
      revisions: new Map(),
      worker: {},
      nextId: () => 'p01.0',
      emit: async () => true,
      context: new AsyncLocalStorage(),
      forget(client) {
        ChildRuntime.prototype.forget.call(this, client)
      },
    }
    const slot = config.clients[0]
    for (let wave = 0; wave < 300; wave++) {
      const rig = new ClientRig({ runtime, slot, incarnation: wave + 1 })
      observer.exchanges.set(
        rig.id,
        Array.from({ length: 32 }, () => ({ apiKey: 79 }))
      )
      rig.object = { disconnect: async () => wave % 2 === 0 || Promise.reject(new Error('close')) }
      await rig.retire().catch(() => {})
    }
    expect(observer.exchanges.size).toBe(0)
  })

  test('a pause state created after its topic was recreated carries the new incarnation and its subscription lifetime', () => {
    const NEW = 'CCCCCCCCCCCCCCCCCCCCCC'
    const recreate = config.topics.recreate[0]
    const ids = { [config.topics.main]: IDS.A, [recreate]: IDS.B }
    const context = new AsyncLocalStorage()
    const runtime = {
      config,
      matrix,
      records: [],
      lifetimes: new Map(),
      revisions: new Map(),
      ids: 0,
      nextId: () => `p01.${runtime.ids++}`,
      emit: (record) => runtime.records.push(record),
      diagnose: (message) => runtime.records.push({ kind: 'diagnostic', message }),
      slotOf: (client) => ChildRuntime.prototype.slotOf.call(runtime, client),
    }
    const observer = new PassiveObserver({
      owners: { ConsumerGroup, SubscriptionState },
      watched: {
        ConsumerGroup: ['retainSubscription', 'invalidateTopic'],
        SubscriptionState: ['pause', 'resume', 'retireTopic'],
      },
      context,
      onEvent: (event) =>
        event.type === 'subscription' &&
        ChildRuntime.prototype.published.call(runtime, event.group, event.names),
    })
    runtime.observer = observer
    observer.lives = (client, topic, topicId) =>
      ChildRuntime.prototype.lifetimeOf.call(runtime, client, topic, topicId)
    observer.install()
    try {
      const group = realGroup()
      group.cluster = { topicId: (topic) => ids[topic] ?? null, targetTopics: new Set() }
      group.preparationChanged = () => {}
      context.run('c01.1', () => {
        group.retainSubscription([config.topics.main, recreate])
        group.subscriptionState.pause([{ topic: recreate, partitions: [0, 1] }])
      })
      ids[recreate] = NEW
      context.run('c01.1', () => {
        group.invalidateTopic(recreate)
        group.subscriptionState.pause([{ topic: recreate, partitions: [0] }])
        group.subscriptionState.resume([{ topic: recreate, partitions: [0] }])
      })
      const entries = observer.pauseStates()
      expect(observer.unknownTopicIds).toBe(0)
      const fresh = entries.find((e) => e.topic === recreate)
      expect(fresh.originTopicId).toBe(NEW)
      expect(fresh.subscriptionLife).toMatch(new RegExp(`^p01\\.\\d+/${NEW}$`))
      const last = runtime.records.filter((r) => r.kind === 'subscription').at(-1)
      expect(last.names).toEqual([
        { topic: config.topics.main, topicId: IDS.A },
        { topic: recreate, topicId: NEW },
      ])
      expect(runtime.lifetimes.get(`c01.1/${config.topics.main}`)).toMatch(/^p01\.0\//)
    } finally {
      observer.restore()
    }
  })
})

describe('final soak handler entries', () => {
  /** @returns {object} the surface a consumer or share worker reads while handling a message */
  const runtime = (observer) => {
    const rt = {
      slot: 'p01',
      config,
      matrix,
      records: [],
      problems: [],
      ids: 0,
      observer,
      topicIds: new Map(),
      clock: { now: () => 0 },
      retained: null,
      nextId: () => `p01.${rt.ids++}`,
      emit: (r) => rt.records.push(r) && Promise.resolve(true),
      diagnose: (message) =>
        rt.records.push({ kind: 'diagnostic', message }) && Promise.resolve(true),
      problem: (message) => rt.problems.push(message) && Promise.resolve(true),
      request: async () => {},
      commits: new AsyncLocalStorage(),
    }
    return rt
  }
  const message = {
    offset: '12',
    key: Buffer.from('c08.3'),
    value: Buffer.from(`${RUN}|c08.3||`),
    headers: {},
  }
  const authority = { client: 'c01.1', run: 'r', token: 1, acquiredAt: 1, recreatedAt: 0 }
  const rig = { id: 'c01.1', slot: config.clients[0], uncommitted: new Map() }

  const entered = (rt) =>
    rt.records.filter((r) => r.kind === 'identity' && r.stage === 'handler-enter')
  const partial = (rt) =>
    rt.records.filter(
      (r) => r.kind === 'diagnostic' && r.message.startsWith('partial handler-enter')
    )

  test.each([
    ['batch authority', { batchAuthority: () => null, topicIdOf: () => IDS.A }],
    ['a topic id', { batchAuthority: () => authority, topicIdOf: () => null }],
  ])(
    'an ordinary handler call without %s is kept as partial evidence and fails the run',
    async (_, source) => {
      const rt = runtime({ mine: () => [{}], ...source })
      const worker = new ConsumerWorker(rt)
      const local = { ...rig, uncommitted: new Map() }
      await worker.handle(local, { topic: config.topics.main, partition: 0, message })
      expect(entered(rt)).toEqual([])
      const [note] = partial(rt)
      expect(JSON.parse(note.message.slice('partial handler-enter '.length))).toMatchObject({
        client: 'c01.1',
        group: local.slot.group,
        id: 'c08.3',
        topic: config.topics.main,
        partition: 0,
        offset: '12',
      })
      expect(rt.problems).toEqual([expect.stringContaining('c08.3')])
      expect(rt.records.some((r) => r.stage === 'handler-end')).toBe(true)
      expect(worker.handled).toBe(1)
    }
  )

  test('an ordinary handler call with its enrichment records the entry and no problem', async () => {
    const rt = runtime({
      mine: () => [{}],
      batchAuthority: () => authority,
      topicIdOf: () => IDS.A,
    })
    const worker = new ConsumerWorker(rt)
    const local = { ...rig, uncommitted: new Map() }
    await worker.handle(local, { topic: config.topics.main, partition: 0, message })
    expect(entered(rt)).toHaveLength(1)
    expect(rt.problems).toEqual([])
  })

  test('a share handler call without its acquisition is kept as partial evidence, fails the run and is still accepted', async () => {
    const rt = runtime({ topicIdOf: () => IDS.A, shareDelivery: () => null })
    const worker = new ShareWorker(rt)
    const accepted = []
    const share = { id: 'c07.1', slot: config.clients.find((c) => c.slot === 'c07') }
    await worker.handle(share, {
      topic: config.topics.main,
      partition: 0,
      message,
      acknowledge: (type) => accepted.push(type),
    })
    expect(entered(rt)).toEqual([])
    expect(partial(rt)).toHaveLength(1)
    expect(rt.problems).toEqual([expect.stringContaining('c08.3')])
    expect(accepted).toEqual(['accept'])
    expect(rt.records.some((r) => r.stage === 'handler-end')).toBe(true)
  })
})

describe('final soak process lifetime', () => {
  test('a process whose supervisor is gone marks its source incomplete, disconnects its clients and stops observing', async () => {
    const calls = []
    const self = {
      channel: { incomplete: null },
      worker: { disconnect: async () => calls.push('disconnect') },
      roles: { stop: () => calls.push('roles') },
      observer: { restore: () => calls.push('restore') },
    }
    await ChildRuntime.prototype.orphaned.call(self)
    expect(self.channel.incomplete).toEqual(expect.stringContaining('supervisor'))
    expect(calls).toEqual(['disconnect', 'roles', 'restore'])
  })

  test('a telemetry push binds to its subscription whichever form of the instance id each side prints', async () => {
    const dir = tmp('instance')
    try {
      const records = []
      const runtime = {
        out: dir,
        slotOf: () => ({ slot: 'c11' }),
        nextId: () => `p16.${records.length}`,
        emit: async (r) => records.push(r),
        diagnose: async () => {},
      }
      const decoder = new DecoderWorker(runtime)
      decoder.dir = path.join(dir, 'sink')
      fs.mkdirSync(decoder.dir)
      await decoder.onTelemetrySubscriptions({
        entries: [{ client: 'c11.1', instanceId: DASHED, subscriptionId: 5 }],
      })
      fs.writeFileSync(path.join(decoder.dir, 'reporter-1.json'), JSON.stringify({ loadedAtMs: 1 }))
      fs.writeFileSync(
        path.join(decoder.dir, 'exports-1.jsonl'),
        `${JSON.stringify({ node: '1', seq: 1, clientId: 'c11.1', clientInstanceId: PRINTED, terminating: false, dataSha256: 'f'.repeat(64), parse: 'ok', metricsTotal: 2, truncated: false })}\n`
      )
      expect(await decoder.onTick()).toEqual({ abort: null })
      expect(records).toEqual([
        expect.objectContaining({ subscriptionId: '5', instanceId: PRINTED }),
      ])
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })
})

describe('final soak finalization', () => {
  test('a virtual input past the largest single read is hashed and counted with one fixed buffer', () => {
    const size = 2 ** 31 + 5
    const buffers = new Set()
    let chunks = 0
    const before = process.memoryUsage().arrayBuffers
    const result = digest(
      {
        size,
        read: (buffer, position) => {
          buffers.add(buffer)
          chunks++
          const n = Math.min(buffer.length, size - position)
          buffer.fill(0x61, 0, n)
          buffer[n - 1] = 0x0a
          return n
        },
      },
      16 * MIB
    )
    expect(result.bytes).toBe(size)
    expect(result.lines).toBe(chunks)
    expect(result.sha256).toMatch(/^[0-9a-f]{64}$/)
    expect(buffers.size).toBe(1)
    expect([...buffers][0].length).toBeLessThanOrEqual(16 * MIB)
    expect(process.memoryUsage().arrayBuffers - before).toBeLessThan(64 * MIB)
  }, 120000)

  test('the digest of a file equals the whole-buffer hash and line count, across chunk boundaries', () => {
    const dir = tmp('digest')
    try {
      const file = path.join(dir, 'f')
      const text = 'alpha\nbeta\n\ngamma\nlast line without newline'
      fs.writeFileSync(file, text)
      for (const chunk of [1, 5, 7, 64])
        expect(digestFile(file, chunk)).toEqual({
          bytes: Buffer.byteLength(text),
          sha256: crypto.createHash('sha256').update(text).digest('hex'),
          lines: 4,
        })
      fs.writeFileSync(file, '')
      expect(digestFile(file)).toMatchObject({ bytes: 0, lines: 0 })
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  /** An artifact directory holding what a supervisor leaves behind. */
  const artifact = ({ receipt = true, outer = null } = {}) => {
    const out = tmp('finalize')
    const originMs = 1000
    const clock = { kind: 'linux-monotonic', originMs, bootId: 'boot-1' }
    fs.writeFileSync(path.join(out, 'clock.json'), JSON.stringify(clock))
    fs.writeFileSync(path.join(out, 'preflight.json'), JSON.stringify({ host: {}, broker: {} }))
    fs.writeFileSync(path.join(out, 'stream.ndjson'), '{"kind":"diagnostic","t":1}\n')
    fs.mkdirSync(path.join(out, 'dump'))
    fs.writeFileSync(path.join(out, 'dump', 'log-main.txt'), 'Partition:0\tOffset:0\tp08.0\n')
    if (receipt)
      fs.writeFileSync(
        path.join(out, 'supervisor.json'),
        JSON.stringify({
          runId: RUN,
          originMs,
          bootId: 'boot-1',
          runStartAt: 10,
          runEndAt: 100,
          requestedMs: 90,
          mode: 'smoke',
          treeSha: TREE,
        })
      )
    if (outer) fs.writeFileSync(path.join(out, 'outer-exit.json'), JSON.stringify(outer))
    return out
  }
  const lastRecord = (out) =>
    JSON.parse(fs.readFileSync(path.join(out, 'stream.ndjson'), 'utf8').trim().split('\n').at(-1))

  test('the exit is witnessed at the time the runbook observed it, with the status it observed', () => {
    const out = artifact({ outer: { status: 0, observedAt: 12345 } })
    try {
      const header = finalize({ out, status: 0 })
      expect(header.exit).toEqual({
        supervisorProcess: 'p15',
        code: 0,
        signal: null,
        observedAt: 12345,
      })
      expect(header.window.observedEndAt).toBe(12345)
      expect(lastRecord(out)).toMatchObject({
        kind: 'process',
        event: 'exit',
        code: 0,
        signal: null,
        at: 12345,
      })
      expect(header.files.map((f) => f.file)).toContain('dump/log-main.txt')
      expect(header.stream.records).toBe(2)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test('a status above 128 is kept as the status the shell saw, never turned into a signal', () => {
    const out = artifact({ outer: { status: 143, observedAt: 500 } })
    try {
      const header = finalize({ out, status: 143 })
      expect(header.exit).toMatchObject({ code: 143, signal: null })
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test('without a supervisor receipt only a partial manifest naming what is absent is written', () => {
    const out = artifact({ receipt: false, outer: { status: 143, observedAt: 500 } })
    try {
      const result = finalize({ out, status: 143 })
      expect(result).toMatchObject({ partial: true })
      expect(fs.existsSync(path.join(out, 'header.json'))).toBe(false)
      const manifest = JSON.parse(fs.readFileSync(path.join(out, 'manifest.partial.json'), 'utf8'))
      expect(manifest).toMatchObject({
        partial: true,
        absent: ['supervisor.json', 'header.json'],
        exit: { status: 143, observedAt: 500 },
      })
      expect(manifest.files.map((f) => f.file)).toEqual(
        expect.arrayContaining(['stream.ndjson', 'dump/log-main.txt'])
      )
      expect(manifest.files.every((f) => /^[0-9a-f]{64}$/.test(f.sha256))).toBe(true)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test('the outer exit receipt places the observation on the run clock at the moment of the exit', () => {
    const out = tmp('outer')
    try {
      const receipt = recordOuterExit(
        { out, status: 143, exitWallMs: 9000 },
        { wallNow: () => 10000, clock: { now: () => 5000 } }
      )
      expect(receipt).toEqual({ status: 143, observedAt: 4000 })
      expect(JSON.parse(fs.readFileSync(path.join(out, 'outer-exit.json'), 'utf8'))).toEqual(
        receipt
      )
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })
})

describe('final soak container dumps', () => {
  const main = config.topics.main
  const share = config.clients.find((c) => c.clientKind === 'share').group
  const groups = [...new Set(config.clients.map((c) => c.group).filter(Boolean))].filter(
    (g) => g !== share
  )

  /** Scripted Apache tools: argv in, stdout chunks and stderr out. */
  const tools = ({ failGroup = null, shortMain = false, notTable = null } = {}) => {
    const calls = []
    const run = async (argv, onData) => {
      calls.push(argv)
      const tool = argv.find((a) => a.endsWith('.sh'))
      const arg = (name) => argv[argv.indexOf(name) + 1]
      let stdout = ''
      let stderr = ''
      let code = 0
      if (tool.endsWith('kafka-console-consumer.sh')) {
        const offsets = arg('--topic') === main ? [0, 1, 2, 3, 4] : [0, 1, 2, 4, 5, 6]
        for (const o of offsets) stdout += `Partition:0\tOffset:${o}\tp08.${o}\t${RUN}|p08.${o}||\n`
        const counted = shortMain && arg('--topic') === main ? 3 : offsets.length
        stderr = `[ERROR] Error processing message: TimeoutException\nProcessed a total of ${counted} messages\n`
      } else {
        const group = arg('--group')
        stdout = `\nGROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n${group} ${main} 0 5 5 0 - - -\n`
        if (group === failGroup) {
          code = 1
          stderr = 'Error: group failed\n'
        }
        if (group === notTable)
          stdout = `\nError: Executing consumer group command failed: Group ${group} is not a consumer group.\n`
      }
      for (let i = 0; i < stdout.length; i += 13) onData(Buffer.from(stdout.slice(i, i + 13)))
      return { code, signal: null, stderr }
    }
    return { run, calls }
  }
  const dump = (out, scripted) => new ContainerDumper({ out, config, run: scripted.run })
  const read = (out, rel) => fs.readFileSync(path.join(out, 'apache', rel), 'utf8')

  test('both standing logs and every group are dumped by the Apache tools in the broker container, each with its stderr, command and end line', async () => {
    const out = tmp('dump')
    try {
      const scripted = tools()
      const { files } = await dump(out, scripted).run()
      expect(files).toEqual([
        'main.dump',
        'transactions.dump',
        ...groups.map((g) => `groups/${g}.describe`),
      ])
      expect(read(out, 'main.dump').split('\n').filter(Boolean)).toEqual(
        [0, 1, 2, 3, 4].map((o) => `Partition:0\tOffset:${o}\tp08.${o}\t${RUN}|p08.${o}||`)
      )
      expect(read(out, 'main.dump.end')).toBe('exit=0 lines=5\n')
      expect(read(out, 'transactions.dump.end')).toBe('exit=0 lines=6\n')
      expect(read(out, 'main.dump.stderr')).toContain('Processed a total of 5 messages')
      for (const group of groups) {
        expect(read(out, `groups/${group}.describe`)).toContain(`${group} ${main} 0 5 5 0`)
        expect(read(out, `groups/${group}.describe.end`)).toBe('exit=0 lines=3\n')
      }
      expect(read(out, 'main.dump.cmd').trim()).toBe(
        [
          'docker compose exec -T kafka1 /opt/kafka/bin/kafka-console-consumer.sh',
          '--bootstrap-server kafka1:29092,kafka2:29095,kafka3:29098',
          `--topic ${main}`,
          '--partition 0',
          '--offset earliest',
          '--isolation-level read_committed',
          '--timeout-ms 60000',
          '--formatter-property print.partition=true',
          '--formatter-property print.offset=true',
          '--formatter-property print.key=true',
        ].join(' ')
      )
      const [first] = groups
      expect(read(out, `groups/${first}.describe.cmd`).trim()).toBe(
        `docker compose exec -T kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:29092,kafka2:29095,kafka3:29098 --describe --group ${first}`
      )
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test('the share group is recorded as unavailable from the consumer-groups tool and never fails the dump', async () => {
    const out = tmp('dump-share')
    try {
      const scripted = tools({ failGroup: share })
      await expect(dump(out, scripted).run()).resolves.toMatchObject({})
      expect(scripted.calls.some((argv) => argv.includes(share))).toBe(false)
      expect(read(out, `groups/${share}.unavailable`)).toBe(
        'unavailable: kafka-consumer-groups.sh does not describe share groups (GroupIdNotFoundException)\n'
      )
      expect(fs.existsSync(path.join(out, 'apache', `groups/${share}.describe`))).toBe(false)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test("an ordinary group's describe is the tool's offsets table, as a real run printed it", () => {
    const sample = fs.readFileSync(path.join(__dirname, 'fixtures/finalSoakDescribe.txt'), 'utf8')
    const group = 'g-21b084a0c66e-c01'
    expect(sample.split('\n')).toHaveLength(5)
    expect(describeProblem(sample, group)).toBeNull()
    expect(describeProblem(sample, 'g-21b084a0c66e-c02')).toBe(
      `unparseable row "${group} fs-21b084a0c66e-main "`
    )
    expect(describeProblem(sample.split('\n').slice(0, 2).join('\n'), group)).toBe(
      'no partition rows'
    )
    const share =
      '\nError: Executing consumer group command failed due to org.apache.kafka.common.errors.GroupIdNotFoundException: Group g-21b084a0c66e-c07 is not a consumer group.\n'
    expect(describeProblem(share, 'g-21b084a0c66e-c07')).toBe('no offsets header row')
    expect(describeProblem(sample.replace('6585            6585', 'x  6585'), group)).toMatch(
      /^unparseable row/
    )
  })

  test("an ordinary group's describe that is not an offsets table fails the dump after every file is written", async () => {
    const out = tmp('dump-table')
    try {
      await expect(dump(out, tools({ notTable: groups[0] })).run()).rejects.toThrow(
        `groups/${groups[0]}.describe (no offsets header row)`
      )
      expect(read(out, `groups/${groups.at(-1)}.describe.end`)).toBe('exit=0 lines=3\n')
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test('a failing tool or a log dump whose count differs from its lines fails the dump loudly after every file is written', async () => {
    const out = tmp('dump-fail')
    const short = tmp('dump-short')
    try {
      await expect(dump(out, tools({ failGroup: groups[1] })).run()).rejects.toThrow(
        new RegExp(`groups/${groups[1]}.describe \\(exit 1\\)`)
      )
      expect(read(out, `groups/${groups[1]}.describe.end`)).toBe('exit=1 lines=3\n')
      expect(read(out, `groups/${groups.at(-1)}.describe.end`)).toBe('exit=0 lines=3\n')
      await expect(dump(short, tools({ shortMain: true })).run()).rejects.toThrow(
        /main\.dump \(the tool counted 3 records, wrote 5 lines\)/
      )
      expect(read(short, 'main.dump.end')).toBe('exit=0 lines=5\n')
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
      fs.rmSync(short, { recursive: true, force: true })
    }
  })
})

describe('final soak record line sizes', () => {
  const { RECORDS, CLIENT_KINDS } = require('../finalSoakSchema')
  const { chunk } = require('../finalSoakCollector')
  const CAP = matrix.host.maxEventLineBytes.value
  const INVENTORY_CAP = matrix.host.maxInventoryLineBytes.value
  /** About 28 hours on the run clock, past the end of a day run with its setup and teardown. */
  const T = 99999999.12345679
  const N = 9999999
  const SHA = 'f'.repeat(64)
  const OFFSET = '999999999999'
  const LONG_ERROR = 'KafkaJSNumberOfRetriesExceeded'
  const topic = (role) => `fs-${RUN}-${role}`
  /** Every partition one member can hold at once: both standing topics, a regex topic, both recreated topics. */
  const KEYS = [
    ['main', 1],
    ['txn', 1],
    ['regex-h23r3', 2],
    ['recreate-0', 2],
    ['recreate-1', 2],
  ].flatMap(([role, n]) =>
    Array.from({ length: n }, (_, partition) => ({ topic: topic(role), topicId: IDS.A, partition }))
  )
  const C = 'c18.9999'
  const RUN_ID = `${C}.r9999`
  const MEMBER = `s-${RUN}-c18-${DASHED}`
  const GROUP = `g-${RUN}-c06`
  const AUTHORITY = {
    client: C,
    run: RUN_ID,
    member: MEMBER,
    epoch: N,
    token: N,
    assignmentEpoch: N,
    acquiredAt: N,
    recreatedAt: N,
  }
  const KEY = KEYS[2]
  const call = (event) => ({
    event,
    client: C,
    phase: 'end',
    call: `${C}.k${N}`,
    result: 'error',
    errorType: LONG_ERROR,
  })
  const identity = (stage, fields) => ({ kind: 'identity', stage, ...fields })

  /** The largest realistic record of every kind and variant the driver builds by hand. */
  const handBuilt = () => [
    {
      kind: 'process',
      event: 'start',
      process: 'p15',
      pid: 4194304,
      parent: 'outer',
      arm: 'observer',
      controlId: null,
      execArgv: ['--expose-gc', '--no-flush-bytecode'],
      nodeOptions: [],
      node: 'v26.13.1',
      v8: '14.6.202.34-node.40',
      bootId: DASHED,
    },
    {
      kind: 'process',
      event: 'exit',
      process: 'p06',
      code: 137,
      signal: 'SIGKILL',
      spawnRecord: `p15.${N}`,
    },
    {
      kind: 'client',
      event: 'construct',
      client: C,
      slot: 'c18',
      clientKind: 'classic-cooperative-static',
      process: 'p06',
      arm: 'unseeded',
      controlId: null,
      purpose: 'subject',
      group: GROUP,
      groupInstanceId: `s-${RUN}-c18`,
      standing: false,
      telemetryEnabled: true,
      configSha256: SHA,
    },
    ...['connect', 'run', 'stop', 'disconnect'].map((event) => ({
      kind: 'client',
      ...call(event),
    })),
    ...['start', 'end'].map((phase) => ({
      kind: 'shape',
      phase,
      shape: 'membership-churn',
      participants: [...CLIENT_KINDS],
      operationCount: phase === 'start' ? 0 : N,
    })),
    {
      kind: 'topic',
      client: `c22.${N}`,
      call: `c22.${N}.t${N}`,
      op: 'metadata',
      topic: KEY.topic,
      topicId: IDS.A,
      partitions: [0, 1],
      errorCode: -1,
    },
    ...['start', 'seal'].map((event) => ({
      kind: 'route',
      event,
      group: GROUP,
      topic: KEY.topic,
      topicId: IDS.A,
      partition: 1,
      from: OFFSET,
      to: event === 'seal' ? OFFSET : null,
      topicRecord: `p15.${N}`,
      scan: event === 'seal' ? `p14.s${N}` : null,
    })),
    ...['begin', 'end'].map((phase) => ({
      kind: 'operation',
      phase,
      operation: `${C}.o${N}`,
      op: 'resume',
      client: C,
      keys: KEYS,
      call: `${C}.o${N}`,
      result: phase === 'end' ? 'error' : null,
      errorType: phase === 'end' ? LONG_ERROR : null,
      causes: [],
    })),
    {
      kind: 'subscription',
      client: C,
      revision: N,
      run: RUN_ID,
      names: KEYS.filter((k) => k.partition === 0).map(({ topic, topicId }) => ({
        topic,
        topicId,
      })),
      mode: 'broker-regex',
      pattern: `^fs-${RUN}-(main|txn|regex-.*|recreate-[01])$`,
      cause: `p06.${N}`,
    },
    {
      kind: 'boundary',
      client: C,
      group: GROUP,
      run: RUN_ID,
      member: MEMBER,
      epoch: N,
      protocol: 'cooperative',
      event: 'revoke-begin',
      key: KEY,
      authority: AUTHORITY,
      position: OFFSET,
      cause: `${C}.n${N}`,
    },
    {
      kind: 'round',
      client: C,
      group: GROUP,
      run: RUN_ID,
      round: `${C}.n${N}`,
      protocol: 'cooperative',
      epoch: N,
      assigned: KEYS.slice(2),
      revoked: KEYS.slice(0, 2),
      causes: [],
    },
    ...['start', 'clear'].map((event) => ({
      kind: 'seed',
      event,
      process: 'p13',
      client: 'c20.9999',
      controlId: 'interning-seeded',
      mapEntries: N,
      cause: `p15.${N}`,
    })),
    identity('offered', {
      client: 'c09.9999',
      id: `c09.${N}`,
      key: KEY,
      payloadSha256: SHA,
      bytes: N,
      transaction: `c09.9999.x${N}`,
    }),
    identity('ack', {
      client: 'c09.9999',
      id: `c09.${N}`,
      outcome: 'rejected',
      key: KEY,
      offset: null,
      errorType: LONG_ERROR,
    }),
    identity('transaction', {
      client: 'c09.9999',
      transaction: `c09.9999.x${N}`,
      decision: 'commit',
      outcome: 'unknown',
      call: `c09.9999.x${N}.commit`,
      errorType: LONG_ERROR,
    }),
    identity('log', {
      reader: 'c21.9999',
      scan: `p14.s${N}`,
      id: `c09.${N}`,
      key: KEY,
      offset: OFFSET,
      payloadSha256: SHA,
      transaction: `c09.9999.x${N}`,
    }),
    identity('handler-enter', {
      client: C,
      group: GROUP,
      handler: `${C}.h${N}`,
      id: `c09.${N}`,
      key: KEY,
      offset: OFFSET,
      payloadSha256: SHA,
      authority: AUTHORITY,
      acquisition: null,
      deliveryCount: null,
    }),
    identity('handler-end', { handler: `${C}.h${N}`, outcome: 'processed', errorType: LONG_ERROR }),
    identity('commit', {
      client: C,
      group: GROUP,
      commit: `${C}.m${N}`,
      key: KEY,
      next: OFFSET,
      authority: AUTHORITY,
      phase: 'end',
      outcome: 'unknown',
      errorType: LONG_ERROR,
    }),
    identity('broker-commit', {
      client: `c22.${N}`,
      group: GROUP,
      commit: `${C}.m${N}`,
      key: KEY,
      next: OFFSET,
      errorCode: -1,
    }),
    identity('share-outcome', {
      client: C,
      group: GROUP,
      key: KEY,
      from: OFFSET,
      to: OFFSET,
      type: 'accept',
      reason: 'membership-lost',
      errorType: LONG_ERROR,
    }),
    {
      kind: 'scan',
      scan: `p14.s${N}`,
      key: KEY,
      from: OFFSET,
      to: OFFSET,
      lso: OFFSET,
      highWatermark: OFFSET,
      rawFile: `raw/p14.s${N}.ndjson`,
      rawSha256: SHA,
      records: N,
    },
    {
      kind: 'replay',
      replay: `${C}.p${N}`,
      group: GROUP,
      key: KEY,
      from: '0',
      to: OFFSET,
      reason: 'handler-retry',
      oldAuthority: AUTHORITY,
      newAuthority: AUTHORITY,
      cause: `p06.${N}`,
    },
    {
      kind: 'share-acquire',
      acquisition: `c07.9999.a${N}`,
      client: 'c07.9999',
      group: `g-${RUN}-c07`,
      key: KEY,
      from: OFFSET,
      to: OFFSET,
      deliveryCount: N,
      receivedAt: T,
      lockMs: N,
      wireFile: `raw/p07.share.${N}.ndjson`,
      wireSha256: SHA,
    },
    {
      kind: 'share-settle',
      acquisition: `c07.9999.a${N}`,
      client: 'c07.9999',
      group: `g-${RUN}-c07`,
      key: KEY,
      offset: OFFSET,
      type: 'release',
      phase: 'response',
      errorCode: -1,
      wireFile: `raw/p07.share.${N}.ndjson`,
      wireSha256: SHA,
    },
    {
      kind: 'telemetry',
      client: 'c11.9999',
      instanceId: IDS.B,
      subscriptionId: '-2081645975',
      broker: 3,
      metrics: N,
      terminating: true,
      rawFile: `telemetry/exports-3.${N}.jsonl`,
      rawSha256: SHA,
      decodedFile: `telemetry/decoded-3.${N}.jsonl`,
      decodedSha256: SHA,
    },
    {
      kind: 'sample',
      process: 'p06',
      role: 'quiet',
      slot: null,
      boundary: 'h23q2',
      reading: 2,
      admittedAt: T - 300,
      completedAt: T,
      rss: 99999999999,
      heapUsed: 99999999999,
      external: 99999999999,
      activeResources: [
        'TCPSocketWrap',
        'Timeout',
        'FSReqPromise',
        'Immediate',
        'PipeWrap',
        'ProcessWrap',
        'SimpleWriteWrap',
      ].map((type) => ({ type, count: N })),
      gc: {
        id: `p06.g${N}`,
        firstBeginAt: T - 299,
        firstEndAt: T - 250,
        secondBeginAt: T - 100,
        secondEndAt: T - 1,
        forcedCollections: 2,
        cumulativeForced: N,
      },
      inventory: `p06.i${N}`,
      clients: ['c06.9999', C],
    },
    {
      kind: 'inventory',
      inventory: `p06.i${N}`,
      process: 'p06',
      client: C,
      section: 'requests',
      chunk: N - 1,
      chunks: N,
      entries: chunk(
        Array.from({ length: 2000 }, (_, i) => ({
          request: `${C}.q${N}.${i}`,
          queue: `${C}.n${N}`,
          apiKey: 92,
          phase: 'inflight',
          scope: `${C}.w${N}`,
          beganAt: T,
          deadlineAt: T,
        })),
        INVENTORY_CAP - 1024
      )[0],
    },
    {
      kind: 'ticket',
      ticket: `t${N}`,
      ledger: `o${N}`,
      client: C,
      ownerRun: RUN_ID,
      phase: 'begin',
      keys: KEYS,
      token: N,
    },
  ]

  /** A stand-in runtime whose ids, clock and journal are at their largest realistic values. */
  const journal = () => {
    const records = []
    return {
      records,
      clock: { now: () => T, sleepUntil: async () => {} },
      nextId: () => `p06.${N}`,
      emit: async (record) => records.push(record) > 0,
    }
  }

  /** The records the driver's own builders make at their largest: waves, outages, failures, notes. */
  const built = async () => {
    const runtime = journal()
    const worker = Object.create(ConsumerWorker.prototype)
    worker.runtime = runtime
    const standing = { id: 'c06.9999' }
    for (const event of ['transfer', 'return'])
      await worker.waveRecord('h23m3w2', event, standing, { id: C }, KEYS, () => KEYS, false)
    const cause = (depth) =>
      Object.assign(new Error('m'.repeat(5000)), {
        name: `KafkaJS${'X'.repeat(40)}`,
        cause: depth > 0 ? cause(depth - 1) : undefined,
      })
    const error = Object.assign(cause(6), {
      type: 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED',
      code: 'ERR_SOCKET_CONNECTION_TIMEOUT',
      retriable: false,
    })
    await ChildRuntime.prototype.failed.call(runtime, `${C}.o${N}`, error)
    await ChildRuntime.prototype.diagnose.call(runtime, `step h23o0 failed: ${'z'.repeat(5000)}`)

    const out = tmp('sizes')
    try {
      const s = new Supervisor({
        mode: 'acceptance',
        matrix: 'scripts/soak/finalSoakMatrix.json',
        expectSha: TREE,
        brokers: ['k:1'],
        out,
      })
      s.runtime = runtime
      s.clock = runtime.clock
      const t = s.config.topics
      s.admin = {
        describeCluster: async () => ({ brokers: [1, 2, 3].map((nodeId) => ({ nodeId })) }),
        fetchTopicMetadata: async () => ({
          topics: [t.main, t.transactions, ...t.recreate].map((name) => ({
            name: name.replace(s.runId, RUN),
            topicId: DASHED,
            partitions: [0, 1].map((partitionId) => ({ partitionId, leader: 3, isr: [3, 2, 1] })),
          })),
        }),
      }
      const processTime = new Date().toISOString().replace('Z', '123456Z')
      const state = { pid: 4194304, startedAt: processTime, finishedAt: processTime }
      for (const event of ['down', 'ready', 'isr'])
        await s.brokerState({ id: 'h22o0', node: 3, downMs: 12000 }, event, state, SHA, `p15.${N}`)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }

    const lines = []
    const collector = new Collector({
      sink: { write: async (b) => lines.push(b.toString()), sync: async () => {} },
      clock: runtime.clock,
      matrix,
    })
    collector.ownSeq = N
    await collector.note(`problem: ${'p'.repeat(5000)}`, { step: 'h23o0' })
    await collector.close()
    expect(collector.failures).toEqual([])
    return [...runtime.records, ...lines.join('').trim().split('\n').map(JSON.parse)]
  }

  /** @param {object} record @returns {object} the record with its largest realistic envelope */
  const enveloped = (record) => ({
    recordId: `p06.${N}`,
    source: 'p06',
    seq: N,
    at: T,
    execution: 'h23m3w2',
    ...record,
    t: T,
  })

  test('every record kind and variant the driver emits fits its line cap at its largest realistic size', async () => {
    const records = [...handBuilt(), ...(await built())].map(enveloped)
    const variantOf = (r) => (RECORDS[r.kind]?.by ? `${r.kind}/${r[RECORDS[r.kind].by]}` : r.kind)
    const every = Object.entries(RECORDS).flatMap(([kind, spec]) =>
      spec.by ? Object.keys(spec.variants).map((v) => `${kind}/${v}`) : [kind]
    )
    expect([...new Set(records.map(variantOf))].sort()).toEqual(every.sort())
    const sizes = records.map((r) => ({
      record: variantOf(r),
      problems: validateRecord(r),
      fits: Buffer.byteLength(JSON.stringify(r)) <= (r.kind === 'inventory' ? INVENTORY_CAP : CAP),
    }))
    expect(sizes).toEqual(sizes.map(({ record }) => ({ record, problems: [], fits: true })))
  })
})
