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
const { buildConfig, digest, digestFile, finalize, recordOuterExit } = require('../finalSoak')
const { Collector, SourceChannel } = require('../finalSoakCollector')
const { PassiveObserver } = require('../finalSoakObserver')
const {
  ChildRuntime,
  ClientRig,
  ConsumerWorker,
  DecoderWorker,
  ShareWorker,
} = require('../finalSoakWorkers')
const { ContainerDumper } = require('../finalSoakDump')
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
    expect(await collector.append(record(1))).toBe(true)
    await tick()
    await tick()
    expect(await collector.append(record(2))).toBe(false)
    await collector.close()
    expect(collector.failures).toEqual([expect.stringContaining('disk full')])
    expect(failed).toEqual([expect.stringContaining('disk full')])
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
      nextId: () => 'p01.0',
      emit: async () => true,
      context: new AsyncLocalStorage(),
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
  const rig = { id: 'c01.1', slot: config.clients[0], processed: new Map(), uncommitted: new Map() }

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
      worker.progress = new Map()
      worker.lastBoundary = new Map()
      const local = { ...rig, processed: new Map(), uncommitted: new Map() }
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
      expect(local.processed.get(`${config.topics.main}|0`)).toBe('13')
    }
  )

  test('an ordinary handler call with its enrichment records the entry and no problem', async () => {
    const rt = runtime({
      mine: () => [{}],
      batchAuthority: () => authority,
      topicIdOf: () => IDS.A,
    })
    const worker = new ConsumerWorker(rt)
    worker.progress = new Map()
    worker.lastBoundary = new Map()
    const local = { ...rig, processed: new Map(), uncommitted: new Map(), authority: new Map() }
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
  const groups = [...new Set(config.clients.map((c) => c.group).filter(Boolean))]

  /** Scripted Apache tools: argv in, stdout chunks and stderr out. */
  const tools = ({ failGroup = null, shortMain = false } = {}) => {
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

  test('the share group is described for the record but its status never fails the dump', async () => {
    const out = tmp('dump-share')
    try {
      const share = config.clients.find((c) => c.clientKind === 'share').group
      await expect(dump(out, tools({ failGroup: share })).run()).resolves.toMatchObject({})
      expect(read(out, `groups/${share}.describe.end`)).toBe('exit=1 lines=3\n')
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
