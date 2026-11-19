const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { EventEmitter } = require('node:events')
const { AsyncLocalStorage } = require('node:async_hooks')
const { spawnSync } = require('node:child_process')
const yaml = require('js-yaml')
const SubscriptionState = require('../../../src/consumer/subscriptionState')
const ConsumerGroup = require('../../../src/consumer/consumerGroup')
const GroupOwnership = require('../../../src/consumer/groupOwnership')
const OffsetOperations = require('../../../src/consumer/offsetManager/offsetOperations')
const ResetObligations = require('../../../src/consumer/offsetManager/resetObligations')
const SeekOffsets = require('../../../src/consumer/seekOffsets')
const dictionary = require('../../../src/utils/dictionary')
const handlerContext = require('../../../src/consumer/handlerContext')
const ISOLATION = require('../../../src/protocol/isolationLevel')
const { ShareAckLedger } = require('../../../src/consumer/share/shareAckLedger')
const shareFetchV2 = require('../../../testHelpers/wire/shareFetchV2')
const {
  CLIENT_KINDS,
  SHAPES,
  validateConfig,
  validateMatrix,
  validateRecord,
} = require('../finalSoakSchema')
const { parseArgs, buildConfig, recipientGroups } = require('../finalSoak')
const { planSchedule, scheduleTotals, Scheduler, SamplingGate } = require('../finalSoakScheduler')
const { Collector, SourceChannel, Sampler, HostMonitor } = require('../finalSoakCollector')
const { PassiveObserver, SocketRegistry, normalizeTopicId } = require('../finalSoakObserver')
const { offeredRecord, parseValue, scanPartition, DecoderWorker } = require('../finalSoakWorkers')
const { Preflight, reporterSource } = require('../finalSoakPreflight')
const { BoundedReporter } = require('../longWindowReporter')

const matrix = JSON.parse(fs.readFileSync(path.join(__dirname, '../finalSoakMatrix.json'), 'utf8'))
const ROOT = path.join(__dirname, '../../..')
const RUNBOOK = path.join(__dirname, '../finalSoakNode.sh')
const WORKFLOW = path.join(ROOT, '.github/workflows/final-soak-smoke.yml')
const TREE = 'b'.repeat(40)
const RUN = '0123456789ab'
const GIB = 1024 ** 3
const ENV = { KAFKA_BROKERS: 'localhost:19092,localhost:19095,localhost:19098' }
const ARGS = ['--out', '/var/run/final-soak', '--expect-sha', TREE]
const IDS = {
  A: 'AAAAAAAAAAAAAAAAAAAAAA',
  B: 'BBBBBBBBBBBBBBBBBBBBBB',
  C: 'CCCCCCCCCCCCCCCCCCCCCC',
}
const kindsWhere = (predicate) => CLIENT_KINDS.filter((k) => predicate(matrix.clientKinds[k]))
const recipients = (role) => matrix.topology.topics.find((t) => t.role === role).recipients
const sorted = (list) => [...list].sort()
/** @returns {string[]} the schema problems of a record body once it has an envelope */
const recordProblems = (kind, body) =>
  validateRecord({
    kind,
    recordId: 'p01.1',
    source: 'p01',
    seq: 1,
    at: 1,
    t: 1,
    execution: null,
    ...body,
  })
/** @returns {string[]} the schema problems of one inventory section's entries */
const sectionProblems = (section, entries, client = 'c03.1') =>
  recordProblems('inventory', {
    inventory: 'p01.i1',
    process: 'p01',
    client,
    section,
    chunk: 0,
    chunks: 1,
    entries,
  })

/** A clock whose sleepers wake in time order when `drive` runs; no wall time passes. */
class VirtualClock {
  constructor() {
    this.t = 0
    this.sleepers = []
  }

  now() {
    return this.t
  }

  sleepUntil(at, signal) {
    if (at <= this.t || signal?.aborted) return Promise.resolve()
    return new Promise((resolve) => {
      const sleeper = { at, resolve }
      this.sleepers.push(sleeper)
      signal?.addEventListener('abort', () => {
        this.sleepers = this.sleepers.filter((s) => s !== sleeper)
        resolve()
      })
    })
  }

  async drive(done) {
    let finished = false
    done.then(
      () => (finished = true),
      () => (finished = true)
    )
    for (;;) {
      for (let i = 0; i < 5; i++) await new Promise(setImmediate)
      if (finished) return done
      if (this.sleepers.length === 0) throw new Error('nothing left to wake')
      this.sleepers.sort((a, b) => a.at - b.at)
      const next = this.sleepers.shift()
      this.t = Math.max(this.t, next.at)
      next.resolve()
    }
  }
}

/**
 * A consumer group made from the real prototype and the real collaborators the observer reads,
 * without the network: the backing fields carry the product's own names.
 */
const realGroup = ({ groupId = 'g-1', kind = 'classic', topicIds }) => {
  const group = Object.create(ConsumerGroup.prototype)
  Object.assign(group, {
    groupId,
    runIdentity: {},
    topicsSubscribed: [],
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
    groupMembership: { kind, grownTopicNames: new Set(), memberId: 'm1', generationId: 5 },
    cluster: { topicId: (t) => topicIds[t] ?? null, targetTopics: new Set() },
  })
  return group
}

describe('final soak driver', () => {
  test('the schedule covers every matrix workload, client kind and shape', () => {
    const plan = planSchedule(matrix, 'acceptance')
    const totals = scheduleTotals(plan)
    const ordinary = kindsWhere((k) => k.role === 'consumer' && k.churn)
    const statics = kindsWhere((k) => k.static)
    expect(totals).toEqual({
      idleWindows: 24,
      regexExecutions: 96,
      recreations: 48,
      membershipExecutions: 96,
      outages: 12,
      pauseSequences: Object.fromEntries(ordinary.map((k) => [k, 288])),
      staticRestarts: Object.fromEntries(statics.map((k) => [k, 24])),
      periodicSlots: 2881,
      quietReadings: 72,
      shapes: sorted(SHAPES),
      kinds: sorted(CLIENT_KINDS),
    })
    expect(ordinary).toHaveLength(6)
    const day = matrix.schedule.totals
    expect(totals.regexExecutions).toBe(day.regexExecutions)
    expect(totals.membershipExecutions).toBe(day.membershipExecutions)

    const executions = plan.steps.filter((s) => s.type === 'execution')
    const participants = (shape) => [
      ...new Set(executions.filter((s) => s.shape === shape).flatMap((s) => s.participants)),
    ]
    expect(sorted(participants('regex-churn'))).toEqual(sorted(recipients('regex')))
    expect(sorted(participants('pause-resume'))).toEqual(sorted(ordinary))
    expect(sorted(participants('membership-churn'))).toEqual(sorted(kindsWhere((k) => k.churn)))
    expect(sorted(participants('broker-outage'))).toEqual(sorted(CLIENT_KINDS))
    expect(sorted(participants('topic-recreation'))).toEqual(
      sorted(new Set([...recipients('recreate'), 'producer-idempotent', 'admin']))
    )

    const idle = plan.steps.filter((s) => s.type === 'idle')
    const work = executions.filter((s) => s.shape !== 'traffic')
    for (const window of idle) {
      expect(window.until - window.at).toBe(600000)
      for (const s of work) expect(s.deadline <= window.at || s.at >= window.until).toBe(true)
    }
    for (let i = 1; i < idle.length; i++)
      expect(idle[i].at).toBeGreaterThanOrEqual(idle[i - 1].until)

    const outages = executions.filter((s) => s.shape === 'broker-outage')
    for (const node of [1, 2, 3]) {
      const mine = outages.filter((s) => s.node === node)
      expect(mine).toHaveLength(4)
      expect(new Set(mine.map((s) => s.stop))).toEqual(new Set(['clean', 'hard']))
    }
    expect(outages.map((s) => s.downMs)).toEqual(
      outages.map((_, i) => (i % 2 === 0 ? 2000 : 12000))
    )
  })

  test('smoke mode keeps every shape in a shortened schedule', () => {
    const plan = planSchedule(matrix, 'smoke')
    const smoke = matrix.schedule.smoke
    const totals = scheduleTotals(plan)
    expect(plan.requestedMs).toBe(matrix.acceptance.modes.smoke.requestedMs.value)
    expect(totals).toMatchObject({
      idleWindows: 1,
      regexExecutions: 1,
      recreations: 1,
      membershipExecutions: 1,
      outages: 2,
      periodicSlots: smoke.periodicSlots,
      quietReadings: 3,
      shapes: sorted(SHAPES),
      kinds: sorted(CLIENT_KINDS),
    })
    expect(Object.values(totals.pauseSequences)).toEqual([3, 3, 3, 3, 3, 3])
    expect(Object.values(totals.staticRestarts)).toEqual([1, 1, 1])
    const [idle] = plan.steps.filter((s) => s.type === 'idle')
    expect([idle.at, idle.until]).toEqual([smoke.idleMs.from, smoke.idleMs.to])
    expect(plan.steps.filter((s) => s.type === 'quiet').map((s) => s.at)).toEqual(smoke.quietAtMs)
    const at = (shape) => plan.steps.find((s) => s.shape === shape).at
    expect([
      at('membership-churn'),
      at('regex-churn'),
      at('topic-recreation'),
      at('broker-outage'),
    ]).toEqual([smoke.membershipAtMs, smoke.regexAtMs, smoke.recreationAtMs, smoke.outageAtMs])
    expect(plan.steps.filter((s) => s.shape === 'broker-outage').at(-1).deadline).toBe(
      smoke.recoveredByMs
    )

    const index = (type, extra = () => true) =>
      plan.steps.findIndex((s) => s.type === type && extra(s))
    const traffic = index('execution', (s) => s.shape === 'traffic')
    const seedStart = index('seed-start')
    const seedClear = index('seed-clear')
    const lastQuiet = plan.steps.findLastIndex((s) => s.type === 'quiet')
    expect(plan.steps[seedStart].at).toBe(0)
    expect(seedStart).toBeLessThan(traffic)
    expect(seedClear).toBeGreaterThan(lastQuiet)
    expect(seedClear).toBeLessThan(index('disconnect'))

    const day = planSchedule(matrix, 'acceptance')
    const firstTriplet = day.steps.find((s) => s.type === 'quiet' && s.reading === 2)
    const dayStart = day.steps.find((s) => s.type === 'seed-start')
    expect(dayStart.at).toBeGreaterThan(firstTriplet.at)
    expect(dayStart.at).toBeLessThan(3600000)
    expect(day.steps.findIndex((s) => s.type === 'seed-clear')).toBeGreaterThan(
      day.steps.findLastIndex((s) => s.type === 'quiet')
    )
  })

  test('acceptance mode rejects --duration', () => {
    expect(() => parseArgs(['--mode', 'acceptance', '--duration', '600000', ...ARGS], ENV)).toThrow(
      '--duration is accepted only in smoke mode'
    )
    expect(parseArgs(['--mode', 'acceptance', ...ARGS], ENV)).toMatchObject({
      mode: 'acceptance',
      requestedMs: 86400000,
      brokers: ENV.KAFKA_BROKERS.split(','),
      expectSha: TREE,
      diagnostic: false,
    })
    expect(parseArgs(['--mode', 'smoke', '--duration', '1800000', ...ARGS], ENV).requestedMs).toBe(
      1800000
    )
    expect(() => parseArgs(['--mode', 'smoke', '--duration', '600000', ...ARGS], ENV)).toThrow(
      'the smoke length is fixed at 1800000 ms'
    )
    expect(() => parseArgs(['--mode', 'acceptance', '--diagnostic', ...ARGS], ENV)).toThrow(
      '--diagnostic is accepted only in smoke mode'
    )
    expect(parseArgs(['--mode', 'smoke', '--diagnostic', ...ARGS], ENV).diagnostic).toBe(true)
    expect(() => parseArgs(['--mode', 'acceptance', ...ARGS], {})).toThrow('KAFKA_BROKERS')
    expect(() => parseArgs(['--mode', 'day', ...ARGS], ENV)).toThrow('--mode')
  })

  test('the collector stays bounded over a simulated 24 h of samples', async () => {
    const clock = new VirtualClock()
    const written = { bytes: 0, lines: 0, maxPending: 0 }
    let pending = 0
    const sink = {
      write(buffer) {
        pending++
        written.maxPending = Math.max(written.maxPending, pending)
        written.bytes += buffer.length
        written.lines += buffer.toString().split('\n').length - 1
        return new Promise((resolve) => setImmediate(() => resolve(pending--)))
      },
      sync: async () => {},
    }
    const collector = new Collector({ sink, clock, matrix, maxQueueBytes: 16 * 1024 })
    let maxQueued = 0
    const sample = (source, seq, slot) => ({
      kind: 'sample',
      recordId: `${source}.${seq}`,
      source,
      seq,
      at: slot * 30000,
      execution: null,
      process: source,
      role: 'periodic',
      slot,
      boundary: null,
      reading: null,
      admittedAt: slot * 30000,
      completedAt: slot * 30000 + 200,
      rss: 90e6,
      heapUsed: 30e6,
      external: 2e6,
      activeResources: [
        { type: 'TCPSocketWrap', count: 3 },
        { type: 'Timeout', count: 4 },
      ],
      gc: null,
      inventory: `${source}.i${slot}`,
      clients: ['c01.1'],
    })
    const processes = Array.from({ length: 16 }, (_, i) => `p${String(i + 1).padStart(2, '0')}`)
    for (let slot = 0; slot < 2881; slot++) {
      clock.t = slot * 30000
      for (const p of processes) {
        await collector.append(sample(p, slot, slot))
        maxQueued = Math.max(maxQueued, collector.queuedBytes)
      }
    }
    await collector.close()
    expect(written.lines).toBe(2881 * 16)
    expect(collector.records).toBe(2881 * 16)
    expect(collector.failures).toEqual([])
    expect(maxQueued).toBeLessThanOrEqual(16 * 1024 + 1024)
    expect(collector.queue).toHaveLength(0)
    expect(collector.nextSeq.size).toBe(16)
    expect(validateRecord(JSON.parse(collector.lastLine))).toEqual([])

    const strict = new Collector({ sink, clock, matrix, maxQueueBytes: 4096 })
    await strict.append(sample('p01', 0, 0))
    await strict.append(sample('p01', 2, 1))
    await strict.append({ ...sample('p02', 0, 0), clients: ['x'.repeat(2000)] })
    await strict.close()
    expect(strict.failures).toEqual([
      'p01 sequence 2 follows 0',
      'p02.0 sample line of 2387 bytes exceeds 1024 bytes',
    ])
    expect(JSON.parse(strict.lastLine)).toMatchObject({
      kind: 'diagnostic',
      recordId: 'p02.0',
      message: 'oversize sample record p02.0 of 2387 bytes not written',
    })

    const capped = new Collector({ sink, clock, matrix, maxStreamBytes: 600 })
    await capped.append(sample('p01', 0, 0))
    await capped.append(sample('p01', 1, 1))
    await capped.append(sample('p01', 2, 2))
    expect(capped.failures).toEqual(['stream exceeds 600 bytes'])
    expect(capped.records).toBe(1)

    const sent = []
    const channel = new SourceChannel({
      source: 'p03',
      clock,
      maxBytes: 2048,
      send: (message, done) => sent.push(done),
    })
    clock.t = 0
    for (let i = 0; i < 3; i++)
      await channel.emit({ kind: 'diagnostic', recordId: `p03.${i}`, message: 'x'.repeat(500) })
    const blocked = channel.emit({
      kind: 'diagnostic',
      recordId: 'p03.3',
      message: 'x'.repeat(500),
    })
    expect(channel.pendingBytes).toBeLessThanOrEqual(2048)
    await clock.drive(blocked)
    expect(channel.incomplete).toBe('p03 could not enqueue p03.3 within 5000 ms')
    for (const done of sent) done()
    expect(channel.pendingBytes).toBe(0)
  })

  test('preflight fails on a dirty checkout, a wrong SHA, a missing share readback or a missing telemetry proof', async () => {
    const healthy = () => ({
      sha: TREE,
      mode: 'acceptance',
      matrix,
      node: 'v26.1.0',
      execArgv: [],
      git: { head: TREE, porcelain: '' },
      deps: { installed: true, lockMatches: true },
      cluster: { brokers: [1, 2, 3] },
      share: { finalized: { 'share.version': { minVersionLevel: 1, maxVersionLevel: 1 } } },
      telemetry: {
        subscription: { metrics: '*', intervalMs: '30000' },
        pushes: [
          { probe: 'producer', metrics: 12, decoded: true },
          { probe: 'consumer', metrics: 9, decoded: true },
        ],
      },
      clock: { bootId: 'f'.repeat(36), platform: 'linux' },
      provenance: {
        kind: 'image',
        nodes: [1, 2, 3].map((node) => ({ node, digest: `sha256:${'a'.repeat(64)}` })),
      },
      reporter: {
        sinkBytes: matrix.host.brokerReporterFileBytes.value,
        jarSha256: 'c'.repeat(64),
        mounted: 'c'.repeat(64),
        loadedCaps: [1, 2, 3].map(() => matrix.host.brokerReporterFileBytes.value),
      },
      host: { memAvailable: 20 * GIB, freeDisk: 300 * GIB, load1m: 1, census: [] },
    })
    const run = (facts, diagnostic = false) =>
      new Preflight({
        sha: facts.sha,
        mode: facts.mode,
        matrix,
        diagnostic,
        read: async () => facts,
      }).run()
    const ok = await run(healthy())
    expect(ok.failed).toEqual([])
    expect(ok.exitCode).toBe(0)

    const failing = async (mutate) => {
      const facts = healthy()
      mutate(facts)
      const result = await run(facts)
      expect(result.exitCode).toBe(1)
      return result.failed
    }
    expect(await failing((f) => (f.git.porcelain = ' M src/index.js\n'))).toEqual([
      'clean-checkout',
    ])
    expect(await failing((f) => (f.git.head = 'c'.repeat(40)))).toEqual(['expected-sha'])
    expect(await failing((f) => (f.share.finalized = {}))).toEqual(['share-readback'])
    expect(await failing((f) => (f.telemetry.pushes[1].metrics = 0))).toEqual(['telemetry-proof'])
    expect(await failing((f) => (f.telemetry.pushes[0].decoded = false))).toEqual([
      'telemetry-proof',
    ])
    expect(await failing((f) => (f.telemetry.subscription.intervalMs = '300000'))).toEqual([
      'telemetry-subscription',
    ])
    expect(await failing((f) => (f.node = 'v20.11.0'))).toEqual(['node-version'])
    expect(await failing((f) => (f.cluster.brokers = [1, 2]))).toEqual(['cluster-ready'])
    expect(
      await failing((f) => (f.host.census = [{ pid: 7, name: 'jest', class: 'test' }]))
    ).toEqual(['quiet-host'])
    expect(await failing((f) => (f.host.memAvailable = 11 * GIB))).toEqual(['memory'])
    expect(await failing((f) => (f.host.freeDisk = 150 * GIB))).toEqual(['disk'])
    expect(await failing((f) => (f.provenance.nodes[1].digest = 'latest'))).toEqual([
      'broker-provenance',
    ])
    expect(await failing((f) => (f.reporter.mounted = 'd'.repeat(64)))).toEqual(['reporter'])
    expect(await failing((f) => (f.reporter.loadedCaps[1] = 2 * 1024 * 1024))).toEqual(['reporter'])
    expect(await failing((f) => f.reporter.loadedCaps.pop())).toEqual(['reporter'])

    const hosted = healthy()
    hosted.mode = 'smoke'
    hosted.host = { memAvailable: 9 * GIB, freeDisk: 20 * GIB, load1m: 4, census: [] }
    const diagnostic = await run(hosted, true)
    expect(diagnostic.failed).toEqual(['memory', 'disk', 'load'])
    expect(diagnostic.exitCode).toBe(0)
    expect(diagnostic.checks.filter((c) => !c.required).map((c) => c.name)).toEqual([
      'quiet-host',
      'memory',
      'disk',
      'load',
    ])
    hosted.share.finalized = {}
    expect((await run(hosted, true)).exitCode).toBe(1)
    expect(
      () => new Preflight({ sha: TREE, mode: 'acceptance', matrix, diagnostic: true })
    ).toThrow('--diagnostic is accepted only in smoke mode')
  })

  test('the final reporter source takes the Kafka line adaptation before its file cap', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-reporter-'))
    const fixture = path.join(ROOT, 'scripts/e2e/telemetry/CountingTelemetryReporter.java')
    const original = fs.readFileSync(fixture, 'utf8')
    const cap = matrix.host.brokerReporterFileBytes.value
    const legacy = 'import org.apache.kafka.common.utils.AppInfoParser;'
    const internal = 'import org.apache.kafka.common.utils.internals.AppInfoParser;'
    try {
      const kafka44 = reporterSource({ version: '4.4.0', sinkBytes: cap, dir: path.join(dir, 'a') })
      expect(kafka44.split('\n')).toContain(internal)
      expect(kafka44.split('\n')).not.toContain(legacy)
      expect(kafka44).toBe(BoundedReporter.source(original.replace(legacy, internal), cap))
      expect(kafka44).toContain(`MAX_FILE_BYTES = ${cap}L;`)

      const kafka43 = reporterSource({ version: '4.3.1', sinkBytes: cap, dir: path.join(dir, 'b') })
      expect(kafka43).toBe(BoundedReporter.source(original, cap))
      expect(fs.readFileSync(fixture, 'utf8')).toBe(original)
      expect(() =>
        reporterSource({ version: '3.9.0', sinkBytes: cap, dir: path.join(dir, 'c') })
      ).toThrow()
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test('preflight writes its record and fails when the facts cannot be gathered', () => {
    const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-preflight-'))
    try {
      const result = spawnSync(
        process.execPath,
        [
          path.join(__dirname, '../finalSoakPreflight.js'),
          '--expect-sha',
          TREE,
          '--mode',
          'smoke',
          '--out',
          out,
        ],
        { env: { PATH: process.env.PATH }, encoding: 'utf8' }
      )
      expect(result.status).toBe(1)
      const record = JSON.parse(fs.readFileSync(path.join(out, 'preflight.json'), 'utf8'))
      expect(record).toMatchObject({ sha: TREE, mode: 'smoke', checks: [], failed: ['facts'] })
      expect(record.error).toContain('KAFKA_BROKERS must list the brokers')
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  /**
   * A three-node scripted cluster answering the bootstrap and cluster-admin APIs on the in-process
   * socket seam, its ApiVersions carrying share.version, and a supplied-provenance evidence file so
   * no `docker` call is made.
   * @param {number[]} [ids] the nodes the cluster has
   */
  const scriptedBrokers = (ids = [1, 2, 3]) => {
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const wire = createScriptedCluster({
      nodes: ids.map((id) => ({ id, host: `kafka${id}`, port: 9092 })),
      profile: 'clusterAdmin',
    })
    wire.answer('ApiVersions', () => ({
      ...wire.apiVersionsAnswer(),
      supportedFeatures: [{ name: 'share.version', minVersion: 0, maxVersion: 2 }],
      finalizedFeatures: [{ name: 'share.version', minVersionLevel: 1, maxVersionLevel: 1 }],
      finalizedFeaturesEpoch: '5',
    }))
    const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-capture-'))
    const evidence = path.join(out, 'supplied.json')
    const nodes = [1, 2, 3].map((node) => {
      const file = path.join(out, `build-${node}.txt`)
      fs.writeFileSync(file, `kafka 4.4.0 node ${node}\n`)
      return { node, version: '4.4.0', file }
    })
    fs.writeFileSync(evidence, JSON.stringify({ nodes }))
    return { wire, out, evidence }
  }

  test('preflight reads every node through a private cluster built with the options the admin client passes, and disconnects it', async () => {
    const { Kafka } = require('../../../index')
    const { captureBroker } = require('../finalSoakPreflight')
    const { validateCapabilities, validateProvenance } = require('../finalSoakSchema')
    // The default retry, as live: the admin's first by-node lookup refreshes its metadata and retries.
    const capture = async (ids) => {
      const { wire, out, evidence } = scriptedBrokers(ids)
      const kafka = new Kafka(wire.kafkaOptions())
      const factory = Object.getOwnPropertySymbols(kafka).find(
        (s) => s.description === 'private:Kafka:createCluster'
      )
      const original = kafka[factory]
      const built = []
      kafka[factory] = (options) => {
        const cluster = original(options)
        built.push({ options, cluster })
        return cluster
      }
      const admin = kafka.admin()
      const env = process.env.FINAL_SOAK_SUPPLIED_EVIDENCE
      process.env.FINAL_SOAK_SUPPLIED_EVIDENCE = evidence
      await admin.connect()
      try {
        const adminOptions = built[0].options
        const result = await captureBroker(kafka, admin, out).then(
          (value) => ({ value }),
          (error) => ({ error })
        )
        return { result, out, adminOptions, extra: built.slice(1) }
      } finally {
        if (env === undefined) delete process.env.FINAL_SOAK_SUPPLIED_EVIDENCE
        else process.env.FINAL_SOAK_SUPPLIED_EVIDENCE = env
        await admin.disconnect()
      }
    }
    const healthy = await capture([1, 2, 3])
    try {
      expect(healthy.result.error).toBeUndefined()
      expect(healthy.extra).toHaveLength(1)
      const [{ options, cluster }] = healthy.extra
      expect(options).toEqual({
        telemetryMetrics: null,
        allowAutoTopicCreation: false,
        instrumentationEmitter: null,
      })
      expect(healthy.adminOptions.allowAutoTopicCreation).toBe(false)
      expect(cluster.isConnected()).toBe(false)
      const read = (file) => JSON.parse(fs.readFileSync(path.join(healthy.out, file), 'utf8'))
      const capabilities = read('broker/capabilities.json')
      expect(validateCapabilities(capabilities)).toEqual([])
      expect(capabilities.nodes.map((n) => n.node)).toEqual([1, 2, 3])
      for (const node of capabilities.nodes) {
        expect(node.apiRanges.find((r) => r.apiKey === 18)).toEqual({
          apiKey: 18,
          minVersion: 4,
          maxVersion: 4,
        })
        expect(node.finalizedFeatures).toEqual([
          { name: 'share.version', minVersionLevel: 1, maxVersionLevel: 1 },
        ])
      }
      expect(validateProvenance(read('broker/provenance.json'), 'supplied')).toEqual([])
      expect(healthy.result.value.broker).toEqual({
        provenanceKind: 'supplied',
        provenanceFile: 'broker/provenance.json',
        capabilitiesFile: 'broker/capabilities.json',
        composeFile: 'broker/compose.json',
      })
    } finally {
      fs.rmSync(healthy.out, { recursive: true, force: true })
    }

    const missing = await capture([1, 2])
    try {
      expect(missing.result.error.message).toMatch(/Broker 3 not found/)
      expect(missing.extra).toHaveLength(1)
      expect(missing.extra[0].cluster.isConnected()).toBe(false)
      expect(fs.existsSync(path.join(missing.out, 'broker/capabilities.json'))).toBe(false)
    } finally {
      fs.rmSync(missing.out, { recursive: true, force: true })
    }
  })

  test('a preflight that fails while gathering records the stage that failed and every fact gathered before it', async () => {
    const { main } = require('../finalSoakPreflight')
    const { wire, out, evidence } = scriptedBrokers()
    const env = process.env.FINAL_SOAK_SUPPLIED_EVIDENCE
    process.env.FINAL_SOAK_SUPPLIED_EVIDENCE = evidence
    try {
      await expect(
        main(['--expect-sha', TREE, '--mode', 'smoke', '--out', out], {
          kafkaOptions: wire.kafkaOptions(),
        })
      ).rejects.toThrow()
      const record = JSON.parse(fs.readFileSync(path.join(out, 'preflight.json'), 'utf8'))
      expect(record).toMatchObject({
        sha: TREE,
        mode: 'smoke',
        checks: [],
        failed: ['facts'],
        stage: 'telemetry-subscription',
      })
      expect(Object.keys(record.completed)).toEqual([
        'describe-cluster',
        'describe-features',
        'capture-broker',
      ])
      expect(record.completed['describe-cluster'].brokers.map((b) => b.nodeId)).toEqual([1, 2, 3])
      expect(record.completed['describe-features'].finalizedFeatures['share.version']).toEqual({
        minVersionLevel: 1,
        maxVersionLevel: 1,
      })
      expect(record.completed['capture-broker'].broker.provenanceKind).toBe('supplied')
      expect(record.error).toMatch(/IncrementalAlterConfigs|incrementalAlterConfigs|44/)
    } finally {
      if (env === undefined) delete process.env.FINAL_SOAK_SUPPLIED_EVIDENCE
      else process.env.FINAL_SOAK_SUPPLIED_EVIDENCE = env
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  /**
   * Runs the runbook against PATH stubs that log every call. `FAIL_ON` makes the call containing it
   * exit with `FAIL_STATUS` (1 by default); the docker stub reports share.version 0 until upgraded;
   * the driver stub writes `config.json` first, as the driver's first act does, and a
   * `supervisor.json` (`SUPERVISOR_JSON`, a green receipt by default) as its last, then with `HANG`
   * never ends by itself; `--driver-limit` prints `DRIVER_LIMIT_S`; the workload check runs the
   * real driver command. The core checker prints `CORE_REPORT` (a smoke's NOT ACCEPTED by default)
   * and exits with `CORE_STATUS` (1 by default), or with `CORE_REAL` set runs the real checker.
   */
  const runbookHarness = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-runbook-'))
    const bin = path.join(dir, 'bin')
    const log = path.join(dir, 'calls.log')
    const artifacts = path.join(dir, 'artifacts')
    fs.mkdirSync(bin)
    const bodies = {
      docker: [
        'echo "$COMPOSE_PROJECT_NAME $COMPOSE_FILE" >> "$CALLS.env"',
        'case "$*" in',
        '  *" describe") level=0; [ -f "$CALLS.upgraded" ] && level=1',
        '    printf "Feature: share.version\\tSupportedMinVersion: 0\\tSupportedMaxVersion: 2\\tFinalizedVersionLevel: %s\\tEpoch: 5\\n" "$level" ;;',
        '  *"upgrade --feature share.version=1") touch "$CALLS.upgraded" ;;',
        'esac',
      ].join('\n'),
      node: 'case "$*" in *waitForKafka*) echo "$COMPOSE_FILE" >> "$CALLS.wait" ;; esac',
    }
    const green = JSON.stringify({
      failures: [],
      aborted: null,
      incomplete: null,
      problems: [],
      forced: [],
    })
    const first = {
      node: [
        'receipt="$SUPERVISOR_JSON"; [ -n "$receipt" ] || receipt="$GREEN"',
        'limit="$DRIVER_LIMIT_S"; [ -n "$limit" ] || limit=86400',
        'case "$*" in *--driver-limit*) echo "$limit" ;;',
        '*--expose-gc*)',
        '  prev=; for a in "$@"; do [ "$prev" = --out ] && echo "{}" > "$a/config.json" && printf "%s" "$receipt" > "$a/supervisor.json"; prev="$a"; done',
        '  [ -z "$HANG" ] || exec /bin/sleep 60 ;;',
        '*--check-workload*) [ -z "$FAIL_ON" ] || case "node $*" in *"$FAIL_ON"*) exit 1 ;; esac',
        '  exec "$REAL_NODE" "$@" ;;',
        '*finalSoakCore.js*) [ -z "$CORE_REAL" ] || exec "$REAL_NODE" "$@"',
        '  printf "%s\\n" "$CORE_REPORT"; exit "$CORE_STATUS" ;;',
        'esac',
      ].join('\n'),
    }
    for (const name of ['git', 'npm', 'docker', 'node', 'tar', 'sleep']) {
      const script = [
        '#!/bin/sh',
        `echo "${name} $*" >> "$CALLS"`,
        first[name] ?? '',
        'if [ -n "$FAIL_ON" ]; then',
        `  case "${name} $*" in *"$FAIL_ON"*) echo "stub failure: ${name} $*" >&2; exit "\${FAIL_STATUS:-1}" ;; esac`,
        'fi',
        bodies[name] ?? '',
        '',
      ].join('\n')
      fs.writeFileSync(path.join(bin, name), script, { mode: 0o755 })
    }
    return {
      run(args, extra = {}, runbook = RUNBOOK) {
        for (const suffix of ['', '.env', '.upgraded', '.wait']) {
          fs.rmSync(`${log}${suffix}`, { force: true })
        }
        fs.rmSync(artifacts, { recursive: true, force: true })
        return spawnSync('bash', [runbook, ...args], {
          env: {
            PATH: `${bin}:/usr/bin:/bin`,
            CALLS: log,
            FINAL_SOAK_ARTIFACTS: artifacts,
            REAL_NODE: process.execPath,
            GREEN: green,
            CORE_REPORT:
              'CORE VERDICT: NOT ACCEPTED (smoke runs are diagnostic and are never ACCEPTED)',
            CORE_STATUS: '1',
            ...extra,
          },
          encoding: 'utf8',
        })
      },
      calls: () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').trim().split('\n') : []),
      env: () => fs.readFileSync(`${log}.env`, 'utf8').trim().split('\n'),
      waited: () => fs.readFileSync(`${log}.wait`, 'utf8').trim().split('\n'),
      /** @returns {{ out: string, receipt: object, log: string }} the run's sibling files */
      evidence() {
        const name = fs.readdirSync(artifacts).find((f) => f.endsWith('.runbook.json'))
        if (!name) throw new Error('the runbook left no receipt beside its artifact')
        const out = path.join(artifacts, name.replace(/\.runbook\.json$/, ''))
        return {
          out,
          receipt: JSON.parse(fs.readFileSync(`${out}.runbook.json`, 'utf8')),
          log: fs.readFileSync(`${out}.runbook.log`, 'utf8'),
        }
      },
      remove: () => fs.rmSync(dir, { recursive: true, force: true }),
    }
  }

  test('the runbook forwards SHA and mode to preflight and driver exactly', () => {
    const harness = runbookHarness()
    try {
      const result = harness.run([TREE, 'smoke'])
      expect(result.stderr).toBe('')
      expect(result.status).toBe(0)
      const calls = harness.calls()
      const { out, receipt, log } = harness.evidence()
      const at = (call) => calls.indexOf(call)
      const features =
        'docker compose exec -T kafka1 /opt/kafka/bin/kafka-features.sh --bootstrap-server kafka1:29092'
      expect(calls.slice(0, 9)).toEqual([
        `git checkout --detach ${TREE}`,
        'npm ci --ignore-scripts',
        `node scripts/soak/finalSoakPreflight.js --prepare-reporter --out ${out}`,
        'docker compose up -d --wait',
        'node scripts/waitForKafka.js',
        `${features} describe`,
        `${features} upgrade --feature share.version=1`,
        'sleep 2',
        `${features} describe`,
      ])
      expect(new Set(harness.env())).toEqual(
        new Set([`kafkajs docker-compose.telemetry.yml:${out}/broker/compose.json`])
      )
      // waitForKafka passes COMPOSE_FILE to `docker compose -f`, which takes one path only.
      expect(harness.waited()).toEqual(['docker-compose.telemetry.yml'])
      const preflight = `node scripts/soak/finalSoakPreflight.js --expect-sha ${TREE} --mode smoke --out ${out}`
      const driver = `node --expose-gc --no-flush-bytecode scripts/soak/finalSoak.js --mode smoke --expect-sha ${TREE} --matrix scripts/soak/finalSoakMatrix.json --out ${out}`
      const finalize = `node scripts/soak/finalSoak.js --finalize --out ${out} --status 0`
      const outerExit = calls.find((c) =>
        new RegExp(
          `^node scripts/soak/finalSoak.js --outer-exit --out ${out} --status 0 --exit-wall-ms \\d{13}$`
        ).test(c)
      )
      const dump = `node scripts/soak/finalSoakDump.js --out ${out}`
      expect(at(preflight)).toBeGreaterThan(at(`${features} describe`))
      expect(at(driver)).toBeGreaterThan(at(preflight))
      expect(at(outerExit)).toBeGreaterThan(at(driver))
      expect(at(dump)).toBeGreaterThan(at(outerExit))
      expect(at(finalize)).toBeGreaterThan(at(dump))
      expect(at(`node scripts/soak/finalSoak.js --cleanup --out ${out}`)).toBeGreaterThan(at(dump))
      expect(at('docker compose down --volumes')).toBeGreaterThan(at(dump))
      expect(at(`node scripts/soak/finalSoak.js --summary --out ${out}`)).toBeGreaterThan(
        at(finalize)
      )
      expect(at(`node scripts/soak/finalSoak.js --check-workload --out ${out}`)).toBeGreaterThan(
        at(`node scripts/soak/finalSoak.js --summary --out ${out}`)
      )
      expect(at('docker compose down --volumes')).toBeGreaterThan(at(finalize))
      expect(at(`node scripts/soak/finalSoak.js --cleanup --out ${out}`)).toBeGreaterThan(
        at(finalize)
      )
      expect(calls.some((c) => c.startsWith('docker compose logs'))).toBe(false)
      expect(log).toContain('FinalizedVersionLevel: 0')
      expect(log).toContain('FinalizedVersionLevel: 1')
      expect(calls.find((c) => c.startsWith('tar '))).toContain(path.basename(out))
      expect(receipt).toEqual({
        artifact: path.basename(out),
        failedStage: null,
        exitStatus: 0,
        driverStatus: 0,
      })
      expect(log).toContain(`artifact: ${out}.tar.gz`)

      expect(harness.run([TREE, 'smoke'], { FINAL_SOAK_DIAGNOSTIC: '1' }).status).toBe(0)
      const diagnostic = harness.calls()
      expect(diagnostic.find((c) => c.includes('--expect-sha'))).toMatch(
        /--mode smoke --out \S+ --diagnostic$/
      )
      expect(diagnostic.find((c) => c.includes('--expose-gc'))).toMatch(/--diagnostic$/)

      expect(harness.run([TREE, 'acceptance'], { FINAL_SOAK_DIAGNOSTIC: '1' }).status).toBe(2)
      expect(harness.run(['main', 'smoke']).status).toBe(2)
      expect(harness.run([TREE, 'day']).status).toBe(2)
      expect(harness.calls()).toEqual([])
    } finally {
      harness.remove()
    }
  })

  test('an aborted workload whose driver exited 0 fails the runbook at stage workload after sealing, keeping the exit code and the broker logs', () => {
    const harness = runbookHarness()
    try {
      const aborted = JSON.stringify({
        failures: [],
        aborted: 'step h00m0 failed: h00m0w1 saw no paused partition transfer in p02, p04',
        incomplete: null,
        problems: ['step h00m0 error: h00m0w1 saw no paused partition transfer in p02, p04'],
        forced: [],
      })
      const result = harness.run([TREE, 'smoke'], { SUPERVISOR_JSON: aborted })
      expect(result.status).toBe(1)
      const { out, receipt, log } = harness.evidence()
      expect(receipt).toEqual({
        artifact: path.basename(out),
        failedStage: 'workload',
        exitStatus: 1,
        driverStatus: 0,
      })
      expect(log).toContain(
        'workload not green: aborted: "step h00m0 failed: h00m0w1 saw no paused partition transfer in p02, p04"'
      )
      expect(log).toContain('runbook failed at stage workload (exit status 1)')
      const calls = harness.calls()
      const at = (prefix) => calls.findIndex((c) => c.startsWith(prefix))
      expect(calls).toContain(`node scripts/soak/finalSoak.js --finalize --out ${out} --status 0`)
      expect(
        calls.some((c) =>
          new RegExp(`^node scripts/soak/finalSoak.js --outer-exit --out ${out} --status 0 `).test(
            c
          )
        )
      ).toBe(true)
      expect(at('node scripts/soak/finalSoak.js --check-workload')).toBeGreaterThan(
        at('node scripts/soak/finalSoak.js --finalize')
      )
      expect(at('docker compose logs --no-color --timestamps')).toBeGreaterThan(
        at('node scripts/soak/finalSoak.js --check-workload')
      )
      expect(at('docker compose down --volumes')).toBeGreaterThan(
        at('docker compose logs --no-color --timestamps')
      )
      expect(calls.find((c) => c.startsWith('tar '))).toContain(path.basename(out))
    } finally {
      harness.remove()
    }
  })

  test('the runbook runs the core checker after the workload check, keeps its report beside the receipt and passes a smoke that is NOT ACCEPTED', () => {
    const harness = runbookHarness()
    try {
      const summary = path.join(os.tmpdir(), `final-soak-summary-${process.pid}.md`)
      fs.rmSync(summary, { force: true })
      const result = harness.run([TREE, 'smoke'], { GITHUB_STEP_SUMMARY: summary })
      expect(result.status).toBe(0)
      const { out, receipt, log } = harness.evidence()
      const calls = harness.calls()
      const core = `node scripts/evidence/finalSoakCore.js ${out}`
      expect(calls.indexOf(core)).toBeGreaterThan(
        calls.indexOf(`node scripts/soak/finalSoak.js --check-workload --out ${out}`)
      )
      expect(calls.indexOf(core)).toBeGreaterThan(
        calls.indexOf(`node scripts/soak/finalSoak.js --finalize --out ${out} --status 0`)
      )
      expect(fs.readFileSync(`${out}.core.txt`, 'utf8')).toMatch(/^CORE VERDICT: NOT ACCEPTED/)
      expect(fs.existsSync(path.join(out, 'core.txt'))).toBe(false)
      expect(log).toContain('core checker verdict: NOT ACCEPTED (exit status 1)')
      expect(log).toContain('A smoke is never ACCEPTED')
      const text = fs.readFileSync(summary, 'utf8')
      fs.rmSync(summary, { force: true })
      expect(text).toContain('A smoke is never ACCEPTED')
      expect(text).toContain('CORE VERDICT: NOT ACCEPTED')
      expect(receipt).toMatchObject({ failedStage: null, exitStatus: 0 })
    } finally {
      harness.remove()
    }
  })

  test('a REJECTED core verdict from the real checker fails the runbook at stage core after sealing', () => {
    const harness = runbookHarness()
    try {
      const result = harness.run([TREE, 'smoke'], { CORE_REAL: '1' })
      expect(result.status).toBe(1)
      const { out, receipt, log } = harness.evidence()
      expect(receipt).toEqual({
        artifact: path.basename(out),
        failedStage: 'core',
        exitStatus: 1,
        driverStatus: 0,
      })
      expect(fs.readFileSync(`${out}.core.txt`, 'utf8')).toMatch(/^CORE VERDICT: REJECTED/)
      expect(log).toContain('core checker verdict: REJECTED (exit status 1)')
      expect(log).toContain('runbook failed at stage core (exit status 1)')
      expect(harness.calls().find((c) => c.startsWith('tar '))).toContain(path.basename(out))
    } finally {
      harness.remove()
    }
  })

  test.each([
    [
      'a checker that printed no verdict',
      'smoke',
      { CORE_REPORT: '', CORE_STATUS: '7' },
      'none (exit status 7)',
    ],
    ['an acceptance run that is NOT ACCEPTED', 'acceptance', {}, 'NOT ACCEPTED (exit status 1)'],
    [
      'a smoke whose checker says ACCEPTED with a failing exit status',
      'smoke',
      { CORE_REPORT: 'CORE VERDICT: ACCEPTED', CORE_STATUS: '1' },
      'ACCEPTED (exit status 1)',
    ],
    [
      'a smoke whose checker prints NOT ACCEPTED but exits with a crash status',
      'smoke',
      { CORE_REPORT: 'CORE VERDICT: NOT ACCEPTED', CORE_STATUS: '2' },
      'NOT ACCEPTED (exit status 2)',
    ],
    [
      'an acceptance run whose checker prints ACCEPTED but exits 1',
      'acceptance',
      { CORE_REPORT: 'CORE VERDICT: ACCEPTED', CORE_STATUS: '1' },
      'ACCEPTED (exit status 1)',
    ],
  ])('%s fails the runbook at stage core', (_, mode, env, said) => {
    const harness = runbookHarness()
    try {
      expect(harness.run([TREE, mode], env).status).toBe(1)
      const { receipt, log } = harness.evidence()
      expect(receipt).toMatchObject({ failedStage: 'core', exitStatus: 1 })
      expect(log).toContain(`core checker verdict: ${said}`)
    } finally {
      harness.remove()
    }
  })

  test('an acceptance run the core checker ACCEPTS passes the core stage', () => {
    const harness = runbookHarness()
    try {
      const result = harness.run([TREE, 'acceptance'], {
        CORE_REPORT: 'CORE VERDICT: ACCEPTED',
        CORE_STATUS: '0',
      })
      expect(result.status).toBe(0)
      const { receipt, log } = harness.evidence()
      expect(receipt).toMatchObject({ failedStage: null, exitStatus: 0 })
      expect(log).toContain('core checker verdict: ACCEPTED (exit status 0)')
      expect(log).not.toContain('A smoke is never ACCEPTED')
    } finally {
      harness.remove()
    }
  })

  test('a failed workload still runs the core checker of the checkout for diagnosis beside the receipt, and its verdict never changes the failed status', () => {
    const harness = runbookHarness()
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-checkout-'))
    try {
      const runbook = path.join(root, 'scripts/soak/finalSoakNode.sh')
      const checker = path.join(root, 'scripts/evidence/finalSoakCore.js')
      fs.mkdirSync(path.dirname(runbook), { recursive: true })
      fs.mkdirSync(path.dirname(checker), { recursive: true })
      fs.copyFileSync(RUNBOOK, runbook)
      fs.writeFileSync(checker, '')
      const failing = { FAIL_ON: '--check-workload', CORE_STATUS: '0' }
      expect(harness.run([TREE, 'smoke'], failing, runbook).status).toBe(1)
      const { out, receipt, log } = harness.evidence()
      expect(receipt).toEqual({
        artifact: path.basename(out),
        failedStage: 'workload',
        exitStatus: 1,
        driverStatus: 0,
      })
      const calls = harness.calls()
      const core = `node scripts/evidence/finalSoakCore.js ${out}`
      expect(calls.indexOf(core)).toBeGreaterThan(
        calls.indexOf(`node scripts/soak/finalSoak.js --check-workload --out ${out}`)
      )
      expect(calls.indexOf(core)).toBeGreaterThan(calls.indexOf('docker compose down --volumes'))
      expect(fs.existsSync(`${out}.core.txt`)).toBe(true)
      expect(log).toContain(
        `core checker report for diagnosis only (the run failed at stage workload): ${out}.core.txt`
      )

      fs.rmSync(checker)
      expect(harness.run([TREE, 'smoke'], failing, runbook).status).toBe(1)
      expect(harness.calls().some((c) => c.includes('finalSoakCore.js'))).toBe(false)
      expect(harness.evidence().receipt).toMatchObject({ failedStage: 'workload', exitStatus: 1 })
    } finally {
      harness.remove()
      fs.rmSync(root, { recursive: true, force: true })
    }
  })

  test('a supplied cluster is refused before anything is done', () => {
    const harness = runbookHarness()
    try {
      const result = harness.run([TREE, 'smoke'], { FINAL_SOAK_SUPPLIED_BROKERS: 'b:1' })
      expect(result.status).toBe(2)
      expect(result.stderr).toContain('a supplied cluster is not supported')
      expect(harness.calls()).toEqual([])
    } finally {
      harness.remove()
    }
  })

  test('a driver past its limit is stopped by the runbook watchdog, which names the escalation, while the heartbeat records stream progress; the run fails at the driver stage after sealing', () => {
    const harness = runbookHarness()
    try {
      const began = Date.now()
      const result = harness.run([TREE, 'smoke'], { HANG: '1', DRIVER_LIMIT_S: '1' })
      expect(Date.now() - began).toBeLessThan(30000)
      const { out, receipt } = harness.evidence()
      expect(result.status).toBe(143)
      expect(receipt).toEqual({
        artifact: path.basename(out),
        failedStage: 'driver',
        exitStatus: 143,
        driverStatus: 143,
      })
      expect(fs.readFileSync(`${out}.watchdog`, 'utf8')).toContain(
        'driver ran past its limit of 1 s: TERM'
      )
      expect(fs.readFileSync(path.join(out, 'heartbeat'), 'utf8')).toMatch(
        /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ stream-bytes=\d+ unchanged-s=\d+\n$/
      )
      const calls = harness.calls()
      expect(calls).toContain(
        `node scripts/soak/finalSoak.js --driver-limit --mode smoke --out ${out}`
      )
      expect(calls.some((c) => c.startsWith('node scripts/soak/finalSoak.js --finalize'))).toBe(
        true
      )
      expect(calls).toContain('docker compose down --volumes')
    } finally {
      harness.remove()
    }
  })

  test('a failing runbook stage keeps its log, names the stage, cleans up what it started and archives', () => {
    const harness = runbookHarness()
    const failAt = (failOn, status = 1) => {
      const result = harness.run([TREE, 'smoke'], { FAIL_ON: failOn, FAIL_STATUS: String(status) })
      const evidence = harness.evidence()
      expect(result.status).toBe(status)
      expect(result.stderr).toBe('')
      expect(harness.calls().find((c) => c.startsWith('tar '))).toContain(
        path.basename(evidence.out)
      )
      return { ...evidence, calls: harness.calls() }
    }
    try {
      const reporter = failAt('--prepare-reporter')
      expect(reporter.receipt).toEqual({
        artifact: path.basename(reporter.out),
        failedStage: 'prepare-reporter',
        exitStatus: 1,
        driverStatus: null,
      })
      expect(reporter.log).toContain(
        `stub failure: node scripts/soak/finalSoakPreflight.js --prepare-reporter --out ${reporter.out}`
      )
      expect(reporter.log).toContain('runbook failed at stage prepare-reporter (exit status 1)')
      expect(reporter.calls.some((c) => c.startsWith('docker'))).toBe(false)
      expect(reporter.calls.some((c) => c.includes('--cleanup'))).toBe(false)

      const share = failAt('upgrade --feature')
      expect(share.receipt.failedStage).toBe('share-version')
      expect(share.calls.filter((c) => c.endsWith(' describe'))).toHaveLength(30)
      expect(share.calls).toContain('docker compose down --volumes')

      const preflight = failAt(`--expect-sha ${TREE} --mode smoke`)
      expect(preflight.receipt).toEqual({
        artifact: path.basename(preflight.out),
        failedStage: 'preflight',
        exitStatus: 1,
        driverStatus: null,
      })
      expect(preflight.calls.some((c) => c.includes('--expose-gc'))).toBe(false)
      expect(preflight.calls.some((c) => c.includes('--cleanup'))).toBe(false)
      expect(preflight.log).toContain(
        'no driver config: the driver created no run topics or groups, so none are removed'
      )
      expect(preflight.log).not.toContain('ENOENT')
      const logs = preflight.calls.indexOf('docker compose logs --no-color --timestamps')
      expect(logs).toBeGreaterThan(-1)
      expect(preflight.calls.indexOf('docker compose down --volumes')).toBeGreaterThan(logs)
      expect(preflight.log).toContain(`broker logs: ${preflight.out}.compose.log`)

      const driver = failAt('--expose-gc', 3)
      expect(driver.receipt).toEqual({
        artifact: path.basename(driver.out),
        failedStage: 'driver',
        exitStatus: 3,
        driverStatus: 3,
      })
      expect(driver.calls).toContain(
        `node scripts/soak/finalSoak.js --finalize --out ${driver.out} --status 3`
      )
      expect(driver.calls).toContain(`node scripts/soak/finalSoakDump.js --out ${driver.out}`)
      expect(
        driver.calls.indexOf(`node scripts/soak/finalSoakDump.js --out ${driver.out}`)
      ).toBeLessThan(
        driver.calls.indexOf(`node scripts/soak/finalSoak.js --cleanup --out ${driver.out}`)
      )
      expect(driver.calls).toContain(`node scripts/soak/finalSoak.js --summary --out ${driver.out}`)
      expect(driver.calls).toContain(`node scripts/soak/finalSoak.js --cleanup --out ${driver.out}`)
      expect(driver.calls).toContain('docker compose down --volumes')

      const dump = failAt('finalSoakDump.js')
      expect(dump.receipt).toMatchObject({ failedStage: 'dump', exitStatus: 1, driverStatus: 0 })
      expect(dump.calls).toContain(
        `node scripts/soak/finalSoak.js --finalize --out ${dump.out} --status 0`
      )
      expect(dump.calls).toContain(`node scripts/soak/finalSoak.js --cleanup --out ${dump.out}`)

      const finalize = failAt('--finalize')
      expect(finalize.receipt).toMatchObject({ failedStage: 'finalize', driverStatus: 0 })
      expect(finalize.calls).toContain(
        `node scripts/soak/finalSoak.js --summary --out ${finalize.out}`
      )
    } finally {
      harness.remove()
    }
  })

  test('the config binds each real kind to its own process beside an isolated seed', () => {
    const config = buildConfig(matrix, {
      mode: 'acceptance',
      runId: RUN,
      treeSha: TREE,
      brokers: ['b:1'],
    })
    expect(validateConfig(config, matrix)).toEqual([])
    expect(config.processes).toHaveLength(16)
    expect(config.diagnostic).toBe(false)
    const subjects = config.clients.filter((c) => c.purpose === 'subject')
    expect(sorted(subjects.map((c) => c.clientKind))).toEqual(sorted(CLIENT_KINDS))
    expect(new Set(subjects.map((c) => c.process)).size).toBe(12)
    const telemetry = subjects.filter((c) => c.telemetryEnabled)
    expect(sorted(telemetry.map((c) => c.clientKind))).toEqual([
      'telemetry-consumer',
      'telemetry-producer',
    ])
    expect(telemetry.find((c) => c.clientKind === 'telemetry-producer').group).toBeNull()
    const seed = config.processes.find((p) => p.role === 'seed')
    expect(config.clients.filter((c) => c.process === seed.slot)).toEqual([
      expect.objectContaining({
        purpose: 'control',
        clientKind: 'classic-eager-dynamic',
        arm: 'interning-seeded',
      }),
    ])
    const extras = config.clients.filter((c) => c.purpose === 'churn')
    expect(sorted(extras.map((c) => c.clientKind))).toEqual(sorted(kindsWhere((k) => k.churn)))
    expect(config.clients.filter((c) => c.purpose === 'observer')).toEqual([
      expect.objectContaining({ slot: 'c21', process: 'p14', group: null }),
      expect.objectContaining({ slot: 'c22', process: 'p15', group: null }),
    ])
  })

  test('the scheduler marks late work failed and never shifts an idle window', async () => {
    const clock = new VirtualClock()
    const steps = [
      { id: 'a', type: 'execution', lane: 'work', at: 0, deadline: 100 },
      { id: 'b', type: 'execution', lane: 'work', at: 50, deadline: 120 },
      { id: 'c', type: 'execution', lane: 'work', at: 60, deadline: 300 },
      { id: 'idle', type: 'idle', lane: 'timer', at: 300, until: 400 },
      { id: 'q', type: 'quiet', lane: 'timer', at: 350, deadline: 390 },
    ]
    const started = {}
    const perform = async (step) => {
      started[step.id] = clock.now()
      if (step.id === 'a') await clock.sleepUntil(150)
      if (step.id === 'c') await clock.sleepUntil(320)
    }
    const outcomes = []
    const scheduler = new Scheduler({ clock, perform, onOutcome: (o) => outcomes.push(o) })
    const summary = await clock.drive(scheduler.run(steps))
    expect(started).toEqual({ a: 0, c: 150, idle: 300, q: 350 })
    expect(outcomes.map((o) => [o.id, o.outcome])).toEqual([
      ['a', 'late'],
      ['b', 'late'],
      ['idle', 'ok'],
      ['c', 'late'],
      ['q', 'ok'],
    ])
    expect(summary).toEqual({ ok: 2, late: 3, error: 0, aborted: 0 })
  })

  test('each sample cuts its inventory, then takes two new collections at least 100 ms apart', async () => {
    const clock = new VirtualClock()
    const calls = []
    const sampler = new Sampler({
      clock,
      gc: () => calls.push(['gc', clock.now()]),
      turn: () => clock.sleepUntil(clock.now()),
      memory: () => {
        calls.push(['memory', clock.now()])
        return { rss: 9, heapUsed: 8, external: 7 }
      },
      resources: () => ['TTYWrap'],
    })
    const read = (n) =>
      sampler.read({
        role: 'quiet',
        slot: null,
        boundary: 'h00',
        reading: n,
        cut: () => {
          calls.push(['cut', clock.now()])
          return `i${n}`
        },
      })
    const first = await clock.drive(read(0))
    const second = await clock.drive(read(1))
    expect(calls.map(([what]) => what)).toEqual([
      'cut',
      'gc',
      'gc',
      'memory',
      'cut',
      'gc',
      'gc',
      'memory',
    ])
    expect(first.gc.secondBeginAt - first.gc.firstEndAt).toBeGreaterThanOrEqual(100)
    expect(first.gc).toMatchObject({ forcedCollections: 2, cumulativeForced: 2 })
    expect(second.gc).toMatchObject({ forcedCollections: 2, cumulativeForced: 4 })
    expect(second.gc.id).not.toBe(first.gc.id)
    expect(first).toMatchObject({ inventory: 'i0', rss: 9, heapUsed: 8, external: 7, reading: 0 })
    expect(first.activeResources).toEqual([
      { type: 'TCPSocketWrap', count: 0 },
      { type: 'Timeout', count: 0 },
      { type: 'TTYWrap', count: 1 },
    ])
  })

  test('the passive observer keeps receivers, results, errors and timing, and inventories every pause state', async () => {
    class Owner {
      plain(x) {
        return this.value ?? x
      }

      later() {
        return Promise.resolve(this)
      }

      broken() {
        throw this.error
      }
    }
    const owner = new Owner()
    owner.error = new Error('kept')
    const promised = Owner.prototype.later
    const observer = new PassiveObserver({
      owners: { Owner },
      watched: { Owner: ['plain', 'later', 'broken'] },
    })
    observer.install()
    const result = {}
    expect(owner.plain(result)).toBe(result)
    const p = owner.later()
    expect(p).toBeInstanceOf(Promise)
    await expect(p).resolves.toBe(owner)
    expect(() => owner.broken()).toThrow(owner.error)
    expect(observer.live('Owner')).toEqual([owner])
    observer.restore()
    expect(Owner.prototype.later).toBe(promised)

    const topicIds = { whole: IDS.A, some: IDS.A, 'never-paused': IDS.A }
    const context = new AsyncLocalStorage()
    const pauses = new PassiveObserver({
      owners: { ConsumerGroup, SubscriptionState },
      watched: {
        ConsumerGroup: ['retainSubscription'],
        SubscriptionState: ['pause', 'resume', 'retainTopics', 'retireTopic', 'clearPauses'],
      },
      context,
    })
    const lives = new Map()
    pauses.lives = (client, topic, topicId) => lives.get(`${client}/${topic}/${topicId}`) ?? null
    pauses.install()
    try {
      const group = realGroup({ topicIds })
      const state = group.subscriptionState
      context.run('c01.1', () => {
        group.retainSubscription(['whole', 'some', 'never-paused'])
        for (const t of Object.keys(topicIds)) lives.set(`c01.1/${t}/${IDS.A}`, `p01.7/${IDS.A}`)
        state.pause([{ topic: 'whole' }])
        state.resume([{ topic: 'whole', partitions: [1] }])
        state.pause([{ topic: 'some', partitions: [0, 1] }])
        state.resume([{ topic: 'some', partitions: [0, 1] }])
        state.resume([{ topic: 'never-paused', partitions: [2] }])
      })
      topicIds.whole = topicIds.some = IDS.B
      const entries = pauses.pauseStates()
      expect(
        entries.map(({ topic, pauseAll, paused, resumed, originTopicId }) => ({
          topic,
          pauseAll,
          paused,
          resumed,
          originTopicId,
        }))
      ).toEqual([
        { topic: 'whole', pauseAll: true, paused: [], resumed: [1], originTopicId: IDS.A },
        { topic: 'some', pauseAll: false, paused: [], resumed: [], originTopicId: IDS.A },
        { topic: 'never-paused', pauseAll: false, paused: [], resumed: [], originTopicId: IDS.A },
      ])
      expect(new Set(entries.map((e) => e.stateId)).size).toBe(3)
      expect(entries.every((e) => e.subscriptionLife === `p01.7/${IDS.A}`)).toBe(true)
      expect(entries.every((e) => e.client === 'c01.1' && e.run === pauses.runOf(group))).toBe(true)
      context.run('c01.1', () => group.retainSubscription(['whole', 'never-paused']))
      expect(pauses.pauseStates().map((e) => e.topic)).toEqual(['whole', 'never-paused'])
    } finally {
      pauses.restore()
    }
  })

  test('every watched method exists on the real product classes', () => {
    const observer = new PassiveObserver()
    observer.install()
    try {
      expect(observer.missing()).toEqual([])
      expect(observer.restores.length).toBe(
        Object.values(PassiveObserver.WATCHED).reduce((n, methods) => n + methods.length, 0)
      )
    } finally {
      observer.restore()
    }
    const removed = new PassiveObserver({
      owners: { ...PassiveObserver.originals(), Runner: class {} },
    })
    removed.install()
    removed.restore()
    expect(removed.missing()).toEqual(['Runner#processEachMessage', 'Runner#processEachBatch'])
  })

  test('the observer reads every ordinary backing table from its real source and reports a missing one', () => {
    const context = new AsyncLocalStorage()
    const events = []
    const observer = new PassiveObserver({
      owners: {
        ConsumerGroup,
        SubscriptionState,
        GroupOwnership,
        OffsetOperations,
        ResetObligations,
      },
      watched: {
        ConsumerGroup: ['retainSubscription'],
        SubscriptionState: ['pause'],
        GroupOwnership: ['retain', 'renew'],
        OffsetOperations: ['begin', 'end'],
        ResetObligations: ['owe', 'discharge'],
      },
      context,
      onEvent: (e) => events.push(e),
    })
    observer.install()
    try {
      const topicIds = { main: IDS.A, txn: IDS.B, grown: IDS.C }
      const group = realGroup({ kind: 'consumer', topicIds })
      group.cluster.targetTopics = new Set(['main', 'txn', 'grown'])
      group.groupMembership.grownTopicNames.add('grown')
      group.topicConfigurations = { main: {}, txn: {}, gone: {} }
      group.preferredReadReplicasPerTopicPartition.main = { 0: { nodeId: 2, expireAt: 9 } }
      group.preferredReadReplicasPerTopicPartition.txn = {}
      group.seekOffset.set('main', 0, '42')
      group.acquired.set('main|0', { acquiredAt: 3, changedAt: 3, recreatedAt: 0 })
      group.haltedPartitions.set('txn|0', { offset: '7', reason: 'corrupt', error: new Error() })
      let ticket
      context.run('c03.1', () => {
        group.retainSubscription(['main', 'txn'])
        group.subscriptionState.pause([{ topic: 'main', partitions: [0] }])
        group.ownership.retain(0)
        group.ownership.renew()
        ticket = group.operations.begin([{ topic: 'main', partition: 0 }], { mutation: true })
        group.obligations.owe('txn', IDS.B)
        group.obligations.discharge('txn', 0, IDS.B)
      })
      const { sections, missingSources } = observer.ordinarySections('c03.1')
      expect(missingSources).toEqual([])
      const tables = Object.fromEntries(
        sections['other-table'].map((t) => [t.table, t.keys.map((k) => [k.topic, k.partition])])
      )
      expect(tables).toEqual({
        acquired: [['main', 0]],
        halted: [['txn', 0]],
        'topic-configurations': [
          ['main', null],
          ['txn', null],
        ],
        'preferred-replicas': [
          ['main', 0],
          ['txn', null],
        ],
        'queued-seeks': [['main', 0]],
        'grown-topic-names': [['grown', null]],
        'cluster-targets': [
          ['main', null],
          ['txn', null],
          ['grown', null],
        ],
      })
      expect(sections['membership-state']).toEqual([
        {
          client: 'c03.1',
          run: observer.runOf(group),
          token: 0,
          current: false,
          holders: 1,
          refused: false,
        },
        {
          client: 'c03.1',
          run: observer.runOf(group),
          token: 1,
          current: true,
          holders: 0,
          refused: false,
        },
      ])
      const [cell] = sections['operation-cell']
      const begun = events.find((e) => e.type === 'ticket').record
      expect(begun).toMatchObject({
        phase: 'begin',
        client: 'c03.1',
        ownerRun: observer.runOf(group),
        token: 1,
        keys: [{ topic: 'main', topicId: IDS.A, partition: 0 }],
      })
      expect(cell).toEqual({
        ledger: begun.ledger,
        topic: 'main',
        partition: 0,
        open: 1,
        tickets: [begun.ticket],
      })
      expect(sections['reset-debt']).toEqual([
        { client: 'c03.1', topic: 'txn', topicId: IDS.B, discharged: [0] },
      ])
      expect(sections.assignment).toEqual([
        {
          client: 'c03.1',
          key: { topic: 'main', topicId: IDS.A, partition: 0 },
          authority: expect.objectContaining({ token: 1, acquiredAt: 3, epoch: 5, member: 'm1' }),
        },
      ])
      expect(sections.subscription[0].accepted).toEqual([
        { topic: 'main', topicId: IDS.A },
        { topic: 'txn', topicId: IDS.B },
      ])
      expect(sections['pause-state']).toHaveLength(0)
      for (const [section, entries] of Object.entries(sections))
        expect(sectionProblems(section, entries)).toEqual([])
      expect(recordProblems('ticket', begun)).toEqual([])

      group.operations.end(ticket)
      group.operations.end(ticket)
      expect(events.filter((e) => e.type === 'ticket').map((e) => e.record.phase)).toEqual([
        'begin',
        'end',
      ])
      expect(observer.ordinarySections('c03.1').sections['operation-cell']).toEqual([])

      delete group.preferredReadReplicasPerTopicPartition
      const missing = observer.ordinarySections('c03.1')
      expect(missing.missingSources).toEqual([
        'ConsumerGroup.preferredReadReplicasPerTopicPartition',
      ])
      expect(missing.sections['other-table'].map((t) => t.table)).not.toContain(
        'preferred-replicas'
      )
    } finally {
      observer.restore()
    }
  })

  test('handler and commit authority comes from the original batch, and commits record their write and publish', async () => {
    const context = new AsyncLocalStorage()
    const commits = new AsyncLocalStorage()
    const events = []
    class Group {
      async fetch() {
        await null
        return [{ topic: 'main', partition: 0, assignmentEpoch: 7, ownershipKey: 'main|0' }]
      }
    }
    const seen = []
    class Runner {
      async processEachMessage() {
        await null
        seen.push(observer.batchAuthority())
      }
    }
    class Broker {
      async offsetCommit() {
        return {}
      }
    }
    class OffsetManager {
      recordCommitted() {}
    }
    const observer = new PassiveObserver({
      owners: { ConsumerGroup: Group, Runner, Broker, OffsetManager },
      watched: {
        ConsumerGroup: ['fetch'],
        Runner: ['processEachMessage'],
        Broker: ['offsetCommit'],
        OffsetManager: ['recordCommitted'],
      },
      context,
      commits,
      onEvent: (e) => events.push(e),
    })
    observer.install()
    const group = new Group()
    Object.assign(group, {
      groupId: 'g-1',
      memberId: 'm1',
      generationId: 5,
      runIdentity: {},
      ownership: new GroupOwnership(),
      acquired: new Map([['main|0', { acquiredAt: 3, recreatedAt: 0 }]]),
    })
    const [batch] = await context.run('c01.1', () => group.fetch())
    group.ownership.renew()
    Object.assign(group, { memberId: 'm2', generationId: 6 })
    group.acquired.set('main|0', { acquiredAt: 9, recreatedAt: 9 })
    const identity = { topic: 'main', partition: 0, assignmentEpoch: 7 }
    await handlerContext.run(identity, () => new Runner().processEachMessage(batch))
    expect(seen).toEqual([
      {
        client: 'c01.1',
        run: observer.runOf(group),
        member: 'm1',
        epoch: 5,
        token: 0,
        assignmentEpoch: 7,
        acquiredAt: 3,
        recreatedAt: 0,
      },
    ])
    expect(observer.batchAuthority({})).toBeNull()

    const commit = { commit: 'c01.1.m4', key: { topic: 'main', topicId: IDS.A, partition: 0 } }
    const manager = new OffsetManager()
    manager.operations = { isSuperseded: (t, topic, partition) => partition === 1 }
    await new Broker().offsetCommit({})
    await commits.run(commit, async () => {
      await new Broker().offsetCommit({})
      manager.recordCommitted({}, [
        {
          topic: 'main',
          partitions: [
            { partition: 0, offset: '5' },
            { partition: 1, offset: '9' },
          ],
        },
      ])
    })
    observer.restore()
    expect(events).toEqual([
      { type: 'commit-stage', phase: 'write', commit },
      {
        type: 'commit-stage',
        phase: 'publish',
        commit,
        written: [{ topic: 'main', partition: 0, offset: '5' }],
      },
    ])
  })

  test('rebalance results become keyed acquire, revoke and loss boundaries with one round', async () => {
    const context = new AsyncLocalStorage()
    const events = []
    class Group {
      async applyRebalanceResult(result) {
        await null
        for (const key of result.revoked) this.acquired.delete(key)
        for (const key of result.added)
          this.acquired.set(key, { acquiredAt: ++this.clock, recreatedAt: 0 })
      }

      async releaseRevoked() {}

      rebalanceProtocol() {
        return 'cooperative'
      }
    }
    const observer = new PassiveObserver({
      owners: { ConsumerGroup: Group, GroupOwnership },
      watched: {
        ConsumerGroup: ['applyRebalanceResult', 'releaseRevoked'],
        GroupOwnership: ['lose'],
      },
      context,
      topicIds: () => IDS.A,
      onEvent: (e) => events.push(e),
    })
    observer.install()
    try {
      const group = new Group()
      Object.assign(group, {
        groupId: 'g-1',
        kind: 'classic',
        memberId: 'm1',
        generationId: 4,
        runIdentity: {},
        clock: 1,
        ownership: new GroupOwnership(),
        acquired: new Map([['t|0', { acquiredAt: 1, recreatedAt: 0 }]]),
      })
      const round = {}
      await context.run('c03.1', async () => {
        await group.releaseRevoked([{ topic: 't', partitions: [0] }], round)
        await group.applyRebalanceResult({ revoked: ['t|0'], added: ['t|1'] }, { round })
      })
      const boundaries = events.filter((e) => e.type === 'boundary').map((e) => e.record)
      for (const b of boundaries) expect(recordProblems('boundary', b)).toEqual([])
      expect(boundaries.map((b) => [b.event, b.key.partition])).toEqual([
        ['revoke-begin', 0],
        ['revoke-end', 0],
        ['acquire', 1],
      ])
      expect(boundaries[2]).toMatchObject({
        client: 'c03.1',
        group: 'g-1',
        protocol: 'cooperative',
        member: 'm1',
        epoch: 4,
        authority: expect.objectContaining({ acquiredAt: 2, token: 0 }),
      })
      const rounds = events.filter((e) => e.type === 'round')
      expect(rounds).toHaveLength(1)
      expect(rounds[0].record).toMatchObject({
        protocol: 'cooperative',
        assigned: [{ topic: 't', topicId: IDS.A, partition: 1 }],
        revoked: [{ topic: 't', topicId: IDS.A, partition: 0 }],
      })
      expect(boundaries[1].cause).toBe(rounds[0].record.round)
      expect(recordProblems('round', rounds[0].record)).toEqual([])

      events.length = 0
      group.ownership.lose(new Error('stale'), 7)
      group.ownership.lose(new Error('gone'), 0)
      group.ownership.lose(new Error('again'), 0)
      expect(events.map((e) => [e.record.event, e.record.key.partition])).toEqual([['lost', 1]])
    } finally {
      observer.restore()
    }
  })

  test('share state reads held records and ranges from the real ledger and binds acquisitions to raw frames', () => {
    const context = new AsyncLocalStorage()
    const events = []
    const dashed = '01020304-0506-0708-090a-0b0c0d0e0f10'
    const id = normalizeTopicId(dashed)
    class Loop {
      constructor() {
        this.cohorts = new Map()
      }

      adopt(exchange, topicId, partition, lockDuration) {
        const walk = {
          deliver: { records: [{ offset: '10' }, { offset: '11' }], deliveryCounts: [1, 2] },
          gaps: [{ firstOffset: '12', lastOffset: '12' }],
          release: [],
          reject: [],
        }
        const token = {}
        const ledger = new ShareAckLedger({
          topic: 'main',
          topicId,
          partition: partition.partitionIndex,
          token,
          sentAt: 1,
          lockDuration,
          walk,
        })
        const row = { topic: 'main', topicId, partition: 0, token, ledger, walk }
        this.cohorts.set(`${topicId}|0`, [row])
        return row
      }
    }
    class Run {
      syncNodes() {}
    }
    const observer = new PassiveObserver({
      owners: { ShareFetchLoop: Loop, ShareRun: Run, ShareAckLedger },
      watched: {
        ShareFetchLoop: ['adopt'],
        ShareRun: ['syncNodes'],
        ShareAckLedger: ['prepare', 'settle', 'invalidate'],
      },
      context,
      onEvent: (e) => events.push(e),
    })
    observer.install()
    try {
      const socket = new EventEmitter()
      socket.write = () => true
      const registry = new SocketRegistry({ base: () => () => socket })
      const exchanges = []
      const make = registry.factory('c07.1', {
        apiKeys: [78, 79],
        onExchange: (e) => exchanges.push(e),
      })
      expect(make({ host: 'h', port: 1 })).toBe(socket)
      const request = Buffer.alloc(14)
      request.writeInt32BE(10, 0)
      request.writeInt16BE(78, 4)
      request.writeInt16BE(2, 6)
      request.writeInt32BE(5, 8)
      socket.write(request)
      const body = shareFetchV2.buildResponse({
        throttleTime: 0,
        errorCode: 0,
        errorMessage: null,
        acquisitionLockTimeoutMs: 15000,
        responses: [
          {
            topicId: dashed,
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                errorMessage: null,
                acknowledgeErrorCode: 0,
                acknowledgeErrorMessage: null,
                currentLeader: { leaderId: 1, leaderEpoch: 0 },
                records: '',
                acquiredRecords: [{ firstOffset: '10', lastOffset: '12', deliveryCount: 1 }],
              },
            ],
          },
        ],
        nodeEndpoints: [],
      })
      const response = Buffer.alloc(9 + body.length)
      response.writeInt32BE(5 + body.length, 0)
      response.writeInt32BE(5, 4)
      body.copy(response, 9)
      socket.emit('data', response.subarray(0, 6))
      socket.emit('data', response.subarray(6))
      expect(exchanges).toHaveLength(1)
      expect(exchanges[0].response.equals(response)).toBe(true)
      observer.exchanged('c07.1', {
        ...exchanges[0],
        file: 'raw/p07.share.0.ndjson',
        sha256: 'e'.repeat(64),
      })

      const loop = new Loop()
      const run = new Run()
      run.nodes = new Map([[1, { session: { epoch: 4 } }]])
      const row = context.run('c07.1', () => {
        run.syncNodes()
        return loop.adopt(
          { sentAt: 1 },
          dashed,
          {
            partitionIndex: 0,
            acquiredRecords: [{ firstOffset: '10', lastOffset: '12', deliveryCount: 1 }],
          },
          15000
        )
      })
      const acquired = events.find((e) => e.type === 'share-acquire')
      expect(acquired).toMatchObject({
        client: 'c07.1',
        key: { topic: 'main', topicId: id, partition: 0 },
        acquired: [{ from: '10', to: '12', deliveryCount: 1 }],
        lockMs: 15000,
        wire: { file: 'raw/p07.share.0.ndjson', sha256: 'e'.repeat(64) },
      })
      expect(observer.shareDelivery('c07.1', 'main', 0, '11')).toEqual({
        acquisition: acquired.acquisition,
        deliveryCount: 2,
      })
      const state = () => observer.shareSections('c07.1')
      expect(state()).toEqual({
        entry: {
          client: 'c07.1',
          run: `c07.1.${observer.idOf(run, 'r')}`,
          sessions: [{ node: 1, session: 4 }],
          records: [
            { acquisition: acquired.acquisition, key: acquired.key, offset: '10', state: 'held' },
            { acquisition: acquired.acquisition, key: acquired.key, offset: '11', state: 'held' },
          ],
          ranges: [{ acquisition: acquired.acquisition, key: acquired.key, from: '12', to: '12' }],
          cohorts: [acquired.acquisition],
        },
        mismatches: [],
      })
      expect(sectionProblems('share-state', [state().entry], 'c07.1')).toEqual([])
      row.ledger.complete(['10', '11'], 'accept')
      const { ticket } = row.ledger.prepare({ token: row.token })
      expect(state().entry.records.map((r) => r.state)).toEqual(['settling', 'settling'])
      row.ledger.settle(ticket, { token: row.token, result: 'ok' })
      expect(row.ledger.counts).toEqual({ records: 0, ranges: 0 })
      expect(state().entry).toMatchObject({ records: [], ranges: [] })
      expect(state().mismatches).toEqual([])
      const settled = events.find((e) => e.type === 'share-settle')
      expect(settled).toMatchObject({
        acquisition: acquired.acquisition,
        offsets: [
          { offset: '10', type: 'accept' },
          { offset: '11', type: 'accept' },
        ],
        result: 'ok',
        errorCode: 0,
        wire: { file: 'raw/p07.share.0.ndjson' },
      })
      socket.emit('close')
      expect(socket.listenerCount('data')).toBe(0)
    } finally {
      observer.restore()
    }
  })

  test("share state after a restart holds only the client's current run: a retired run's sessions and its fetch loops' records stay out while still reachable, and the section names the run", () => {
    const context = new AsyncLocalStorage()
    const dashed = '01020304-0506-0708-090a-0b0c0d0e0f10'
    class Loop {
      constructor(membership) {
        this.membership = membership
        this.cohorts = new Map()
      }

      adopt(exchange, topicId, partition, lockDuration) {
        const offset = partition.acquiredRecords[0].firstOffset
        const walk = {
          deliver: { records: [{ offset }], deliveryCounts: [1] },
          gaps: [],
          release: [],
          reject: [],
        }
        const token = {}
        const ledger = new ShareAckLedger({
          topic: 'main',
          topicId,
          partition: 0,
          token,
          sentAt: 1,
          lockDuration,
          walk,
        })
        const row = { topic: 'main', topicId, partition: 0, token, ledger, walk }
        this.cohorts.set(`${topicId}|0`, [row])
        return row
      }
    }
    class Run {
      syncNodes() {}
    }
    const observer = new PassiveObserver({
      owners: { ShareFetchLoop: Loop, ShareRun: Run, ShareAckLedger },
      watched: {
        ShareFetchLoop: ['adopt'],
        ShareRun: ['syncNodes'],
        ShareAckLedger: ['prepare', 'settle', 'invalidate'],
      },
      context,
      onEvent: () => {},
    })
    observer.install()
    try {
      const runner = { current: null }
      const run = (retired, node, epoch) =>
        Object.assign(new Run(), {
          runner,
          retired,
          membership: {},
          nodes: new Map([[node, { session: { epoch } }]]),
        })
      const dropped = run(true, 1, 9)
      const restarted = run(false, 2, 3)
      runner.current = restarted
      const loops = [new Loop(dropped.membership), new Loop(restarted.membership)]
      context.run('c07.1', () => {
        dropped.syncNodes()
        restarted.syncNodes()
        loops.forEach((loop, n) =>
          loop.adopt(
            { sentAt: 1 },
            dashed,
            {
              partitionIndex: 0,
              acquiredRecords: [{ firstOffset: `${10 * (n + 1)}`, lastOffset: `${10 * (n + 1)}` }],
            },
            15000
          )
        )
      })
      const { entry, mismatches } = observer.shareSections('c07.1')
      expect(entry.sessions).toEqual([{ node: 2, session: 3 }])
      expect(entry.records.map((r) => r.offset)).toEqual(['20'])
      expect(entry.cohorts).toHaveLength(1)
      expect(entry.run).toBe(`c07.1.${observer.idOf(restarted, 'r')}`)
      expect(mismatches).toEqual([])
      expect(sectionProblems('share-state', [entry], 'c07.1')).toEqual([])
      runner.current = null
      restarted.retired = true
      expect(observer.shareSections('c07.1').entry).toMatchObject({
        run: null,
        sessions: [],
        records: [],
        cohorts: [],
      })
    } finally {
      observer.restore()
    }
  })

  test('the raw reader scans from the last scan to the LSO and keeps aborted and control records out', async () => {
    const record = (offset, producerId, inTransaction, extra = {}) => ({
      offset: String(offset),
      key: Buffer.from(`k${offset}`),
      value: offeredRecord({ runId: RUN, slot: 'c09', seq: offset, valueBytes: 256 }).value,
      headers: {},
      batchContext: {
        producerId,
        inTransaction,
        isControlBatch: false,
        firstOffset: String(offset),
        lastOffsetDelta: 0,
      },
      ...extra,
    })
    const control = record(12, '7', true)
    control.batchContext.isControlBatch = true
    control.key = Buffer.from([0, 0, 0, 0])
    const fetches = []
    const broker = {
      listOffsets: async ({ isolationLevel }) => ({
        responses: [
          { partitions: [{ offset: isolationLevel === ISOLATION.READ_COMMITTED ? '13' : '15' }] },
        ],
      }),
      fetch: async ({ topics }) => {
        fetches.push(topics[0].partitions[0].fetchOffset)
        return {
          responses: [
            {
              partitions: [
                {
                  messages: [
                    record(9, '1', false),
                    record(10, '1', false),
                    record(11, '7', true),
                    control,
                    record(13, '1', false),
                  ],
                  abortedTransactions: [{ producerId: '7', firstOffset: '11' }],
                },
              ],
            },
          ],
        }
      },
    }
    const batches = []
    const result = await scanPartition(broker, { topic: 't', partition: 0, from: 10n }, (b) =>
      batches.push(b)
    )
    expect(fetches).toEqual(['10'])
    expect(batches.map((b) => b.map((r) => r.offset))).toEqual([['10']])
    expect(result).toEqual({ to: '13', lso: '13', highWatermark: '15' })
    expect(parseValue(batches[0][0].value, RUN)).toEqual({ id: 'c09.10', transaction: null })
  })

  test('the decoder binds pushes to the recorded subscription and fails on reporter drops or overflow', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-decoder-'))
    const sink = path.join(dir, 'sink')
    fs.mkdirSync(sink)
    const records = []
    const notes = []
    const runtime = {
      out: dir,
      slotOf: (client) => (client?.startsWith('c11.') ? { slot: 'c11' } : undefined),
      nextId: () => `p16.${records.length + notes.length}`,
      emit: async (r) => records.push(r),
      diagnose: async (m) => notes.push(m),
    }
    const push = (seq, clientId = 'c11.1', extra = {}) =>
      JSON.stringify({
        v: 1,
        node: '1',
        seq,
        clientInstanceId: IDS.A,
        terminating: false,
        clientId,
        dataSha256: 'f'.repeat(64),
        parse: 'ok',
        metricsTotal: 6,
        truncated: false,
        ...extra,
      }) + '\n'
    try {
      const decoder = new DecoderWorker(runtime)
      decoder.dir = sink
      fs.writeFileSync(path.join(sink, 'reporter-1.json'), JSON.stringify({ loadedAtMs: 100 }))
      fs.writeFileSync(path.join(sink, 'exports-1.jsonl'), push(1) + push(2, 'other') + push(3))
      expect(await decoder.onTick()).toEqual({ abort: null })
      expect(records).toEqual([])
      expect(notes).toEqual([
        'telemetry push 1.1 has no recorded subscription',
        'telemetry push 1.3 has no recorded subscription',
      ])
      await decoder.onTelemetrySubscriptions({
        entries: [{ client: 'c11.1', instanceId: IDS.A, subscriptionId: 77 }],
      })
      fs.appendFileSync(path.join(sink, 'exports-1.jsonl'), push(4))
      expect(await decoder.onTick()).toEqual({ abort: null })
      expect(records).toEqual([
        expect.objectContaining({
          kind: 'telemetry',
          client: 'c11.1',
          instanceId: IDS.A,
          subscriptionId: '77',
          broker: 1,
          metrics: 6,
          terminating: false,
        }),
      ])
      expect(
        validateRecord({ ...records[0], source: 'p16', seq: 0, at: 1, t: 1, execution: null })
      ).toEqual([])
      fs.writeFileSync(path.join(sink, 'reporter-1.json'), JSON.stringify({ loadedAtMs: 200 }))
      fs.appendFileSync(path.join(sink, 'exports-1.jsonl'), push(1) + push(2))
      expect(await decoder.onTick()).toEqual({ abort: null })
      fs.appendFileSync(path.join(sink, 'exports-1.jsonl'), push(5))
      expect(await decoder.onTick()).toEqual({ abort: 'reporter 1 sequence 5 follows 2 in boot 2' })
      fs.writeFileSync(path.join(sink, 'OVERFLOW-2.json'), '{}')
      expect(await decoder.onTick()).toEqual({ abort: 'reporter overflow marker OVERFLOW-2.json' })
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test('sampling admits at most two processes at once, in order', async () => {
    const clock = new VirtualClock()
    const gate = new SamplingGate({ limit: 2 })
    const order = []
    let active = 0
    let peak = 0
    const task = (n) => async () => {
      active++
      peak = Math.max(peak, active)
      order.push(n)
      await clock.sleepUntil(clock.now() + 100)
      active--
      return n
    }
    const all = Promise.all(Array.from({ length: 16 }, (_, n) => gate.run(task(n))))
    expect(await clock.drive(all.then(() => gate.run(task(16))))).toBe(16)
    expect(peak).toBe(2)
    expect(gate.peak).toBe(2)
    expect(order).toEqual(Array.from({ length: 17 }, (_, n) => n))
    expect(clock.now()).toBe(900)
  })

  test('every configured client kind constructs a real client from the options the driver passes', () => {
    const { Kafka } = require('../../../index')
    const { clientOptions } = require('../finalSoakWorkers')
    const config = buildConfig(matrix, {
      mode: 'smoke',
      runId: RUN,
      treeSha: TREE,
      brokers: ['k:1'],
    })
    const kafka = new Kafka({ clientId: 'construct', brokers: ['k:1'] })
    const made = []
    for (const slot of config.clients.filter((c) => c.purpose !== 'observer')) {
      const { factory, options } = clientOptions(config, matrix, slot)
      const client = kafka[factory](options)
      expect(typeof client.connect).toBe('function')
      made.push(`${slot.clientKind}:${factory}`)
    }
    expect(new Set(made)).toEqual(
      new Set(
        CLIENT_KINDS.map((kind) => {
          const role = matrix.clientKinds[kind].role
          const factory = { producer: 'producer', share: 'shareConsumer', admin: 'admin' }[role]
          return `${kind}:${factory ?? 'consumer'}`
        })
      )
    )
    const options = (kind) =>
      clientOptions(
        config,
        matrix,
        config.clients.find((c) => c.clientKind === kind && c.purpose === 'subject')
      ).options
    expect(options('consumer-static')).not.toHaveProperty('sessionTimeout')
    expect(options('consumer-static')).not.toHaveProperty('heartbeatInterval')
    expect(options('consumer-static')).toMatchObject({ groupProtocol: 'consumer' })
    expect(options('classic-eager-static')).toMatchObject({
      sessionTimeout: config.timeouts.sessionTimeout,
      heartbeatInterval: config.timeouts.heartbeatInterval,
    })
  })

  test('a command to a process whose channel has closed resolves with an error instead of crashing the supervisor', async () => {
    const { Supervisor } = require('../finalSoak')
    const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-command-'))
    try {
      const supervisor = new Supervisor({
        mode: 'smoke',
        matrix: 'scripts/soak/finalSoakMatrix.json',
        expectSha: TREE,
        brokers: ['k:1'],
        out,
      })
      const closed = (connected) => {
        const child = new EventEmitter()
        child.connected = connected
        child.send = (message, callback) => {
          const error = Object.assign(new Error('Channel closed'), {
            code: 'ERR_IPC_CHANNEL_CLOSED',
          })
          if (callback) process.nextTick(callback, error)
          else process.nextTick(() => child.emit('error', error))
          return false
        }
        return child
      }
      supervisor.children.set('p16', closed(false))
      supervisor.children.set('p04', closed(true))
      const step = { type: 'periodic', id: 'k60', slot: 60 }
      await expect(supervisor.command('p16', step)).resolves.toMatchObject({
        error: expect.stringContaining('p16'),
        reply: null,
      })
      await expect(supervisor.command('p04', step)).resolves.toMatchObject({
        error: expect.stringContaining('Channel closed'),
        reply: null,
      })
      expect(supervisor.waiting.size).toBe(0)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test("the supervisor ends its evidence in bounded time: a stream close that never settles and a child that outlived its kill are named in a separate incomplete receipt, the child's handles are released, and the run ends with an error", async () => {
    const { Supervisor } = require('../finalSoak')
    const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-finish-'))
    const within = (promise, ms) =>
      Promise.race([promise, new Promise((resolve) => setTimeout(resolve, ms, { hung: true }))])
    try {
      const supervisor = new Supervisor({
        mode: 'smoke',
        matrix: 'scripts/soak/finalSoakMatrix.json',
        expectSha: TREE,
        brokers: ['k:1'],
        out,
      })
      supervisor.finishStepMs = 50
      supervisor.untilExited = async () => {}
      supervisor.runtime = { stop: async () => {}, channel: { pendingBytes: 0 } }
      supervisor.collector = { close: async () => {}, streamBytes: 10, queuedBytes: 3, records: 2 }
      const child = { unref: jest.fn(), channel: { unref: jest.fn() } }
      supervisor.children.set('p07', child)
      const missing = await within(supervisor.finish({ close: () => new Promise(() => {}) }), 2000)
      expect(missing).toEqual([
        'stream file close: did not settle within 50 ms',
        'process p07: still running after its kill; its handles were released',
      ])
      expect(child.unref).toHaveBeenCalledTimes(1)
      expect(child.channel.unref).toHaveBeenCalledTimes(1)
      expect(() => supervisor.writeReceipts({ runId: 'r' }, missing)).toThrow(
        'the run ended incomplete: stream file close: did not settle within 50 ms; process p07: still running after its kill; its handles were released'
      )
      expect(JSON.parse(fs.readFileSync(path.join(out, 'incomplete.json'), 'utf8'))).toEqual({
        runId: 'r',
        missing,
        records: 2,
        streamBytes: 10,
        unwrittenBytes: 3,
      })
      expect(JSON.parse(fs.readFileSync(path.join(out, 'supervisor.json'), 'utf8'))).toEqual({
        runId: 'r',
      })
      fs.rmSync(path.join(out, 'incomplete.json'))
      supervisor.writeReceipts({ runId: 'r' }, [])
      expect(fs.existsSync(path.join(out, 'incomplete.json'))).toBe(false)
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test("the supervisor's final sample, taken through its bounded finish, counts the same timers as its baseline", async () => {
    const { Supervisor } = require('../finalSoak')
    const { MonotonicClock } = require('../finalSoakScheduler')
    const out = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-final-sample-'))
    try {
      const supervisor = new Supervisor({
        mode: 'smoke',
        matrix: 'scripts/soak/finalSoakMatrix.json',
        expectSha: TREE,
        brokers: ['k:1'],
        out,
      })
      const sampler = new Sampler({ clock: new MonotonicClock(), gc: () => {}, gcGapMs: 1 })
      const timeouts = (sample) => sample.activeResources.find((r) => r.type === 'Timeout').count
      const baseline = await sampler.read({ role: 'baseline', cut: () => 'p15.i0' })
      let final = null
      supervisor.untilExited = async () => {}
      supervisor.runtime = {
        stop: async () => {
          final = await sampler.read({ role: 'final', cut: () => 'p15.i1' })
        },
        channel: { pendingBytes: 0 },
      }
      supervisor.collector = { close: async () => {} }
      expect(await supervisor.finish({ close: async () => {} })).toEqual([])
      expect(timeouts(final)).toBe(timeouts(baseline))
    } finally {
      fs.rmSync(out, { recursive: true, force: true })
    }
  })

  test('a sample counts a timer the code under test armed, but never a wait or bound the harness armed', async () => {
    const { MonotonicClock } = require('../finalSoakScheduler')
    const clock = new MonotonicClock()
    const sampler = new Sampler({ clock, gc: () => {}, gcGapMs: 1 })
    const timeouts = async () =>
      (await sampler.read({ role: 'periodic', slot: 1, cut: () => 'p01.i0' })).activeResources.find(
        (r) => r.type === 'Timeout'
      ).count
    const before = await timeouts()
    const wait = new AbortController()
    const waiting = clock.sleepUntil(clock.now() + 60000, wait.signal)
    const collector = new Collector({
      sink: { write: async () => ({}), sync: async () => {} },
      clock,
      matrix,
      writeTimeoutMs: 60000,
    })
    let settle = null
    const bounded = collector.bounded(new Promise((resolve) => (settle = resolve)))
    const client = setTimeout(() => {}, 60000)
    try {
      expect(await timeouts()).toBe(before + 1)
    } finally {
      clearTimeout(client)
      wait.abort()
      settle()
      await Promise.all([waiting, bounded])
    }
    expect(await timeouts()).toBe(before)
  })

  test('the sampling gate reports when every admitted and waiting task has finished', async () => {
    const clock = new VirtualClock()
    const gate = new SamplingGate({ limit: 2 })
    await gate.idle()
    let finished = 0
    const task = () => async () => {
      await clock.sleepUntil(clock.now() + 100)
      finished++
    }
    for (let n = 0; n < 5; n++) gate.run(task())
    let idleAt = null
    const idle = gate.idle().then(() => (idleAt = finished))
    await clock.drive(idle)
    expect(idleAt).toBe(5)
    expect(clock.now()).toBe(300)
  })

  test('a preflight command check is true exactly when the command exits zero', () => {
    const { tryRun } = require('../finalSoakPreflight')
    expect(tryRun(process.execPath, ['-e', ''])).toBe(true)
    expect(tryRun(process.execPath, ['-e', 'process.exit(3)'])).toBe(false)
    expect(tryRun('final-soak-no-such-command', [])).toBe(false)
  })

  test('the host monitor aborts on low MemAvailable or a short disk reservation, and an abort tears the run down', async () => {
    const host = { mem: 6 * GIB, disk: 300 * GIB }
    const monitor = new HostMonitor({
      read: { memAvailable: () => host.mem, freeDisk: () => host.disk },
      minMemAvailable: matrix.host.abortMemAvailableBytes.value,
      reserve: () => 92 * GIB,
    })
    expect(monitor.check(0)).toEqual({
      memAvailable: 6 * GIB,
      freeDisk: 300 * GIB,
      reserve: 92 * GIB,
      abort: null,
    })
    host.mem = 3 * GIB
    expect(monitor.check(10000)).toEqual({ abort: null })
    expect(monitor.check(30000).abort).toBe(`MemAvailable ${3 * GIB} below ${4 * GIB}`)
    host.mem = 8 * GIB
    expect(monitor.check(60000).abort).toBe(`MemAvailable ${3 * GIB} below ${4 * GIB}`)
    const disk = new HostMonitor({
      read: { memAvailable: () => 8 * GIB, freeDisk: () => host.disk },
      minMemAvailable: 4 * GIB,
      reserve: () => 92 * GIB,
    })
    host.disk = 90 * GIB
    expect(disk.check(0).abort).toBe(
      `free disk ${90 * GIB} below the remaining reservation ${92 * GIB}`
    )

    const clock = new VirtualClock()
    const performed = []
    const outcomes = []
    const scheduler = new Scheduler({
      clock,
      perform: async (step) => {
        performed.push([step.id, clock.now()])
        if (step.id === 'work') {
          await clock.sleepUntil(60)
          scheduler.abort('MemAvailable low')
        }
      },
      onOutcome: (o) => outcomes.push([o.id, o.outcome]),
    })
    const summary = await clock.drive(
      scheduler.run([
        { id: 'work', type: 'execution', lane: 'work', at: 0, deadline: 500 },
        { id: 'k1', type: 'periodic', lane: 'timer', at: 30, deadline: 35 },
        { id: 'k2', type: 'periodic', lane: 'timer', at: 90, deadline: 95 },
        { id: 'quiet', type: 'quiet', lane: 'timer', at: 3500, deadline: 3590 },
        { id: 'next', type: 'execution', lane: 'work', at: 100, deadline: 300 },
        { id: 'disconnect', type: 'disconnect', lane: 'work', at: 86400 },
      ])
    )
    expect(performed).toEqual([
      ['work', 0],
      ['k1', 30],
      ['disconnect', 60],
    ])
    expect(outcomes).toEqual(
      expect.arrayContaining([
        ['k2', 'aborted'],
        ['quiet', 'aborted'],
        ['next', 'aborted'],
        ['disconnect', 'ok'],
      ])
    )
    expect(summary).toEqual({ ok: 3, late: 0, error: 0, aborted: 3 })
    expect(scheduler.aborted).toBe('MemAvailable low')
    expect(clock.sleepers).toEqual([])
  })

  test('the archive binds every driver module the driver is split into', () => {
    const modules = fs
      .readdirSync(path.join(ROOT, 'scripts/soak'))
      .filter((f) => /^finalSoak[A-Z]?\w*\.js$/.test(f) && !/Preflight|Schema|Dump/.test(f))
      .map((f) => `scripts/soak/${f}`)
    expect(sorted(modules)).toEqual([
      'scripts/soak/finalSoak.js',
      'scripts/soak/finalSoakCollector.js',
      'scripts/soak/finalSoakObserver.js',
      'scripts/soak/finalSoakScheduler.js',
      'scripts/soak/finalSoakWorkers.js',
    ])
    expect(matrix.acceptance.archive).toEqual(expect.arrayContaining(modules))
    expect(validateMatrix(matrix)).toEqual([])
    for (const module of modules) {
      const edited = JSON.parse(JSON.stringify(matrix))
      edited.acceptance.archive = edited.acceptance.archive.filter((f) => f !== module)
      expect(validateMatrix(edited).join('\n')).toContain('matrix.acceptance.archive is not')
    }
    for (const module of modules.filter((m) => !m.endsWith('finalSoak.js'))) {
      const source = fs.readFileSync(path.join(ROOT, module), 'utf8')
      for (const [, required] of source.matchAll(/require\('\.\/(finalSoak\w*)'\)/g))
        expect(modules).toContain(
          `scripts/soak/${required}.js`.replace('finalSoakSchema', 'finalSoak')
        )
    }
  })

  test('routes are declared for every recipient group, the seed group on the main topic too', () => {
    const config = buildConfig(matrix, {
      mode: 'smoke',
      runId: RUN,
      treeSha: TREE,
      brokers: ['b:1'],
    })
    const groupOf = (kind) =>
      config.clients.find((c) => c.clientKind === kind && c.purpose === 'subject').group
    const seed = config.clients.find((c) => c.purpose === 'control').group
    expect(recipientGroups(config, matrix, 'main')).toEqual([
      ...recipients('main').map(groupOf),
      seed,
    ])
    expect(recipientGroups(config, matrix, 'transactions')).toEqual(
      recipients('transactions').map(groupOf)
    )
    expect(recipientGroups(config, matrix, 'regex')).toEqual(recipients('regex').map(groupOf))
    expect(recipientGroups(config, matrix, 'recreate')).toEqual(recipients('recreate').map(groupOf))
    expect(recipientGroups(config, matrix, 'main')).toHaveLength(9)
    expect(recipientGroups(config, matrix, 'recreate')).toContain(groupOf('share'))
    expect(recipientGroups(config, matrix, 'transactions')).not.toContain(groupOf('share'))
  })

  test('topic ids are normalized to their printed form and values carry their id and transaction', () => {
    expect(normalizeTopicId('01020304-0506-0708-090a-0b0c0d0e0f10')).toBe('AQIDBAUGBwgJCgsMDQ4PEA')
    expect(normalizeTopicId(IDS.A)).toBe(IDS.A)
    expect(normalizeTopicId('unknown')).toBeNull()
    expect(normalizeTopicId(null)).toBeNull()
    const offered = offeredRecord({
      runId: RUN,
      slot: 'c09',
      seq: 4,
      valueBytes: 256,
      transaction: 'c09.1.x2',
    })
    expect(offered.value).toHaveLength(256)
    expect(parseValue(offered.value, RUN)).toEqual({ id: 'c09.4', transaction: 'c09.1.x2' })
    expect(parseValue(offered.value, 'ffffffffffff')).toBeNull()
    expect(parseValue(null, RUN)).toBeNull()
  })

  test('the smoke workflow runs the runbook in diagnostic smoke mode on dispatch or a smoke branch push', () => {
    const workflow = yaml.load(fs.readFileSync(WORKFLOW, 'utf8'))
    expect(workflow.on.push).toEqual({ branches: ['p11/soak-smoke*'] })
    expect(Object.keys(workflow.on)).toEqual(['workflow_dispatch', 'push'])
    for (const input of Object.values(workflow.on.workflow_dispatch.inputs ?? {}))
      expect(input.default).toBeDefined()
    const [job] = Object.values(workflow.jobs)
    expect(Object.keys(workflow.jobs)).toHaveLength(1)
    const smoke = matrix.acceptance.modes.smoke
    const runbook = job.steps.find((s) => (s.run ?? '').includes('finalSoakNode.sh'))
    const [, grace, limit] = /^timeout -k (\d+)s (\d+)s bash /.exec(runbook.run).map(Number)
    expect(limit * 1000).toBeGreaterThanOrEqual(
      smoke.requestedMs.value + smoke.teardownAllowanceMs.value + 15 * 60000
    )
    expect(grace).toBeGreaterThanOrEqual(120)
    expect(runbook['timeout-minutes'] * 60).toBeGreaterThan(limit + grace)
    const steps = job.steps.reduce((sum, s) => sum + s['timeout-minutes'], 0)
    expect(job['timeout-minutes']).toBeGreaterThanOrEqual(steps)
    expect(job.env).toMatchObject({ KAFKA_VERSION: '4.4.0', FINAL_SOAK_DIAGNOSTIC: '1' })
    const runs = job.steps.map((s) => s.run ?? '')
    expect(
      runs.some((r) =>
        /bash scripts\/soak\/finalSoakNode\.sh "\$\(git rev-parse HEAD\)" smoke$/.test(r)
      )
    ).toBe(true)
    expect(runs.some((r) => r.includes('acceptance'))).toBe(false)
    const upload = job.steps.find((s) => s.uses?.startsWith('actions/upload-artifact'))
    expect(upload).toMatchObject({ if: 'always()', with: { 'if-no-files-found': 'error' } })
    const print = job.steps.find((s) => (s.run ?? '').includes('summary.json'))
    expect(print).toMatchObject({ if: 'always()' })
    expect(print.run).toContain('*.runbook.json')
    expect(print.run).toContain('no driver summary')
  })
})
