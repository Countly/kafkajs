const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const SubscriptionState = require('../../../src/consumer/subscriptionState')
const { CLIENT_KINDS, SHAPES, validateConfig, validateRecord } = require('../finalSoakSchema')
const {
  parseArgs,
  buildConfig,
  planSchedule,
  scheduleTotals,
  Scheduler,
  Collector,
  SourceChannel,
  Sampler,
  PassiveObserver,
} = require('../finalSoak')
const { Preflight } = require('../finalSoakPreflight')

const matrix = JSON.parse(fs.readFileSync(path.join(__dirname, '../finalSoakMatrix.json'), 'utf8'))
const RUNBOOK = path.join(__dirname, '../finalSoakNode.sh')
const TREE = 'b'.repeat(40)
const RUN = '0123456789ab'
const GIB = 1024 ** 3
const ENV = { KAFKA_BROKERS: 'localhost:19092,localhost:19095,localhost:19098' }
const ARGS = ['--out', '/var/run/final-soak', '--expect-sha', TREE]
const kindsWhere = (predicate) => CLIENT_KINDS.filter((k) => predicate(matrix.clientKinds[k]))
const recipients = (role) => matrix.topology.topics.find((t) => t.role === role).recipients
const sorted = (list) => [...list].sort()

/** A clock whose sleepers wake in time order when `drive` runs; no wall time passes. */
class VirtualClock {
  constructor() {
    this.t = 0
    this.sleepers = []
  }

  now() {
    return this.t
  }

  sleepUntil(at) {
    if (at <= this.t) return Promise.resolve()
    return new Promise((resolve) => this.sleepers.push({ at, resolve }))
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
    expect(new Set(outages.map((s) => s.downMs))).toEqual(new Set([2000, 12000]))
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
      outages: 1,
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
    expect(plan.steps.find((s) => s.shape === 'broker-outage').deadline).toBe(smoke.recoveredByMs)

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
    })
    expect(parseArgs(['--mode', 'smoke', '--duration', '1800000', ...ARGS], ENV).requestedMs).toBe(
      1800000
    )
    expect(() => parseArgs(['--mode', 'smoke', '--duration', '600000', ...ARGS], ENV)).toThrow(
      'the smoke length is fixed at 1800000 ms'
    )
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
      'p02.0 sample line exceeds 1024 bytes',
    ])
    expect(JSON.parse(strict.lastLine)).toMatchObject({ kind: 'diagnostic', source: 'p15' })

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
      },
      host: { memAvailable: 20 * GIB, freeDisk: 300 * GIB, load1m: 1, census: [] },
    })
    const run = (facts) =>
      new Preflight({ sha: facts.sha, mode: facts.mode, matrix, read: async () => facts }).run()
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
  })

  test('the runbook forwards SHA and mode to preflight and driver exactly', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-runbook-'))
    const bin = path.join(dir, 'bin')
    const log = path.join(dir, 'calls.log')
    fs.mkdirSync(bin)
    for (const name of ['git', 'npm', 'docker', 'node', 'tar']) {
      fs.writeFileSync(path.join(bin, name), `#!/bin/sh\necho "${name} $*" >> "$CALLS"\n`, {
        mode: 0o755,
      })
    }
    const run = (args) =>
      spawnSync('bash', [RUNBOOK, ...args], {
        env: {
          PATH: `${bin}:/usr/bin:/bin`,
          CALLS: log,
          FINAL_SOAK_ARTIFACTS: path.join(dir, 'artifacts'),
        },
        encoding: 'utf8',
      })
    try {
      const result = run([TREE, 'smoke'])
      expect(result.stderr).toBe('')
      expect(result.status).toBe(0)
      const calls = fs.readFileSync(log, 'utf8').trim().split('\n')
      const out = calls.find((c) => c.includes('finalSoakPreflight.js')).match(/--out (\S+)/)[1]
      expect(calls).toContain(`git checkout --detach ${TREE}`)
      expect(calls).toContain('npm ci --ignore-scripts')
      expect(calls).toContain(
        `node scripts/soak/finalSoakPreflight.js --expect-sha ${TREE} --mode smoke --out ${out}`
      )
      expect(calls).toContain(
        `node --expose-gc --no-flush-bytecode scripts/soak/finalSoak.js --mode smoke --expect-sha ${TREE} --matrix scripts/soak/finalSoakMatrix.json --out ${out}`
      )
      expect(calls).toContain(`node scripts/soak/finalSoak.js --finalize --out ${out}`)
      expect(
        calls.indexOf(calls.find((c) => c.includes('compose') && c.includes(' down')))
      ).toBeGreaterThan(calls.findIndex((c) => c.includes('--finalize')))
      expect(calls.find((c) => c.startsWith('tar '))).toContain(out)

      fs.writeFileSync(log, '')
      expect(run(['main', 'smoke']).status).toBe(2)
      expect(run([TREE, 'day']).status).toBe(2)
      expect(fs.readFileSync(log, 'utf8')).toBe('')
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
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
    expect(
      config.clients.filter((c) => c.purpose === 'observer').every((c) => c.group === null)
    ).toBe(true)
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
    expect(summary).toEqual({ ok: 2, late: 3, error: 0 })
  })

  test('each sample cuts its inventory, then takes two new collections at least 100 ms apart', async () => {
    const clock = new VirtualClock()
    const calls = []
    const sampler = new Sampler({
      clock,
      gc: () => calls.push(['gc', clock.now()]),
      turn: () => clock.sleepUntil(clock.now()),
      memory: () => (calls.push(['memory', clock.now()]), { rss: 9, heapUsed: 8, external: 7 }),
      resources: () => ['TTYWrap'],
    })
    const read = (n) =>
      sampler.read({
        role: 'quiet',
        slot: null,
        boundary: 'h00',
        reading: n,
        cut: () => (calls.push(['cut', clock.now()]), `i${n}`),
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

    const ids = { A: 'AAAAAAAAAAAAAAAAAAAAAA', B: 'BBBBBBBBBBBBBBBBBBBBBB' }
    let current = ids.A
    const pauses = new PassiveObserver({
      owners: { SubscriptionState },
      watched: {
        SubscriptionState: ['pause', 'resume', 'retainTopics', 'retireTopic', 'clearPauses'],
      },
      lifetime: (topic) => ({
        topicId: current,
        life: `c01.1.s1/${current}`,
        client: 'c01.1',
        run: 'r1',
      }),
    })
    pauses.install()
    try {
      const state = new SubscriptionState()
      state.pause([{ topic: 'whole' }])
      state.resume([{ topic: 'whole', partitions: [1] }])
      state.pause([{ topic: 'some', partitions: [0, 1] }])
      state.resume([{ topic: 'some', partitions: [0, 1] }])
      state.resume([{ topic: 'never-paused', partitions: [2] }])
      current = ids.B
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
        { topic: 'whole', pauseAll: true, paused: [], resumed: [1], originTopicId: ids.A },
        { topic: 'some', pauseAll: false, paused: [], resumed: [], originTopicId: ids.A },
        { topic: 'never-paused', pauseAll: false, paused: [], resumed: [], originTopicId: ids.A },
      ])
      expect(new Set(entries.map((e) => e.stateId)).size).toBe(3)
      expect(entries.every((e) => e.subscriptionLife === `c01.1.s1/${ids.A}`)).toBe(true)
      state.retireTopic('some')
      expect(pauses.pauseStates().map((e) => e.topic)).toEqual(['whole', 'never-paused'])
    } finally {
      pauses.restore()
    }
  })
})
