'use strict'
/**
 * Final soak driver: runs the reviewed matrix's twelve real client kinds, the isolated interning
 * seed and the observer processes for one exactly timed smoke or 24-hour acceptance run, and writes
 * the schema-3 artifact the independent verifier judges. It judges nothing itself.
 *
 * Usage (the runbook `finalSoakNode.sh <sha> <mode>` is the normal entry point):
 *
 *   KAFKA_BROKERS=localhost:19092,localhost:19095,localhost:19098 \
 *     node --expose-gc --no-flush-bytecode scripts/soak/finalSoak.js --mode smoke|acceptance \
 *       --expect-sha <40-hex> --matrix scripts/soak/finalSoakMatrix.json --out <artifact dir> \
 *       [--diagnostic]
 *   node scripts/soak/finalSoak.js --outer-exit --out <artifact dir> --status <wait status> \
 *     --exit-wall-ms <epoch ms the wait returned>
 *   node scripts/soak/finalSoak.js --finalize --out <artifact dir> --status <wait status> \
 *     [--companion <name>=<file>[@<run url>]]...
 *   node scripts/soak/finalSoak.js --summary --out <artifact dir>
 *   node scripts/soak/finalSoak.js --cleanup --out <artifact dir>
 *
 * The first command is the collector/supervisor process. It spawns the fifteen other persistent
 * processes (`--child <slot>`), is the only stream writer, and exits naturally after every child
 * has. The outer observer (the runbook) witnesses that exit at once with `--outer-exit`, then runs
 * `--finalize`, which appends the parent-observed supervisor exit, hashes every file and writes
 * `header.json`; without a supervisor receipt it writes a partial manifest naming what is absent. `--duration` is
 * accepted only in smoke mode, where the matrix fixes its length. `--diagnostic` (smoke only) marks
 * a run on a host that does not meet the quiet-host floors, such as a hosted CI runner; such a run
 * proves only that the driver works against a live cluster. The modules it is split into
 * (scheduler, collector, observer, workers) are archived with it.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const readline = require('node:readline')
const { spawn, execFile } = require('node:child_process')
const { SHAPES, validateRecord, validateConfig } = require('./finalSoakSchema')
const {
  valueOf,
  MonotonicClock,
  planSchedule,
  Scheduler,
  SamplingGate,
} = require('./finalSoakScheduler')
const {
  SUPERVISOR,
  OUTER,
  Collector,
  HostMonitor,
  memAvailable,
  freeDisk,
} = require('./finalSoakCollector')
const { normalizeTopicId } = require('./finalSoakObserver')
const { bootId, ChildRuntime, child } = require('./finalSoakWorkers')

const ROOT = path.resolve(__dirname, '../..')
const MATRIX = 'scripts/soak/finalSoakMatrix.json'
const MIB = 1024 ** 2
const GIB = 1024 ** 3
/** Free disk kept back beyond the stream's remaining reservation (the planned 32 GiB safety). */
const SAFETY_BYTES = 32 * GIB
/** The children's explicit auxiliary byte budgets: raw scans 8 GiB, raw share wire 4 GiB, client logs. */
const AUXILIARY_BYTES = 16 * GIB
/** The disk a diagnostic run keeps free: enough to finish writing and archive a smoke. */
const DIAGNOSTIC_RESERVE_BYTES = 2 * GIB
/** What a command may take beyond its step's deadline, and what one without a deadline may take. */
const COMMAND_GRACE_MS = 60000
const DEFAULT_COMMAND_MS = 300000
/** How long one docker command may run before it is killed. */
const DOCKER_MS = 60000
/** How long a child killed at the end of the teardown allowance may take to end. */
const KILL_WAIT_MS = 10000
/** How long the final drain waits for every member to resolve the logs' ends. */
const FINAL_DRAIN_MS = 300000
/** How long the decoder may read the reporters for the telemetry clients' terminating pushes. */
const TELEMETRY_DRAIN_MS = 15000
/** Problems kept in the supervisor receipt; later ones are counted, not listed. */
const MAX_PROBLEMS = 200

/** @param {string} prefix @param {number} n @returns {string} a two-digit slot id */
const slotId = (prefix, n) => `${prefix}${String(n).padStart(2, '0')}`

/**
 * Parses the driver command line.
 * @param {string[]} argv arguments after the script name
 * @param {Record<string, string|undefined>} env the environment, read for `KAFKA_BROKERS`
 * @returns {object} the parsed options of the chosen command
 * @throws {Error} for an unknown mode, `--duration` or `--diagnostic` outside smoke mode, a smoke
 *   length other than the matrix's, a malformed SHA, or missing brokers or output directory
 */
function parseArgs(argv, env) {
  const flags = { companion: [] }
  const switches = [
    '--finalize',
    '--cleanup',
    '--summary',
    '--diagnostic',
    '--outer-exit',
    '--check-workload',
  ]
  for (let i = 0; i < argv.length; i++) {
    const name = argv[i]
    if (!name.startsWith('--')) throw new Error(`unexpected argument ${name}`)
    if (switches.includes(name)) flags[name.slice(2)] = true
    else if (name === '--companion') flags.companion.push(argv[++i])
    else flags[name.slice(2)] = argv[++i]
  }
  if (!flags.out) throw new Error('--out <artifact dir> is required')
  const out = path.resolve(flags.out)
  if (flags.finalize) {
    const companions = flags.companion.map((spec) => {
      const match = /^([a-z-]+)=([^@]+)(?:@(.+))?$/.exec(spec ?? '')
      if (!match) throw new Error('--companion takes <name>=<file>[@<run url>]')
      return { name: match[1], file: path.resolve(match[2]), runUrl: match[3] ?? null }
    })
    return { command: 'finalize', out, status: Number(flags.status), companions }
  }
  if (flags['outer-exit'])
    return {
      command: 'outer-exit',
      out,
      status: Number(flags.status),
      exitWallMs: Number(flags['exit-wall-ms'] || NaN),
    }
  if (flags.cleanup) return { command: 'cleanup', out }
  if (flags['check-workload']) return { command: 'check-workload', out }
  if (flags.summary) return { command: 'summary', out }
  if (flags.child) return { command: 'child', child: flags.child, out }
  if (!['smoke', 'acceptance'].includes(flags.mode))
    throw new Error('--mode must be smoke or acceptance')
  const matrix = JSON.parse(fs.readFileSync(path.resolve(ROOT, flags.matrix ?? MATRIX), 'utf8'))
  const fixed = valueOf(matrix.acceptance.modes[flags.mode].requestedMs)
  if (flags.duration !== undefined) {
    if (flags.mode !== 'smoke') throw new Error('--duration is accepted only in smoke mode')
    if (Number(flags.duration) !== fixed)
      throw new Error(`the smoke length is fixed at ${fixed} ms`)
  }
  if (flags.diagnostic && flags.mode !== 'smoke')
    throw new Error('--diagnostic is accepted only in smoke mode')
  if (!/^[0-9a-f]{40}$/.test(flags['expect-sha'] ?? ''))
    throw new Error('--expect-sha must be a 40-hex commit')
  const brokers = (env.KAFKA_BROKERS ?? '').split(',').filter(Boolean)
  if (brokers.length === 0) throw new Error('KAFKA_BROKERS must list the brokers')
  return {
    command: 'run',
    mode: flags.mode,
    requestedMs: fixed,
    brokers,
    expectSha: flags['expect-sha'],
    matrix: flags.matrix ?? MATRIX,
    out,
    diagnostic: flags.diagnostic === true,
  }
}

/**
 * Builds the run config: sixteen persistent processes (twelve subjects in matrix kind order, the
 * seed, reader, collector/supervisor and telemetry decoder), one standing subject per kind, one
 * churn extra per churn kind in its own kind's process, the seed's single control client and the
 * observer clients (the reader's c21 and the supervisor's c22). Names follow the short profile ids
 * so every ordinary record fits its line cap.
 * @param {object} matrix the reviewed matrix
 * @param {{ mode: string, runId: string, treeSha: string, brokers: string[],
 *   diagnostic?: boolean }} options
 * @returns {object} a config that `validateConfig` accepts
 */
function buildConfig(matrix, { mode, runId, treeSha, brokers, diagnostic = false }) {
  const kinds = Object.keys(matrix.clientKinds)
  const processes = kinds.map((kind, i) => ({
    slot: slotId('p', i + 1),
    role: 'subject',
    arm: 'unseeded',
    controlId: null,
    kind,
  }))
  const control = 'interning-seeded'
  processes.push(
    {
      slot: 'p13',
      role: 'seed',
      arm: control,
      controlId: control,
      kind: matrix.controls[control].clientKind,
    },
    { slot: 'p14', role: 'reader', arm: 'observer', controlId: null, kind: null },
    { slot: SUPERVISOR, role: 'collector', arm: 'observer', controlId: null, kind: null },
    { slot: 'p16', role: 'decoder', arm: 'observer', controlId: null, kind: null }
  )
  const clients = []
  const add = (process, clientKind, purpose, owner) => {
    const slot = slotId('c', clients.length + 1)
    const kind = matrix.clientKinds[clientKind]
    const proc = processes.find((p) => p.slot === process)
    const real = purpose !== 'observer'
    clients.push({
      slot,
      process,
      clientKind,
      purpose,
      arm: proc.arm,
      controlId: proc.controlId,
      group: real && kind.grouped ? `g-${runId}-${owner ?? slot}` : null,
      groupInstanceId: real && kind.static ? `s-${runId}-${slot}` : null,
      standing: purpose !== 'churn',
      telemetryEnabled: real && purpose !== 'control' && kind.telemetry,
    })
    return slot
  }
  const subjectSlot = new Map(
    kinds.map((kind, i) => [kind, add(processes[i].slot, kind, 'subject')])
  )
  kinds.forEach((kind, i) => {
    if (matrix.clientKinds[kind].churn) add(processes[i].slot, kind, 'churn', subjectSlot.get(kind))
  })
  add('p13', matrix.controls[control].clientKind, 'control')
  add('p14', 'admin', 'observer')
  add(SUPERVISOR, 'admin', 'observer')
  const topic = (role) => `fs-${runId}-${role}`
  return {
    schema: matrix.schema,
    mode,
    runId,
    treeSha,
    brokers,
    diagnostic,
    processes: processes.map(({ kind, ...p }) => p),
    clients,
    topics: {
      main: topic('main'),
      transactions: topic('txn'),
      regex: topic('regex'),
      recreate: [topic('recreate-0'), topic('recreate-1')],
    },
    profile: matrix.acceptance.profile,
    timeouts: {
      metadataMaxAge: valueOf(matrix.traffic.metadataMaxAgeMs),
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      rebalanceTimeout: 60000,
      requestTimeout: 30000,
    },
    companions: matrix.acceptance.companions,
  }
}

/**
 * The groups that must read a topic role: the run-scoped group of each recipient kind's subject,
 * plus the seed's group on the main topic.
 * @param {object} config @param {object} matrix @param {string} role a topology topic role
 * @returns {string[]} group ids
 */
function recipientGroups(config, matrix, role) {
  const entry = matrix.topology.topics.find((t) => t.role === role)
  const groups = entry.recipients.map(
    (kind) => config.clients.find((c) => c.clientKind === kind && c.purpose === 'subject').group
  )
  if (entry.seed) groups.push(config.clients.find((c) => c.purpose === 'control').group)
  return groups
}

/**
 * The collector/supervisor process: creates the run topics and declares their routes, spawns the
 * fifteen children, holds the readiness barrier, runs the plan with gated sampling, owns topic and
 * broker churn with their broker-state receipts, relays readbacks, scans and telemetry
 * subscriptions, watches the host, samples itself like every persistent process, records every
 * child exit it observes and leaves after them. It never unrefs a handle or calls `process.exit`,
 * and kills a child only when it is still alive a teardown allowance after its disconnect, naming
 * it in the receipt. Every failure of the run (a lost or silent child, a refused or unwritten
 * record, a late or failed step, an unconfirmed outage) is listed in the receipt's `problems`, so
 * a natural exit is never read as a successful day.
 */
class Supervisor {
  /** @param {ReturnType<typeof parseArgs>} options */
  constructor(options) {
    this.options = options
    this.out = options.out
    this.matrix = JSON.parse(fs.readFileSync(path.resolve(ROOT, options.matrix), 'utf8'))
    this.runId = crypto.randomBytes(6).toString('hex')
    this.config = buildConfig(this.matrix, {
      mode: options.mode,
      runId: this.runId,
      treeSha: options.expectSha,
      brokers: options.brokers,
      diagnostic: options.diagnostic,
    })
    const problems = validateConfig(this.config, this.matrix)
    if (problems.length > 0) throw new Error(`invalid config: ${problems.join('; ')}`)
    this.clock = new MonotonicClock(originOf(this.out))
    this.children = new Map()
    this.waiting = new Map()
    this.commands = 0
    this.terminal = new Set()
    this.exitRecords = new Map()
    this.topicIds = []
    this.routes = new Map()
    this.scanned = new Map()
    this.ended = new Map()
    this.waves = new Map()
    this.gate = new SamplingGate({ limit: 2 })
    this.problems = []
    this.forced = []
    this.leaving = new Set()
    this.lost = new Set()
    this.aborted = null
    this.quiesced = null
    this.dockerMs = DOCKER_MS
  }

  /**
   * Records a failure of the run: it is listed in the receipt and noted in the stream. Nothing here
   * hides a failed run behind a natural exit.
   * @param {string} message
   * @returns {void}
   */
  problem(message) {
    if (this.problems.length < MAX_PROBLEMS) this.problems.push(message)
    this.collector?.note(`problem: ${message}`)
  }

  /** @returns {number} the teardown allowance of this run's mode, in ms */
  teardownAllowanceMs() {
    return valueOf(this.matrix.acceptance.modes[this.options.mode].teardownAllowanceMs)
  }

  /**
   * @param {object} step a command's step
   * @returns {number} how long a child may take to answer it: the teardown allowance for the
   *   disconnect, the step's deadline plus a grace for one that has one, else a fixed bound
   */
  budget(step) {
    if (step.type === 'disconnect') return this.teardownAllowanceMs()
    if (step.deadline !== undefined && step.deadline !== Infinity)
      return Math.max(0, step.deadline - this.clock.now()) + COMMAND_GRACE_MS
    return DEFAULT_COMMAND_MS
  }

  /**
   * @returns {{ failures: string[], aborted: string|null, incomplete: string|null,
   *   problems: string[], forced: string[] }} what failed or was cut short; a run is green only when
   *   all of it is empty
   */
  result() {
    return {
      failures: this.collector.failures,
      aborted: this.aborted,
      incomplete: this.incomplete ?? null,
      problems: this.problems,
      forced: this.forced,
    }
  }

  /**
   * Records a settled plan step that was not on time or not performed. A step that failed stops the
   * admission of later ones, so a run whose workload cannot be trusted does not spend the rest of
   * its day; a step that only ended after its deadline is recorded and the run goes on.
   * @param {{ id: string, outcome: string, error: string|null }} outcome a scheduler outcome
   * @returns {void}
   */
  stepOutcome({ id, outcome, error }) {
    if (outcome === 'ok' || outcome === 'aborted') return
    this.problem(`step ${id} ${outcome}${error ? `: ${error}` : ''}`)
    if (outcome === 'error') this.abort(`step ${id} failed: ${error}`)
  }

  /**
   * Sends every process its start command and checks each started: a process that could not
   * construct or connect its clients, or was given no partition, fails readiness and the day does
   * not start.
   * @returns {Promise<boolean>} whether every process is ready
   */
  async start() {
    const slots = [...this.children.keys()]
    const replies = await Promise.all(
      slots.map((slot) => this.command(slot, { type: 'start', id: 'start' }))
    )
    const failed = slots.flatMap((slot, i) =>
      replies[i].error ? [`${slot}: ${replies[i].error}`] : []
    )
    if (failed.length > 0) {
      this.problem(`readiness failed: ${failed.join('; ')}`)
      this.abort(`readiness failed: ${failed.join('; ')}`)
    }
    return failed.length === 0
  }

  /** @param {object} record @returns {Promise<string>} appends a supervisor record, returns its id */
  async emit(record) {
    const recordId = this.runtime.nextId()
    this.count(record)
    await this.runtime.emit({ recordId, ...record })
    return recordId
  }

  /** @param {object} record tallies an ended operation under the execution it carries */
  count(record) {
    if (record.kind === 'operation' && record.phase === 'end' && record.execution)
      this.ended.set(record.execution, (this.ended.get(record.execution) ?? 0) + 1)
  }

  /** @returns {Promise<void>} runs the whole lifecycle */
  async run() {
    fs.mkdirSync(path.join(this.out, 'raw'), { recursive: true })
    for (const file of this.matrix.acceptance.archive) {
      fs.mkdirSync(path.dirname(path.join(this.out, 'archive', file)), { recursive: true })
      fs.copyFileSync(path.join(ROOT, file), path.join(this.out, 'archive', file))
    }
    fs.writeFileSync(
      path.join(this.out, 'config.json'),
      JSON.stringify(this.config, null, 2) + '\n'
    )
    const handle = await fs.promises.open(path.join(this.out, 'stream.ndjson'), 'wx')
    const sink = { write: (b) => handle.write(b), sync: () => handle.sync() }
    this.collector = new Collector({ sink, clock: this.clock, matrix: this.matrix })
    this.runtime = new ChildRuntime({
      slot: SUPERVISOR,
      config: this.config,
      matrix: this.matrix,
      originMs: this.clock.originMs,
      out: this.out,
      send: (m, done) => this.collector.append(m.record).then((ok) => done(!ok)),
      notify: (m) => this.receive(SUPERVISOR, m),
    })
    this.collector.onFailure = (problem) => this.abort(`collector: ${problem}`)
    this.runtime.request = (body) => this.readback(null, body)
    this.collector.own = (record) =>
      this.runtime.emit({ recordId: this.runtime.nextId(), ...record })
    await this.runtime.start()
    const rig = this.runtime.worker.rig(this.config.clients.find((c) => c.process === SUPERVISOR))
    this.admin = await rig.construct((kafka) => kafka.admin())
    await rig.call('connect', () => this.admin.connect())
    this.runtime.worker.standing.push(rig)
    this.rig = rig
    for (const p of this.config.processes) if (p.slot !== SUPERVISOR) this.spawn(p.slot)
    await this.createTopics()
    await this.configureShareGroup().catch((e) => {
      this.problem(`share group setup failed: ${e.message}`)
      this.abort(`share group setup failed: ${e.message}`)
    })
    await this.start()
    this.runStartAt = this.clock.now()
    await this.collector.note('readiness barrier', { runStartAt: this.runStartAt })
    const plan = planSchedule(this.matrix, this.options.mode)
    this.runEndAt = this.runStartAt + plan.requestedMs
    const shift = (t) => (t === undefined ? t : t + this.runStartAt)
    const steps = plan.steps.map((st) => ({
      ...st,
      at: shift(st.at),
      deadline: shift(st.deadline),
      until: shift(st.until),
    }))
    for (const st of steps.filter((s) => s.lane === 'within'))
      this.waves.set(st.parent, [...(this.waves.get(st.parent) ?? []), st])
    const outages = new Map(
      steps.filter((s) => s.shape === 'broker-outage').map((s) => [s.id, null])
    )
    this.scheduler = new Scheduler({
      clock: this.clock,
      graceMs: COMMAND_GRACE_MS,
      perform: (step) => this.perform(step),
      onOutcome: (o) => {
        this.collector.note(`step ${o.id} ${o.outcome}`, { step: o.id })
        this.stepOutcome(o)
        if (outages.has(o.id)) outages.set(o.id, o.outcome)
      },
    })
    if (this.aborted) this.scheduler.abort(this.aborted)
    this.monitor = new HostMonitor({
      read: { memAvailable, freeDisk: () => freeDisk(this.out) },
      minMemAvailable: valueOf(this.matrix.host.abortMemAvailableBytes),
      reserve: () =>
        this.config.diagnostic
          ? DIAGNOSTIC_RESERVE_BYTES
          : Math.max(0, valueOf(this.matrix.host.maxStreamBytes) - this.collector.streamBytes) +
            AUXILIARY_BYTES +
            SAFETY_BYTES,
    })
    this.monitor.start(this.clock, (reading) => {
      if (reading.abort) this.abort(reading.abort)
    })
    const summary = await this.scheduler.run(steps)
    this.monitor.stop()
    if (this.options.mode === 'smoke') this.requireOutages(outages)
    await this.collector.note('schedule finished', { ...summary, gatePeak: this.gate.peak })
    await this.untilExited()
    await this.runtime.stop()
    while (this.runtime.channel.pendingBytes > 0)
      await new Promise((resolve) => setImmediate(resolve))
    await this.collector.close()
    await handle.close()
    const run = {
      runId: this.runId,
      originMs: this.clock.originMs,
      bootId: bootId(),
      runStartAt: this.runStartAt,
      runEndAt: this.runEndAt,
      requestedMs: plan.requestedMs,
      observedEndAt: this.observedEndAt,
      ...this.result(),
      records: this.collector.records,
      steps: summary,
      mode: this.options.mode,
      diagnostic: this.config.diagnostic,
      treeSha: this.options.expectSha,
    }
    fs.writeFileSync(path.join(this.out, 'supervisor.json'), JSON.stringify(run) + '\n')
  }

  /**
   * Sets `share.auto.offset.reset=earliest` on the run's fresh share group through the public admin
   * API and waits until the broker reads it back, before any member joins. The broker's default is
   * `latest`, which would start the group after the records offered before its first fetch.
   * @returns {Promise<void>}
   * @throws {Error} when the broker refuses the config or does not read it back in time
   */
  async configureShareGroup() {
    const share = this.config.clients.find(
      (c) => this.matrix.clientKinds[c.clientKind]?.role === 'share' && c.purpose === 'subject'
    )
    if (!share) return
    // loaded here so child processes never load the test helpers
    const { setShareGroupConfigs } = require('../../testHelpers/shareFixtures')
    await setShareGroupConfigs(this.admin, share.group, { 'share.auto.offset.reset': 'earliest' })
    await this.collector.note(
      `share group ${share.group} reads back share.auto.offset.reset=earliest`,
      { group: share.group }
    )
  }

  /**
   * Fails a smoke whose planned broker outages did not all end ok: the smoke exists to show both
   * outage lengths and their recovery, so an outage cut short or never run leaves it unproven.
   * @param {Map<string, string|null>} outcomes each planned outage's outcome, null if never settled
   * @returns {void}
   */
  requireOutages(outcomes) {
    for (const [id, outcome] of outcomes)
      if (outcome !== 'ok')
        this.problem(`smoke outage ${id} has no receipt (${outcome ?? 'not run'})`)
  }

  /**
   * Stops admitting work for the rest of the run; teardown still runs.
   * @param {string} reason @returns {void}
   */
  abort(reason) {
    if (this.aborted) return
    this.aborted = reason
    this.collector?.note(`abort: ${reason}`)
    this.scheduler?.abort(reason)
  }

  /**
   * Creates one run topic (RF3, min ISR 2, no retention truncation), records the admin result and
   * declares a route from offset 0 for every recipient group and partition. The creation counts
   * only when the broker answered without error with a topic id that is not the id the name had
   * before, so a recreation that left the old incarnation in place fails.
   * @param {string} topic @param {number} partitions @param {string} role its topology role
   * @param {string|null} [execution]
   * @returns {Promise<string>} the new incarnation's topic id
   * @throws {Error} when the creation failed or did not produce a new incarnation
   */
  async createTopic(topic, partitions, role, execution = null) {
    const before = this.topicIds.find(([t]) => t === topic)?.[1] ?? null
    const configEntries = [
      { name: 'min.insync.replicas', value: '2' },
      { name: 'retention.ms', value: '-1' },
      { name: 'retention.bytes', value: '-1' },
      { name: 'cleanup.policy', value: 'delete' },
    ]
    let errorCode = 0
    try {
      await this.admin.createTopics({
        topics: [{ topic, numPartitions: partitions, replicationFactor: 3, configEntries }],
        waitForLeaders: true,
      })
    } catch (e) {
      errorCode = Number.isInteger(e.code) ? e.code : -1
    }
    const [meta] = (await this.admin.fetchTopicMetadata({ topics: [topic] })).topics
    const topicId = normalizeTopicId(meta?.topicId)
    const recordId = await this.topicRecord(
      'create',
      topic,
      topicId,
      meta?.partitions ?? [],
      errorCode,
      execution
    )
    if (errorCode !== 0 || !topicId || topicId === before)
      throw new Error(
        `creating ${topic} gave ${topicId === before ? 'its old topic id' : `topic id ${topicId}`} (error code ${errorCode})`
      )
    await this.publish(topic, topicId)
    for (const group of recipientGroups(this.config, this.matrix, role))
      for (let partition = 0; partition < partitions; partition++) {
        const route = {
          group,
          topic,
          topicId,
          partition,
          from: '0',
          to: null,
          topicRecord: recordId,
          scan: null,
        }
        this.routes.set(`${group}/${topicId}/${partition}`, route)
        await this.emit({ kind: 'route', event: 'start', execution, ...route })
        await this.readback(null, {
          group,
          client: this.rig.id,
          commit: null,
          key: { topic, topicId, partition },
        })
      }
    return topicId
  }

  /**
   * Deletes a run topic and confirms its absence from metadata.
   * @param {string} topic @param {string|null} [execution]
   * @returns {Promise<void>}
   * @throws {Error} when the broker refused the deletion or the name is still listed
   */
  async deleteTopic(topic, execution = null) {
    const topicId = this.topicIds.find(([t]) => t === topic)?.[1] ?? null
    let errorCode = 0
    try {
      await this.admin.deleteTopics({ topics: [topic] })
    } catch (e) {
      errorCode = Number.isInteger(e.code) ? e.code : -1
    }
    let listed = true
    for (let i = 0; i < 40 && (listed = (await this.admin.listTopics()).includes(topic)); i++)
      await this.clock.sleepUntil(this.clock.now() + 250)
    await this.topicRecord('delete', topic, topicId, [], errorCode, execution)
    for (const key of [...this.scanned.keys()])
      if (key.startsWith(`${topicId}/`)) this.scanned.delete(key)
    if (errorCode !== 0 || listed)
      throw new Error(
        `deleting ${topic} ${listed ? 'left it listed' : 'failed'} (error code ${errorCode})`
      )
  }

  /**
   * Waits until the broker's metadata shows a new incarnation with its id and a leader and two
   * in-sync replicas on every partition, before anything is offered to it.
   * @param {string} topic @param {string} topicId @param {number} partitions
   * @param {number} deadline on the run clock
   * @returns {Promise<void>}
   * @throws {Error} when the metadata is not there by the deadline
   */
  async ready(topic, topicId, partitions, deadline) {
    const suitable = async () => {
      const { topics } = await this.admin
        .fetchTopicMetadata({ topics: [topic] })
        .catch(() => ({ topics: [] }))
      const meta = topics[0]
      return (
        normalizeTopicId(meta?.topicId) === topicId &&
        meta.partitions.length === partitions &&
        meta.partitions.every((p) => p.leader >= 0 && p.isr.length >= 2)
      )
    }
    while (!(await suitable())) {
      if (this.clock.now() >= deadline)
        throw new Error(`${topic} has no ready metadata for ${topicId} by the deadline`)
      await this.clock.sleepUntil(Math.min(deadline, this.clock.now() + 250))
    }
  }

  /** Tells every process that a deleted name has no current id any more. */
  async unpublish(topic) {
    this.topicIds = this.topicIds.filter(([t]) => t !== topic)
    this.runtime.topicIds.delete(topic)
    await Promise.all(
      [...this.children.keys()].map((slot) =>
        this.command(slot, { type: 'topic-ids', id: 'topic-ids', topicIds: [[topic, null]] })
      )
    )
  }

  /** Emits one admin topic result record. @returns {Promise<string>} its record id */
  topicRecord(op, topic, topicId, partitions, errorCode, execution) {
    return this.emit({
      kind: 'topic',
      execution,
      client: this.rig.id,
      call: `${this.rig.id}.t${this.runtime.ids}`,
      op,
      topic,
      topicId,
      partitions: partitions.map((p) => p.partitionId),
      errorCode,
    })
  }

  /** Tells every process a topic's current id and waits until each has it. */
  async publish(topic, topicId) {
    this.topicIds = [...this.topicIds.filter(([t]) => t !== topic), [topic, topicId]]
    this.runtime.topicIds.set(topic, topicId)
    await Promise.all(
      [...this.children.keys()].map((slot) =>
        this.command(slot, { type: 'topic-ids', id: 'topic-ids', topicIds: [[topic, topicId]] })
      )
    )
  }

  /** Creates the standing run topics; a failure aborts the run before it starts. */
  async createTopics() {
    const t = this.config.topics
    try {
      await this.createTopic(t.main, 1, 'main')
      await this.createTopic(t.transactions, 1, 'transactions')
      for (const topic of t.recreate) await this.createTopic(topic, 2, 'recreate')
    } catch (e) {
      this.problem(`standing topics: ${e.message}`)
      this.abort(`standing topics: ${e.message}`)
    }
  }

  /**
   * Scans one key from where its last scan ended to its last stable offset.
   * @param {string} topic @param {number} partition @param {string|null} execution
   * @returns {Promise<{ scan: string, to: string }|null>}
   */
  async scan(topic, partition, execution = null) {
    const topicId = this.topicIds.find(([t]) => t === topic)?.[1]
    if (!topicId) return null
    const key = `${topicId}/${partition}`
    const reply = await this.command('p14', {
      type: 'scan',
      id: `scan.${key}`,
      topic,
      topicId,
      partition,
      from: this.scanned.get(key) ?? '0',
      execution,
    })
    if (reply.error || !reply.reply) return null
    this.scanned.set(key, reply.reply.to)
    return reply.reply
  }

  /**
   * Seals every declared route of a topic incarnation at its terminal scan: producers closed for
   * it, the scan reached its settled end, the groups drained and committed before this is called.
   * @param {string} topic @param {string|null} execution
   * @returns {Promise<void>}
   */
  async seal(topic, execution = null) {
    const topicId = this.topicIds.find(([t]) => t === topic)?.[1]
    const partitions = new Set(
      [...this.routes.values()].filter((r) => r.topicId === topicId).map((r) => r.partition)
    )
    for (const partition of partitions) {
      const terminal = await this.scan(topic, partition, execution)
      for (const [id, route] of this.routes) {
        if (route.topicId !== topicId || route.partition !== partition) continue
        this.routes.delete(id)
        if (!terminal) {
          await this.collector.note(`route ${id} has no terminal scan`)
          continue
        }
        await this.readback(null, {
          group: route.group,
          client: this.rig.id,
          commit: null,
          key: { topic, topicId, partition },
        })
        await this.emit({
          kind: 'route',
          event: 'seal',
          execution,
          ...route,
          to: terminal.to,
          scan: terminal.scan,
        })
      }
    }
  }

  /** @param {string} slot spawns one child with the sampling flags and an IPC channel */
  spawn(slot) {
    const args = [
      '--expose-gc',
      '--no-flush-bytecode',
      __filename,
      '--child',
      slot,
      '--out',
      this.out,
    ]
    const child = spawn(process.execPath, args, {
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
      env: process.env,
    })
    this.children.set(slot, child)
    this.exited ??= new Promise((resolve) => (this.allExited = resolve))
    this.watch(slot, child)
    if (!child.connected) return
    child.send(
      {
        type: 'init',
        config: this.config,
        matrix: this.matrix,
        originMs: this.clock.originMs,
        topicIds: this.topicIds,
      },
      (e) => e && this.lose(slot, e.message)
    )
  }

  /**
   * Follows one child: its messages, and its loss. A child that exits, closes its channel or fails
   * to spawn settles every command waiting on it; unless it was told to disconnect, that is a
   * failure of the run. A process exit is journaled as it happened. Every attempted child reaches
   * one terminal state at its `close`, which follows both an exit and a spawn that failed (with no
   * exit, so none is journaled).
   * @param {string} slot @param {import('node:events').EventEmitter} child
   * @returns {void}
   */
  watch(slot, child) {
    child.on('message', (m) => this.receive(slot, m))
    child.on('error', (e) => this.lose(slot, e.message))
    child.on('disconnect', () => this.lose(slot, 'closed its channel'))
    child.on('exit', (code, signal) => {
      this.observedEndAt = this.clock.now()
      this.lose(slot, `exited with ${signal ?? code}`)
      this.exitRecords.set(
        slot,
        this.emit({
          kind: 'process',
          event: 'exit',
          process: slot,
          code,
          signal,
          spawnRecord: `${slot}.0`,
        }).catch(() => {})
      )
    })
    child.on('close', () => this.settleChild(slot))
  }

  /**
   * Records a child's terminal state once, after its exit record (if it exited) is appended; the
   * last one releases `exited`.
   * @param {string} slot @returns {Promise<void>}
   */
  async settleChild(slot) {
    if (this.terminal.has(slot)) return
    this.terminal.add(slot)
    await this.exitRecords.get(slot)
    this.exitRecords.delete(slot)
    if (this.terminal.size === this.children.size) this.allExited?.()
  }

  /**
   * Settles every command waiting on a lost child and, if the child was not leaving, fails the run.
   * @param {string} slot @param {string} reason
   * @returns {void}
   */
  lose(slot, reason) {
    for (const entry of [...this.waiting.values()])
      if (entry.slot === slot)
        entry.settle({
          type: 'done',
          id: entry.id,
          error: `process ${slot} ${reason}`,
          reply: null,
        })
    if (this.leaving.has(slot) || this.lost.has(slot)) return
    this.lost.add(slot)
    this.problem(`process ${slot} ${reason} before its disconnect`)
    this.abort(`process ${slot} ${reason}`)
  }

  /**
   * Waits for every child to reach its terminal state by itself. A child still alive after the
   * teardown allowance is killed and named in the receipt, so the run ends with its evidence but can
   * never be green; one that has not ended `KILL_WAIT_MS` after its kill (a kill that failed) is
   * named too, and the teardown goes on without it. The whole wait is bounded.
   * @returns {Promise<void>}
   */
  async untilExited() {
    const allowance = this.teardownAllowanceMs()
    if (await this.exitedWithin(allowance)) return
    for (const [slot, child] of this.children) {
      if (this.terminal.has(slot)) continue
      this.forced.push(slot)
      this.leaving.add(slot)
      this.problem(`process ${slot} did not exit within ${allowance} ms of its disconnect; killed`)
      try {
        child.kill('SIGKILL')
      } catch (e) {
        this.problem(`process ${slot} could not be killed: ${e.message}`)
      }
    }
    if (await this.exitedWithin(KILL_WAIT_MS)) return
    for (const slot of this.children.keys())
      if (!this.terminal.has(slot)) {
        this.problem(`process ${slot} did not end ${KILL_WAIT_MS} ms after its kill`)
        this.incomplete ??= `process ${slot} did not end`
      }
  }

  /** @param {number} ms @returns {Promise<boolean>} whether every child ended within `ms` */
  async exitedWithin(ms) {
    const timer = new AbortController()
    const winner = await Promise.race([
      (this.exited ?? Promise.resolve()).then(() => true),
      this.clock.sleepUntil(this.clock.now() + ms, timer.signal).then(() => false),
    ])
    timer.abort()
    return winner
  }

  /** Routes a child message: records to the collector, outcomes, readbacks and subscriptions. */
  receive(slot, message) {
    const child = this.children.get(slot)
    if (message.type === 'record') {
      const { seq } = message.record
      this.count(message.record)
      this.collector
        .append(message.record)
        .then((ok) => child.connected && child.send({ type: 'ack', seq, refused: !ok }, () => {}))
    } else if (message.type === 'done') {
      if (message.incomplete) this.incomplete ??= message.incomplete
      this.waiting.get(message.id)?.settle(message)
    } else if (message.type === 'problem') {
      this.problem(`${slot}: ${message.message}`)
      this.abort(`${slot}: ${message.message}`)
    } else if (message.type === 'readback') this.readback(slot, message)
    else if (message.type === 'telemetry-subscription')
      this.command('p16', {
        type: 'telemetry-subscriptions',
        id: 'telemetry-subscriptions',
        entries: [
          {
            client: message.client,
            instanceId: message.instanceId,
            subscriptionId: message.subscriptionId,
          },
        ],
      })
  }

  /**
   * Reads a group's committed offset back for one key, records it and answers the asking child.
   * @param {string|null} slot the child to answer, or none
   * @param {{ request?: number, group: string, commit: string|null, client: string, key: object }} body
   * @returns {Promise<void>}
   */
  async readback(slot, { request, group, commit, client, key }) {
    let next = null
    let errorCode = 0
    try {
      const [topic] = await this.admin.fetchOffsets({ groupId: group, topics: [key.topic] })
      const partition = topic?.partitions.find((p) => p.partition === key.partition)
      next = partition && partition.offset !== '-1' ? partition.offset : null
    } catch (e) {
      errorCode = Number.isInteger(e.code) ? e.code : -1
    }
    await this.emit({
      kind: 'identity',
      stage: 'broker-commit',
      client,
      group,
      commit,
      key,
      next,
      errorCode,
    })
    const child = slot ? this.children.get(slot) : null
    if (child?.connected) child.send({ type: 'reply', request }, () => {})
  }

  /**
   * Sends a step to a child and waits for its outcome, which always arrives: the child's answer,
   * an error when it is gone or its channel has closed (so a sample admitted after the teardown
   * disconnected a process fails that step, not the run), or a failure of the run when it does not
   * answer within the step's budget.
   * @param {string} slot @param {object} step
   * @returns {Promise<{ error: string|null, reply: unknown }>} the child's outcome
   */
  command(slot, step) {
    const id = ++this.commands
    const child = this.children.get(slot)
    const closed = (reason) => ({ type: 'done', id, error: reason, reply: null })
    if (!child?.connected) return Promise.resolve(closed(`process ${slot} has disconnected`))
    if (step.type === 'disconnect') this.leaving.add(slot)
    return new Promise((resolve) => {
      const entry = {
        id,
        slot,
        timer: null,
        settle: (message) => {
          clearTimeout(entry.timer)
          if (this.waiting.delete(id)) resolve(message)
        },
      }
      const ms = this.budget(step)
      entry.timer = setTimeout(() => {
        const reason = `process ${slot} did not answer ${step.type} ${step.id} within ${ms} ms`
        this.problem(reason)
        this.abort(reason)
        entry.settle(closed(reason))
      }, ms)
      this.waiting.set(id, entry)
      child.send({ type: 'command', id, step }, (e) => {
        if (e) entry.settle(closed(`process ${slot}: ${e.message}`))
      })
    })
  }

  /** @param {string[]} slots @param {object} step @returns {Promise<object[]>} throws if any failed */
  async send(slots, step) {
    const replies = await Promise.all(slots.map((slot) => this.command(slot, step)))
    const failed = replies.filter((r) => r.error)
    if (failed.length > 0) throw new Error(failed.map((r) => r.error).join('; '))
    return replies
  }

  /**
   * Samples processes through the gate: at most two collect at once, in a fixed order.
   * @param {string[]} slots @param {object} step a periodic or quiet step
   * @returns {Promise<void>}
   */
  async sampleAll(slots, step) {
    const results = await Promise.all(
      slots.map((slot) =>
        this.gate.run(() =>
          slot === SUPERVISOR
            ? this.runtime.sample({ role: 'periodic', slot: step.slot }).then(() => ({}))
            : this.command(slot, step)
        )
      )
    )
    const decoder = results[slots.indexOf('p16')]
    if (decoder?.reply?.abort) this.abort(decoder.reply.abort)
    const failed = results.filter((r) => r?.error)
    if (failed.length > 0) throw new Error(failed.map((r) => r.error).join('; '))
  }

  /** @param {string} kind @returns {string} the process of the kind's standing subject */
  processOf(kind) {
    return this.config.clients.find((c) => c.clientKind === kind && c.purpose === 'subject').process
  }

  /** @returns {string[]} every process that consumes: ordinary subjects, telemetry consumer, seed */
  consumers() {
    return this.config.processes
      .filter((p) => {
        const c = this.config.clients.find((x) => x.process === p.slot && x.purpose !== 'observer')
        return c && ['consumer'].includes(this.matrix.clientKinds[c.clientKind].role)
      })
      .map((p) => p.slot)
  }

  /**
   * Refuses a quiet sample unless the run is quiet: every process confirmed the idle window, no
   * work step is running (one reported late included) and the run was not aborted.
   * @param {object} step the quiet step
   * @returns {void}
   * @throws {Error} naming what was not quiet
   */
  requireQuiet(step) {
    const busy = [
      ...(this.quiesced ? [] : ['the processes have not confirmed idle']),
      ...[...this.scheduler.inflight].map((id) => `${id} is still running`),
      ...(this.aborted ? [`the run is aborted`] : []),
    ]
    if (busy.length > 0) throw new Error(`${step.id} is not a quiet sample: ${busy.join(', ')}`)
  }

  /** Performs one plan step across the processes it involves. */
  async perform(step) {
    const all = [...this.children.keys()]
    const judged = this.config.processes
      .filter((p) => p.role === 'subject' || p.role === 'seed')
      .map((p) => p.slot)
    if (step.type === 'periodic') return this.sampleAll([...all, SUPERVISOR], step)
    if (step.type === 'quiet') {
      this.requireQuiet(step)
      await this.sampleAll(judged, step)
      return this.requireQuiet(step)
    }
    if (step.type === 'seed-start' || step.type === 'seed-clear') return this.send(['p13'], step)
    if (step.type === 'disconnect') return this.teardown(step)
    if (step.shape === 'traffic') {
      await this.shape(step, 'start')
      return this.send(all, { type: 'active', id: step.id })
    }
    if (step.type === 'idle') {
      await this.shape(step, 'start')
      try {
        await this.send(all, step)
        this.quiesced = step.id
        await this.clock.sleepUntil(step.until, this.scheduler.controller.signal)
        this.quiesced = null
        if (step.until < this.runEndAt && !this.scheduler.aborted)
          await this.send(all, { ...step, type: 'active' })
      } finally {
        this.quiesced = null
        await this.shape(step, 'end')
      }
      return
    }
    await this.shape(step, 'start')
    try {
      const participants = [...new Set(step.participants.map((k) => this.processOf(k)))]
      if (step.shape === 'broker-outage') await this.outage(step, participants)
      else if (step.shape === 'membership-churn') await this.churnMembers(step, participants)
      else if (step.shape === 'regex-churn' || step.shape === 'topic-recreation')
        await this.churnTopic(step, participants)
      else {
        const drain = step.shape === 'drain'
        const slots = drain ? [...new Set([...participants, ...this.consumers()])] : participants
        const replies = await this.send(slots, step)
        const short = slots.filter((_, i) => replies[i].reply?.reached === false)
        if (short.length > 0) throw new Error(`${step.id}: ${short.join(', ')} did not drain`)
        if (drain) {
          const t = this.config.topics
          for (const [topic, partitions] of [
            [t.main, 1],
            [t.transactions, 1],
            ...t.recreate.map((r) => [r, 2]),
          ])
            for (let partition = 0; partition < partitions; partition++)
              await this.scan(topic, partition, step.id)
        }
      }
    } finally {
      await this.shape(step, 'end')
    }
  }

  /**
   * The end of the run. Producers go idle and their in-flight sends and transactions settle; the
   * logs' ends are then read; every consumer waits until it has resolved those ends, filtered
   * suffix included, and commits what it resolved, and the share member waits until every offset
   * had a handler entry; every live route gets its terminal scan and seal; then every process but
   * the telemetry decoder disconnects, the decoder reads the telemetry clients' terminating pushes
   * and disconnects last. The cluster and the two standing topics stay up for the runbook's dumps.
   * @param {object} step the disconnect step
   * @returns {Promise<void>}
   */
  async teardown(step) {
    const all = [...this.children.keys()]
    await this.send(all, { type: 'idle', id: 'final-idle' }).catch((e) =>
      this.problem(`final idle: ${e.message}`)
    )
    await this.finalDrain().catch((e) => this.problem(`final drain: ${e.message}`))
    const t = this.config.topics
    for (const topic of [t.main, t.transactions, ...t.recreate]) await this.seal(topic, null)
    await this.shape(
      {
        ...step,
        id: 'traffic',
        shape: 'traffic',
        participants: Object.keys(this.matrix.clientKinds),
      },
      'end'
    )
    await this.gate.idle()
    const decoder = this.config.processes.find((p) => p.role === 'decoder')?.slot
    let failed = null
    await this.send(
      all.filter((slot) => slot !== decoder),
      step
    ).catch((e) => (failed = e))
    if (all.includes(decoder)) {
      await this.drainTelemetry(decoder)
      await this.send([decoder], step).catch((e) => (failed ??= e))
    }
    if (failed) throw failed
  }

  /**
   * Has the decoder read the reporters until the disconnected telemetry clients' terminating
   * pushes are in the stream. A push not read in time is noted; the raw reporter files keep it.
   * @param {string} decoder the decoder's process
   * @returns {Promise<void>}
   */
  async drainTelemetry(decoder) {
    const clients = this.config.clients.filter((c) => c.telemetryEnabled).map((c) => c.slot)
    const step = {
      type: 'drain-telemetry',
      id: 'final-telemetry',
      clients,
      deadline: this.clock.now() + TELEMETRY_DRAIN_MS,
    }
    const [r] = await this.send([decoder], step).catch((e) => [{ error: e.message }])
    if (r.error) this.problem(`final telemetry: ${r.error}`)
    if (r.reply?.abort) this.problem(`final telemetry: ${r.reply.abort}`)
    if (r.reply?.missing?.length > 0)
      await this.collector.note(`final telemetry: no terminating push read for ${r.reply.missing}`)
  }

  /**
   * Asks every consuming process to commit its group to the two standing logs' settled ends, and
   * the share process to deliver up to them. When the ends moved during the drain (a late marker)
   * the drain runs again with the new ends, until they hold or the deadline passes.
   * @returns {Promise<void>}
   * @throws {Error} when the log ends cannot be read
   */
  async finalDrain() {
    const deadline = this.clock.now() + FINAL_DRAIN_MS
    const slots = [...new Set([...this.consumers(), this.processOf('share')])]
    let ends = await this.settledEnds(deadline)
    for (let round = 1; ; round++) {
      const step = { type: 'drain', id: `final-drain.${round}`, ends, deadline }
      const replies = await Promise.all(slots.map((slot) => this.command(slot, step)))
      const after = await this.settledEnds(deadline)
      const moved = Object.keys(ends).some((k) => ends[k] !== after[k])
      if (!moved || this.clock.now() >= deadline) {
        if (moved) this.problem("final drain: the logs' ends moved after the deadline")
        replies.forEach((r, i) => {
          if (r.error || r.reply?.reached === false)
            this.problem(`final drain: ${slots[i]} ${r.error ?? "did not resolve the logs' ends"}`)
        })
        return
      }
      ends = after
    }
  }

  /**
   * Reads logs' ends until no transaction is open on them: the read-committed end equals the high
   * watermark, which Apache's describe prints as the log end.
   * @param {number} deadline on the run clock; past it, the high watermarks are returned and the
   *   open transaction is a problem of the run
   * @param {string[]} [topics] the logs, by default the two standing ones
   * @returns {Promise<Record<string, string>>} `topic|partition` to log end
   * @throws {Error} when the log ends cannot be read
   */
  async settledEnds(deadline, topics = [this.config.topics.main, this.config.topics.transactions]) {
    for (;;) {
      const ends = {}
      let open = false
      for (const topic of topics) {
        const committed = await this.admin.fetchTopicOffsets(topic, {
          isolationLevel: 'read_committed',
        })
        const high = await this.admin.fetchTopicOffsets(topic, {
          isolationLevel: 'read_uncommitted',
        })
        for (const { partition, offset } of high) {
          ends[`${topic}|${partition}`] = offset
          open ||= committed.find((c) => c.partition === partition)?.offset !== offset
        }
      }
      if (!open) return ends
      if (this.clock.now() >= deadline) {
        this.problem("final drain: a transaction stayed open on the logs' ends")
        return ends
      }
      await this.clock.sleepUntil(Math.min(deadline, this.clock.now() + 250))
    }
  }

  /**
   * A membership execution, wave by wave. Each wave is its own pause-resume execution: every
   * participant runs the wave's `leave` phase (pause, join an extra, observe the transfer, remove
   * the extra, see the paused partitions return), the participants are sampled closed through the
   * gate, every participant resumes, and the wave ends with its operation count and its outcome
   * note. A wave starts no earlier than its planned time. A failed phase still resumes and ends the
   * wave; no later wave starts, and the step fails with the error. An aborted run starts no wave.
   * @param {object} step the membership execution @param {string[]} participants its processes
   * @returns {Promise<void>}
   * @throws {Error} the first failed phase of any wave
   */
  async churnMembers(step, participants) {
    let first = null
    for (const [index, wave] of (this.waves.get(step.id) ?? []).entries()) {
      if (first || this.aborted) break
      await this.clock.sleepUntil(wave.at, this.scheduler?.controller.signal)
      if (this.aborted) break
      await this.shape(wave, 'start')
      const base = { ...step, wave: index, execution: wave.id, waveDeadline: wave.deadline }
      let failed = null
      try {
        const leave = await this.send(participants, { ...base, phase: 'leave' })
        const none = participants.filter((_, i) => leave[i].reply?.transfers === 0)
        if (none.length > 0)
          throw new Error(`${wave.id} saw no paused partition transfer in ${none.join(', ')}`)
        const kept = participants.filter((_, i) => leave[i].reply?.returned === false)
        if (kept.length > 0)
          throw new Error(`${wave.id} saw paused partitions not return in ${kept.join(', ')}`)
        await this.sampleAll(participants, {
          type: 'closed',
          id: `${wave.id}.closed`,
          boundary: wave.id,
        })
      } catch (e) {
        failed = e
      }
      await this.send(participants, { ...base, phase: 'resume' }).catch((e) => (failed ??= e))
      await this.shape(wave, 'end')
      const late = this.clock.now() > wave.deadline
      await this.collector.note(`step ${wave.id} ${failed ? 'error' : late ? 'late' : 'ok'}`, {
        step: wave.id,
      })
      first ??= failed
    }
    if (first) throw first
  }

  /**
   * A regex execution creates a fresh topic and its routes, waits for its metadata, lets every
   * applicable client accept it, offers probe ids, drains the recipients to its ends, scans and
   * seals it, deletes it, lets the clients retire it and forgets its id. A recreation probes,
   * drains, scans and seals the old incarnation, deletes it, creates the same name again under a
   * new id with new routes, waits for its metadata and for every recipient to be assigned it, then
   * probes the new one. Every stage checks what it did: a creation must give a new id, a deletion
   * must be confirmed, every probe offer acknowledged, every recipient drained, accepting,
   * retiring and assigned as asked. No stage starts after the execution's deadline and no wait runs
   * past it: the execution fails and later stages are not attempted.
   * @param {object} step the execution @param {string[]} participants its processes
   * @returns {Promise<void>}
   * @throws {Error} when a stage cannot start before the deadline or did not do its work
   */
  async churnTopic(step, participants) {
    const within = (stage) => {
      if (this.clock.now() >= step.deadline)
        throw new Error(`${step.id}: ${stage} is past the execution deadline`)
      if (this.aborted) throw new Error(`${step.id}: ${stage} not started, the run is aborted`)
    }
    const stage = async (name, work) => {
      within(name)
      return work()
    }
    const settle = () =>
      this.clock.sleepUntil(
        Math.min(step.deadline, this.clock.now() + 3 * this.config.timeouts.metadataMaxAge)
      )
    const producer = this.processOf('producer-idempotent')
    const probe = (topic) =>
      stage('probe', async () => {
        const count = 10
        const [{ reply }] = await this.send([producer], {
          type: 'probe',
          id: `${step.id}.probe`,
          execution: step.id,
          topic,
          partitions: 2,
          count,
          deadline: step.deadline,
        })
        if (reply?.acknowledged !== count)
          throw new Error(
            `${step.id}: ${reply?.acknowledged ?? 0} of ${count} probe offers to ${topic} were acknowledged${reply?.stopped ? ', stopped at the deadline' : ''}`
          )
      })
    const drain = (topic) =>
      stage('drain', async () => {
        const ends = await this.settledEnds(step.deadline, [topic])
        const replies = await this.send(participants, {
          type: 'drain',
          id: `${step.id}.drain`,
          ends,
          deadline: step.deadline,
        })
        const short = participants.filter((_, i) => replies[i].reply?.reached === false)
        if (short.length > 0)
          throw new Error(`${step.id}: ${short.join(', ')} did not drain ${topic} to its end`)
      })
    /** Sends a stage to the participants; any of them answering `field: false` fails it. */
    const each = (name, body, field) =>
      stage(name, async () => {
        const replies = await this.send(participants, body)
        const short = participants.filter((_, i) => replies[i].reply?.[field] === false)
        if (short.length > 0) throw new Error(`${step.id}: ${name} failed in ${short.join(', ')}`)
      })
    const create = (topic, role) =>
      stage('create', async () => {
        const topicId = await this.createTopic(topic, 2, role, step.id)
        await this.ready(topic, topicId, 2, step.deadline)
      })
    if (step.shape === 'regex-churn') {
      const topic = `${this.config.topics.regex}-${step.id}`
      await create(topic, 'regex')
      await each('accept', { ...step, topicName: topic }, 'accepted')
      await probe(topic)
      await drain(topic)
      await stage('seal', () => this.seal(topic, step.id))
      await stage('delete', () => this.deleteTopic(topic, step.id))
      await each('retire', { ...step, type: 'retired', topicName: topic }, 'retired')
      await this.unpublish(topic)
      return
    }
    const topic = this.config.topics.recreate[step.topic]
    await probe(topic)
    await drain(topic)
    await stage('seal', () => this.seal(topic, step.id))
    await stage('pause', () => this.send(participants, { ...step, topicName: topic }))
    await stage('delete', () => this.deleteTopic(topic, step.id))
    await settle()
    await create(topic, 'recreate')
    await each('recreated', { ...step, type: 'recreated', topicName: topic }, 'assigned')
    await probe(topic)
  }

  /**
   * Runs a docker command, killed if it runs longer than `dockerMs`.
   * @param {string[]} args @returns {Promise<string>} its trimmed stdout
   * @throws {Error} when it failed or was killed
   */
  docker(args) {
    return new Promise((resolve, reject) =>
      execFile(
        'docker',
        args,
        { env: process.env, timeout: this.dockerMs, killSignal: 'SIGKILL' },
        (e, stdout) => (e ? reject(e) : resolve(stdout.trim()))
      )
    )
  }

  /**
   * Stops and restarts one broker as the step says, recording the supervisor receipts and the
   * broker state (down, ready, in-sync) from raw metadata, then asks every kind for its own
   * recovery witness. The step fails, after all of it ran, unless the stop and the start succeeded
   * and showed in the container's process, the cluster regained all three brokers, every run
   * partition returned to three in-sync replicas and every kind produced its witness.
   * @param {object} step @param {string[]} participants
   * @returns {Promise<void>}
   * @throws {Error} listing everything that could not be confirmed
   */
  async outage(step, participants) {
    const service = `kafka${step.node}`
    const failures = []
    const container = await this.docker(['compose', 'ps', '-aq', service]).catch(() => '')
    const inspect = async () => {
      const format = '{{.State.Pid}} {{.State.StartedAt}} {{.State.FinishedAt}}'
      const out = await this.docker(['inspect', '-f', format, container]).catch(() => '')
      const [pid, startedAt = null, finishedAt = null] = out.split(' ')
      return { pid: out ? Number(pid) : NaN, startedAt, finishedAt }
    }
    const op = async (name, args) => {
      const operation = `${SUPERVISOR}.b${this.runtime.ids}`
      const base = {
        kind: 'operation',
        operation,
        op: name,
        client: null,
        keys: [],
        call: operation,
        execution: step.id,
        causes: [],
      }
      await this.emit({ ...base, phase: 'begin', result: null, errorType: null })
      let ok = true
      try {
        await this.docker(['compose', ...args])
        await this.emit({ ...base, phase: 'end', result: 'ok', errorType: null })
      } catch (e) {
        ok = false
        failures.push(`${name} of ${service} failed: ${e.message}`)
        await this.emit({ ...base, phase: 'end', result: 'error', errorType: e.name ?? 'Error' })
      }
      return { operation, ok }
    }
    await this.send(participants, { ...step, type: 'broker-outage' }).catch(() => {})
    const stopped = await op(
      'broker-stop',
      step.stop === 'clean' ? ['stop', service] : ['kill', '-s', 'SIGKILL', service]
    )
    const afterStop = await inspect()
    const down = afterStop.pid
    await this.brokerState(step, 'down', afterStop, container, stopped.operation)
    if (stopped.ok && down !== 0)
      failures.push(`${service} still runs (pid ${down}) after its stop`)
    await this.clock.sleepUntil(this.clock.now() + step.downMs)
    const started = await op('broker-start', ['start', service])
    const ready = async () =>
      (await this.admin.describeCluster().catch(() => ({ brokers: [] }))).brokers.length === 3
    while (!(await ready()) && this.clock.now() < step.deadline)
      await this.clock.sleepUntil(this.clock.now() + 500)
    const afterStart = await inspect()
    const up = afterStart.pid
    await this.brokerState(step, 'ready', afterStart, container, started.operation)
    if (!(await ready())) failures.push(`the cluster did not regain three brokers by the deadline`)
    else if (started.ok && !(up > 0))
      failures.push(`${service} has no running process after its start`)
    const expected = 2 + 2 * this.config.topics.recreate.length
    const inSync = async () => {
      const states = await this.partitionStates()
      return states.length >= expected && states.every((p) => p.isr.length === 3)
    }
    while (!(await inSync()) && this.clock.now() < step.deadline)
      await this.clock.sleepUntil(this.clock.now() + 500)
    await this.brokerState(step, 'isr', afterStart, container, started.operation)
    if (!(await inSync())) failures.push('the run partitions are not all in sync by the deadline')
    const replies = await Promise.all(
      participants.map((slot) => this.command(slot, { ...step, type: 'recovered' }))
    )
    replies.forEach((r, i) => {
      if (!r.reply?.ok) failures.push(`${participants[i]} has no recovery witness after ${step.id}`)
    })
    if (failures.length > 0) throw new Error(failures.join('; '))
  }

  /** @returns {Promise<object[]>} raw leader and ISR of every run partition */
  async partitionStates() {
    const t = this.config.topics
    const { topics } = await this.admin
      .fetchTopicMetadata({ topics: [t.main, t.transactions, ...t.recreate] })
      .catch(() => ({ topics: [] }))
    return topics.flatMap((topic) =>
      topic.partitions.map((p) => ({
        key: {
          topic: topic.name,
          topicId: normalizeTopicId(topic.topicId),
          partition: p.partitionId,
        },
        leader: p.leader,
        isr: [...p.isr].sort((a, b) => a - b),
      }))
    )
  }

  /**
   * Emits one broker-state record: the configured down interval, and the container's own instant
   * for the event (its process stop for `down`, its process start for `ready` and `isr`), raw as
   * Docker reports it and on the run clock, so a reader can check the actual down time against the
   * configured one. An instant Docker does not report (zero or unparseable) is null. The leader and
   * ISR of each run partition follow, one `partition-state` record each citing the broker state.
   * @param {object} step the outage step @param {'down'|'ready'|'isr'} event
   * @param {{ pid: number, startedAt: string|null, finishedAt: string|null }} state from inspect
   * @param {string} container @param {string} cause the stop or start operation
   * @returns {Promise<void>}
   */
  async brokerState(step, event, state, container, cause) {
    const { brokers } = await this.admin.describeCluster().catch(() => ({ brokers: [] }))
    const raw = event === 'down' ? state.finishedAt : state.startedAt
    const wall = Date.parse(raw)
    const processAt = wall > 0 ? this.clock.now() - (Date.now() - wall) : null
    const partitions = (await this.partitionStates()).filter((p) => p.key.topicId)
    const recordId = await this.emit({
      kind: 'broker-state',
      execution: step.id,
      node: step.node,
      pid: state.pid > 0 ? state.pid : 0,
      container: container || 'unknown-container',
      event,
      members: brokers.map((b) => b.nodeId).sort((a, b) => a - b),
      partitions: partitions.length,
      cause,
      downMs: step.downMs,
      processAt: processAt !== null && processAt >= 0 ? processAt : null,
      processTime: processAt !== null && processAt >= 0 ? raw : null,
    })
    for (const p of partitions)
      await this.emit({ kind: 'partition-state', execution: step.id, cause: recordId, ...p })
  }

  /**
   * Emits a shape boundary record for an execution of one of the seven shapes (a drain is a step,
   * not a shape, and has none). The end carries the operations that ended under the execution;
   * every one has been appended by then, since each child acknowledges its records before it
   * answers the step.
   * @param {object} step @param {'start'|'end'} phase
   * @returns {Promise<string|null>} the record id, or null for a step that is not a shape
   */
  async shape(step, phase) {
    if (!SHAPES.includes(step.shape)) return null
    const operationCount = phase === 'end' ? (this.ended.get(step.id) ?? 0) : 0
    if (phase === 'end') this.ended.delete(step.id)
    return this.emit({
      kind: 'shape',
      execution: step.id,
      phase,
      shape: step.shape,
      participants: step.participants ?? [],
      operationCount,
    })
  }
}

/**
 * The run's clock origin: the one the preflight recorded on this boot, so preflight facts are on
 * the run clock, or now when there is none.
 * @param {string} out the artifact directory
 * @returns {number} the origin in monotonic ms
 */
const originOf = (out) => {
  try {
    const clock = JSON.parse(fs.readFileSync(path.join(out, 'clock.json'), 'utf8'))
    if (clock.bootId === bootId()) return clock.originMs
  } catch {
    // no preflight origin: the run starts its own
  }
  return MonotonicClock.raw()
}

/**
 * Removes the run's topics and groups after the run, from `config.json`. Called by the runbook;
 * failures are reported and leave the artifact untouched.
 * @param {{ out: string }} options
 * @returns {Promise<void>}
 */
async function cleanup({ out }) {
  const config = JSON.parse(fs.readFileSync(path.join(out, 'config.json'), 'utf8'))
  const { Kafka, logLevel } = require('../../index')
  const admin = new Kafka({
    clientId: 'final-soak-cleanup',
    brokers: config.brokers,
    logLevel: logLevel.NOTHING,
  }).admin()
  await admin.connect()
  try {
    const topics = (await admin.listTopics()).filter((t) => t.startsWith(`fs-${config.runId}-`))
    if (topics.length > 0) await admin.deleteTopics({ topics })
    const groups = [...new Set(config.clients.map((c) => c.group).filter(Boolean))]
    if (groups.length > 0) await admin.deleteGroups(groups).catch(() => {})
  } finally {
    await admin.disconnect()
  }
}

/**
 * Why a sealed run's workload is not green, from its `supervisor.json`: any failure, problem or
 * forced exit, an abort or an incomplete source. The parent-observed exit codes stay what they
 * were; this is the separate verdict the runbook fails on.
 * @param {object|null} supervisor the parsed receipt, or null when it is missing or unreadable
 * @returns {string[]} the reasons; empty for a green workload
 */
function workloadProblems(supervisor) {
  if (!supervisor) return ['supervisor.json is missing or unreadable']
  const shown = (v) => JSON.stringify(v)?.slice(0, 500)
  return [
    ...['failures', 'problems', 'forced']
      .filter((f) => !Array.isArray(supervisor[f]) || supervisor[f].length > 0)
      .map((f) => `${f}: ${shown(supervisor[f])}`),
    ...['aborted', 'incomplete']
      .filter((f) => supervisor[f] !== null)
      .map((f) => `${f}: ${shown(supervisor[f])}`),
  ]
}

/**
 * Checks a sealed run's workload receipt and fails when it is not green.
 * @param {{ out: string }} options
 * @returns {string[]} the reasons, also printed to stderr; empty for a green workload
 */
function checkWorkload({ out }) {
  let supervisor = null
  try {
    supervisor = JSON.parse(fs.readFileSync(path.join(out, 'supervisor.json'), 'utf8'))
  } catch {}
  const problems = workloadProblems(supervisor)
  for (const p of problems) console.error(`workload not green: ${p}`)
  return problems
}

/**
 * Hashes and counts a byte source with one fixed buffer, so the memory used never grows with the
 * source, however many GiB it holds.
 * @param {{ size: number, read: (buffer: Buffer, position: number) => number }} source `read` fills
 *   the buffer from `position` and returns the bytes it put there
 * @param {number} [chunkBytes] the buffer size, 8 MiB by default
 * @returns {{ bytes: number, sha256: string, lines: number }} size, SHA-256 and newline count
 * @throws {Error} when a read returns no bytes before the source's end
 */
function digest({ size, read }, chunkBytes = 8 * MIB) {
  const buffer = Buffer.allocUnsafe(Math.max(1, Math.min(chunkBytes, size)))
  const hash = crypto.createHash('sha256')
  let lines = 0
  for (let position = 0; position < size;) {
    const bytes = read(buffer, position)
    if (!(bytes > 0)) throw new Error(`a read at byte ${position} of ${size} returned no bytes`)
    const view = buffer.subarray(0, bytes)
    hash.update(view)
    for (let i = view.indexOf(0x0a); i !== -1; i = view.indexOf(0x0a, i + 1)) lines++
    position += bytes
  }
  return { bytes: size, sha256: hash.digest('hex'), lines }
}

/**
 * @param {string} file @param {number} [chunkBytes]
 * @returns {{ bytes: number, sha256: string, lines: number }} the file's digest, read in chunks
 */
function digestFile(file, chunkBytes) {
  const fd = fs.openSync(file, 'r')
  try {
    const { size } = fs.fstatSync(fd)
    const read = (buffer, position) => fs.readSync(fd, buffer, 0, buffer.length, position)
    return digest({ size, read }, chunkBytes)
  } finally {
    fs.closeSync(fd)
  }
}

/**
 * Records, at once, the status the shell's wait returned for the supervisor and the run-clock time
 * it returned, in `outer-exit.json`. The time is placed from the wall-clock moment of the return,
 * so a later finalizer cannot move it. The status is kept as the shell saw it: a number above 128
 * may be a code or a signal and is not guessed.
 * @param {{ out: string, status: number, exitWallMs: number }} options
 * @param {{ wallNow?: () => number, clock?: { now: () => number } }} [seams]
 * @returns {{ status: number, observedAt: number }} the receipt written
 */
function recordOuterExit({ out, status, exitWallMs }, seams = {}) {
  const { wallNow = Date.now, clock = new MonotonicClock(originOf(out)) } = seams
  const ago = Number.isFinite(exitWallMs) ? Math.max(0, wallNow() - exitWallMs) : 0
  const receipt = { status, observedAt: Math.max(0, clock.now() - ago) }
  fs.writeFileSync(path.join(out, 'outer-exit.json'), JSON.stringify(receipt) + '\n')
  return receipt
}

/**
 * Digests every file of the artifact except the files that describe it, streaming each.
 * @param {string} out @returns {{ file: string, bytes: number, sha256: string, lines: number }[]}
 */
function digestArtifact(out) {
  const files = []
  const walk = (dir) => {
    for (const entry of fs.readdirSync(path.join(out, dir), { withFileTypes: true })) {
      const rel = dir ? `${dir}/${entry.name}` : entry.name
      if (entry.isDirectory()) walk(rel)
      else if (entry.isFile() && !['header.json', 'manifest.partial.json'].includes(rel))
        files.push({ file: rel, ...digestFile(path.join(out, rel)) })
    }
  }
  walk('')
  return files
}

/**
 * The outer handover after the supervisor has exited: appends the parent-observed supervisor exit
 * as the `outer` source, copies each supplied companion into the artifact, hashes and fsyncs every
 * file (streamed, never read whole) and writes `header.json`. The exit is what the runbook's
 * receipt says it observed, at the time it observed it; the window's observed end is that time, so
 * every record (the supervisor's closing ones and this receipt) is within it. When the supervisor
 * left no receipt there is no run to describe: only `manifest.partial.json` is written, naming the
 * files and the absent ones, and the result is `{ partial: true }`.
 * @param {{ out: string, status: number, companions?: { name: string, file: string,
 *   runUrl: string|null }[] }} options the artifact, the shell's wait status and the companions
 * @returns {object} the header written, or `{ partial: true, manifest }`
 */
function finalize({ out, status, companions = [] }) {
  const read = (file) => {
    try {
      return JSON.parse(fs.readFileSync(path.join(out, file), 'utf8'))
    } catch {
      return null
    }
  }
  const run = read('supervisor.json')
  const receipt = read('outer-exit.json')
  if (!run) {
    const manifest = {
      partial: true,
      absent: ['supervisor.json', 'header.json'],
      exit: { status: receipt?.status ?? status, observedAt: receipt?.observedAt ?? null },
      files: digestArtifact(out).map(({ file, bytes, sha256: hash }) => ({
        file,
        bytes,
        sha256: hash,
      })),
    }
    fs.writeFileSync(
      path.join(out, 'manifest.partial.json'),
      JSON.stringify(manifest, null, 2) + '\n'
    )
    return { partial: true, manifest }
  }
  const observedAt = receipt?.observedAt ?? new MonotonicClock(run.originMs).now()
  const code = receipt?.status ?? status
  const streamFile = path.join(out, 'stream.ndjson')
  const exit = {
    kind: 'process',
    recordId: `${OUTER}.0`,
    source: OUTER,
    seq: 0,
    at: observedAt,
    t: observedAt,
    execution: null,
    event: 'exit',
    process: SUPERVISOR,
    code,
    signal: null,
    spawnRecord: `${SUPERVISOR}.0`,
  }
  const problems = validateRecord(exit)
  if (problems.length > 0) throw new Error(problems.join('; '))
  fs.appendFileSync(streamFile, JSON.stringify(exit) + '\n')
  const fd = fs.openSync(streamFile, 'r+')
  fs.fsyncSync(fd)
  fs.closeSync(fd)
  const companionEntries = companions.map(({ name, file, runUrl }) => {
    const rel = `companions/${name}${path.extname(file)}`
    fs.mkdirSync(path.join(out, 'companions'), { recursive: true })
    fs.copyFileSync(file, path.join(out, rel))
    return { name, treeSha: run.treeSha, file: rel, sha256: digestFile(file).sha256, runUrl }
  })
  const files = digestArtifact(out)
  const stream = files.find((f) => f.file === 'stream.ndjson')
  const preflight = read('preflight.json')
  const header = {
    schema: 3,
    mode: run.mode,
    runId: run.runId,
    treeSha: run.treeSha,
    node: process.version,
    v8: process.versions.v8,
    platform: process.platform,
    files: files.map(({ file, bytes, sha256: hash }) => ({ file, bytes, sha256: hash })),
    clock: { kind: 'linux-monotonic', originMs: run.originMs, bootId: run.bootId },
    window: {
      runStartAt: run.runStartAt,
      runEndAt: run.runEndAt,
      observedEndAt: observedAt,
      requestedMs: run.requestedMs,
    },
    host: preflight?.host,
    broker: preflight?.broker,
    exit: { supervisorProcess: SUPERVISOR, code, signal: null, observedAt },
    stream: {
      file: 'stream.ndjson',
      bytes: stream.bytes,
      records: stream.lines,
      sha256: stream.sha256,
    },
    companions: companionEntries,
  }
  fs.writeFileSync(path.join(out, 'header.json'), JSON.stringify(header, null, 2) + '\n')
  return header
}

/**
 * The driver's own summary of an artifact, read in one streamed pass: the run window and exit,
 * step outcomes, collector failures, record counts by kind and the distinct diagnostics. It judges
 * nothing; it is what a smoke run prints for whoever reads its log.
 * @param {{ out: string }} options
 * @returns {Promise<object>}
 */
async function summarize({ out }) {
  const read = (file) => {
    try {
      return JSON.parse(fs.readFileSync(path.join(out, file), 'utf8'))
    } catch {
      return null
    }
  }
  const kinds = {}
  const diagnostics = new Map()
  const lines = readline.createInterface({
    input: fs.createReadStream(path.join(out, 'stream.ndjson')),
  })
  for await (const line of lines) {
    if (!line) continue
    const record = JSON.parse(line)
    kinds[record.kind] = (kinds[record.kind] ?? 0) + 1
    if (record.kind === 'diagnostic') {
      const message = record.message.replace(/\b[pc]\d{2}\.\d+\b|\d+/g, '#')
      if (diagnostics.size < 200 || diagnostics.has(message))
        diagnostics.set(message, (diagnostics.get(message) ?? 0) + 1)
    }
  }
  const header = read('header.json')
  return {
    supervisor: read('supervisor.json'),
    preflight: read('preflight.json')?.checks ?? null,
    exit: header?.exit ?? null,
    window: header?.window ?? null,
    records: kinds,
    diagnostics: Object.fromEntries(diagnostics),
  }
}

/** @param {string[]} argv @returns {Promise<void>} the command-line entry */
async function main(argv) {
  const options = parseArgs(argv, process.env)
  if (options.command === 'finalize') process.exitCode = finalize(options).partial ? 1 : 0
  else if (options.command === 'outer-exit') recordOuterExit(options)
  else if (options.command === 'cleanup') await cleanup(options)
  else if (options.command === 'check-workload')
    process.exitCode = checkWorkload(options).length > 0 ? 1 : 0
  else if (options.command === 'summary')
    console.log(JSON.stringify(await summarize(options), null, 2))
  else if (options.command === 'child') child(options)
  else await new Supervisor(options).run()
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((e) => {
    console.error(e.stack)
    process.exitCode = 1
  })
}

module.exports = {
  parseArgs,
  buildConfig,
  recipientGroups,
  digest,
  digestFile,
  recordOuterExit,
  finalize,
  summarize,
  workloadProblems,
  checkWorkload,
  Supervisor,
}
