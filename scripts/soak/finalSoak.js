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
 *   node scripts/soak/finalSoak.js --finalize --out <artifact dir> --status <wait status> \
 *     [--companion <name>=<file>[@<run url>]]...
 *   node scripts/soak/finalSoak.js --summary --out <artifact dir>
 *   node scripts/soak/finalSoak.js --cleanup --out <artifact dir>
 *
 * The first command is the collector/supervisor process. It spawns the fifteen other persistent
 * processes (`--child <slot>`), is the only stream writer, and exits naturally after every child
 * has. The outer observer (the runbook) witnesses that exit and runs `--finalize`, which appends
 * the parent-observed supervisor exit, hashes every file and writes `header.json`. `--duration` is
 * accepted only in smoke mode, where the matrix fixes its length. `--diagnostic` (smoke only) marks
 * a run on a host that does not meet the quiet-host floors, such as a hosted CI runner; such a run
 * proves only that the driver works against a live cluster. The modules it is split into
 * (scheduler, collector, observer, workers) are archived with it.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const readline = require('node:readline')
const { spawn, execFile } = require('node:child_process')
const { validateRecord, validateConfig } = require('./finalSoakSchema')
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
const { sha256, bootId, ChildRuntime, child } = require('./finalSoakWorkers')

const ROOT = path.resolve(__dirname, '../..')
const MATRIX = 'scripts/soak/finalSoakMatrix.json'
const GIB = 1024 ** 3
/** Free disk kept back beyond the stream's remaining reservation (the planned 32 GiB safety). */
const SAFETY_BYTES = 32 * GIB
/** The disk a diagnostic run keeps free: enough to finish writing and archive a smoke. */
const DIAGNOSTIC_RESERVE_BYTES = 2 * GIB

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
  const switches = ['--finalize', '--cleanup', '--summary', '--diagnostic']
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
  if (flags.cleanup) return { command: 'cleanup', out }
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
 * child exit it observes and leaves after them. It never kills a child, unrefs a handle or calls
 * `process.exit`.
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
    this.exits = 0
    this.topicIds = []
    this.topicRecords = new Map()
    this.routes = new Map()
    this.scanned = new Map()
    this.gate = new SamplingGate({ limit: 2 })
  }

  /** @param {object} record @returns {Promise<string>} appends a supervisor record, returns its id */
  async emit(record) {
    const recordId = this.runtime.nextId()
    await this.runtime.emit({ recordId, ...record })
    return recordId
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
      send: (m, done) => this.collector.append(m.record).then(done),
    })
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
    await Promise.all(
      [...this.children.keys()].map((slot) => this.command(slot, { type: 'start', id: 'start' }))
    )
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
    this.scheduler = new Scheduler({
      clock: this.clock,
      perform: (step) => this.perform(step),
      onOutcome: (o) => this.collector.note(`step ${o.id} ${o.outcome}`, { step: o.id }),
    })
    this.monitor = new HostMonitor({
      read: { memAvailable, freeDisk: () => freeDisk(this.out) },
      minMemAvailable: valueOf(this.matrix.host.abortMemAvailableBytes),
      reserve: () =>
        this.config.diagnostic
          ? DIAGNOSTIC_RESERVE_BYTES
          : Math.max(0, valueOf(this.matrix.host.maxStreamBytes) - this.collector.streamBytes) +
            SAFETY_BYTES,
    })
    this.monitor.start(this.clock, (reading) => {
      if (reading.abort) this.abort(reading.abort)
    })
    const summary = await this.scheduler.run(steps)
    this.monitor.stop()
    await this.collector.note('schedule finished', { ...summary, gatePeak: this.gate.peak })
    await this.exited
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
      failures: this.collector.failures,
      aborted: this.scheduler.aborted,
      incomplete: this.incomplete ?? null,
      records: this.collector.records,
      steps: summary,
      mode: this.options.mode,
      diagnostic: this.config.diagnostic,
      treeSha: this.options.expectSha,
    }
    fs.writeFileSync(path.join(this.out, 'supervisor.json'), JSON.stringify(run) + '\n')
  }

  /** @param {string} reason stops admitting work; teardown still runs @returns {void} */
  abort(reason) {
    if (this.scheduler?.aborted) return
    this.collector.note(`abort: ${reason}`)
    this.scheduler?.abort(reason)
  }

  /**
   * Creates one run topic (RF3, min ISR 2, no retention truncation), records the admin result and
   * declares a route from offset 0 for every recipient group and partition.
   * @param {string} topic @param {number} partitions @param {string} role its topology role
   * @param {string|null} [execution]
   * @returns {Promise<string|null>} the topic id
   */
  async createTopic(topic, partitions, role, execution = null) {
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
    if (!topicId) {
      await this.collector.note(`missing-source topic id for created ${topic}`)
      return null
    }
    this.topicRecords.set(topicId, recordId)
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
   */
  async deleteTopic(topic, execution = null) {
    const topicId = this.topicIds.find(([t]) => t === topic)?.[1] ?? null
    let errorCode = 0
    try {
      await this.admin.deleteTopics({ topics: [topic] })
    } catch (e) {
      errorCode = Number.isInteger(e.code) ? e.code : -1
    }
    for (let i = 0; i < 40 && (await this.admin.listTopics()).includes(topic); i++)
      await this.clock.sleepUntil(this.clock.now() + 250)
    await this.topicRecord('delete', topic, topicId, [], errorCode, execution)
    for (const key of [...this.scanned.keys()])
      if (key.startsWith(`${topicId}/`)) this.scanned.delete(key)
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

  /** Creates the standing run topics. */
  async createTopics() {
    const t = this.config.topics
    await this.createTopic(t.main, 1, 'main')
    await this.createTopic(t.transactions, 1, 'transactions')
    for (const topic of t.recreate) await this.createTopic(topic, 2, 'recreate')
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
    child.on('message', (m) => this.receive(slot, m))
    child.send({
      type: 'init',
      config: this.config,
      matrix: this.matrix,
      originMs: this.clock.originMs,
      topicIds: this.topicIds,
    })
    child.on('exit', (code, signal) => {
      this.observedEndAt = this.clock.now()
      this.emit({
        kind: 'process',
        event: 'exit',
        process: slot,
        code,
        signal,
        spawnRecord: `${slot}.0`,
      }).finally(() => {
        if (++this.exits === this.children.size) this.allExited()
      })
    })
  }

  /** Routes a child message: records to the collector, outcomes, readbacks and subscriptions. */
  receive(slot, message) {
    const child = this.children.get(slot)
    if (message.type === 'record') {
      const { seq } = message.record
      this.collector
        .append(message.record)
        .then(() => child.connected && child.send({ type: 'ack', seq }))
    } else if (message.type === 'done') {
      if (message.incomplete) this.incomplete ??= message.incomplete
      const resolve = this.waiting.get(message.id)
      this.waiting.delete(message.id)
      resolve?.(message)
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
    if (slot) this.children.get(slot)?.send({ type: 'reply', request })
  }

  /** @returns {Promise<object>} sends a step to a child and waits for its outcome */
  command(slot, step) {
    const id = ++this.commands
    return new Promise((resolve) => {
      this.waiting.set(id, resolve)
      this.children.get(slot).send({ type: 'command', id, step })
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

  /** Performs one plan step across the processes it involves. */
  async perform(step) {
    const all = [...this.children.keys()]
    const judged = this.config.processes
      .filter((p) => p.role === 'subject' || p.role === 'seed')
      .map((p) => p.slot)
    if (step.type === 'periodic') return this.sampleAll([...all, SUPERVISOR], step)
    if (step.type === 'quiet') return this.sampleAll(judged, step)
    if (step.type === 'seed-start' || step.type === 'seed-clear') return this.send(['p13'], step)
    if (step.type === 'disconnect') return this.teardown(step)
    if (step.shape === 'traffic') {
      await this.shape(step, 'start')
      return this.send(all, { type: 'active', id: step.id })
    }
    if (step.type === 'idle') {
      await this.shape(step, 'start')
      await this.send(all, step)
      await this.clock.sleepUntil(step.until, this.scheduler.controller.signal)
      if (step.until < this.runEndAt && !this.scheduler.aborted)
        await this.send(all, { ...step, type: 'active' })
      return this.shape(step, 'end')
    }
    await this.shape(step, 'start')
    const participants = [...new Set(step.participants.map((k) => this.processOf(k)))]
    if (step.shape === 'broker-outage') await this.outage(step, participants)
    else if (step.shape === 'regex-churn' || step.shape === 'topic-recreation')
      await this.churnTopic(step, participants)
    else {
      const drain = step.shape === 'drain'
      await this.send(
        drain ? [...new Set([...participants, ...this.consumers()])] : participants,
        step
      )
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
    await this.shape(step, 'end')
  }

  /**
   * The end of the run: producers are already idle; every consumer drains and commits, every live
   * route gets its terminal scan and seal, then every process disconnects.
   * @param {object} step the disconnect step
   * @returns {Promise<void>}
   */
  async teardown(step) {
    const all = [...this.children.keys()]
    await this.send(all, { type: 'idle', id: 'final-idle' }).catch(() => {})
    await this.send(this.consumers(), { type: 'drain', id: 'final-drain' }).catch(() => {})
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
    await this.send(all, step)
  }

  /**
   * A regex execution creates a fresh topic and its routes, lets every applicable client accept
   * it, offers probe ids, drains, scans and seals it, deletes it and lets the clients retire it. A
   * recreation drains, scans and seals the old incarnation, deletes it, creates the same name
   * again with new routes and probes the new one.
   */
  async churnTopic(step, participants) {
    const settle = () =>
      this.clock.sleepUntil(this.clock.now() + 3 * this.config.timeouts.metadataMaxAge)
    const producer = this.processOf('producer-idempotent')
    const probe = (topic) =>
      this.send([producer], {
        type: 'probe',
        id: `${step.id}.probe`,
        execution: step.id,
        topic,
        partitions: 2,
        count: 10,
      })
    const drain = () => this.send(participants, { type: 'drain', id: `${step.id}.drain` })
    if (step.shape === 'regex-churn') {
      const topic = `${this.config.topics.regex}-${step.id}`
      await this.createTopic(topic, 2, 'regex', step.id)
      await settle()
      await this.send(participants, { ...step, topicName: topic })
      await probe(topic)
      await settle()
      await drain()
      await this.seal(topic, step.id)
      await this.deleteTopic(topic, step.id)
      await this.send(participants, { ...step, type: 'retired', topicName: topic })
      return
    }
    const topic = this.config.topics.recreate[step.topic]
    await probe(topic)
    await settle()
    await drain()
    await this.seal(topic, step.id)
    await this.send(participants, { ...step, topicName: topic })
    await this.deleteTopic(topic, step.id)
    await settle()
    await this.createTopic(topic, 2, 'recreate', step.id)
    await settle()
    await probe(topic)
    await this.send(participants, { ...step, type: 'recreated', topicName: topic })
  }

  /**
   * Stops and restarts one broker as the step says, recording the supervisor receipts and the
   * broker state (down, ready, in-sync) from raw metadata, then asks every kind for its own
   * recovery witness.
   */
  async outage(step, participants) {
    const service = `kafka${step.node}`
    const docker = (args) =>
      new Promise((resolve, reject) =>
        execFile('docker', args, { env: process.env }, (e, stdout) =>
          e ? reject(e) : resolve(stdout.trim())
        )
      )
    const container = await docker(['compose', 'ps', '-aq', service]).catch(() => '')
    const pid = async () =>
      Number(await docker(['inspect', '-f', '{{.State.Pid}}', container]).catch(() => 0))
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
      try {
        await docker(['compose', ...args])
        await this.emit({ ...base, phase: 'end', result: 'ok', errorType: null })
      } catch (e) {
        await this.emit({ ...base, phase: 'end', result: 'error', errorType: e.name ?? 'Error' })
      }
      return operation
    }
    await this.send(participants, { ...step, type: 'broker-outage' }).catch(() => {})
    const stopped = await op(
      'broker-stop',
      step.stop === 'clean' ? ['stop', service] : ['kill', '-s', 'SIGKILL', service]
    )
    await this.brokerState(step, 'down', await pid(), container, stopped)
    await this.clock.sleepUntil(this.clock.now() + step.downMs)
    const started = await op('broker-start', ['start', service])
    const ready = async () =>
      (await this.admin.describeCluster().catch(() => ({ brokers: [] }))).brokers.length === 3
    while (!(await ready()) && this.clock.now() < step.deadline)
      await this.clock.sleepUntil(this.clock.now() + 500)
    await this.brokerState(step, 'ready', await pid(), container, started)
    const inSync = async () => (await this.partitionStates()).every((p) => p.isr.length === 3)
    while (!(await inSync()) && this.clock.now() < step.deadline)
      await this.clock.sleepUntil(this.clock.now() + 500)
    await this.brokerState(step, 'isr', await pid(), container, started)
    const replies = await Promise.all(
      participants.map((slot) => this.command(slot, { ...step, type: 'recovered' }))
    )
    replies.forEach((r, i) => {
      if (!r.reply?.ok)
        this.collector.note(`${participants[i]} has no recovery witness after ${step.id}`)
    })
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

  /** Emits one broker-state record. */
  async brokerState(step, event, pid, container, cause) {
    const { brokers } = await this.admin.describeCluster().catch(() => ({ brokers: [] }))
    await this.emit({
      kind: 'broker-state',
      execution: step.id,
      node: step.node,
      pid,
      container: container || 'unknown-container',
      event,
      members: brokers.map((b) => b.nodeId).sort((a, b) => a - b),
      partitions: (await this.partitionStates()).filter((p) => p.key.topicId),
      cause,
    })
  }

  /** @param {object} step @param {'start'|'end'} phase emits the shape boundary record */
  shape(step, phase) {
    return this.emit({
      kind: 'shape',
      execution: step.id,
      phase,
      shape: step.shape,
      participants: step.participants ?? [],
      operationCount: 0,
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
 * The outer handover after the supervisor has exited: appends the parent-observed supervisor exit
 * as the `outer` source, copies each supplied companion into the artifact, hashes and fsyncs every
 * file and writes `header.json`.
 * @param {{ out: string, status: number, companions?: { name: string, file: string,
 *   runUrl: string|null }[] }} options the artifact, the shell's wait status and the companions
 * @returns {object} the header written
 */
function finalize({ out, status, companions = [] }) {
  const run = JSON.parse(fs.readFileSync(path.join(out, 'supervisor.json'), 'utf8'))
  const clock = new MonotonicClock(run.originMs)
  const observedAt = clock.now()
  const signal =
    status > 128
      ? (Object.keys(os.constants.signals).find((s) => os.constants.signals[s] === status - 128) ??
        null)
      : null
  const code = signal ? null : status
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
    signal,
    spawnRecord: `${SUPERVISOR}.0`,
  }
  const problems = validateRecord(exit)
  if (problems.length > 0) throw new Error(problems.join('; '))
  fs.appendFileSync(streamFile, JSON.stringify(exit) + '\n')
  const fd = fs.openSync(streamFile, 'r+')
  fs.fsyncSync(fd)
  fs.closeSync(fd)
  const companionEntries = companions.map(({ name, file, runUrl }) => {
    const bytes = fs.readFileSync(file)
    const rel = `companions/${name}${path.extname(file)}`
    fs.mkdirSync(path.join(out, 'companions'), { recursive: true })
    fs.writeFileSync(path.join(out, rel), bytes)
    return { name, treeSha: run.treeSha, file: rel, sha256: sha256(bytes), runUrl }
  })
  const files = []
  const walk = (dir) => {
    for (const entry of fs.readdirSync(path.join(out, dir), { withFileTypes: true })) {
      const rel = dir ? `${dir}/${entry.name}` : entry.name
      if (entry.isDirectory()) walk(rel)
      else if (entry.isFile() && rel !== 'header.json') {
        const bytes = fs.readFileSync(path.join(out, rel))
        files.push({ file: rel, bytes: bytes.length, sha256: sha256(bytes) })
      }
    }
  }
  walk('')
  const stream = files.find((f) => f.file === 'stream.ndjson')
  const preflight = JSON.parse(fs.readFileSync(path.join(out, 'preflight.json'), 'utf8'))
  let records = 0
  const bytes = fs.readFileSync(streamFile)
  for (let i = bytes.indexOf(0x0a); i !== -1; i = bytes.indexOf(0x0a, i + 1)) records++
  const header = {
    schema: 3,
    mode: run.mode,
    runId: run.runId,
    treeSha: run.treeSha,
    node: process.version,
    v8: process.versions.v8,
    platform: process.platform,
    files,
    clock: { kind: 'linux-monotonic', originMs: run.originMs, bootId: run.bootId },
    window: {
      runStartAt: run.runStartAt,
      runEndAt: run.runEndAt,
      observedEndAt: run.observedEndAt,
      requestedMs: run.requestedMs,
    },
    host: preflight.host,
    broker: preflight.broker,
    exit: { supervisorProcess: SUPERVISOR, code, signal, observedAt },
    stream: { file: 'stream.ndjson', bytes: stream.bytes, records, sha256: stream.sha256 },
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
  if (options.command === 'finalize') finalize(options)
  else if (options.command === 'cleanup') await cleanup(options)
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

module.exports = { parseArgs, buildConfig, recipientGroups, finalize, summarize, Supervisor }
