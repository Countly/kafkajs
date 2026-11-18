'use strict'
/**
 * Final soak workers: the runtime of each persistent child process and the workload of its role.
 * Subjects and the seed act through public client calls only; the reader, the telemetry decoder and
 * the supervisor's own clients are observers that join no judged group. Every record carries the
 * actual ids, offsets and topic ids the product and the broker report; a value that cannot be read
 * is reported as a missing source, never invented.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { AsyncLocalStorage } = require('node:async_hooks')
const { valueOf, MonotonicClock } = require('./finalSoakScheduler')
const { SUPERVISOR, OUTER, SourceChannel, Sampler, chunk } = require('./finalSoakCollector')
const { PassiveObserver, SocketRegistry, normalizeTopicId } = require('./finalSoakObserver')

/** Bytes after which a raw wire segment file is closed and the next one started. */
const WIRE_SEGMENT_BYTES = 64 * 1024 * 1024

/** @param {string|Buffer} data @returns {string} lowercase hex SHA-256 */
const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex')

/** @returns {string} this boot's id, read from the kernel */
const bootId = () => fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim()

/**
 * @param {Buffer|null} key @param {Buffer|null} value @param {Record<string, Buffer>} headers
 * @returns {string} the payload hash over length-prefixed bytes; null is encoded as length -1
 */
const payloadSha256 = (key, value, headers) => {
  const hash = crypto.createHash('sha256')
  const part = (bytes) => {
    const length = Buffer.alloc(4)
    length.writeInt32BE(bytes == null ? -1 : bytes.length)
    hash.update(length)
    if (bytes != null) hash.update(bytes)
  }
  part(key)
  part(value)
  const names = Object.keys(headers ?? {})
  part(Buffer.from(String(names.length)))
  for (const name of names) {
    part(Buffer.from(name))
    part(headers[name] == null ? null : Buffer.from(headers[name]))
  }
  return hash.digest('hex')
}

/**
 * Builds an offered record of exactly the matrix value width. The value starts with
 * `runId|id|transaction|`, so the reader and handlers recover the id and transaction from the
 * bytes they receive; the payload hash covers key, value and headers.
 * @param {{ runId: string, slot: string, seq: number, valueBytes: number,
 *   transaction?: string|null }} options
 * @returns {{ id: string, key: Buffer, value: Buffer, headers: object, payloadSha256: string }}
 */
const offeredRecord = ({ runId, slot, seq, valueBytes, transaction = null }) => {
  const id = `${slot}.${seq}`
  const value = Buffer.alloc(valueBytes, 0x2e)
  value.write(`${runId}|${id}|${transaction ?? ''}|`, 'ascii')
  const key = Buffer.from(id)
  return { id, key, value, headers: {}, payloadSha256: payloadSha256(key, value, {}) }
}

/**
 * @param {Buffer|null} value a received value
 * @param {string} runId the run's id
 * @returns {{ id: string, transaction: string|null }|null} what an offered value carries, or null
 *   for a value this run did not offer
 */
const parseValue = (value, runId) => {
  if (!value) return null
  const [run, id, transaction] = value.toString('ascii', 0, 160).split('|')
  return run === runId && id ? { id, transaction: transaction || null } : null
}

/**
 * Appends raw exchanges to rotating segment files in the artifact, one JSON line each, and returns
 * where each line lives and its hash. Each segment is written once and never copied.
 */
class WireLog {
  /** @param {{ dir: string, prefix: string }} options artifact directory and file prefix */
  constructor({ dir, prefix }) {
    Object.assign(this, { dir, prefix })
    this.segment = 0
    this.bytes = 0
    fs.mkdirSync(path.join(dir, 'raw'), { recursive: true })
  }

  /**
   * @param {object} exchange `{ apiKey, request: Buffer, response: Buffer }` and any extra fields
   * @returns {{ file: string, sha256: string }}
   */
  append({ request, response, ...rest }) {
    const line =
      JSON.stringify({
        ...rest,
        request: request.toString('hex'),
        response: response.toString('hex'),
      }) + '\n'
    if (this.bytes + line.length > WIRE_SEGMENT_BYTES) {
      this.segment++
      this.bytes = 0
    }
    const file = `raw/${this.prefix}.${this.segment}.ndjson`
    fs.appendFileSync(path.join(this.dir, file), line)
    this.bytes += line.length
    return { file, sha256: sha256(line) }
  }
}

/**
 * One actual client in a child process: its construct record, its public calls with begin and end
 * records, and the async context that attributes everything it creates to it.
 */
class ClientRig {
  /**
   * @param {object} options
   * @param {ChildRuntime} options.runtime the hosting process
   * @param {object} options.slot the config client slot
   * @param {number} options.incarnation 1 for the first actual object of the slot
   */
  constructor({ runtime, slot, incarnation }) {
    this.runtime = runtime
    this.slot = slot
    this.id = `${slot.slot}.${incarnation}`
    this.calls = 0
    this.object = null
    this.kafka = null
  }

  /**
   * Constructs the actual client inside its own async context, with a socket factory that counts
   * its sockets (and captures share wire frames for a share client).
   * @param {(kafka: object) => object} factory makes the client from its own Kafka instance
   * @param {object|null} [capture] wire capture options for `SocketRegistry#factory`
   * @returns {Promise<object>} the actual client
   */
  async construct(factory, capture = null) {
    const { Kafka, logLevel } = require('../../index')
    const { runtime, slot } = this
    this.kafka = runtime.context.run(
      this.id,
      () =>
        new Kafka({
          clientId: this.id,
          brokers: runtime.config.brokers,
          logLevel: logLevel.NOTHING,
          socketFactory: runtime.sockets.factory(this.id, capture),
        })
    )
    this.object = runtime.context.run(this.id, () => factory(this.kafka))
    await runtime.emit({
      kind: 'client',
      recordId: runtime.nextId(),
      event: 'construct',
      client: this.id,
      slot: slot.slot,
      clientKind: slot.clientKind,
      process: runtime.slot,
      arm: slot.arm,
      controlId: slot.controlId,
      purpose: slot.purpose,
      group: slot.group,
      groupInstanceId: slot.groupInstanceId,
      standing: slot.standing,
      telemetryEnabled: slot.telemetryEnabled,
      configSha256: runtime.configSha256,
    })
    return this.object
  }

  /**
   * Makes one public call with its begin and end records.
   * @param {'connect'|'run'|'stop'|'disconnect'} event
   * @param {() => Promise<unknown>} work the public call
   * @param {string|null} [execution]
   * @returns {Promise<unknown>} the call's result
   * @throws {Error} the call's error, after its end record
   */
  async call(event, work, execution = null) {
    const call = `${this.id}.k${++this.calls}`
    const base = { kind: 'client', event, client: this.id, call, execution }
    const { runtime } = this
    await runtime.emit({
      ...base,
      recordId: runtime.nextId(),
      phase: 'begin',
      result: null,
      errorType: null,
    })
    try {
      const result = await runtime.context.run(this.id, work)
      await runtime.emit({
        ...base,
        recordId: runtime.nextId(),
        phase: 'end',
        result: 'ok',
        errorType: null,
      })
      return result
    } catch (e) {
      await runtime.emit({
        ...base,
        recordId: runtime.nextId(),
        phase: 'end',
        result: 'error',
        errorType: e.name ?? 'Error',
      })
      throw e
    }
  }

  /**
   * Disconnects the client with its call records, then drops what the passive observer kept for
   * it, so history of retired clients never accumulates in a long-lived process.
   * @param {string|null} [execution]
   * @returns {Promise<void>}
   * @throws {Error} the disconnect's error, after the observer's state is dropped
   */
  async retire(execution = null) {
    try {
      await this.call('disconnect', () => this.object.disconnect(), execution)
    } finally {
      this.runtime.observer.forget(this.id)
    }
  }
}

/**
 * One persistent child process: its stream source, sampler, passive observer, socket registry and
 * resource roles, and the workload of its role. It exits naturally after the supervisor's
 * `disconnect` command: every client disconnects, the final sample is taken, the IPC channel closes
 * and nothing is left to run.
 */
class ChildRuntime {
  /**
   * @param {object} options
   * @param {string} options.slot this process slot
   * @param {object} options.config the run config
   * @param {object} options.matrix the reviewed matrix
   * @param {number} options.originMs the run's clock origin
   * @param {string} options.out the artifact directory, for raw files
   * @param {(message: object, done: () => void) => void} options.send IPC send with acknowledgement
   * @param {(message: object) => void} [options.notify] one-way message to the supervisor
   */
  constructor({ slot, config, matrix, originMs, out, send, notify = () => {} }) {
    const { ResourceRoles } = require('../../testHelpers/resourceProbe')
    Object.assign(this, { slot, config, matrix, out, notify })
    this.configSha256 = sha256(JSON.stringify(config))
    this.clock = new MonotonicClock(originMs)
    this.context = new AsyncLocalStorage()
    this.commits = new AsyncLocalStorage()
    this.channel = new SourceChannel({ source: slot, clock: this.clock, send })
    this.sampler = new Sampler({ clock: this.clock, process: slot })
    this.roles = new ResourceRoles({
      types: ['Timeout', 'TCPWRAP', 'PIPEWRAP', 'MESSAGEPORT'],
    }).start()
    this.process = config.processes.find((p) => p.slot === slot)
    this.slots = config.clients.filter((c) => c.process === slot)
    this.topicIds = new Map()
    this.lifetimes = new Map()
    this.revisions = new Map()
    this.ids = 0
    this.cuts = 0
    this.sockets = new SocketRegistry()
    this.wire = new WireLog({ dir: out, prefix: `${slot}.share` })
    this.observer = new PassiveObserver({
      context: this.context,
      commits: this.commits,
      clock: this.clock,
      topicIds: (topic) => this.topicIds.get(topic) ?? null,
      onEvent: (event) => this.observed(event),
    })
    this.observer.lives = (client, topic, topicId) =>
      this.lifetimes.get(`${client}/${topic}`)?.endsWith(`/${topicId}`)
        ? this.lifetimes.get(`${client}/${topic}`)
        : null
    this.observer.install()
    this.worker = createWorker(this)
  }

  /** @returns {string} a fresh record id of this source */
  nextId() {
    return `${this.slot}.${this.ids++}`
  }

  /** @param {object} record @returns {Promise<boolean>} */
  emit(record) {
    return this.channel.emit(record)
  }

  /** @param {string} message @returns {Promise<boolean>} a diagnostic note */
  diagnose(message) {
    return this.emit({ kind: 'diagnostic', recordId: this.nextId(), message })
  }

  /**
   * Records a failure of this process's own evidence or workload and tells the supervisor, which
   * marks the run failed and stops admitting work.
   * @param {string} message @returns {Promise<boolean>}
   */
  problem(message) {
    this.notify({ type: 'problem', message })
    return this.diagnose(`problem: ${message}`)
  }

  /**
   * Reports every observer hook that failed since the last call, by hook identity: a hook that
   * could not record its source leaves the evidence incomplete.
   * @returns {Promise<void>}
   */
  async reportObserverFailures() {
    for (const failure of this.observer.drainFailures()) await this.problem(failure)
  }

  /** @param {string} client @returns {object|undefined} the client's config slot */
  slotOf(client) {
    return this.config.clients.find((c) => client?.startsWith(`${c.slot}.`))
  }

  /**
   * Turns one observer event into its record(s).
   * @param {object} event a `PassiveObserver` event
   * @returns {void}
   */
  observed(event) {
    const id = () => this.nextId()
    switch (event.type) {
      case 'boundary':
      case 'round':
      case 'ticket':
        this.emit({ kind: event.type, recordId: id(), ...event.record })
        if (event.type === 'round') this.worker.onRound?.(event)
        if (event.type === 'boundary') this.worker.onBoundary?.(event.record)
        return
      case 'subscription':
        return this.published(event.group, event.names)
      case 'commit-stage':
        return this.worker.onCommitStage?.(event)
      case 'share-acquire':
        if (!event.wire) {
          this.diagnose(`missing-source share wire for ${event.acquisition}`)
          return
        }
        for (const range of event.acquired)
          this.emit({
            kind: 'share-acquire',
            recordId: id(),
            acquisition: event.acquisition,
            client: event.client,
            group: this.slotOf(event.client).group,
            key: event.key,
            from: range.from,
            to: range.to,
            deliveryCount: range.deliveryCount,
            receivedAt: event.receivedAt,
            lockMs: event.lockMs,
            wireFile: event.wire.file,
            wireSha256: event.wire.sha256,
          })
        return
      case 'share-settle':
        if (!event.wire) {
          this.diagnose(`missing-source share ack wire for ${event.acquisition}`)
          return
        }
        for (const phase of ['request', 'response'])
          for (const { offset, type } of event.offsets)
            this.emit({
              kind: 'share-settle',
              recordId: id(),
              acquisition: event.acquisition,
              client: event.client,
              group: this.slotOf(event.client).group,
              key: event.key,
              offset,
              type,
              phase,
              errorCode: phase === 'request' ? null : event.errorCode,
              wireFile: event.wire.file,
              wireSha256: event.wire.sha256,
            })
        return
      case 'telemetry-subscription':
        this.notify({ type: 'telemetry-subscription', ...event })
        this.diagnose(`telemetry subscription ${event.subscriptionId} for ${event.client}`)
    }
  }

  /**
   * Emits an accepted subscription publication and keeps each accepted `(client, topic, id)`
   * lifetime: it starts at its first publication and ends when the name leaves the subscription or
   * its id changes. Names without a known id are reported, not published.
   * @param {object} group the publishing consumer group
   * @param {string[]} topics its accepted names
   * @returns {void}
   */
  published(group, topics) {
    const client = this.observer.clientOf(group)
    if (!client) return
    const recordId = this.nextId()
    const run = this.observer.runOf(group)
    const names = []
    for (const topic of topics) {
      const topicId = this.observer.topicIdOf(topic, group)
      if (topicId) names.push({ topic, topicId })
      else this.diagnose(`missing-source topic id for accepted ${topic}`)
    }
    for (const { topic, topicId } of names) {
      const key = `${client}/${topic}`
      if (!this.lifetimes.get(key)?.endsWith(`/${topicId}`))
        this.lifetimes.set(key, `${recordId}/${topicId}`)
    }
    for (const key of [...this.lifetimes.keys()])
      if (key.startsWith(`${client}/`) && !names.some((n) => key === `${client}/${n.topic}`))
        this.lifetimes.delete(key)
    const revision = (this.revisions.get(client) ?? 0) + 1
    this.revisions.set(client, revision)
    const kind = this.slotOf(client)?.clientKind
    const regex = this.matrix.topology.topics
      .find((t) => t.role === 'regex')
      .recipients.includes(kind)
    const mode = kind === 'consumer-dynamic' ? 'broker-regex' : regex ? 'client-pattern' : 'literal'
    this.emit({
      kind: 'subscription',
      recordId,
      client,
      revision,
      run,
      names,
      mode,
      pattern: mode === 'literal' ? null : patternOf(this.config, kind).source,
      cause: recordId,
    })
  }

  /** @returns {Promise<void>} the start handshake and the preconstruction baseline */
  async start() {
    await this.emit({
      kind: 'process',
      recordId: this.nextId(),
      event: 'start',
      process: this.slot,
      pid: process.pid,
      parent: this.slot === SUPERVISOR ? OUTER : SUPERVISOR,
      arm: this.process.arm,
      controlId: this.process.controlId,
      execArgv: process.execArgv.filter((a) => a.startsWith('--')),
      nodeOptions: (process.env.NODE_OPTIONS ?? '').split(/\s+/).filter(Boolean),
      node: process.version,
      v8: process.versions.v8,
      bootId: bootId(),
    })
    for (const missing of this.observer.missing()) await this.diagnose(`missing-source ${missing}`)
    await this.sample({ role: 'baseline' })
  }

  /**
   * Takes a sample after any sample of this process still running, so two readings never overlap
   * and each one's two forced collections follow the previous reading's.
   * @param {object} options sample role, slot, boundary and reading
   * @returns {Promise<void>}
   */
  sample(options) {
    const next = (this.sampling ?? Promise.resolve()).then(() => this.take(options))
    this.sampling = next.catch(() => {})
    return next
  }

  /**
   * Takes one sample: the synchronous inventory cut (every section the config requires for this
   * process and its live clients), then the sampler's two forced collections and memory reading.
   * @param {object} options sample role, slot, boundary and reading
   * @returns {Promise<void>}
   */
  async take(options) {
    const sent = []
    const notes = []
    let clients = []
    const cut = () => {
      const inventory = `${this.slot}.i${this.cuts++}`
      const at = this.clock.now()
      clients = this.worker.rigs().map((r) => r.id)
      const parts = []
      for (const [section, entries] of Object.entries(
        this.observer.processSections(this.roles, this.sockets, at)
      ))
        parts.push([null, section, entries])
      for (const rig of this.worker.rigs()) {
        const kind = this.matrix.clientKinds[rig.slot.clientKind]
        if (rig.slot.purpose === 'observer') continue
        if (kind.inventory === 'ordinary') {
          const { sections, missingSources } = this.observer.ordinarySections(rig.id)
          for (const [section, entries] of Object.entries(sections))
            parts.push([rig.id, section, entries])
          for (const source of missingSources) notes.push(`missing-source ${source} for ${rig.id}`)
        } else if (kind.inventory === 'share') {
          const { entry, mismatches } = this.observer.shareSections(rig.id)
          parts.push([rig.id, 'share-state', [entry]])
          notes.push(...mismatches.map((m) => `share-state mismatch ${m}`))
        }
      }
      for (const [client, section, entries] of parts) {
        const pieces = chunk(entries, valueOf(this.matrix.host.maxInventoryLineBytes) - 1024)
        for (const [i, piece] of pieces.entries())
          sent.push(
            this.emit({
              kind: 'inventory',
              recordId: this.nextId(),
              at,
              inventory,
              process: this.slot,
              client,
              section,
              chunk: i,
              chunks: pieces.length,
              entries: piece,
            })
          )
      }
      return inventory
    }
    const reading = await this.sampler.read({ ...options, cut })
    await Promise.all(sent)
    await this.reportObserverFailures()
    for (const note of notes) await this.diagnose(note)
    if (this.observer.unknownTopicIds > 0) {
      await this.diagnose(`missing-source topic id for ${this.observer.unknownTopicIds} entries`)
      this.observer.unknownTopicIds = 0
    }
    await this.emit({
      kind: 'sample',
      recordId: this.nextId(),
      process: this.slot,
      clients,
      ...reading,
    })
  }

  /**
   * Handles one supervisor command and replies with its outcome.
   * @param {{ id: number, step: object }} command
   * @returns {Promise<void>}
   */
  async handle({ id, step }) {
    let error = null
    let reply = null
    try {
      if (step.type === 'periodic') {
        await this.sample({ role: 'periodic', slot: step.slot })
        reply = (await this.worker.onTick?.(step)) ?? null
      } else if (step.type === 'quiet')
        await this.sample({ role: 'quiet', boundary: step.boundary, reading: step.reading })
      else if (step.type === 'closed')
        await this.sample({ role: 'closed', boundary: step.boundary })
      else if (step.type === 'topic-ids')
        for (const [topic, topicId] of step.topicIds) this.topicIds.set(topic, topicId)
      else if (step.type === 'disconnect') await this.stop()
      else reply = (await this.worker.perform(step)) ?? null
    } catch (e) {
      error = e.message
      await this.diagnose(`step ${step.id} failed: ${e.message}`)
    }
    process.send({ type: 'done', id, error, reply, incomplete: this.channel.incomplete })
  }

  /** @returns {Promise<void>} disconnects every client, takes the final sample and lets the process end */
  async stop() {
    await this.worker.disconnect()
    await this.sample({ role: 'final' })
    this.roles.stop()
    this.observer.restore()
  }
}

/**
 * The client factory and options the driver constructs one config client slot's actual client
 * with. Classic consumers take the run's session and heartbeat timeouts; a `consumer`-protocol
 * (KIP-848) member takes neither, since the broker owns them and the client rejects both.
 * @param {object} config the run config
 * @param {object} matrix the reviewed matrix
 * @param {object} slot a non-observer config client slot
 * @returns {{ factory: 'producer'|'consumer'|'shareConsumer'|'admin', options: object }}
 */
function clientOptions(config, matrix, slot) {
  const kind = matrix.clientKinds[slot.clientKind]
  const t = config.timeouts
  const telemetry = slot.telemetryEnabled ? { enableMetricsPush: true } : {}
  if (kind.role === 'admin') return { factory: 'admin', options: {} }
  if (kind.role === 'share')
    return {
      factory: 'shareConsumer',
      options: { groupId: slot.group, metadataMaxAge: t.metadataMaxAge },
    }
  if (kind.role === 'producer')
    return {
      factory: 'producer',
      options: {
        idempotent: true,
        maxInFlightRequests: 1,
        allowAutoTopicCreation: false,
        ...(slot.clientKind === 'producer-transactional'
          ? { transactionalId: `t-${config.runId}-${slot.slot}` }
          : {}),
        ...telemetry,
      },
    }
  const { PartitionAssigners } = require('../../index')
  const assigners = {
    cooperative: PartitionAssigners.cooperativeSticky,
    eager: PartitionAssigners.roundRobin,
  }
  return {
    factory: 'consumer',
    options: {
      groupId: slot.group,
      ...(slot.groupInstanceId ? { groupInstanceId: slot.groupInstanceId } : {}),
      ...(kind.protocol === 'consumer'
        ? { groupProtocol: 'consumer' }
        : {
            partitionAssigners: [assigners[kind.protocol]],
            sessionTimeout: t.sessionTimeout,
            heartbeatInterval: t.heartbeatInterval,
          }),
      metadataMaxAge: t.metadataMaxAge,
      allowAutoTopicCreation: false,
      readUncommitted: false,
      ...telemetry,
    },
  }
}

/**
 * @param {object} config @param {string} kind
 * @returns {RegExp} the pattern a discovering kind subscribes with
 */
const patternOf = (config, kind) =>
  kind === 'consumer-dynamic'
    ? new RegExp(`^fs-${config.runId}-(main|txn|regex-.*|recreate-[01])$`)
    : new RegExp(`^fs-${config.runId}-regex-`)

/**
 * The workload of one process role. The base class owns the process's live clients and disconnects
 * them, newest first, at the end.
 */
class Worker {
  /** @param {ChildRuntime} runtime */
  constructor(runtime) {
    this.runtime = runtime
    this.standing = []
    this.extras = []
    this.incarnations = new Map()
    this.active = false
  }

  /** @returns {ClientRig[]} the live actual clients of this process */
  rigs() {
    return [...this.standing, ...this.extras]
  }

  /** @param {object} slot @returns {ClientRig} a new actual client of the slot */
  rig(slot) {
    const n = (this.incarnations.get(slot.slot) ?? 0) + 1
    this.incarnations.set(slot.slot, n)
    return new ClientRig({ runtime: this.runtime, slot, incarnation: n })
  }

  /**
   * Dispatches one step to the role's handler (`onMembershipChurn`, `onDrain`, ...).
   * @param {object} step @returns {Promise<unknown>} the handler's reply, if any
   */
  async perform(step) {
    if (step.type === 'start') return this.begin()
    if (step.type === 'idle') this.active = false
    if (step.type === 'active') this.active = true
    const what = step.type === 'execution' ? step.shape : step.type
    const name = `on${what.replace(/(^|-)(\w)/g, (_, __, c) => c.toUpperCase())}`
    return this[name]?.(step)
  }

  /** @returns {Promise<void>} starts the standing clients */
  async begin() {}

  /**
   * Disconnects every client, newest first. A failed disconnect is a problem of the run, never
   * only an end record.
   * @returns {Promise<void>}
   */
  async disconnect() {
    for (const rig of this.rigs().reverse()) {
      try {
        await rig.retire()
      } catch (e) {
        await this.runtime.problem(`disconnect of ${rig.id} failed: ${e.message}`)
      }
    }
    this.standing = []
    this.extras = []
  }

  /**
   * Polls until `test` holds or the deadline passes.
   * @param {() => boolean} test @param {number} deadline run-clock ms
   * @returns {Promise<boolean>} whether it held in time
   */
  async until(test, deadline) {
    const { clock } = this.runtime
    while (!test()) {
      if (clock.now() >= deadline) return false
      await clock.sleepUntil(Math.min(deadline, clock.now() + 250))
    }
    return true
  }
}

/** A producing subject: idempotent, transactional or telemetry-enabled. */
class ProducerWorker extends Worker {
  /** @returns {Promise<void>} */
  async begin() {
    const { runtime } = this
    const slot = runtime.slots.find((c) => c.purpose === 'subject')
    const rig = this.rig(slot)
    const transactional = slot.clientKind === 'producer-transactional'
    const { options } = clientOptions(runtime.config, runtime.matrix, slot)
    const producer = await rig.construct((kafka) => kafka.producer(options))
    await rig.call('connect', () => producer.connect())
    this.standing.push(rig)
    this.seq = 0
    this.transactions = 0
    this.current = null
    this.loops = new AbortController()
    const traffic = runtime.matrix.traffic
    const rate =
      slot.clientKind === 'telemetry-producer'
        ? valueOf(traffic.telemetryProducerRate)
        : valueOf(traffic.mainRate)
    this.loop = (transactional ? this.transactLoop(rig) : this.sendLoop(rig, rate)).catch((e) =>
      runtime.problem(`producer loop failed: ${e.message}`)
    )
  }

  /** @returns {boolean} whether the loops may start another round */
  running(rig) {
    return !this.loops.signal.aborted && this.standing.includes(rig)
  }

  /** @returns {Promise<void>} offers main records at the rate while active */
  async sendLoop(rig, rate) {
    const { clock, config } = this.runtime
    const every = 1000 / rate
    let next = clock.now()
    while (this.running(rig)) {
      next += every
      if (this.active) {
        this.current = this.offer(rig, config.topics.main, 0, null)
        await this.current
      }
      await clock.sleepUntil(next, this.loops.signal)
    }
  }

  /** @returns {Promise<void>} one transaction per period while active, every tenth aborted */
  async transactLoop(rig) {
    const { clock, matrix } = this.runtime
    let next = clock.now()
    while (this.running(rig)) {
      next += valueOf(matrix.traffic.transactionEveryMs)
      if (this.active) {
        this.current = this.transact(rig, ++this.transactions)
        await this.current
      }
      await clock.sleepUntil(next, this.loops.signal)
    }
  }

  /**
   * One whole transaction: its records, the decision and the confirmed outcome, never inferred.
   * @param {ClientRig} rig @param {number} n the transaction's number
   * @returns {Promise<void>}
   */
  async transact(rig, n) {
    const { config, matrix } = this.runtime
    const transaction = `${rig.id}.x${n}`
    const decision = n % valueOf(matrix.traffic.abortEvery) === 0 ? 'abort' : 'commit'
    let outcome = decision === 'commit' ? 'committed' : 'aborted'
    let errorType = null
    let txn = null
    try {
      txn = await rig.object.transaction()
      for (let i = 0; i < valueOf(matrix.traffic.transactionRecords); i++)
        await this.offer(rig, config.topics.transactions, 0, transaction, txn)
      await (decision === 'commit' ? txn.commit() : txn.abort())
    } catch (e) {
      outcome = 'unknown'
      errorType = e.name ?? 'Error'
      // a transaction left open would block every later one of this producer
      if (txn?.isActive?.()) await txn.abort().catch(() => {})
    }
    this.settled = (this.settled ?? 0) + 1
    this.lastOutcome = outcome
    await this.runtime.emit({
      kind: 'identity',
      recordId: this.runtime.nextId(),
      stage: 'transaction',
      client: rig.id,
      transaction,
      decision,
      outcome,
      call: `${transaction}.${decision}`,
      errorType,
    })
  }

  /** @returns {Promise<void>} answers an idle command once the send or transaction in flight settles */
  async onIdle() {
    await this.current
  }

  /**
   * Stops the loops and waits until each has ended, so no send or transaction is in flight and no
   * loop competes with the close for the event loop, then disconnects.
   * @returns {Promise<void>}
   */
  async disconnect() {
    this.loops?.abort()
    await this.loop
    return super.disconnect()
  }

  /**
   * Offers one logical id once: its offered record, the send, then its settled outcome. An id
   * whose topic id is not yet known is not offered.
   * @returns {Promise<'acknowledged'|'unknown'|'skipped'>} the send's outcome
   */
  async offer(rig, topic, partition, transaction, sender = rig.object, execution = null) {
    const { runtime } = this
    const topicId = runtime.topicIds.get(topic)
    if (!topicId) {
      await runtime.diagnose(`missing-source topic id for offer to ${topic}`)
      return 'skipped'
    }
    const record = offeredRecord({
      runId: runtime.config.runId,
      slot: rig.slot.slot,
      seq: this.seq++,
      valueBytes: valueOf(runtime.matrix.traffic.valueBytes),
      transaction,
    })
    const key = { topic, topicId, partition }
    await runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      execution,
      stage: 'offered',
      client: rig.id,
      id: record.id,
      key,
      payloadSha256: record.payloadSha256,
      bytes: record.value.length,
      transaction,
    })
    let ack
    try {
      const [meta] = await sender.send({
        topic,
        acks: -1,
        messages: [{ key: record.key, value: record.value, partition }],
      })
      const offset = meta?.baseOffset ?? null
      ack = { outcome: offset === null ? 'unknown' : 'acknowledged', offset, errorType: null }
    } catch (e) {
      ack = { outcome: 'unknown', offset: null, errorType: e.name ?? 'Error' }
    }
    await runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      execution,
      stage: 'ack',
      client: rig.id,
      id: record.id,
      key,
      ...ack,
    })
    return ack.outcome
  }

  /**
   * Offers the probe ids an auxiliary execution asks for, and stops starting sends once the
   * execution's deadline has passed.
   * @param {{ execution: string, topic: string, partitions: number, count: number,
   *   deadline?: number }} step
   * @returns {Promise<{ offered: number, stopped: boolean }>}
   */
  async onProbe(step) {
    let offered = 0
    for (; offered < step.count; offered++) {
      if (this.runtime.clock.now() >= (step.deadline ?? Infinity)) return { offered, stopped: true }
      await this.offer(
        this.standing[0],
        step.topic,
        offered % step.partitions,
        null,
        undefined,
        step.execution
      )
    }
    return { offered, stopped: false }
  }

  /** @returns {Promise<{ ok: boolean }>} a post-recovery send the broker acknowledged */
  async onRecovered(step) {
    if (this.runtime.slots[0].clientKind === 'producer-transactional') {
      const before = this.settled
      const ok = await this.until(
        () => this.settled > before && this.lastOutcome !== 'unknown',
        step.deadline
      )
      return { ok }
    }
    const outcome = await this.offer(
      this.standing[0],
      this.runtime.config.topics.main,
      0,
      null,
      undefined,
      step.id
    )
    return { ok: outcome === 'acknowledged' }
  }
}

/**
 * An ordinary consumer subject (or the seed): explicit commits every hundred processed records and
 * at drains, handler entries with the authority captured from the original batch, replays derived
 * from repeated offsets under a new owner, pause waves with an extra member that must actually take
 * a paused partition, static restarts, and the seed's retained offset map.
 */
class ConsumerWorker extends Worker {
  /** @returns {Promise<void>} */
  async begin() {
    const slot = this.runtime.slots.find((c) => c.purpose === 'subject' || c.purpose === 'control')
    this.seeded = slot.purpose === 'control'
    this.retained = null
    this.progress = new Map()
    this.lastBoundary = new Map()
    const rig = await this.join(slot)
    this.standing.push(rig)
    if (
      !(await this.until(() => this.assignedKeys(rig).length > 0, this.runtime.clock.now() + 60000))
    )
      throw new Error(`${rig.id} was given no partition within 60000 ms`)
  }

  /** @param {ClientRig} rig @returns {object[]} the keys the member is assigned right now */
  assignedKeys(rig) {
    return this.runtime.observer.ordinarySections(rig.id).sections.assignment.map((a) => a.key)
  }

  /** @param {object} slot @returns {Promise<ClientRig>} a connected, running member of the slot */
  async join(slot, execution = null) {
    const { runtime } = this
    const rig = this.rig(slot)
    const { options } = clientOptions(runtime.config, runtime.matrix, slot)
    const consumer = await rig.construct((kafka) => kafka.consumer(options))
    rig.processed = new Map()
    rig.uncommitted = new Map()
    rig.committed = new Map()
    rig.authority = new Map()
    consumer.on(consumer.events.END_BATCH_PROCESS, ({ payload }) => this.onBatchEnd(rig, payload))
    await rig.call('connect', () => consumer.connect(), execution)
    await this.operation(rig, 'subscribe', [], execution, () =>
      consumer.subscribe({ topics: this.topicsFor(slot), fromBeginning: true })
    )
    await rig.call(
      'run',
      () =>
        consumer.run({ autoCommit: false, eachMessage: (payload) => this.handle(rig, payload) }),
      execution
    )
    return rig
  }

  /** @param {object} slot @returns {(string|RegExp)[]} the client's subscription */
  topicsFor(slot) {
    const { topics } = this.runtime.config
    if (this.seeded) return [topics.main]
    if (slot.clientKind === 'consumer-dynamic')
      return [patternOf(this.runtime.config, slot.clientKind)]
    const recipient = (role) =>
      this.runtime.matrix.topology.topics
        .find((t) => t.role === role)
        .recipients.includes(slot.clientKind)
    return [
      topics.main,
      topics.transactions,
      ...(recipient('recreate') ? topics.recreate : []),
      ...(recipient('regex') ? [patternOf(this.runtime.config, slot.clientKind)] : []),
    ]
  }

  /**
   * One handler call: the entry record before any await, with the authority captured from the
   * original batch, then the settled outcome, then a commit when the key reaches its threshold. A
   * repeated offset under a different authority is a replay and is recorded with both authorities.
   * @returns {Promise<void>}
   */
  async handle(rig, { topic, partition, message }) {
    const { runtime } = this
    const authority = runtime.observer.batchAuthority()
    const [group] = runtime.observer.mine(rig.id, 'ConsumerGroup')
    const topicId = runtime.observer.topicIdOf(topic, group)
    const handler = `${rig.id}.h${runtime.ids}`
    if (!topicId || !authority) {
      runtime.diagnose(`missing-source ${!topicId ? 'topic id' : 'batch authority'} at ${handler}`)
    } else {
      const key = { topic, topicId, partition }
      const parsed = parseValue(message.value, runtime.config.runId)
      this.replayCheck(rig, key, message.offset, authority)
      runtime.emit({
        kind: 'identity',
        recordId: runtime.nextId(),
        stage: 'handler-enter',
        client: rig.id,
        group: rig.slot.group,
        handler,
        id: parsed?.id ?? `foreign.${message.offset}`,
        key,
        offset: message.offset,
        payloadSha256: payloadSha256(message.key, message.value, message.headers),
        authority,
        acquisition: null,
        deliveryCount: null,
      })
      rig.authority.set(`${topic}|${partition}`, { key, authority })
    }
    if (this.retained && topic === runtime.config.topics.main)
      this.retained.set(`${partition}:${message.offset}`, String(message.offset))
    await runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      stage: 'handler-end',
      handler,
      outcome: 'processed',
      errorType: null,
    })
    const k = `${topic}|${partition}`
    rig.processed.set(k, (BigInt(message.offset) + 1n).toString())
    rig.uncommitted.set(k, (rig.uncommitted.get(k) ?? 0) + 1)
    if (rig.uncommitted.get(k) >= valueOf(runtime.matrix.traffic.commitEvery))
      await this.commit(rig, k)
  }

  /**
   * Records a replay when a key's offset arrives again below what an earlier owner of this group
   * processed: a new member, a restarted run or a new membership token.
   */
  replayCheck(rig, key, offset, authority) {
    const progressKey = `${rig.slot.group}/${key.topicId}/${key.partition}`
    const seen = this.progress.get(progressKey)
    const at = BigInt(offset)
    if (seen && at < seen.next) {
      const changed =
        seen.authority.client !== authority.client ||
        seen.authority.run !== authority.run ||
        seen.authority.token !== authority.token
      if (changed)
        this.runtime.emit({
          kind: 'replay',
          recordId: this.runtime.nextId(),
          replay: `${rig.id}.y${this.runtime.ids}`,
          group: rig.slot.group,
          key,
          from: String(at),
          to: String(seen.next),
          reason: this.outage ? 'outage' : 'rebalance',
          oldAuthority: seen.authority,
          newAuthority: authority,
          cause:
            this.lastBoundary.get(`${authority.client}/${key.topicId}/${key.partition}`) ??
            seen.cause,
        })
    }
    if (!seen || at + 1n > seen.next)
      this.progress.set(progressKey, {
        next: at + 1n,
        authority,
        cause: `${authority.client}.e${offset}`,
      })
  }

  /** @param {object} boundary a boundary record @returns {void} remembers the key's last boundary */
  onBoundary(boundary) {
    if (boundary.event === 'acquire')
      this.lastBoundary.set(
        `${boundary.client}/${boundary.key.topicId}/${boundary.key.partition}`,
        boundary.cause
      )
  }

  /** Asks the supervisor for an initial readback of every newly acquired key. */
  onRound(event) {
    const slot = this.runtime.slotOf(event.record.client)
    for (const key of event.acquired)
      this.runtime.request({
        type: 'readback',
        client: event.record.client,
        group: slot.group,
        commit: null,
        key,
      })
  }

  /**
   * Commits a key's processed next offset under the authority its last handler carried, inside the
   * commit store so the write and publish stages are recorded, then asks for the broker readback.
   * Only a commit that succeeded settles the offsets it covered; a failed one leaves them owed.
   * @param {ClientRig} rig @param {string} k `topic|partition`
   * @returns {Promise<void>}
   */
  async commit(rig, k) {
    const { runtime } = this
    const next = rig.processed.get(k)
    const captured = rig.authority.get(k)
    if (!next || !captured) return
    const owed = rig.uncommitted.get(k) ?? 0
    const { topic, partition } = { topic: captured.key.topic, partition: captured.key.partition }
    const commit = `${rig.id}.m${runtime.ids}`
    const base = {
      kind: 'identity',
      stage: 'commit',
      client: rig.id,
      group: rig.slot.group,
      commit,
      key: captured.key,
      next,
      authority: captured.authority,
    }
    await runtime.emit({
      ...base,
      recordId: runtime.nextId(),
      phase: 'begin',
      outcome: null,
      errorType: null,
    })
    let outcome = 'ok'
    let errorType = null
    try {
      await runtime.commits.run(base, () =>
        rig.object.commitOffsets([{ topic, partition, offset: next }])
      )
      rig.committed.set(k, next)
      rig.uncommitted.set(k, Math.max(0, (rig.uncommitted.get(k) ?? 0) - owed))
    } catch (e) {
      outcome =
        e.name === 'KafkaJSCommitFailedError' || e.name === 'KafkaJSStaleAssignmentError'
          ? 'refused'
          : 'unknown'
      errorType = e.name ?? 'Error'
    }
    await runtime.emit({ ...base, recordId: runtime.nextId(), phase: 'end', outcome, errorType })
    await runtime.request({
      type: 'readback',
      client: rig.id,
      group: rig.slot.group,
      commit,
      key: captured.key,
    })
  }

  /** Records the write and publish stages of the driver's own commits. */
  onCommitStage({ phase, commit, written }) {
    if (
      phase === 'publish' &&
      !written.some((w) => w.topic === commit.key.topic && w.partition === commit.key.partition)
    )
      return
    this.runtime.emit({
      ...commit,
      recordId: this.runtime.nextId(),
      phase,
      outcome: null,
      errorType: null,
    })
  }

  /**
   * One phase of one membership wave, its own pause-resume execution (`step.execution`). `leave`:
   * pause the standing member's assigned keys, join the extra, wait for the extra to take at least
   * one paused key, remove the extra and wait for the paused key to return; the supervisor then
   * takes the closed sample. `resume`: resume the keys paused in `leave`. The first wave of an
   * execution with a restart restarts the static extra with the same static id and a new client id.
   * @param {{ execution: string, wave: number, phase: 'leave'|'resume', restart?: string[],
   *   waveDeadline?: number }} step `waveDeadline` is the wave's scheduled end on the run clock
   * @returns {Promise<{ transfers: number }>} 1 when the wave observed a transfer
   */
  async onMembershipChurn(step) {
    const { runtime } = this
    const [standing] = this.standing
    const extraSlot = runtime.slots.find((c) => c.purpose === 'churn')
    const { execution } = step
    if (!standing || !extraSlot) return { transfers: 0 }
    if (step.phase === 'resume') {
      const keys = this.paused ?? []
      this.paused = null
      await this.operation(standing, 'resume', keys, execution, () =>
        standing.object.resume(byTopic(keys))
      )
      return { transfers: 0 }
    }
    const waveMs = runtime.matrix.schedule.executions.find(
      (e) => e.shape === 'membership-churn'
    ).waveMaxMs
    const deadline = (step.waveDeadline ?? runtime.clock.now() + waveMs) - 5000
    const keys = this.assignedKeys(standing)
    this.paused = keys
    await this.operation(standing, 'pause', keys, execution, () =>
      standing.object.pause(byTopic(keys))
    )
    this.extras.push(await this.join(extraSlot, execution))
    const held = (rig) =>
      this.assignedKeys(rig).some((a) =>
        keys.some((k) => k.topicId === a.topicId && k.partition === a.partition)
      )
    const transferred = await this.until(() => held(this.extras.at(-1)), deadline)
    if (!transferred) await runtime.diagnose(`${execution} saw no transfer of a paused partition`)
    if (step.wave === 0 && step.restart?.includes(extraSlot.clientKind)) {
      const old = this.extras.pop()
      await this.operation(old, 'restart', [], execution, () =>
        old.object.disconnect({ leaveGroup: false })
      )
      runtime.observer.forget(old.id)
      this.extras.push(await this.join(extraSlot, execution))
    }
    for (const rig of this.extras.splice(0)) await rig.retire(execution)
    await this.until(() => held(standing), deadline)
    return { transfers: transferred ? 1 : 0 }
  }

  /**
   * Waits until the client's accepted subscription includes a regex execution's new topic.
   * @param {{ topicName: string, deadline: number }} step
   * @returns {Promise<{ accepted: boolean }>}
   */
  async onRegexChurn(step) {
    return { accepted: await this.until(() => this.accepts(step.topicName), step.deadline) }
  }

  /** @returns {Promise<{ retired: boolean }>} waits until the deleted name left the subscription */
  async onRetired(step) {
    return { retired: await this.until(() => !this.accepts(step.topicName), step.deadline) }
  }

  /**
   * Before a recreation deletes the old incarnation: pauses the standing member's assigned keys of
   * the topic, the old pause witness the product must retire with the incarnation.
   * @returns {Promise<void>}
   */
  async onTopicRecreation(step) {
    const [rig] = this.standing
    const keys = this.assignedOf(rig, step.topicName)
    if (keys.length > 0)
      await this.operation(rig, 'pause', keys, step.id, () => rig.object.pause(byTopic(keys)))
  }

  /**
   * After a recreation: waits for the new incarnation to be assigned, then pauses and resumes it,
   * the new pause witness on a new state.
   * @returns {Promise<{ assigned: boolean }>}
   */
  async onRecreated(step) {
    const [rig] = this.standing
    const fresh = () => {
      const id = this.runtime.topicIds.get(step.topicName)
      return this.assignedOf(rig, step.topicName).filter((k) => k.topicId === id)
    }
    const assigned = await this.until(() => fresh().length > 0, step.deadline)
    const keys = fresh()
    if (keys.length > 0) {
      await this.operation(rig, 'pause', keys, step.id, () => rig.object.pause(byTopic(keys)))
      await this.operation(rig, 'resume', keys, step.id, () => rig.object.resume(byTopic(keys)))
    }
    return { assigned }
  }

  /** @param {string} topic @returns {boolean} whether the standing member's group accepted it */
  accepts(topic) {
    const [rig] = this.standing
    return this.runtime.observer
      .mine(rig.id, 'ConsumerGroup')
      .some((g) => [...(g.topicsSubscribed ?? [])].includes(topic))
  }

  /** @returns {object[]} the client's assigned keys of one topic */
  assignedOf(rig, topic) {
    return this.assignedKeys(rig).filter((k) => k.topic === topic)
  }

  /**
   * Records that the member resolved a key through a batch that held no application message (only
   * control records or aborted records): the product itself resolved it, so the member's next
   * offset moves past it. A batch that carried messages is resolved by its handlers, never by the
   * batch's last offset, which a pause or stale assignment can leave unprocessed.
   * @param {ClientRig} rig
   * @param {{ topic: string, partition: number, batchSize: number, lastOffset: string|null }} batch
   * @returns {void}
   */
  onBatchEnd(rig, { topic, partition, batchSize, lastOffset }) {
    if (batchSize !== 0 || lastOffset == null) return
    const k = `${topic}|${partition}`
    const next = BigInt(lastOffset) + 1n
    if (next > BigInt(rig.processed.get(k) ?? 0)) rig.processed.set(k, next.toString())
  }

  /** @param {ClientRig} rig @param {string} topic @returns {boolean} whether the member reads it */
  reads(rig, topic) {
    return this.topicsFor(rig.slot).some((t) =>
      typeof t === 'string' ? t === topic : t.test(topic)
    )
  }

  /**
   * Commits every key whose resolved next offset is ahead of the member's last successful commit,
   * outside any handler. A drain with `ends` is the final one: for each key the member reads it
   * first waits, until the deadline, for the member to resolve the key's end, then commits what it
   * resolved whatever authority it holds. An intermediate drain skips a key whose captured
   * authority is no longer the member's own: the new owner's progress covers it.
   * @param {{ ends?: Record<string, string>, deadline?: number }} [step] `ends` maps `topic|partition`
   *   to the log end the member must have resolved
   * @returns {Promise<{ reached: boolean }>} whether every key reached its end in time
   */
  async onDrain({ ends = {}, deadline = Infinity } = {}) {
    const { observer } = this.runtime
    const final = Object.keys(ends).length > 0
    let reached = true
    for (const rig of this.rigs()) {
      for (const [k, end] of Object.entries(ends)) {
        if (!this.reads(rig, k.slice(0, k.lastIndexOf('|')))) continue
        const resolved = () => BigInt(rig.processed.get(k) ?? 0) >= BigInt(end)
        if (!(await this.until(resolved, deadline))) reached = false
      }
      const [group] = observer.mine(rig.id, 'ConsumerGroup')
      for (const [k, next] of [...(rig.processed ?? [])]) {
        if (BigInt(next) <= BigInt(rig.committed.get(k) ?? 0)) continue
        const now = group && group.acquired?.has(k) ? observer.currentAuthority(group, k) : null
        const then = rig.authority.get(k)?.authority
        const same =
          now &&
          then &&
          now.run === then.run &&
          now.token === then.token &&
          now.acquiredAt === then.acquiredAt &&
          now.recreatedAt === then.recreatedAt
        if (final || same) await this.commit(rig, k)
        else
          await this.runtime.diagnose(`drain commit of ${k} by ${rig.id} skipped: authority moved`)
      }
    }
    return { reached }
  }

  /** @returns {{ ok: boolean }} whether the standing member handled and committed after recovery */
  async onRecovered(step) {
    const [rig] = this.standing
    const before = [...(rig?.processed?.values() ?? [])].join()
    const ok = await this.until(
      () => [...(rig?.processed?.values() ?? [])].join() !== before,
      this.runtime.clock.now() + 60000
    )
    this.outage = false
    await this.onDrain()
    return { ok }
  }

  /** @param {object} step marks an outage in progress, so replays are attributed to it */
  async onBrokerOutage() {
    this.outage = true
  }

  /** Starts or clears the seed's retained offset map. */
  async perform(step) {
    if (this.seeded && (step.type === 'seed-start' || step.type === 'seed-clear')) {
      const [rig] = this.standing
      const event = step.type === 'seed-start' ? 'start' : 'clear'
      const mapEntries = this.retained?.size ?? 0
      this.retained = event === 'start' ? new Map() : null
      return this.runtime.emit({
        kind: 'seed',
        recordId: this.runtime.nextId(),
        event,
        process: this.runtime.slot,
        client: rig.id,
        controlId: 'interning-seeded',
        mapEntries,
        cause: step.id,
      })
    }
    return super.perform(step)
  }

  /** Records a public call with its keys and settlement. */
  async operation(rig, op, keys, execution, work) {
    const { runtime } = this
    const operation = `${rig.id}.o${runtime.ids}`
    const base = {
      kind: 'operation',
      operation,
      op,
      client: rig.id,
      keys,
      call: operation,
      execution,
      causes: [],
    }
    await runtime.emit({
      ...base,
      recordId: runtime.nextId(),
      phase: 'begin',
      result: null,
      errorType: null,
    })
    try {
      await runtime.context.run(rig.id, work)
      await runtime.emit({
        ...base,
        recordId: runtime.nextId(),
        phase: 'end',
        result: 'ok',
        errorType: null,
      })
    } catch (e) {
      await runtime.emit({
        ...base,
        recordId: runtime.nextId(),
        phase: 'end',
        result: 'error',
        errorType: e.name ?? 'Error',
      })
    }
  }
}

/** @param {object[]} keys @returns {{ topic: string, partitions: number[] }[]} */
const byTopic = (keys) => {
  const map = new Map()
  for (const k of keys) map.set(k.topic, [...(map.get(k.topic) ?? []), k.partition])
  return [...map].map(([topic, partitions]) => ({ topic, partitions }))
}

/**
 * The share subject: a real share consumer that accepts every record it is handed. Its sockets
 * capture ShareFetch and ShareAcknowledge exchanges into the artifact's wire segments.
 */
class ShareWorker extends Worker {
  /** @returns {Promise<void>} */
  async begin() {
    this.standing.push(await this.join(this.runtime.slots.find((c) => c.purpose === 'subject')))
  }

  /**
   * Notes that a record of a key was handed to a handler: the highest offset delivered per key and
   * the time of the latest delivery.
   * @param {string} k `topic|partition` @param {string|number} offset
   * @returns {void}
   */
  delivered(k, offset) {
    this.deliveredAt = this.runtime.clock.now()
    const high = BigInt(this.highest?.get(k) ?? -1)
    this.highest ??= new Map()
    if (BigInt(offset) > high) this.highest.set(k, String(offset))
  }

  /** @returns {Promise<ClientRig>} */
  async join(slot, execution = null) {
    const { runtime } = this
    const rig = this.rig(slot)
    const capture = {
      apiKeys: [78, 79],
      onExchange: (exchange) => {
        const where = runtime.wire.append({ client: rig.id, ...exchange })
        runtime.observer.exchanged(rig.id, { ...exchange, ...where })
      },
    }
    const { options } = clientOptions(runtime.config, runtime.matrix, slot)
    const consumer = await rig.construct((kafka) => kafka.shareConsumer(options), capture)
    await rig.call('connect', () => consumer.connect(), execution)
    await consumer.subscribe({
      topics: [runtime.config.topics.main, ...runtime.config.topics.recreate],
    })
    await rig.call(
      'run',
      () => consumer.run({ eachMessage: (payload) => this.handle(rig, payload) }),
      execution
    )
    return rig
  }

  /** One share handler call: entry with its real acquisition and delivery count, then accept. */
  async handle(rig, { topic, partition, message, acknowledge }) {
    const { runtime } = this
    const handler = `${rig.id}.h${runtime.ids}`
    const topicId = runtime.observer.topicIdOf(topic, null)
    const delivery = runtime.observer.shareDelivery(rig.id, topic, partition, message.offset)
    if (!topicId || !delivery) {
      runtime.diagnose(
        `missing-source ${!topicId ? 'topic id' : 'share acquisition'} at ${handler}`
      )
    } else {
      runtime.emit({
        kind: 'identity',
        recordId: runtime.nextId(),
        stage: 'handler-enter',
        client: rig.id,
        group: rig.slot.group,
        handler,
        id: parseValue(message.value, runtime.config.runId)?.id ?? `foreign.${message.offset}`,
        key: { topic, topicId, partition },
        offset: message.offset,
        payloadSha256: payloadSha256(message.key, message.value, message.headers),
        authority: null,
        acquisition: delivery.acquisition,
        deliveryCount: delivery.deliveryCount,
      })
    }
    acknowledge('accept')
    this.handled = (this.handled ?? 0) + 1
    this.delivered(`${topic}|${partition}`, message.offset)
    await runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      stage: 'handler-end',
      handler,
      outcome: 'processed',
      errorType: null,
    })
  }

  /**
   * One extra share member joins and leaves in a wave's `leave` phase; `resume` has nothing to do.
   * @param {{ execution: string, phase: 'leave'|'resume' }} step
   * @returns {Promise<void>}
   */
  async onMembershipChurn(step) {
    const slot = this.runtime.slots.find((c) => c.purpose === 'churn')
    if (!slot || step.phase === 'resume') return
    this.extras.push(await this.join(slot, step.execution))
    await this.runtime.clock.sleepUntil(this.runtime.clock.now() + 20000)
    for (const rig of this.extras.splice(0)) await rig.retire(step.execution)
  }

  /**
   * Waits until the last offset of each main log end has been delivered and deliveries have been
   * quiet for `quietMs`, so records a lock expiry returned are handed out again first.
   * @param {{ ends?: Record<string, string>, deadline?: number, quietMs?: number }} [step]
   * @returns {Promise<{ reached: boolean }>}
   */
  async onDrain({ ends = {}, deadline = Infinity, quietMs = 5000 } = {}) {
    const { clock, config } = this.runtime
    const wanted = Object.entries(ends).filter(([k]) => k.startsWith(`${config.topics.main}|`))
    const done = () =>
      wanted.every(([k, end]) => BigInt(this.highest?.get(k) ?? -1) >= BigInt(end) - 1n) &&
      clock.now() - (this.deliveredAt ?? -Infinity) >= quietMs
    return { reached: wanted.length === 0 || (await this.until(done, deadline)) }
  }

  /** @returns {Promise<{ ok: boolean }>} whether a record was handled and accepted after recovery */
  async onRecovered() {
    const before = this.handled ?? 0
    return {
      ok: await this.until(() => (this.handled ?? 0) > before, this.runtime.clock.now() + 60000),
    }
  }
}

/** The admin subject: topic metadata readback and its recovery witness, through public calls. */
class AdminWorker extends Worker {
  /** @returns {Promise<void>} */
  async begin() {
    const slot = this.runtime.slots.find((c) => c.purpose === 'subject')
    const rig = this.rig(slot)
    const { options } = clientOptions(this.runtime.config, this.runtime.matrix, slot)
    const admin = await rig.construct((kafka) => kafka.admin(options))
    await rig.call('connect', () => admin.connect())
    this.standing.push(rig)
  }

  /** @returns {Promise<{ ok: boolean }>} reads the run topics' metadata, one topic record each */
  async metadata(step) {
    const [rig] = this.standing
    const { runtime } = this
    const topics = [runtime.config.topics.main, runtime.config.topics.transactions]
    try {
      const answer = await runtime.context.run(rig.id, () =>
        rig.object.fetchTopicMetadata({ topics })
      )
      for (const t of answer.topics)
        await runtime.emit({
          kind: 'topic',
          recordId: runtime.nextId(),
          execution: step.id,
          client: rig.id,
          call: `${rig.id}.q${runtime.ids}`,
          op: 'metadata',
          topic: t.name,
          topicId: normalizeTopicId(t.topicId),
          partitions: t.partitions.map((p) => p.partitionId),
          errorCode: 0,
        })
      return { ok: true }
    } catch (e) {
      await runtime.diagnose(`admin metadata failed: ${e.name}`)
      return { ok: false }
    }
  }

  /** @param {object} step */
  async onRecovered(step) {
    return this.metadata(step)
  }

  /** @param {object} step */
  async onTopicRecreation(step) {
    return this.metadata(step)
  }
}

/**
 * The independent raw reader, client c21 of the reader process. Each scan reads one partition from
 * where the last scan of that key ended to the leader's last stable offset, over its own broker
 * connection whose every frame is archived to the scan's raw file, and emits the committed records
 * a read_committed consumer would be given: no producer journal is consulted.
 */
class ReaderWorker extends Worker {
  /** @returns {Promise<void>} builds the reader's admin client */
  async begin() {
    const rig = this.rig(this.runtime.slots.find((c) => c.purpose === 'observer'))
    const admin = await rig.construct((kafka) => kafka.admin())
    await rig.call('connect', () => admin.connect())
    this.standing.push(rig)
  }

  /**
   * @param {{ id: string, topic: string, topicId: string, partition: number, from: string,
   *   execution?: string|null }} step one key's scan request
   * @returns {Promise<{ scan: string, to: string }>} the scan id and where it ended
   */
  async onScan(step) {
    const { runtime } = this
    const [rig] = this.standing
    const scan = `${runtime.slot}.s${runtime.ids}`
    const rawFile = `raw/${scan}.ndjson`
    const raw = path.join(runtime.out, rawFile)
    const { leader } = await this.leaderOf(rig, step.topic, step.partition)
    const broker = await openRawBroker(leader, (exchange) =>
      fs.appendFileSync(
        raw,
        JSON.stringify({
          apiKey: exchange.apiKey,
          request: exchange.request.toString('hex'),
          response: exchange.response.toString('hex'),
        }) + '\n'
      )
    )
    try {
      const result = await scanPartition(broker, { ...step, from: BigInt(step.from) })
      const key = { topic: step.topic, topicId: step.topicId, partition: step.partition }
      let records = 0
      for (const r of result.records) {
        const parsed = parseValue(r.value, runtime.config.runId)
        records++
        await runtime.emit({
          kind: 'identity',
          recordId: runtime.nextId(),
          execution: step.execution ?? null,
          stage: 'log',
          reader: rig.id,
          scan,
          id: parsed?.id ?? `foreign.${r.offset}`,
          key,
          offset: String(r.offset),
          payloadSha256: payloadSha256(r.key, r.value, r.headers ?? {}),
          transaction: parsed?.transaction ?? null,
        })
      }
      await runtime.emit({
        kind: 'scan',
        recordId: runtime.nextId(),
        execution: step.execution ?? null,
        scan,
        key,
        from: String(step.from),
        to: result.to,
        lso: result.lso,
        highWatermark: result.highWatermark,
        rawFile,
        rawSha256: sha256(fs.readFileSync(raw)),
        records,
      })
      return { scan, to: result.to }
    } finally {
      await broker.disconnect()
    }
  }

  /**
   * Finds a partition's leader address through the reader's own admin client.
   * @returns {Promise<{ leader: { host: string, port: number } }>}
   * @throws {Error} when the partition has no known leader
   */
  async leaderOf(rig, topic, partition) {
    const call = (work) => this.runtime.context.run(rig.id, work)
    const { topics } = await call(() => rig.object.fetchTopicMetadata({ topics: [topic] }))
    const leader = topics[0]?.partitions.find((p) => p.partitionId === partition)?.leader
    const { brokers } = await call(() => rig.object.describeCluster())
    const broker = brokers.find((b) => b.nodeId === leader)
    if (!broker) throw new Error(`${topic}-${partition} has no known leader`)
    return { leader: { host: broker.host, port: broker.port } }
  }
}

/**
 * Opens one broker connection whose frames are all handed to `onExchange`.
 * @param {{ host: string, port: number }} address
 * @param {(exchange: object) => void} onExchange
 * @returns {Promise<object>} a connected `Broker`
 */
const openRawBroker = async ({ host, port }, onExchange) => {
  const Broker = require('../../src/broker')
  const ConnectionPool = require('../../src/network/connectionPool')
  const { createLogger, LEVELS } = require('../../src/loggers')
  const LoggerConsole = require('../../src/loggers/console')
  const registry = new SocketRegistry()
  const logger = createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole })
  const broker = new Broker({
    connectionPool: new ConnectionPool({
      host,
      port,
      logger,
      socketFactory: registry.factory('reader', { apiKeys: [1, 2, 3, 18], onExchange }),
      connectionTimeout: 3000,
      requestTimeout: 30000,
    }),
    logger,
  })
  await broker.connect()
  return broker
}

/**
 * Reads one partition read_committed from `from` to its last stable offset taken at the start,
 * with the high watermark beside it. Records of aborted transactions and control batches are left
 * out by the same rules a read_committed consumer follows (`committedRecords`), applied per Fetch
 * response, whose aborted-transaction list covers that response's range.
 * @param {object} broker a connected `Broker` to the partition leader
 * @param {{ topic: string, partition: number, from: bigint }} read
 * @returns {Promise<{ records: object[], to: string, lso: string, highWatermark: string }>}
 * @throws {Error} when fifty fetches in a row make no progress
 */
const scanPartition = async (broker, { topic, partition, from }) => {
  const { committedRecords } = require('../../testHelpers/rawLog')
  const ISOLATION = require('../../src/protocol/isolationLevel')
  const end = async (isolationLevel) => {
    const { responses } = await broker.listOffsets({
      replicaId: -1,
      isolationLevel,
      topics: [{ topic, partitions: [{ partition, timestamp: -1 }] }],
    })
    return BigInt(responses[0].partitions[0].offset)
  }
  const lso = await end(ISOLATION.READ_COMMITTED)
  const highWatermark = await end(ISOLATION.READ_UNCOMMITTED)
  const records = []
  let position = from
  let idle = 0
  while (position < lso) {
    const { responses } = await broker.fetch({
      isolationLevel: ISOLATION.READ_COMMITTED,
      maxWaitTime: 100,
      topics: [
        { topic, partitions: [{ partition, fetchOffset: String(position), maxBytes: 1048576 }] },
      ],
    })
    const { messages, abortedTransactions } = responses[0].partitions[0]
    const inRange = messages.filter((m) => BigInt(m.offset) >= position && BigInt(m.offset) < lso)
    records.push(
      ...committedRecords({ records: inRange, abortedTransactions: abortedTransactions ?? [] })
    )
    let next = position
    for (const m of messages) if (BigInt(m.offset) + 1n > next) next = BigInt(m.offset) + 1n
    if (next === position && ++idle >= 50)
      throw new Error(`${topic}-${partition} scan stalled at ${position}`)
    if (next > position) idle = 0
    position = next > lso ? lso : next
  }
  return { records, to: String(lso), lso: String(lso), highWatermark: String(highWatermark) }
}

/**
 * Splits each broker reporter's file into boot segments and checks them: within one boot the
 * reporter's sequence numbers must run on without a gap (a gap is a dropped push), a new boot must
 * restart them at 1 after a new load proof, and an overflow marker means the reporter dropped rows.
 */
class ReporterSegments {
  constructor() {
    this.nodes = new Map()
  }

  /**
   * @param {string} node @param {number} seq the line's reporter sequence
   * @param {number|null} loadedAtMs the node's current load proof time
   * @returns {string|null} a problem, or null
   */
  push(node, seq, loadedAtMs) {
    const state = this.nodes.get(node) ?? { last: 0, boot: loadedAtMs, boots: 1 }
    this.nodes.set(node, state)
    const last = state.last
    state.last = seq
    if (seq === last + 1) return null
    if (seq === 1 && loadedAtMs !== null && loadedAtMs !== state.boot) {
      Object.assign(state, { boot: loadedAtMs, boots: state.boots + 1 })
      return null
    }
    return `reporter ${node} sequence ${seq} follows ${last} in boot ${state.boots}`
  }
}

/**
 * The telemetry decoder: tails each broker reporter's file, archives each new chunk once, checks
 * its boot segments and overflow marker, and emits one telemetry record per push of a run client,
 * bound to the subscription and instance id that client actually received.
 */
class DecoderWorker extends Worker {
  /** @param {ChildRuntime} runtime */
  constructor(runtime) {
    super(runtime)
    this.offsets = new Map()
    this.segments = new ReporterSegments()
    this.subscriptions = new Map()
    this.dir = process.env.TELEMETRY_EVIDENCE_DIR ?? null
  }

  /** @param {{ entries: object[] }} step subscriptions the clients received */
  async onTelemetrySubscriptions(step) {
    for (const e of step.entries) this.subscriptions.set(e.instanceId, e)
  }

  /** @returns {Promise<{ abort: string|null }>} reads what the reporters wrote since the last tick */
  async onTick() {
    if (!this.dir) return { abort: 'telemetry evidence directory is not set' }
    const { runtime } = this
    let abort = null
    const files = fs.readdirSync(this.dir)
    for (const marker of files.filter((f) => /^OVERFLOW-\d+\.json$/.test(f)))
      abort ??= `reporter overflow marker ${marker}`
    for (const file of files.filter((f) => /^exports-\d+\.jsonl$/.test(f))) {
      const node = file.match(/\d+/)[0]
      const from = this.offsets.get(file) ?? 0
      const fd = fs.openSync(path.join(this.dir, file), 'r')
      const size = fs.fstatSync(fd).size
      const bytes = Buffer.alloc(size - from)
      fs.readSync(fd, bytes, 0, bytes.length, from)
      fs.closeSync(fd)
      const end = bytes.lastIndexOf(0x0a) + 1
      if (end === 0) continue
      this.offsets.set(file, from + end)
      const raw = bytes.subarray(0, end)
      const rawFile = `telemetry/exports-${node}.${from}.jsonl`
      const decodedFile = `telemetry/decoded-${node}.${from}.jsonl`
      fs.mkdirSync(path.join(runtime.out, 'telemetry'), { recursive: true })
      fs.writeFileSync(path.join(runtime.out, rawFile), raw)
      const rawSha256 = sha256(raw)
      const loaded = this.loadedAt(node)
      for (const line of raw.toString('utf8').split('\n').filter(Boolean)) {
        const push = JSON.parse(line)
        const problem = this.segments.push(node, Number(push.seq), loaded)
        if (problem) abort ??= problem
        if (push.parse !== 'ok' || push.truncated === true)
          abort ??= `reporter ${node} push ${push.seq} not decoded`
        if (!runtime.slotOf(push.clientId)) continue
        const subscription = this.subscriptions.get(String(push.clientInstanceId))
        if (!subscription) {
          await runtime.diagnose(`telemetry push ${node}.${push.seq} has no recorded subscription`)
          continue
        }
        const decoded =
          JSON.stringify({
            node: push.node,
            seq: push.seq,
            clientId: push.clientId,
            clientInstanceId: push.clientInstanceId,
            subscriptionId: subscription.subscriptionId,
            metricsTotal: push.metricsTotal,
            terminating: push.terminating,
            dataSha256: push.dataSha256,
          }) + '\n'
        fs.appendFileSync(path.join(runtime.out, decodedFile), decoded)
        await runtime.emit({
          kind: 'telemetry',
          recordId: runtime.nextId(),
          client: push.clientId,
          instanceId: String(push.clientInstanceId),
          subscriptionId: String(subscription.subscriptionId),
          broker: Number(push.node),
          metrics: Number(push.metricsTotal ?? 0),
          terminating: push.terminating === true,
          rawFile,
          rawSha256,
          decodedFile,
          decodedSha256: sha256(decoded),
        })
      }
    }
    if (abort) await runtime.diagnose(`telemetry abort: ${abort}`)
    return { abort }
  }

  /** @param {string} node @returns {number|null} the node reporter's current load proof time */
  loadedAt(node) {
    try {
      return JSON.parse(fs.readFileSync(path.join(this.dir, `reporter-${node}.json`), 'utf8'))
        .loadedAtMs
    } catch {
      return null
    }
  }
}

/** @param {ChildRuntime} runtime @returns {Worker} the worker of the process's role and kind */
function createWorker(runtime) {
  const { role } = runtime.process
  if (role === 'reader') return new ReaderWorker(runtime)
  if (role === 'decoder') return new DecoderWorker(runtime)
  if (role === 'seed') return new ConsumerWorker(runtime)
  if (role === 'collector') return new Worker(runtime)
  const kind = runtime.slots.find((c) => c.purpose === 'subject')?.clientKind
  const def = runtime.matrix.clientKinds[kind]
  if (def.role === 'producer') return new ProducerWorker(runtime)
  if (def.role === 'share') return new ShareWorker(runtime)
  if (def.role === 'admin') return new AdminWorker(runtime)
  return new ConsumerWorker(runtime)
}

/**
 * Runs a child process: `init` builds the runtime and takes the start handshake and baseline before
 * any command is served; commands then run concurrently, so sampling never waits behind a workload.
 * The process ends by itself once the `disconnect` command has run and the channel is closed.
 * @param {{ child: string, out: string }} options
 * @returns {void}
 */
function child(options) {
  let ready = null
  const replies = new Map()
  const acks = new Map()
  process.on('message', async (message) => {
    if (message.type === 'init') {
      ready = (async () => {
        const runtime = new ChildRuntime({
          slot: options.child,
          config: message.config,
          matrix: message.matrix,
          originMs: message.originMs,
          out: options.out,
          send: (m, done) => {
            acks.set(m.record.seq, done)
            process.send(m)
          },
          notify: (m) => process.send(m),
        })
        for (const [topic, topicId] of message.topicIds) runtime.topicIds.set(topic, topicId)
        let requests = 0
        runtime.request = (body) =>
          new Promise((resolve) => {
            const request = ++requests
            replies.set(request, resolve)
            process.send({ ...body, request })
          })
        await runtime.start()
        return runtime
      })()
    } else if (message.type === 'ack') {
      acks.get(message.seq)?.()
      acks.delete(message.seq)
    } else if (message.type === 'reply') {
      replies.get(message.request)?.()
      replies.delete(message.request)
    } else if (message.type === 'command') {
      const runtime = await ready
      await runtime.handle(message)
      if (message.step.type === 'disconnect') {
        while (runtime.channel.pendingBytes > 0)
          await new Promise((resolve) => setTimeout(resolve, 10))
        process.disconnect()
      }
    }
  })
}

module.exports = {
  sha256,
  bootId,
  normalizeTopicId,
  payloadSha256,
  offeredRecord,
  parseValue,
  WireLog,
  ClientRig,
  ChildRuntime,
  clientOptions,
  Worker,
  ProducerWorker,
  ShareWorker,
  ConsumerWorker,
  ReaderWorker,
  DecoderWorker,
  ReporterSegments,
  scanPartition,
  openRawBroker,
  createWorker,
  child,
}
