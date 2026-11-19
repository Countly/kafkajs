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
const {
  SUPERVISOR,
  OUTER,
  DIAGNOSTIC_BYTES,
  SourceChannel,
  Sampler,
  chunk,
  clip,
} = require('./finalSoakCollector')
const { PassiveObserver, SocketRegistry, normalizeTopicId } = require('./finalSoakObserver')

/** Delivered share offsets kept above a key's first missing one before the gap is given up on. */
const SHARE_AHEAD_MAX = 100000

/** How long a drain without its own deadline, and a recovery witness, may take. */
const DRAIN_MS = 60000

/** @param {string} k `topic|partition` @returns {[string, number]} the topic and partition */
const splitKey = (k) => {
  const at = k.lastIndexOf('|')
  return [k.slice(0, at), Number(k.slice(at + 1))]
}
/** Bytes after which a raw wire segment file is closed and the next one started. */
const WIRE_SEGMENT_BYTES = 64 * 1024 * 1024
/** The byte budget of one process's raw share wire files, all segments together. */
const WIRE_TOTAL_BYTES = 4 * 1024 ** 3
/** The byte budget of the reader's raw scan files, all scans together. */
const RAW_SCAN_BYTES = 8 * 1024 ** 3
/** The most of a telemetry reporter file the decoder reads in one poll. */
const TELEMETRY_READ_BYTES = 8 * 1024 * 1024

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

/** JSON bytes kept of each error message, so a failure record always fits its line cap. */
const ERROR_MESSAGE_BYTES = 120
/** JSON bytes kept of an error's name, type or string code. */
const ERROR_NAME_BYTES = 48
/** Errors of a `cause` chain kept after the error itself. */
const ERROR_CAUSES = 2

/**
 * What a failed public call returned, cut to fit one record line: the error's own fields and the
 * name and message of each error in its `cause` chain, each string clipped to a fixed JSON size.
 * @param {unknown} error what the call threw
 * @returns {{ name: string|null, type: string|null, code: number|string|null,
 *   retriable: boolean|null, message: string, causes: { name: string|null, message: string }[] }}
 */
const errorDetail = (error) => {
  const name = (v) => (typeof v === 'string' ? clip(v, ERROR_NAME_BYTES) : (v ?? null))
  const message = (e) => clip(String(e?.message ?? e), ERROR_MESSAGE_BYTES)
  const causes = []
  for (let c = error?.cause; c && causes.length < ERROR_CAUSES; c = c.cause)
    causes.push({ name: name(c.name), message: message(c) })
  return {
    name: name(error?.name),
    type: name(error?.type),
    code: name(error?.code),
    retriable: typeof error?.retriable === 'boolean' ? error.retriable : null,
    message: message(error),
    causes,
  }
}

/**
 * Keeps a handler entry the observer could not enrich (no batch authority, topic id or share
 * acquisition) as partial evidence and fails the run: the callback really happened, so it must not
 * vanish from the record, and nothing is invented to make the entry whole.
 * @param {object} runtime the hosting process
 * @param {{ client: string, group: string, handler: string, id: string, topic: string,
 *   partition: number, offset: string, payloadSha256: string }} entry what the callback gave
 * @param {string[]} missing the enrichment that could not be read
 * @returns {void}
 */
const partialEntry = (runtime, entry, missing) => {
  runtime.diagnose(`partial handler-enter ${JSON.stringify({ ...entry, missing })}`)
  runtime.problem(
    `handler entry ${entry.id} at ${entry.topic}/${entry.partition}:${entry.offset} lacks ${missing.join(', ')}`
  )
}

/**
 * Appends raw exchanges to rotating segment files in the artifact, one JSON line each, and returns
 * where each line lives and its hash. Each segment is written once and never copied.
 */
class WireLog {
  /** @param {{ dir: string, prefix: string }} options artifact directory and file prefix */
  constructor({ dir, prefix, budget = WIRE_TOTAL_BYTES, onFull = () => {} }) {
    Object.assign(this, { dir, prefix, budget, onFull })
    this.segment = 0
    this.bytes = 0
    this.total = 0
    this.dropped = 0
    fs.mkdirSync(path.join(dir, 'raw'), { recursive: true })
  }

  /**
   * Appends one exchange, unless that would pass the byte budget of all segments: the exchange is
   * then counted as dropped and `onFull` is told once, so the run fails instead of filling the disk.
   * @param {object} exchange `{ apiKey, request: Buffer, response: Buffer }` and any extra fields
   * @returns {{ file: string, sha256: string }|null} where it was written, null when dropped
   */
  append({ request, response, ...rest }) {
    const line =
      JSON.stringify({
        ...rest,
        request: request.toString('hex'),
        response: response.toString('hex'),
      }) + '\n'
    if (this.total + line.length > this.budget) {
      if (this.dropped++ === 0) this.onFull(`raw wire budget of ${this.budget} bytes reached`)
      return null
    }
    this.total += line.length
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

/** The kafkajs WARN level; ERROR is below it. */
const LOG_WARN = 2
/** The byte budget of one process's client log file, its closing summary included. */
const CLIENT_LOG_BYTES = 8 * 1024 * 1024
/** Bytes of the budget kept for the closing summary. */
const CLIENT_LOG_SUMMARY_BYTES = 64 * 1024
/** JSON bytes kept of one client log message. */
const CLIENT_LOG_MESSAGE_BYTES = 1000
/** Lines past the budget kept for the closing summary, the newest ones. */
const CLIENT_LOG_TAIL = 20

/**
 * One process's client WARN and ERROR lines, as JSON lines in a file of the artifact. The file
 * never passes its byte budget: each message is clipped, lines that would pass the budget are only
 * counted, and the newest of them are kept for the summary line written at `close`, with the total,
 * written and dropped counts, so silence in the file is never mistaken for health. A failed write
 * is recorded and ends the file; it never reaches the client that logged.
 */
class ClientLog {
  /**
   * @param {{ file: string, maxBytes?: number, files?: typeof fs }} options the file, created or
   *   truncated, its byte budget and the file system
   */
  constructor({ file, maxBytes = CLIENT_LOG_BYTES, files = fs }) {
    Object.assign(this, { file, maxBytes, files })
    this.total = 0
    this.written = 0
    this.dropped = 0
    this.bytes = 0
    this.tail = []
    this.failed = null
    this.fd = null
    try {
      this.fd = files.openSync(file, 'w', 0o600)
    } catch (e) {
      this.failed = e.message
    }
  }

  /** @returns {(level: number) => (entry: object) => void} a `logCreator` writing into this log */
  creator() {
    return () => (entry) => {
      try {
        this.add(entry)
      } catch (e) {
        this.failed ??= e.message
      }
    }
  }

  /** @param {{ namespace: string, level: number, log: object }} entry INFO and DEBUG are ignored */
  add({ namespace, level, log }) {
    if (level > LOG_WARN) return
    this.total++
    const { message, memberId, groupId, error, errorCode, timestamp } = log
    const line =
      JSON.stringify({
        at: Date.parse(timestamp) || Date.now(),
        level,
        namespace,
        member: memberId,
        group: groupId,
        message: clip(String(message ?? ''), CLIENT_LOG_MESSAGE_BYTES),
        errorType: typeof error === 'object' && error !== null ? error.name : undefined,
        errorCode: errorCode ?? error?.code ?? error?.type,
      }) + '\n'
    const bytes = Buffer.byteLength(line)
    if (this.fd !== null && this.bytes + bytes <= this.maxBytes - CLIENT_LOG_SUMMARY_BYTES) {
      if (this.write(line)) {
        this.written++
        this.bytes += bytes
        return
      }
    }
    this.dropped++
    this.tail.push(line)
    if (this.tail.length > CLIENT_LOG_TAIL) this.tail.shift()
  }

  /** @param {string} text @returns {boolean} whether all of it was written; a failure ends the file */
  write(text) {
    const buffer = Buffer.from(text)
    try {
      for (let offset = 0; offset < buffer.length;)
        offset += this.files.writeSync(this.fd, buffer, offset)
      return true
    } catch (e) {
      this.failed ??= e.message
      this.end()
      return false
    }
  }

  /** Closes the file, quietly. */
  end() {
    const { fd } = this
    this.fd = null
    try {
      if (fd !== null) this.files.closeSync(fd)
    } catch (e) {
      this.failed ??= e.message
    }
  }

  /**
   * Writes the summary line and closes the file, once.
   * @returns {{ total: number, written: number, dropped: number, failed: string|null }}
   */
  close() {
    const counts = { total: this.total, written: this.written, dropped: this.dropped }
    if (this.fd !== null) {
      const tail = this.tail.map((line) => JSON.parse(line))
      this.write(JSON.stringify({ summary: true, ...counts, failed: this.failed, tail }) + '\n')
      this.end()
    }
    return { ...counts, failed: this.failed }
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
   * its sockets (and captures share wire frames for a share client) and its WARN and ERROR log
   * lines written to its process's bounded client log.
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
          logLevel: logLevel.WARN,
          logCreator: runtime.clientLogCreator(),
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
      await runtime.failed(call, e)
      throw e
    }
  }

  /**
   * Disconnects the client with its call records, then drops what the passive observer kept for
   * it, so history of retired clients never accumulates in a long-lived process.
   * @param {string|null} [execution]
   * @param {{ leaveGroup?: boolean }} [options] the public `disconnect` options; omitted keeps the
   *   client's default (a static member stays in its group)
   * @returns {Promise<void>}
   * @throws {Error} the disconnect's error, after the observer's state is dropped
   */
  async retire(execution = null, options = undefined) {
    try {
      await this.call('disconnect', () => this.object.disconnect(options), execution)
    } finally {
      this.runtime.forget(this.id)
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
    this.wire = new WireLog({
      dir: out,
      prefix: `${slot}.share`,
      onFull: (message) => this.problem(message),
    })
    this.observer = new PassiveObserver({
      context: this.context,
      commits: this.commits,
      clock: this.clock,
      topicIds: (topic) => this.topicIds.get(topic) ?? null,
      onEvent: (event) => this.observed(event),
    })
    this.observer.lives = (client, topic, topicId) => this.lifetimeOf(client, topic, topicId)
    this.observer.install()
    this.worker = createWorker(this)
  }

  /** @returns {string} a fresh record id of this source */
  nextId() {
    return `${this.slot}.${this.ids++}`
  }

  /**
   * The `logCreator` of this process's clients: their WARN and ERROR lines go to one bounded
   * JSON-lines file in the artifact, `client-log.<slot>.ndjson`; lines past its cap are only counted.
   * @returns {(level: number) => (entry: object) => void}
   */
  clientLogCreator() {
    this.clientLog ??= new ClientLog({
      file: path.join(this.out, `client-log.${this.slot}.ndjson`),
    })
    return this.clientLog.creator()
  }

  /**
   * Closes the process's client log and journals its counts, so a reader can tell a quiet client
   * from a truncated or failed log.
   * @returns {Promise<void>}
   */
  async closeClientLog() {
    if (!this.clientLog) return
    const { total, written, dropped, failed } = this.clientLog.close()
    await this.diagnose(
      `client log client-log.${this.slot}.ndjson: ${total} WARN or ERROR lines, ${written} written, ${dropped} dropped${failed ? `, failed: ${failed}` : ''}`
    )
  }

  /** @param {object} record @returns {Promise<boolean>} */
  emit(record) {
    return this.channel.emit(record)
  }

  /**
   * @param {string} message clipped to fit the line cap
   * @returns {Promise<boolean>} a diagnostic note
   */
  diagnose(message) {
    return this.emit({
      kind: 'diagnostic',
      recordId: this.nextId(),
      message: clip(message, DIAGNOSTIC_BYTES),
    })
  }

  /**
   * Records exactly what a failed public call returned (see `errorDetail`), so a client error is
   * kept with its type, code and causes, never only its name.
   * @param {string} call the failed call's id @param {unknown} error what it threw
   * @returns {Promise<boolean>}
   */
  failed(call, error) {
    return this.emit({
      kind: 'diagnostic',
      recordId: this.nextId(),
      message: `public call ${call} failed`,
      failedCall: call,
      error: errorDetail(error),
    })
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
        if (!event.wire?.file) {
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
        if (!event.wire?.file) {
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
   * @param {string} client @param {string} topic @param {string} topicId
   * @returns {string|null} the accepted lifetime `<publication>/<topicId>` the name is in for this
   *   incarnation, or null when no publication accepted it with that id
   */
  lifetimeOf(client, topic, topicId) {
    const life = this.lifetimes.get(`${client}/${topic}`)
    return life?.endsWith(`/${topicId}`) ? life : null
  }

  /**
   * Emits an accepted subscription publication and keeps each accepted `(client, topic, id)`
   * lifetime: it starts at its first publication and ends when the name leaves the subscription or
   * its id changes. A recreation (`ConsumerGroup#invalidateTopic`) publishes too, so the new id
   * starts its own lifetime. Names without a known id are reported, not published.
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

  /**
   * The lifetime boundary of a retired client: everything kept per client (the observer's
   * exchanges, its subscription lifetimes and revision, the worker's per-client state) is dropped,
   * so retained harness state follows the live clients, not the run's churn.
   * @param {string} client @returns {void}
   */
  forget(client) {
    this.observer.forget(client)
    for (const key of [...this.lifetimes.keys()])
      if (key.startsWith(`${client}/`)) this.lifetimes.delete(key)
    this.revisions.delete(client)
    this.worker.forgetClient?.(client)
  }

  /**
   * Sets a name's current topic id, or removes a deleted name (`topicId` null). The incarnation it
   * replaces is retired: the worker drops what it kept for that topic id.
   * @param {string} topic @param {string|null} topicId
   * @returns {void}
   */
  setTopicId(topic, topicId) {
    const old = this.topicIds.get(topic)
    if (topicId) this.topicIds.set(topic, topicId)
    else this.topicIds.delete(topic)
    if (old && old !== topicId) this.worker.forgetTopic?.(old)
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
        for (const [topic, topicId] of step.topicIds) this.setTopicId(topic, topicId)
      else if (step.type === 'disconnect') await this.stop()
      else reply = (await this.worker.perform(step)) ?? null
    } catch (e) {
      error = e.message
      await this.diagnose(`step ${step.id} failed: ${e.message}`)
    }
    this.notify({ type: 'done', id, error, reply, incomplete: this.channel.incomplete })
  }

  /**
   * Winds the process down by itself when the supervisor is gone: its source is marked incomplete,
   * so nothing more is sent, every client disconnects and the process is left with nothing to run.
   * @returns {Promise<void>}
   */
  async orphaned() {
    this.channel.incomplete ??= 'the supervisor closed its channel'
    await this.worker.disconnect()
    this.clientLog?.close()
    this.roles.stop()
    this.observer.restore()
  }

  /** @returns {Promise<void>} disconnects every client, takes the final sample and lets the process end */
  async stop() {
    await this.worker.disconnect()
    await this.closeClientLog()
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
  /** Aborted when an idle window begins, so a transaction stops retrying its commit. */
  quiesce = new AbortController()

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

  /**
   * @returns {Promise<void>} one transaction per period while active, every tenth aborted, and in a
   *   smoke also the matrix's early one
   */
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
   * One whole transaction: its records, the decision and the outcome the client confirmed, never
   * inferred. A send that fails stops the transaction's sends and turns the decision into an abort,
   * as the client's contract asks of a transaction an operation failed in; so does a commit the
   * client refuses. The next transaction then begins as usual.
   * @param {ClientRig} rig @param {number} n the transaction's number
   * @returns {Promise<void>}
   */
  async transact(rig, n) {
    const { config, matrix } = this.runtime
    const transaction = `${rig.id}.x${n}`
    const early = config.mode === 'smoke' && n === matrix.schedule.smoke.abortAtTransaction
    let decision = early || n % valueOf(matrix.traffic.abortEvery) === 0 ? 'abort' : 'commit'
    let ended = { outcome: 'unknown', errorType: null }
    try {
      const txn = await rig.object.transaction()
      for (let i = 0; i < valueOf(matrix.traffic.transactionRecords); i++) {
        const sent = await this.offer(rig, config.topics.transactions, 0, transaction, txn)
        if (sent !== 'acknowledged') {
          decision = 'abort'
          break
        }
      }
      ended = await this.end(txn, transaction, decision)
      if (ended.aborted) decision = 'abort'
    } catch (e) {
      ended.errorType = e.name ?? 'Error'
      await this.runtime.failed(`${transaction}.begin`, e)
    }
    this.settled = (this.settled ?? 0) + 1
    this.lastOutcome = ended.outcome
    await this.runtime.emit({
      kind: 'identity',
      recordId: this.runtime.nextId(),
      stage: 'transaction',
      client: rig.id,
      transaction,
      decision,
      outcome: ended.outcome,
      call: `${transaction}.${decision}`,
      errorType: ended.errorType,
    })
  }

  /**
   * Ends a transaction by the client's public contract. A commit whose outcome is unknown
   * (`retriable`) is sent again until it is confirmed or the loops stop; a commit the client
   * refuses, or one still unconfirmed when an idle window begins, is followed by an abort, so no
   * transaction runs on into the window. Only an end the client confirmed is `committed` or
   * `aborted`.
   * @param {object} txn the client's transaction
   * @param {string} transaction the journal's transaction id
   * @param {'commit'|'abort'} decision
   * @returns {Promise<{ outcome: 'committed'|'aborted'|'unknown', errorType: ?string,
   *   aborted: boolean }>} `aborted` when the driver ended it with an abort; `errorType` names the
   *   last error the client returned, null for a confirmed commit (earlier attempts stay in the
   *   failed-call diagnostics)
   */
  async end(txn, transaction, decision) {
    const { clock } = this.runtime
    let errorType = null
    if (decision === 'commit') {
      for (;;) {
        try {
          await txn.commit()
          return { outcome: 'committed', errorType: null, aborted: false }
        } catch (e) {
          errorType = e.name ?? 'Error'
          await this.runtime.failed(`${transaction}.commit`, e)
          if (!e.retriable) break
          if (this.loops?.signal.aborted) return { outcome: 'unknown', errorType, aborted: false }
          if (this.quiesce?.signal.aborted) break
          const signals = [this.loops?.signal, this.quiesce?.signal].filter(Boolean)
          await clock.sleepUntil(clock.now() + 1000, AbortSignal.any(signals))
          if (this.quiesce?.signal.aborted) break
        }
      }
    }
    try {
      await txn.abort()
      return { outcome: 'aborted', errorType, aborted: true }
    } catch (e) {
      await this.runtime.failed(`${transaction}.abort`, e)
      return { outcome: 'unknown', errorType: e.name ?? 'Error', aborted: true }
    }
  }

  /**
   * Ends the retries of a transaction in flight (it is aborted), then answers an idle command once
   * the send or transaction in flight, and every probe send that outlived its execution deadline,
   * has settled.
   * @returns {Promise<void>}
   */
  async onIdle() {
    this.quiesce?.abort()
    await this.current
    await Promise.all(this.lateSends ?? [])
  }

  /** @returns {Promise<void>} lets the next transaction retry its commit again */
  async onActive() {
    this.quiesce = new AbortController()
  }

  /**
   * Stops the loops and waits until each has ended, so no send or transaction is in flight and no
   * loop competes with the close for the event loop, then disconnects.
   * @returns {Promise<void>}
   */
  async disconnect() {
    this.loops?.abort()
    await this.loop
    await Promise.all(this.lateSends ?? [])
    return super.disconnect()
  }

  /**
   * Offers one logical id once: its offered record, the send, then its settled outcome. An id
   * whose topic id is not yet known is not offered. A send the client reports as a success with
   * offset `-1` (the broker deduplicated it and returned no location) is acknowledged with no
   * offset: the location is unknown, never invented. A send still in flight at `deadline` is
   * recorded `unknown` (`ExecutionDeadline`) so the caller answers in time; the client cannot cancel
   * it, so it is kept until it settles (bounded by the producer's delivery timeout) and its late
   * result is noted, never recorded as a second outcome.
   * @param {number} [deadline] run-clock ms past which the offer stops waiting for the send
   * @returns {Promise<'acknowledged'|'unknown'|'skipped'>} the send's outcome
   */
  async offer(
    rig,
    topic,
    partition,
    transaction,
    sender = rig.object,
    execution = null,
    deadline = Infinity
  ) {
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
    const sending = Promise.resolve()
      .then(() =>
        sender.send({
          topic,
          acks: -1,
          messages: [{ key: record.key, value: record.value, partition }],
        })
      )
      .then(
        ([meta] = []) => ({ meta }),
        (error) => ({ error })
      )
    const settled = await this.within(sending, deadline)
    let ack
    if (!settled) {
      ack = { outcome: 'unknown', offset: null, errorType: 'ExecutionDeadline' }
      this.late(sending, record.id, execution)
    } else if (settled.error) {
      ack = { outcome: 'unknown', offset: null, errorType: settled.error.name ?? 'Error' }
      await runtime.failed(`${record.id}.send`, settled.error)
    } else {
      const offset = settled.meta?.baseOffset ?? null
      // '-1' is the client's success for a send the broker already had, at no known location
      ack =
        offset === null
          ? { outcome: 'unknown', offset: null, errorType: null }
          : { outcome: 'acknowledged', offset: offset === '-1' ? null : offset, errorType: null }
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
   * @param {Promise<object>} promise a settled-shape send
   * @param {number} deadline run-clock ms
   * @returns {Promise<object|null>} its settlement, or null once the deadline passed first; the
   *   deadline timer is cleared either way
   */
  async within(promise, deadline) {
    if (deadline === Infinity) return promise
    const timer = new AbortController()
    try {
      return await Promise.race([
        promise,
        this.runtime.clock.sleepUntil(deadline, timer.signal).then(() => null),
      ])
    } finally {
      timer.abort()
    }
  }

  /**
   * Keeps a send that outlived its execution deadline until it settles, then notes its result (a
   * failure with its details), so idle and disconnect wait for it and nothing is left in flight.
   * @param {Promise<{ meta?: object, error?: Error }>} sending @param {string} id the offered id
   * @param {string|null} execution
   * @returns {void}
   */
  late(sending, id, execution) {
    this.lateSends ??= new Set()
    const done = sending.then(async ({ error }) => {
      const result = error ? (error.name ?? 'Error') : 'ok'
      await this.runtime.diagnose(
        `probe send ${id} settled after the ${execution} deadline: ${result}`
      )
      if (error) await this.runtime.failed(`${id}.send`, error)
      this.lateSends.delete(done)
    })
    this.lateSends.add(done)
  }

  /**
   * Offers the probe ids an auxiliary execution asks for, and stops starting sends once the
   * execution's deadline has passed; a send in flight at the deadline stops being awaited there.
   * @param {{ execution: string, topic: string, partitions: number, count: number,
   *   deadline?: number }} step
   * @returns {Promise<{ offered: number, acknowledged: number, stopped: boolean }>} how many
   *   offers were made and how many of them the broker acknowledged
   */
  async onProbe(step) {
    let offered = 0
    let acknowledged = 0
    for (; offered < step.count; offered++) {
      if (this.runtime.clock.now() >= (step.deadline ?? Infinity))
        return { offered, acknowledged, stopped: true }
      const outcome = await this.offer(
        this.standing[0],
        step.topic,
        offered % step.partitions,
        null,
        undefined,
        step.execution,
        step.deadline ?? Infinity
      )
      if (outcome === 'acknowledged') acknowledged++
    }
    return { offered, acknowledged, stopped: false }
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
  /** How many handler calls of this process's members have settled. */
  handled = 0
  /** Per `group/topicId/partition`, the highest next offset entered and the authority entering it. */
  progress = new Map()
  /** Per `client/topicId/partition`, the cause of the key's last acquisition boundary. */
  lastBoundary = new Map()
  /** The seed's retained offset map while the seed runs, else null. */
  retained = null

  /** @returns {Promise<void>} */
  async begin() {
    const slot = this.runtime.slots.find((c) => c.purpose === 'subject' || c.purpose === 'control')
    this.seeded = slot.purpose === 'control'
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
    rig.uncommitted = new Map()
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
    const parsed = parseValue(message.value, runtime.config.runId)
    const id = parsed?.id ?? `foreign.${message.offset}`
    const payload = payloadSha256(message.key, message.value, message.headers)
    if (!topicId || !authority) {
      partialEntry(
        runtime,
        {
          client: rig.id,
          group: rig.slot.group,
          handler,
          id,
          topic,
          partition,
          offset: message.offset,
          payloadSha256: payload,
        },
        [...(topicId ? [] : ['topic id']), ...(authority ? [] : ['batch authority'])]
      )
    } else {
      const key = { topic, topicId, partition }
      this.replayCheck(rig, key, message.offset, authority)
      runtime.emit({
        kind: 'identity',
        recordId: runtime.nextId(),
        stage: 'handler-enter',
        client: rig.id,
        group: rig.slot.group,
        handler,
        id,
        key,
        offset: message.offset,
        payloadSha256: payload,
        authority,
        acquisition: null,
        deliveryCount: null,
      })
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
    this.handled++
    if (!topicId || !authority) return
    const k = `${topicId}|${partition}`
    rig.uncommitted.set(k, (rig.uncommitted.get(k) ?? 0) + 1)
    if (rig.uncommitted.get(k) >= valueOf(runtime.matrix.traffic.commitEvery))
      await this.commit(
        rig,
        { key: { topic, topicId, partition }, authority },
        (BigInt(message.offset) + 1n).toString()
      )
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
   * Commits one next offset of a key under the given authority, inside the commit store so the
   * write and publish stages are recorded, then asks for the broker readback. A commit that
   * succeeded settles the offsets the member owed on the key; a failed one leaves them owed.
   * @param {ClientRig} rig
   * @param {{ key: { topic: string, topicId: string, partition: number }, authority: object }} captured
   *   the key and the authority the commit is made under
   * @param {string} next the decimal next offset to commit
   * @returns {Promise<'ok'|'refused'|'unknown'>} the commit's outcome
   */
  async commit(rig, captured, next) {
    const { runtime } = this
    const k = `${captured.key.topicId}|${captured.key.partition}`
    const owed = rig.uncommitted.get(k) ?? 0
    const { topic, partition } = captured.key
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
      const left = (rig.uncommitted.get(k) ?? 0) - owed
      if (left > 0) rig.uncommitted.set(k, left)
      else rig.uncommitted.delete(k)
    } catch (e) {
      outcome =
        e.name === 'KafkaJSCommitFailedError' || e.name === 'KafkaJSStaleAssignmentError'
          ? 'refused'
          : 'unknown'
      errorType = e.name ?? 'Error'
      await runtime.failed(commit, e)
    }
    await runtime.emit({ ...base, recordId: runtime.nextId(), phase: 'end', outcome, errorType })
    await runtime.request({
      type: 'readback',
      client: rig.id,
      group: rig.slot.group,
      commit,
      key: captured.key,
    })
    return outcome
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
   * Journals what one member held of a wave's paused keys when the wave observed the transfer to
   * the extra or the return to the standing member (or, when not observed, at the wave deadline),
   * so the transfer can be audited from the journal. One `wave` record counts the paused and held
   * keys, then one `wave-key` record per paused key cites it and says whether the member held it,
   * so every record stays small however many keys a member holds. Each record takes its own `at`
   * when emitted, so another record of this source emitted between them keeps the source's order.
   * @param {string} execution the wave @param {'transfer'|'return'} event
   * @param {ClientRig} standing @param {ClientRig} member the extra, or the standing member
   * @param {object[]} paused the keys paused in this wave
   * @param {(rig: ClientRig) => object[]} heldBy the paused keys a member holds now
   * @param {boolean} observed whether the event was seen before the deadline
   * @returns {Promise<void>}
   */
  async waveRecord(execution, event, standing, member, paused, heldBy, observed) {
    const { runtime } = this
    const held = new Set(heldBy(member))
    const cause = runtime.nextId()
    await runtime.emit({
      kind: 'wave',
      recordId: cause,
      execution,
      event,
      client: standing.id,
      member: member.id,
      paused: paused.length,
      held: held.size,
      observed,
    })
    for (const key of paused)
      await runtime.emit({
        kind: 'wave-key',
        recordId: runtime.nextId(),
        execution,
        cause,
        key,
        held: held.has(key),
      })
  }

  /**
   * One phase of one membership wave, its own pause-resume execution (`step.execution`). `leave`:
   * pause the standing member's assigned keys, join the extra, wait for the extra to take at least
   * one paused key, remove the extra and wait until the standing member holds every paused key
   * again; the supervisor then takes the closed sample. `resume`: resume the keys paused in
   * `leave`. The first wave of an execution with a restart restarts the static extra with the same
   * static id and a new client id, without leaving. A static extra's final retirement leaves the
   * group explicitly: the default disconnect keeps static membership, so the next wave's extra
   * would rejoin with its old assignment instead of taking a paused key. The wave works to the
   * earlier of its scheduled end and its budget counted from its actual start.
   * @param {{ execution: string, wave: number, phase: 'leave'|'resume', restart?: string[],
   *   waveDeadline?: number }} step `waveDeadline` is the wave's scheduled end on the run clock
   * @returns {Promise<{ transfers: number, returned?: boolean }>} `transfers` is 1 when the wave
   *   observed a transfer; `returned` whether every paused key came back to the standing member
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
    const deadline = Math.min(step.waveDeadline ?? Infinity, runtime.clock.now() + waveMs) - 5000
    const keys = this.assignedKeys(standing)
    this.paused = keys
    await this.operation(standing, 'pause', keys, execution, () =>
      standing.object.pause(byTopic(keys))
    )
    this.extras.push(await this.join(extraSlot, execution))
    const heldBy = (rig) =>
      keys.filter((k) =>
        this.assignedKeys(rig).some((a) => k.topicId === a.topicId && k.partition === a.partition)
      )
    const extra = this.extras.at(-1)
    const transferred = await this.until(() => heldBy(extra).length > 0, deadline)
    await this.waveRecord(execution, 'transfer', standing, extra, keys, heldBy, transferred)
    if (!transferred) await runtime.diagnose(`${execution} saw no transfer of a paused partition`)
    if (step.wave === 0 && step.restart?.includes(extraSlot.clientKind)) {
      const old = this.extras.pop()
      await this.operation(old, 'restart', [], execution, () =>
        old.object.disconnect({ leaveGroup: false })
      )
      runtime.forget(old.id)
      this.extras.push(await this.join(extraSlot, execution))
    }
    const leave = extraSlot.groupInstanceId ? { leaveGroup: true } : undefined
    for (const rig of this.extras.splice(0)) {
      if (leave) await runtime.diagnose(`${execution} retires ${rig.id} with an explicit leave`)
      await rig.retire(execution, leave)
    }
    const returned = await this.until(() => heldBy(standing).length === keys.length, deadline)
    await this.waveRecord(execution, 'return', standing, standing, keys, heldBy, returned)
    if (!returned)
      await runtime.diagnose(`${execution} saw the paused partitions not return to ${standing.id}`)
    return { transfers: transferred ? 1 : 0, returned }
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

  /** @param {ClientRig} rig @param {string} topic @returns {boolean} whether the member reads it */
  reads(rig, topic) {
    return this.topicsFor(rig.slot).some((t) =>
      typeof t === 'string' ? t === topic : t.test(topic)
    )
  }

  /**
   * The topic id the member's group reads for a topic, when it is the run's current incarnation.
   * @param {ClientRig} rig @param {string} topic
   * @returns {string|null} the id, or null when the group has none or still reads another one
   */
  incarnation(rig, topic) {
    const { observer, topicIds } = this.runtime
    const [group] = observer.mine(rig.id, 'ConsumerGroup')
    const topicId = group ? observer.topicIdOf(topic, group) : null
    return topicId && topicId === topicIds.get(topic) ? topicId : null
  }

  /**
   * @param {ClientRig} rig @param {string} topic @param {number} partition
   * @returns {Promise<string|null>} the group's committed next offset of the partition, read from
   *   the broker through the member's public `committed()`; null when none or the read failed
   */
  async committedOf(rig, topic, partition) {
    const [entry] = await rig.object.committed([{ topic, partition }]).catch(() => [])
    return entry?.offset ?? null
  }

  /**
   * Commits the member's live resolved position of one partition it holds and confirms it through
   * the group. The position is the public `position()` of the acquisition the member holds now: the
   * records its handlers resolved and the batches the client resolved with only control or aborted
   * records. It is committed only when the member held the partition, under the same authority and
   * the run's current topic id, both before and after the position was read; the public
   * `committed()` readback must then return exactly that offset. A cached offset of an earlier
   * acquisition or incarnation is never committed.
   * @param {ClientRig} rig @param {string} topic @param {number} partition
   * @param {bigint} [min] commit only a position at or past it
   * @returns {Promise<{ position: string|null, confirmed: boolean }>} the position read, null when
   *   the member does not hold the partition or its acquisition or incarnation moved meanwhile; and
   *   whether that position was committed and read back
   */
  async settle(rig, topic, partition, min = 0n) {
    const { observer } = this.runtime
    const k = `${topic}|${partition}`
    const view = () => {
      const held = rig.object
        .assignment()
        .some((a) => a.topic === topic && a.partitions.includes(partition))
      const [group] = observer.mine(rig.id, 'ConsumerGroup')
      const topicId = this.incarnation(rig, topic)
      if (!held || !topicId || !group?.acquired?.has(k)) return null
      return { topicId, authority: observer.currentAuthority(group, k) }
    }
    const before = view()
    const position = before && (await rig.object.position({ topic, partition }).catch(() => null))
    const after = view()
    if (!position || !after || JSON.stringify(before) !== JSON.stringify(after))
      return { position: null, confirmed: false }
    if (BigInt(position) < min) return { position, confirmed: false }
    const key = { topic, topicId: after.topicId, partition }
    if ((await this.commit(rig, { key, authority: after.authority }, position)) !== 'ok')
      return { position, confirmed: false }
    return { position, confirmed: (await this.committedOf(rig, topic, partition)) === position }
  }

  /**
   * Commits the live position of every partition the members hold, outside any handler, each
   * confirmed by its readback (`settle`). A partition whose acquisition moved is tried again until
   * the deadline; one a member no longer holds is left to its current owner. A drain with `ends`
   * is a drain to those ends (`drainTo`).
   * @param {{ ends?: Record<string, string>, deadline?: number }} [step] `ends` maps `topic|partition`
   *   to the log end the group must commit
   * @returns {Promise<{ reached: boolean, confirmed: number }>} whether every held partition (or
   *   every end) was committed and read back in time, and how many commits were confirmed
   */
  async onDrain({ ends = {}, deadline = this.runtime.clock.now() + DRAIN_MS } = {}) {
    if (Object.keys(ends).length > 0) return this.drainTo(ends, deadline)
    const { clock } = this.runtime
    const pending = new Map()
    for (const rig of this.rigs())
      for (const { topic, partitions } of rig.object.assignment())
        for (const partition of partitions)
          pending.set(`${rig.id} ${topic}|${partition}`, { rig, topic, partition })
    let confirmed = 0
    while (pending.size > 0 && clock.now() < deadline) {
      for (const [id, { rig, topic, partition }] of pending) {
        const held = rig.object
          .assignment()
          .some((a) => a.topic === topic && a.partitions.includes(partition))
        const settled = held ? await this.settle(rig, topic, partition) : null
        if (settled?.confirmed) confirmed++
        if (!held || settled.confirmed) pending.delete(id)
      }
      if (pending.size > 0) await clock.sleepUntil(Math.min(deadline, clock.now() + 250))
    }
    for (const id of pending.keys())
      await this.runtime.diagnose(`drain: ${id} was not committed and read back in time`)
    return { reached: pending.size === 0, confirmed }
  }

  /**
   * A drain to given ends: for each key a member reads, the group's committed offset must reach the
   * log end. It is reached when the group's broker readback already shows the end for the run's
   * current incarnation, or when the member that holds the key commits a live position at or past
   * the end and reads it back (`settle`). Until the deadline every refused, unconfirmed or early
   * attempt is made again.
   * @param {Record<string, string>} ends `topic|partition` to log end
   * @param {number} deadline on the run clock
   * @returns {Promise<{ reached: boolean, confirmed: number }>} whether every key was committed to
   *   its end in time, and how many commits were confirmed
   */
  async drainTo(ends, deadline) {
    const { clock } = this.runtime
    let reached = true
    let confirmed = 0
    for (const [k, end] of Object.entries(ends)) {
      const [topic, partition] = splitKey(k)
      const rigs = this.rigs().filter((rig) => this.reads(rig, topic))
      if (rigs.length === 0) continue
      const target = BigInt(end)
      const atEnd = async () =>
        target === 0n ||
        (this.incarnation(rigs[0], topic) !== null &&
          BigInt((await this.committedOf(rigs[0], topic, partition)) ?? -1) >= target)
      let done = await atEnd()
      while (!done && clock.now() < deadline) {
        for (const rig of rigs) {
          if ((await this.settle(rig, topic, partition, target)).confirmed) {
            confirmed++
            done = true
            break
          }
        }
        if (!done) await clock.sleepUntil(Math.min(deadline, clock.now() + 250))
        done ||= await atEnd()
      }
      if (!done) {
        reached = false
        await this.runtime.diagnose(`drain: ${k} not committed to ${end}`)
      }
    }
    return { reached, confirmed }
  }

  /**
   * The recovery witness: a handler of this process settled after the recovery, and the drain then
   * committed and read back every held partition with at least one confirmed commit.
   * @returns {Promise<{ ok: boolean }>}
   */
  async onRecovered() {
    const before = this.handled
    const deadline = this.runtime.clock.now() + DRAIN_MS
    const handled = await this.until(() => this.handled > before, deadline)
    this.outage = false
    const drained = await this.onDrain({ deadline })
    return { ok: handled && drained.reached && drained.confirmed > 0 }
  }

  /** @param {string} client drops the boundaries remembered for a retired client */
  forgetClient(client) {
    for (const key of [...this.lastBoundary.keys()])
      if (key.startsWith(`${client}/`)) this.lastBoundary.delete(key)
  }

  /** @param {string} topicId drops what is kept for a retired incarnation */
  forgetTopic(topicId) {
    for (const map of [this.progress, this.lastBoundary])
      for (const key of [...map.keys()]) if (key.split('/')[1] === topicId) map.delete(key)
    for (const rig of this.rigs())
      for (const key of [...rig.uncommitted.keys()])
        if (key.startsWith(`${topicId}|`)) rig.uncommitted.delete(key)
  }

  /** @param {object} step marks an outage in progress, so replays are attributed to it */
  async onBrokerOutage() {
    this.outage = true
  }

  /**
   * Starts or clears the seed's retained offset map. A clear with no started map (a run aborted
   * before the seed started) records nothing.
   */
  async perform(step) {
    if (this.seeded && (step.type === 'seed-start' || step.type === 'seed-clear')) {
      const [rig] = this.standing
      const event = step.type === 'seed-start' ? 'start' : 'clear'
      if (event === 'clear' && !this.retained) return undefined
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
      await runtime.failed(operation, e)
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
  /**
   * Joins the standing member and waits until its first share fetch for the main log was answered
   * without error, so the broker has initialized the share partition before any traffic.
   * @returns {Promise<void>}
   * @throws {Error} when no such fetch is answered within 60000 ms
   */
  async begin() {
    const { runtime } = this
    const rig = await this.join(runtime.slots.find((c) => c.purpose === 'subject'))
    this.standing.push(rig)
    if (!(await this.until(() => this.fetchAnswered(rig), runtime.clock.now() + 60000)))
      throw new Error(`${rig.id} had no share fetch of the main log answered within 60000 ms`)
  }

  /**
   * @param {ClientRig} rig a share member
   * @returns {boolean} whether a captured share fetch of the member was answered without error for
   *   partition 0 of the main log (the first answer of a share session lists every partition)
   */
  fetchAnswered(rig) {
    const { runtime } = this
    const main = normalizeTopicId(runtime.topicIds.get(runtime.config.topics.main))
    const answered = (d) =>
      d.errorCode === 0 &&
      d.topics.some(
        (t) =>
          normalizeTopicId(t.topicId) === main &&
          t.partitions.some((p) => p.partitionIndex === 0 && p.fetchErrorCode === 0)
      )
    return runtime.observer.wireFor(rig.id, 78, answered) !== null
  }

  /**
   * Notes that a record of a key was handed to a handler and when. Per key it keeps the next offset
   * below which every offset had a handler entry, and the delivered offsets above it; a gap that
   * would keep more than `SHARE_AHEAD_MAX` offsets ahead is remembered and skipped, so the memory
   * stays bounded and the drain still fails.
   * @param {string} k `topicId|partition`, so a recreated name starts a new key
   * @param {string|number} offset
   * @returns {void}
   */
  delivered(k, offset) {
    this.deliveredAt = this.runtime.clock.now()
    this.entered ??= new Map()
    const state = this.entered.get(k) ?? { next: 0n, ahead: new Set(), gap: null }
    this.entered.set(k, state)
    const at = BigInt(offset)
    if (at >= state.next) state.ahead.add(at)
    if (state.ahead.size > SHARE_AHEAD_MAX) {
      state.gap ??= String(state.next)
      state.next = [...state.ahead].reduce((a, b) => (b < a ? b : a))
    }
    while (state.ahead.delete(state.next)) state.next++
  }

  /** @returns {Promise<ClientRig>} */
  async join(slot, execution = null) {
    const { runtime } = this
    const rig = this.rig(slot)
    const capture = {
      apiKeys: [78, 79],
      onExchange: (exchange) => {
        const where = runtime.wire.append({ client: rig.id, ...exchange })
        runtime.observer.exchanged(rig.id, { ...exchange, file: null, sha256: null, ...where })
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
    const id = parseValue(message.value, runtime.config.runId)?.id ?? `foreign.${message.offset}`
    const payload = payloadSha256(message.key, message.value, message.headers)
    if (!topicId || !delivery) {
      partialEntry(
        runtime,
        {
          client: rig.id,
          group: rig.slot.group,
          handler,
          id,
          topic,
          partition,
          offset: message.offset,
          payloadSha256: payload,
        },
        [...(topicId ? [] : ['topic id']), ...(delivery ? [] : ['share acquisition'])]
      )
    } else {
      runtime.emit({
        kind: 'identity',
        recordId: runtime.nextId(),
        stage: 'handler-enter',
        client: rig.id,
        group: rig.slot.group,
        handler,
        id,
        key: { topic, topicId, partition },
        offset: message.offset,
        payloadSha256: payload,
        authority: null,
        acquisition: delivery.acquisition,
        deliveryCount: delivery.deliveryCount,
      })
    }
    acknowledge('accept')
    this.handled = (this.handled ?? 0) + 1
    if (topicId) this.delivered(`${topicId}|${partition}`, message.offset)
    await runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      stage: 'handler-end',
      handler,
      outcome: 'processed',
      errorType: null,
    })
  }

  /** @param {string} topicId drops the delivered offsets kept for a retired incarnation */
  forgetTopic(topicId) {
    for (const key of [...(this.entered?.keys() ?? [])])
      if (key.startsWith(`${topicId}|`)) this.entered.delete(key)
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
   * Waits until every offset below each log end of a topic the member subscribes has had a handler
   * entry for the run's current incarnation of the topic, and deliveries have been quiet for
   * `quietMs`, so records a lock expiry returned are handed out again first. Every offset is an
   * offered id, so this checks each eligible id, not only the last one.
   * @param {{ ends?: Record<string, string>, deadline?: number, quietMs?: number }} [step]
   * @returns {Promise<{ reached: boolean }>}
   */
  async onDrain({ ends = {}, deadline = Infinity, quietMs = 5000 } = {}) {
    const { clock, config, topicIds } = this.runtime
    const subscribed = new Set([config.topics.main, ...config.topics.recreate])
    const wanted = Object.entries(ends).filter(([k]) => subscribed.has(splitKey(k)[0]))
    const entered = (k) => {
      const [topic, partition] = splitKey(k)
      return this.entered?.get(`${topicIds.get(topic)}|${partition}`) ?? { next: 0n, gap: null }
    }
    const done = () =>
      wanted.every(([k, end]) => entered(k).gap === null && entered(k).next >= BigInt(end)) &&
      clock.now() - (this.deliveredAt ?? -Infinity) >= quietMs
    const reached = wanted.length === 0 || (await this.until(done, deadline))
    for (const [k, end] of wanted) {
      const { next, gap } = entered(k)
      if (gap !== null || next < BigInt(end))
        await this.runtime.diagnose(
          `share drain: ${k} has no handler entry at offset ${gap ?? next}`
        )
    }
    return { reached }
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
      await runtime.failed(`${rig.id}.metadata`, e)
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

  /** Raw scan bytes written so far, against `RAW_SCAN_BYTES`. */
  rawBytes = 0
  /** Opens the raw broker connection a scan reads through. */
  openBroker = openRawBroker

  /**
   * Scans one key and journals each committed record as it arrives, one fetch at a time, so the
   * reader holds no more than one fetch response whatever the interval. Every raw exchange is
   * appended to the scan's raw file and to its digest as it is written; the file is never read back.
   * A scan that would pass the raw byte budget of all scans fails.
   * @param {{ id: string, topic: string, topicId: string, partition: number, from: string,
   *   execution?: string|null }} step one key's scan request
   * @returns {Promise<{ scan: string, to: string }>} the scan id and where it ended
   * @throws {Error} when the scan fails or the raw budget is reached
   */
  async onScan(step) {
    const { runtime } = this
    const [rig] = this.standing
    const scan = `${runtime.slot}.s${runtime.ids}`
    const rawFile = `raw/${scan}.ndjson`
    const raw = path.join(runtime.out, rawFile)
    const digest = crypto.createHash('sha256')
    let full = null
    const { leader } = await this.leaderOf(rig, step.topic, step.partition)
    const broker = await this.openBroker(leader, (exchange) => {
      const line =
        JSON.stringify({
          apiKey: exchange.apiKey,
          request: exchange.request.toString('hex'),
          response: exchange.response.toString('hex'),
        }) + '\n'
      if (this.rawBytes + line.length > RAW_SCAN_BYTES) {
        full ??= `raw scan budget of ${RAW_SCAN_BYTES} bytes reached at ${scan}`
        return
      }
      this.rawBytes += line.length
      fs.appendFileSync(raw, line)
      digest.update(line)
    })
    try {
      const key = { topic: step.topic, topicId: step.topicId, partition: step.partition }
      let records = 0
      const result = await scanPartition(
        broker,
        { ...step, from: BigInt(step.from) },
        async (batch) => {
          if (full) throw new Error(full)
          for (const r of batch) {
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
        }
      )
      if (full) throw new Error(full)
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
        rawSha256: digest.digest('hex'),
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
 * response, whose aborted-transaction list covers that response's range. Each response's committed
 * records are handed to `onRecords`, awaited before the next fetch, and not kept.
 * @param {object} broker a connected `Broker` to the partition leader
 * @param {{ topic: string, partition: number, from: bigint }} read
 * @param {(records: object[]) => Promise<void>} onRecords
 * @returns {Promise<{ to: string, lso: string, highWatermark: string }>}
 * @throws {Error} when fifty fetches in a row make no progress, or `onRecords` throws
 */
const scanPartition = async (broker, { topic, partition, from }, onRecords) => {
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
    await onRecords(
      committedRecords({ records: inRange, abortedTransactions: abortedTransactions ?? [] })
    )
    let next = position
    for (const m of messages) if (BigInt(m.offset) + 1n > next) next = BigInt(m.offset) + 1n
    if (next === position && ++idle >= 50)
      throw new Error(`${topic}-${partition} scan stalled at ${position}`)
    if (next > position) idle = 0
    position = next > lso ? lso : next
  }
  return { to: String(lso), lso: String(lso), highWatermark: String(highWatermark) }
}

/**
 * @param {unknown} id a client instance id, as the client prints it (dashed hex) or the reporter
 *   does (base64url)
 * @returns {string} one form for both, so a push binds to the subscription its client received
 */
const instanceKey = (id) => normalizeTopicId(String(id)) ?? String(id)

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
    this.terminated = new Set()
    this.dir = process.env.TELEMETRY_EVIDENCE_DIR ?? null
  }

  /**
   * Reads the reporters again after the telemetry clients disconnected, until each named client's
   * terminating push is read or the deadline passes, so those pushes reach the stream before the
   * decoder itself disconnects.
   * @param {{ clients: string[], deadline: number }} step the config slots of the disconnected
   *   telemetry clients
   * @returns {Promise<{ abort: string|null, missing: string[] }>} the last tick's abort reason and
   *   the slots none of whose clients' terminating push was read
   */
  async onDrainTelemetry({ clients, deadline }) {
    const { clock } = this.runtime
    const missing = () =>
      clients.filter((slot) => ![...this.terminated].some((id) => id.startsWith(`${slot}.`)))
    let tick = await this.onTick()
    while (!tick.abort && missing().length > 0 && clock.now() < deadline) {
      await clock.sleepUntil(Math.min(deadline, clock.now() + 500))
      tick = await this.onTick()
    }
    return { abort: tick.abort, missing: missing() }
  }

  /** @param {{ entries: object[] }} step subscriptions the clients received */
  async onTelemetrySubscriptions(step) {
    for (const e of step.entries) this.subscriptions.set(instanceKey(e.instanceId), e)
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
      const bytes = Buffer.alloc(Math.min(size - from, TELEMETRY_READ_BYTES))
      fs.readSync(fd, bytes, 0, bytes.length, from)
      fs.closeSync(fd)
      const end = bytes.lastIndexOf(0x0a) + 1
      if (end === 0) {
        if (bytes.length === TELEMETRY_READ_BYTES)
          abort ??= `reporter ${node} line at ${from} exceeds ${TELEMETRY_READ_BYTES} bytes`
        continue
      }
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
        if (push.terminating === true) this.terminated.add(push.clientId)
        const subscription = this.subscriptions.get(instanceKey(push.clientInstanceId))
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
 * The process ends by itself once the `disconnect` command has run and the channel is closed, or,
 * if the supervisor goes away first, once its clients have disconnected. When the channel closes,
 * every readback and record acknowledgement still awaited is settled (a record as refused), so no
 * handler or disconnect waits on a supervisor that is gone; nothing is sent on a closed channel. A
 * runtime that failed to start answers each command with that error.
 * @param {{ child: string, out: string }} options
 * @param {{ proc?: NodeJS.Process, Runtime?: typeof ChildRuntime }} [seams] the process channel and
 *   runtime class, replaced only by tests
 * @returns {void}
 */
function child(options, { proc = process, Runtime = ChildRuntime } = {}) {
  let ready = null
  let leaving = false
  const replies = new Map()
  const acks = new Map()
  const post = (message) => proc.connected && proc.send(message)
  proc.on('disconnect', () => {
    for (const resolve of replies.values()) resolve()
    replies.clear()
    for (const done of acks.values()) done(true)
    acks.clear()
    if (!leaving && ready) ready.then((runtime) => runtime.orphaned()).catch(() => {})
  })
  proc.on('message', async (message) => {
    if (message.type === 'init') {
      ready = (async () => {
        const runtime = new Runtime({
          slot: options.child,
          config: message.config,
          matrix: message.matrix,
          originMs: message.originMs,
          out: options.out,
          send: (m, done) => {
            if (!proc.connected) return done(true)
            acks.set(m.record.seq, done)
            proc.send(m)
          },
          notify: post,
        })
        for (const [topic, topicId] of message.topicIds) runtime.topicIds.set(topic, topicId)
        let requests = 0
        runtime.request = (body) =>
          new Promise((resolve) => {
            if (!proc.connected) return resolve()
            const request = ++requests
            replies.set(request, resolve)
            proc.send({ ...body, request })
          })
        await runtime.start()
        return runtime
      })()
      ready.catch(() => {})
    } else if (message.type === 'ack') {
      acks.get(message.seq)?.(message.refused === true)
      acks.delete(message.seq)
    } else if (message.type === 'reply') {
      replies.get(message.request)?.()
      replies.delete(message.request)
    } else if (message.type === 'command') {
      if (message.step.type === 'disconnect') leaving = true
      const runtime = await ready.catch((e) => {
        post({ type: 'done', id: message.id, error: `start failed: ${e.message}`, reply: null })
        return null
      })
      if (runtime) await runtime.handle(message)
      if (message.step.type === 'disconnect') {
        const pending = () => proc.connected && (runtime?.channel.pendingBytes ?? 0) > 0
        while (pending()) await new Promise((resolve) => setTimeout(resolve, 10))
        if (proc.connected) proc.disconnect()
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
  ClientLog,
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
