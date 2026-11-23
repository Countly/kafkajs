#!/usr/bin/env node
'use strict'

/**
 * Load profile driver: offers a target record rate through producers of each kind, consumes it in
 * one group, optionally runs a fault schedule, then reads every partition once with Apache's own
 * console consumer and judges it with the sequence oracle. Separate from the final soak's acceptance plan.
 *
 * Usage: see `loadProfile.USAGE`; `--recheck <artifact>` judges a sealed artifact again offline.
 * A run needs the repository's three-broker Compose cluster
 * (`docker-compose.yml`, or `COMPOSE_FILE`). Writes the artifact to `--out` and sets the exit code
 * from `verdictOf`: 0 pass, 1 product failure, 2 inconclusive (also when a scheduled fault did not
 * run), 3 rate not reached, 4 flow stalled.
 *
 * Artifact: `profile.json`, `producers.json`, `consumers.json`, `committed.json`, `faults.json`,
 * `apache/<topic>.dump.gz` with `.end` (or one `apache/all.dump.gz` for a profile with topic
 * headers), `apache/<group>.describe`, `throughput.json` (the per-interval series), `report.json`,
 * and `header.json`, written last, which binds every other file by SHA-256 (the seal).
 */

const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const readline = require('node:readline')
const zlib = require('node:zlib')
const { execFile, fork, spawn } = require('node:child_process')
const { promisify } = require('node:util')
const { monitorEventLoopDelay } = require('node:perf_hooks')
const { finished } = require('node:stream/promises')
const {
  BROKER_SERVICES,
  FAULT_SCHEDULES,
  LatencyHistogram,
  OfferSchedule,
  parseLoadArgs,
  flowVerdict,
  phaseCpu,
  phaseList,
  phaseMemory,
  rateLimitedBy,
  rateReport,
  throughputReport,
  verdictOf,
} = require('./loadProfile')
const {
  BrokerCpuProbe,
  ClientMeter,
  FlowMeter,
  HostMeter,
  LagProbe,
  ThroughputSeries,
} = require('./loadMeters')
const { DeliveryLedger, SendLedger, SequenceChecker, parseDumpLine } = require('./sequenceOracle')

const BROKERS = Object.freeze(['localhost:9092', 'localhost:9095', 'localhost:9098'])
const BOOTSTRAP = 'kafka1:29092,kafka2:29095,kafka3:29098'
const TOOLS = '/opt/kafka/bin'
/** How long Apache's console consumer may see no record before it ends a topic's read. */
const READ_IDLE_MS = 10000
/** The idle end of the one read of every topic: a fetch across thousands of partitions starts slower. */
const READ_ALL_IDLE_MS = 30000
/** Topics read at once: each read is one JVM in the broker container. */
const READ_CONCURRENCY = 3
/** Topics per CreateTopics request: keeps each controller operation small with thousands of topics. */
const CREATE_CHUNK = 250
const ADMIN_TIMEOUT_MS = 60000
const SAMPLE_MS = 2000
const COMPRESSION = Object.freeze({ none: 0, gzip: 1, snappy: 2, lz4: 3, zstd: 4 })
const execFileAsync = promisify(execFile)

/** @param {number} ms @param {AbortSignal} [signal] @returns {Promise<void>} */
const realSleep = (ms, signal) =>
  new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    if (signal?.aborted) done()
    else signal?.addEventListener('abort', done, { once: true })
  })

/**
 * Apache's own tools, run in the kafka1 container: independent of the client under test.
 */
class ApacheReader {
  /** @param {{ composeFile: string, spawnTool?: typeof spawn }} options */
  constructor({ composeFile, spawnTool = spawn }) {
    Object.assign(this, { composeFile, spawnTool })
  }

  /**
   * Reads one topic from the beginning, read_committed, every partition, and hands over each line.
   * @param {string} topic
   * @param {(line: string) => void} onLine
   * @returns {Promise<{ exit: number, lines: number, processed: number|null, stderr: string }>}
   *   `processed` is the count the tool printed on stderr
   */
  readTopic(topic, onLine) {
    return this.#consume(['--topic', topic], READ_IDLE_MS, false, onLine)
  }

  /**
   * Reads every topic matching `pattern` in one console consumer, printing each record's headers
   * (the load producer's `topic` header names its topic, which the formatter cannot print).
   * @param {string} pattern a Java regular expression over topic names
   * @param {(line: string) => void} onLine
   * @returns {Promise<{ exit: number, lines: number, processed: number|null, stderr: string }>}
   */
  readAll(pattern, onLine) {
    return this.#consume(['--include', pattern], READ_ALL_IDLE_MS, true, onLine)
  }

  #consume(select, idleMs, headers, onLine) {
    const property = (name) => ['--formatter-property', name]
    return this.#run(
      [
        `${TOOLS}/kafka-console-consumer.sh`,
        '--bootstrap-server',
        BOOTSTRAP,
        ...select,
        '--from-beginning',
        '--isolation-level',
        'read_committed',
        '--timeout-ms',
        String(idleMs),
        ...property('print.partition=true'),
        ...property('print.offset=true'),
        ...(headers ? property('print.headers=true') : []),
        ...property('print.key=true'),
        ...property('print.value=false'),
      ],
      onLine
    ).then((result) => {
      const m = /Processed a total of (\d+) messages/.exec(result.stderr)
      return { ...result, processed: m ? Number(m[1]) : null }
    })
  }

  /**
   * @param {string} group
   * @returns {Promise<{ exit: number, text: string, stderr: string }>} `kafka-consumer-groups.sh --describe`
   */
  async describeGroup(group) {
    const out = []
    const result = await this.#run(
      [
        `${TOOLS}/kafka-consumer-groups.sh`,
        '--bootstrap-server',
        BOOTSTRAP,
        '--describe',
        '--group',
        group,
      ],
      (line) => out.push(line)
    )
    return { exit: result.exit, text: out.join('\n') + '\n', stderr: result.stderr }
  }

  /**
   * @param {string[]} args the command inside the container
   * @param {(line: string) => void} onLine
   * @returns {Promise<{ exit: number, lines: number, stderr: string }>} `exit` is -1 when the tool
   *   could not start
   */
  #run(args, onLine) {
    return new Promise((resolve) => {
      const child = this.spawnTool(
        'docker',
        ['compose', '-f', this.composeFile, 'exec', '-T', 'kafka1', ...args],
        { stdio: ['ignore', 'pipe', 'pipe'] }
      )
      let stderr = ''
      let lines = 0
      child.stderr.on('data', (chunk) => {
        if (stderr.length < 65536) stderr += chunk
      })
      const rl = readline.createInterface({ input: child.stdout, crlfDelay: Infinity })
      rl.on('line', (line) => {
        lines++
        onLine(line)
      })
      let spawnError = null
      child.on('error', (error) => {
        spawnError = error
      })
      child.on('close', (code) => {
        rl.close()
        resolve({
          exit: spawnError ? -1 : (code ?? -1),
          lines,
          stderr: stderr + (spawnError?.message ?? ''),
        })
      })
    })
  }
}

/**
 * Parses `kafka-consumer-groups.sh --describe`: a nine-column header, then one row per partition.
 * @param {string} text @param {string} group
 * @returns {{ committed: Record<string, number>, logEnds: Record<string, number> }} per
 *   `topic/partition`; a partition with no committed offset (`-`) has none
 * @throws {Error} when the header row is missing
 */
const parseDescribe = (text, group) => {
  const body = text.split('\n').filter((l) => l.trim() !== '' && !l.startsWith('Consumer group'))
  const header =
    /^GROUP\s+TOPIC\s+PARTITION\s+CURRENT-OFFSET\s+LOG-END-OFFSET\s+LAG\s+CONSUMER-ID\s+HOST\s+CLIENT-ID$/
  const at = body.findIndex((l) => header.test(l.trim()))
  if (at === -1) throw new Error(`no describe header for group ${group}`)
  const committed = {}
  const logEnds = {}
  for (const line of body.slice(at + 1)) {
    const c = line.trim().split(/\s+/)
    if (c[0] !== group || !/^\d+$/.test(c[2])) continue
    if (/^\d+$/.test(c[3])) committed[`${c[1]}/${c[2]}`] = Number(c[3])
    if (/^\d+$/.test(c[4])) logEnds[`${c[1]}/${c[2]}`] = Number(c[4])
  }
  return { committed, logEnds }
}

const TOPIC_LINE = /^Partition:(\d{1,9})\tOffset:(\d{1,15})\ttopic:([^\t]+)\t(.*)$/

/**
 * Parses one line of the read of every topic: `parseDumpLine` with the `topic` header between the
 * offset and the key.
 * @param {string} line
 * @returns {{ topic: string, partition: number, offset: number, key: string }|null}
 */
const parseTopicLine = (line) => {
  const m = TOPIC_LINE.exec(line)
  return m && { topic: m[3], partition: Number(m[1]), offset: Number(m[2]), key: m[4] }
}

/**
 * One load producer: a client producer of one kind with its offer schedule. A transactional
 * producer sends into the open transaction and ends it every `transactionMs`, aborting every
 * `abortEvery`-th and any whose send failed; a transaction that cannot end recreates the producer
 * as a new incarnation.
 *
 * Its partitions are split into lanes (one, unless `laneOf` names a lane per partition leader);
 * each lane has its share of the rate, its own offer schedule and unsettled cap, and its own send
 * per tick, so a lane whose broker is slow holds back no other lane's records.
 */
class LoadProducer {
  /**
   * @param {object} options
   * @param {object} options.kafka a `Kafka`
   * @param {string} options.id `p<n>`
   * @param {'idempotent'|'plain'|'transactional'} options.kind
   * @param {object} options.profile `parseLoadArgs`
   * @param {string[]} options.topics
   * @param {string} options.runId
   * @param {LatencyHistogram} options.latency
   * @param {{ now: () => number }} options.clock
   * @param {(ms: number) => Promise<void>} options.sleep
   * @param {(cell: { topic: string, partition: number }) => string} [options.laneOf]
   * @param {FlowMeter|null} [options.flow] counts acknowledged records per lane (not for a
   *   transactional producer)
   * @param {ClientMeter|null} [options.meter] listens to every incarnation's client
   * @param {ThroughputSeries|null} [options.series] counts sent, acknowledged (or aborted) and
   *   failed records per interval; per lane too, except for a transactional producer
   */
  constructor({
    kafka,
    id,
    kind,
    profile,
    topics,
    runId,
    latency,
    clock,
    sleep,
    laneOf = () => 'all',
    flow = null,
    meter = null,
    series = null,
  }) {
    Object.assign(this, { kafka, id, kind, profile, topics, runId, latency, clock, sleep, meter })
    this.series = series
    this.flow = kind === 'transactional' ? null : flow
    this.rate = profile.rate / profile.producers
    const cells = topics.flatMap((topic) =>
      Array.from({ length: profile.partitions }, (_, partition) => ({ topic, partition }))
    )
    const byLane = new Map()
    for (const cell of cells) {
      const key = laneOf(cell)
      if (!byLane.has(key)) byLane.set(key, [])
      byLane.get(key).push(cell)
    }
    this.lanes = [...byLane].map(([key, laneCells]) => {
      const rate = (this.rate * laneCells.length) / cells.length
      const cap = Math.max(1, Math.ceil(rate * profile.unsettledSeconds))
      return { key, cells: laneCells, rate, cap, cursor: 0, unsettled: 0, schedule: null }
    })
    this.headers = profile.topicHeader ? new Map(topics.map((t) => [t, { topic: t }])) : null
    this.unsettled = 0
    /** @type {SendLedger[]} */
    this.ledgers = []
    this.stats = { sent: 0, acked: 0, failed: 0, aborted: 0, incarnations: 0, transactions: 0 }
    this.errors = []
    this.values = Array.from({ length: 64 }, () => crypto.randomBytes(profile.recordBytes))
    this.tx = null
    this.txSends = []
    this.txOpening = null
    this.txEnding = null
  }

  /** @returns {number} the producer's `maxInFlightRequests` */
  get maxInFlight() {
    return 5
  }

  /**
   * Creates the client producer as a new incarnation and connects it.
   * @returns {Promise<void>}
   */
  async connect() {
    const options = {
      maxInFlightRequests: this.maxInFlight,
      idempotent: this.kind !== 'plain',
      ...(this.kind === 'transactional' && {
        transactionalId: `${this.runId}-${this.id}-${this.stats.incarnations}`,
      }),
    }
    this.producer = this.kafka.producer(options)
    this.meter?.observe(this.producer)
    this.ledger = new SendLedger({
      producer: this.id,
      incarnation: this.stats.incarnations++,
      kind: this.kind,
      maxInFlight: this.maxInFlight,
    })
    this.ledgers.push(this.ledger)
    await this.producer.connect()
  }

  /** @param {number} start the offer window's start @returns {void} */
  begin(start) {
    for (const lane of this.lanes) lane.schedule = new OfferSchedule({ rate: lane.rate, start })
    this.lastTxAt = start
  }

  /**
   * Sends what each lane's schedule made due, as far as its unsettled cap allows.
   * @param {number} now
   * @returns {void}
   */
  tick(now) {
    if (this.kind === 'transactional' && !this.#transactionReady(now)) return
    for (const lane of this.lanes) this.#tickLane(lane, now)
  }

  /** @param {object} lane @param {number} now @returns {void} */
  #tickLane(lane, now) {
    const { cells } = lane
    const n = Math.min(lane.schedule.due(now), lane.cap - lane.unsettled)
    if (n <= 0) return
    const { dueAt } = lane.schedule.take(n)
    const byTopic = new Map()
    const issued = []
    const per = Math.floor(n / cells.length)
    let extra = n % cells.length
    for (let k = 0; k < cells.length; k++) {
      const cell = cells[(lane.cursor + k) % cells.length]
      const count = per + (extra > 0 ? 1 : 0)
      if (extra > 0) extra--
      if (count === 0) break
      const { first, key } = this.ledger.issue(cell.topic, cell.partition, count)
      issued.push({ ...cell, first, count })
      if (!byTopic.has(cell.topic)) byTopic.set(cell.topic, [])
      const messages = byTopic.get(cell.topic)
      const headers = this.headers?.get(cell.topic)
      for (let i = 0; i < count; i++)
        messages.push({
          partition: cell.partition,
          key: key(i),
          value: this.values[(first + i) & 63],
          ...(headers && { headers }),
        })
    }
    lane.cursor = (lane.cursor + (n % cells.length)) % cells.length
    lane.unsettled += n
    this.unsettled += n
    this.stats.sent += n
    this.series?.count('sent', n, now, this.#seriesGroup(lane))
    const batch = {
      topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })),
      acks: -1,
      compression: COMPRESSION[this.profile.compression],
    }
    const target = this.kind === 'transactional' ? this.tx : this.producer
    const ledger = this.ledger
    const send = target.sendBatch(batch).then(
      (metadata) => ({ ok: true, metadata }),
      (error) => ({ ok: false, error })
    )
    const entry = { send, issued, ledger, n, dueAt, lane }
    if (this.kind === 'transactional') {
      this.txSends.push(entry)
      return
    }
    send.then((result) => this.#settle(entry, result.ok ? 'acked' : 'failed', result))
  }

  /**
   * Waits for every send, and ends an open transaction by committing it. Polls on the run's clock.
   * @param {number} budgetMs
   * @returns {Promise<boolean>} whether everything settled in time
   */
  async join(budgetMs) {
    const deadline = this.clock.now() + budgetMs
    const until = async (done) => {
      while (!done() && this.clock.now() < deadline) await this.sleep(100)
      return done()
    }
    if (!(await until(() => !this.txOpening && !this.txEnding))) return false
    if (this.tx)
      this.txEnding = this.#endTransaction(false).finally(() => {
        this.txEnding = null
      })
    return until(() => this.unsettled === 0 && !this.txEnding && !this.txOpening)
  }

  /** @returns {Promise<void>} */
  async close() {
    await this.producer?.disconnect()
  }

  /** @returns {object[]} `SendLedger#toJSON` of every incarnation, in order */
  ledgerReports() {
    return this.ledgers.map((l) => l.toJSON())
  }

  /** @param {number} now @returns {boolean} whether a transaction is open for sends */
  #transactionReady(now) {
    if (this.txEnding || this.txOpening) return false
    if (!this.tx) {
      this.txOpening = this.producer
        .transaction()
        .then((tx) => {
          this.tx = tx
          this.lastTxAt = this.clock.now()
        })
        .catch((error) => this.#recordError('transaction', error))
        .finally(() => {
          this.txOpening = null
        })
      return false
    }
    if (now - this.lastTxAt >= this.profile.transactionMs) {
      const abort = (this.stats.transactions + 1) % this.profile.abortEvery === 0
      this.txEnding = this.#endTransaction(abort).finally(() => {
        this.txEnding = null
      })
      return false
    }
    return true
  }

  /**
   * Ends the open transaction once its sends settled, and settles their records by its outcome.
   * @param {boolean} abort
   * @returns {Promise<void>}
   */
  async #endTransaction(abort) {
    const tx = this.tx
    const sends = this.txSends
    this.tx = null
    this.txSends = []
    this.stats.transactions++
    const results = await Promise.all(sends.map((entry) => entry.send))
    const failedSend = results.some((r) => !r.ok)
    let outcome
    try {
      if (abort || failedSend) {
        await tx.abort()
        outcome = 'aborted'
      } else {
        await tx.commit()
        outcome = 'acked'
      }
    } catch (error) {
      this.#recordError(abort || failedSend ? 'abort' : 'commit', error)
      outcome = 'failed'
    }
    sends.forEach((entry, i) => this.#settle(entry, outcome, results[i]))
    if (outcome === 'failed')
      await this.#recreate().catch((error) => this.#recordError('recreate', error))
  }

  /** @returns {Promise<void>} replaces a producer whose transaction could not end */
  async #recreate() {
    try {
      await this.producer.disconnect()
    } catch (error) {
      this.#recordError('disconnect', error)
    }
    await this.connect()
  }

  /**
   * @param {{ issued: object[], ledger: SendLedger, n: number, dueAt: (i: number) => number, lane: object }} entry
   * @param {'acked'|'failed'|'aborted'} status
   * @param {{ ok: boolean, metadata?: object[], error?: Error }} result the send's own result
   * @returns {void}
   */
  #settle(entry, status, result) {
    const offsets = new Map()
    for (const m of result.metadata ?? [])
      offsets.set(`${m.topicName}/${m.partition}`, Number(m.baseOffset))
    for (const { topic, partition, first } of entry.issued)
      entry.ledger.settle(topic, partition, first, status, offsets.get(`${topic}/${partition}`))
    entry.lane.unsettled -= entry.n
    this.unsettled -= entry.n
    this.stats[status] += entry.n
    const now = this.clock.now()
    this.series?.count(
      status === 'failed' ? 'failed' : 'acked',
      entry.n,
      now,
      this.#seriesGroup(entry.lane)
    )
    if (status === 'acked') {
      this.flow?.add('acked', entry.lane.key, entry.n, now)
      for (let i = 0; i < entry.n; i++) this.latency.record(now - entry.dueAt(i))
    } else if (!result.ok) this.#recordError('send', result.error)
  }

  /**
   * A transaction waits for every partition it wrote, so its records count in `all` only.
   * @param {{ key: string }} lane @returns {string} the series group of the lane's records
   */
  #seriesGroup(lane) {
    return this.kind === 'transactional' ? 'all' : lane.key
  }

  /** @param {string} where @param {Error} error @returns {void} */
  #recordError(where, error) {
    if (this.errors.length < 20) this.errors.push(`${where}: ${error?.name}: ${error?.message}`)
  }
}

/** How long a producer process may take to connect its producers. */
const PROCESS_CONNECT_MS = 120000
/** How much longer than the join budget the parent waits for a producer process's report. */
const PROCESS_REPORT_MARGIN_MS = 30000
/** How long a producer process may take to disconnect its producers and exit. */
const PROCESS_CLOSE_MS = 30000

/**
 * The parent side of one producer process (`loadProducerChild.js`): forks it, hands it its
 * producers, and stands in for them in the run. Its `views` carry each hosted producer's id, kind,
 * rate, stats, errors and ledger reports, which the child's messages fill in; `reported` turns true
 * once the child sent its ledgers.
 */
class ProducerProcess {
  /**
   * @param {object} options
   * @param {string} options.id `process-<n>`
   * @param {{ id: string, kind: string }[]} options.producers the producers it hosts
   * @param {object} options.profile @param {string} options.runId @param {string[]} options.topics
   * @param {LatencyHistogram} options.latency the run's, which the child's histogram is merged into
   * @param {{ now: () => number, timeOrigin?: number }} options.clock
   * @param {(ms: number) => Promise<void>} options.sleep
   * @param {() => object} options.fork starts the child: an object with `send`, `on` and `kill`
   * @param {ThroughputSeries|null} [options.series] the run's, which the child's series is merged
   *   into; the child's own is kept as `seriesSnapshot`
   */
  constructor({
    id,
    producers,
    profile,
    runId,
    topics,
    latency,
    clock,
    sleep,
    fork: start,
    series = null,
  }) {
    Object.assign(this, { id, profile, runId, topics, latency, clock, sleep, start, series })
    this.seriesSnapshot = null
    this.hosted = producers
    this.views = producers.map(({ id: producerId, kind }) => ({
      id: producerId,
      kind,
      rate: profile.rate / profile.producers,
      stats: { sent: 0, acked: 0, failed: 0, aborted: 0, incarnations: 0, transactions: 0 },
      errors: [],
      ledgers: [],
      settled: false,
      ledgerReports() {
        return this.ledgers
      },
    }))
    Object.assign(this, { ready: false, reported: false, closed: false, exited: false })
    this.failure = null
    this.cpu = null
  }

  /**
   * Starts the child and waits until it connected its producers.
   * @returns {Promise<void>}
   * @throws {Error} when the child failed, exited or did not connect in time
   */
  async connect() {
    this.child = this.start()
    this.child.on('message', (message) => this.#receive(message))
    this.child.on('exit', (code) => {
      this.exited = true
      if (!this.closed) this.failure ??= `exited with code ${code}`
    })
    const { profile, runId, topics, hosted } = this
    this.child.send({ type: 'setup', profile, runId, topics, producers: hosted })
    if (!(await this.#until(() => this.ready, PROCESS_CONNECT_MS)))
      throw new Error(`${this.id}: producers not connected after ${PROCESS_CONNECT_MS} ms`)
  }

  /** @param {number} start the window's start on the run's clock @returns {void} */
  begin(start) {
    this.child.send({ type: 'begin', startEpochMs: start + (this.clock.timeOrigin ?? 0) })
  }

  /** The child offers on its own schedule. @returns {void} */
  tick() {}

  /**
   * Waits for the child's report, sent once it joined its producers.
   * @param {number} budgetMs the producers' join budget
   * @returns {Promise<boolean>} whether it reported and every one of its producers settled
   * @throws {Error} when the child failed or exited first
   */
  async join(budgetMs) {
    const reported = await this.#until(() => this.reported, budgetMs + PROCESS_REPORT_MARGIN_MS)
    return reported && this.views.every((v) => v.settled)
  }

  /**
   * Asks the child to disconnect its producers and waits for it, then ends it if it is still
   * running.
   * @returns {Promise<void>}
   */
  async close() {
    if (!this.child || this.exited) return
    this.child.send({ type: 'close' })
    const deadline = this.clock.now() + PROCESS_CLOSE_MS
    while (!this.closed && !this.exited && !this.failure && this.clock.now() < deadline)
      await this.sleep(100)
    if (!this.exited) this.child.kill()
    if (!this.closed) throw new Error(`${this.id}: ${this.failure ?? 'did not close in time'}`)
  }

  #receive(message) {
    if (message.type === 'ready') this.ready = true
    else if (message.type === 'progress')
      message.sent.forEach((n, i) => {
        this.views[i].stats.sent = n
      })
    else if (message.type === 'done') {
      message.producers.forEach(({ stats, errors, ledgers, settled }, i) =>
        Object.assign(this.views[i], { stats, errors, ledgers, settled })
      )
      this.latency.merge(message.latency)
      this.seriesSnapshot = message.series
      this.series?.merge(message.series)
      this.cpu = message.cpu
      this.reported = true
    } else if (message.type === 'closed') this.closed = true
    else if (message.type === 'failed') this.failure ??= message.error
  }

  /**
   * @param {() => boolean} done @param {number} budgetMs
   * @returns {Promise<boolean>} `done()` at the end
   * @throws {Error} the child's failure
   */
  async #until(done, budgetMs) {
    const deadline = this.clock.now() + budgetMs
    while (!done() && !this.failure && this.clock.now() < deadline) await this.sleep(100)
    if (this.failure && !done()) throw new Error(`${this.id}: ${this.failure}`)
    return done()
  }
}

/**
 * One member of the load group, recording what it was delivered. Commits are the client's own
 * (auto-commit after each batch is resolved, and on disconnect).
 */
class LoadConsumer {
  /**
   * @param {object} options
   * @param {object} options.kafka @param {string} options.groupId @param {string} options.member
   * @param {string[]} options.topics @param {{ now: () => number }} options.clock
   * @param {((cell: { topic: string, partition: number }) => string)|null} [options.laneOf]
   * @param {FlowMeter|null} [options.flow] counts delivered records per lane, with `laneOf`
   * @param {ClientMeter|null} [options.meter]
   * @param {ThroughputSeries|null} [options.series] counts consumed records per interval, per lane
   *   with `laneOf`
   */
  constructor({
    kafka,
    groupId,
    member,
    topics,
    clock,
    laneOf = null,
    flow = null,
    meter = null,
    series = null,
  }) {
    Object.assign(this, { kafka, groupId, member, topics, clock, laneOf, flow, meter, series })
    this.ledger = new DeliveryLedger({ group: groupId, member })
    this.delivered = 0
    this.lastDeliveryAt = clock.now()
    this.crashes = 0
  }

  /** @returns {Promise<void>} */
  async start() {
    this.consumer = this.kafka.consumer({ groupId: this.groupId })
    this.consumer.on(this.consumer.events.CRASH, () => {
      this.crashes++
    })
    this.meter?.observe(this.consumer, this.member)
    await this.consumer.connect()
    this.meter?.subscribing(this.member)
    await this.consumer.subscribe({ topics: this.topics, fromBeginning: true })
    await this.consumer.run({
      eachBatch: async ({ batch }) => {
        for (const message of batch.messages)
          this.ledger.deliver(batch.topic, batch.partition, Number(message.offset))
        this.delivered += batch.messages.length
        this.lastDeliveryAt = this.clock.now()
        const lane = this.laneOf?.(batch) ?? 'all'
        this.flow?.add('delivered', lane, batch.messages.length, this.lastDeliveryAt)
        this.series?.count('consumed', batch.messages.length, this.lastDeliveryAt, lane)
      },
    })
  }

  /** @returns {Promise<void>} */
  async stop() {
    await this.consumer?.disconnect()
  }
}

/**
 * One run of the load profile.
 */
class LoadRun {
  /**
   * @param {object} options
   * @param {object} options.profile `parseLoadArgs`
   * @param {object} options.kafka a `Kafka` for the clients under test
   * @param {{ readTopic: Function, describeGroup: Function }} options.reader an `ApacheReader`
   * @param {{ rollingBounce: Function, step: Function }|null} [options.bouncer] a
   *   `BrokerBouncer`, for a schedule that restarts or pauses brokers
   * @param {{ check: Function, apply: Function, clear: Function }|null} [options.netem] a
   *   `NetemDelay`, for the slow-broker schedule
   * @param {{ start: Function, stop: Function, maxBetween: Function }|null} [options.lagProbe] a
   *   `LagProbe`; one on the run's clock by default
   * @param {{ now: () => number }} [options.clock]
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} [options.sleep]
   * @param {(line: string) => void} [options.log]
   * @param {string|null} [options.treeSha] the commit the run is of, for the seal
   * @param {HostMeter} [options.host] this process's and the machine's CPU
   * @param {{ start: Function, stop: Function, toJSON: Function }|null} [options.brokerCpu] a
   *   `BrokerCpuProbe`, run while the window is open
   * @param {() => object} [options.fork] starts one producer process (`ProducerProcess`)
   */
  constructor({
    profile,
    kafka,
    reader,
    bouncer = null,
    netem = null,
    lagProbe = null,
    clock = performance,
    sleep = realSleep,
    log = (line) => console.log(line),
    treeSha = null,
    host = new HostMeter(),
    brokerCpu = null,
    fork: start = forkProducerChild,
  }) {
    Object.assign(this, { profile, kafka, reader, bouncer, netem, clock, sleep, log, treeSha })
    Object.assign(this, { host, brokerCpu, fork: start })
    this.lagProbe = lagProbe ?? new LagProbe({ clock })
    this.runId = `load-${Date.now()}`
    this.topics = Array.from({ length: profile.topics }, (_, i) => `${this.runId}-${i}`)
    this.groupId = `${this.runId}-group`
    this.latency = new LatencyHistogram()
    this.errors = []
  }

  /**
   * Checks the fault mechanism, then sets up, offers, drains, reads and judges; writes and seals
   * the artifact.
   * @returns {Promise<object>} the report, with `verdict`
   * @throws {Error} the fault schedule's `check`, before anything is created
   */
  async run() {
    const { profile } = this
    const schedule = FAULT_SCHEDULES[profile.faults]
    await schedule.check?.({ netem: this.netem, bouncer: this.bouncer })
    fs.mkdirSync(path.join(profile.out, 'apache'), { recursive: true })
    this.#write('profile.json', { ...profile, runId: this.runId, topics: this.topics })
    const admin = this.kafka.admin()
    const meter = new ClientMeter({ clock: this.clock, lag: this.lagProbe })
    this.lagProbe.start(meter.origin)
    this.flow = profile.leaderLanes ? new FlowMeter() : null
    this.series = ThroughputSeries.forProfile(profile)
    const setup = { createTopicsMs: null, leaders: null }
    let producers = []
    let units = []
    let processes = []
    let consumers = []
    const resources = { samples: [], maxHeapUsed: 0, maxRss: 0 }
    const loop = monitorEventLoopDelay({ resolution: 20 })
    let totals = { offered: 0, sent: 0, acked: 0, failed: 0 }
    let faults = []
    let oracle = null
    let windowMs = profile.durationSeconds * 1000
    /** Host reads at the end of each stage, with the clock: the offer window, the join, the drain. */
    const stages = {}
    const mark = (name) => {
      stages[name] = { at: this.clock.now(), ...this.host.read() }
    }
    try {
      await admin.connect()
      setup.createTopicsMs = await this.#createTopics(admin)
      let laneOf
      if (profile.leaderLanes) {
        const leaders = await this.#leaders(admin)
        laneOf = ({ topic, partition }) => leaders.get(`${topic}/${partition}`) ?? 'unknown'
        setup.leaders = {}
        for (const node of leaders.values()) setup.leaders[node] = (setup.leaders[node] ?? 0) + 1
      }
      ;({ producers, units, processes } = this.#producers(laneOf, meter))
      consumers = Array.from(
        { length: profile.consumers },
        (_, i) =>
          new LoadConsumer({
            kafka: this.kafka,
            groupId: this.groupId,
            member: `c${i}`,
            topics: this.topics,
            clock: this.clock,
            laneOf,
            flow: this.flow,
            meter,
            series: this.series,
          })
      )
      for (const consumer of consumers) await consumer.start()
      for (const unit of units) await unit.connect()
      loop.enable()
      mark('start')
      const result = await this.#offer(producers, units, admin, resources)
      mark('offer')
      ;({ faults, windowMs } = result)
      totals = result.totals
      for (const unit of units)
        if (!(await unit.join(profile.joinSeconds * 1000)))
          this.errors.push(`${unit.id}: sends still unsettled after ${profile.joinSeconds} s`)
      mark('join')
      totals.sent = producers.reduce((n, p) => n + p.stats.sent, 0)
      for (const producer of producers) {
        totals.acked += producer.stats.acked + producer.stats.aborted
        totals.failed += producer.stats.failed
      }
      await this.#drain(consumers)
      mark('drain')
    } catch (error) {
      this.errors.push(`driver: ${error?.stack ?? error}`)
    } finally {
      loop.disable()
      this.lagProbe.stop()
      for (const client of [...units, ...consumers]) {
        try {
          await (client.close?.() ?? client.stop())
        } catch (error) {
          this.errors.push(`close: ${error?.message}`)
        }
      }
    }
    const producerReports = producers.flatMap((p) => p.ledgerReports())
    const consumerReports = consumers.map((c) => c.ledger.toJSON())
    this.#write('producers.json', producerReports)
    this.#write('consumers.json', consumerReports)
    this.#write('faults.json', faults)
    const unreported = processes.filter((p) => !p.reported).map((p) => p.id)
    if (unreported.length)
      this.errors.push(`read: not judged, no send ledgers from ${unreported.join(', ')}`)
    else
      try {
        oracle = await this.#judge(producerReports, consumerReports)
      } catch (error) {
        oracle = error.verdict ?? null
        this.errors.push(`read: ${error?.message}`)
      }
    try {
      await admin.deleteTopics({ topics: this.topics, timeout: ADMIN_TIMEOUT_MS })
    } catch (error) {
      this.log(`topic cleanup failed: ${error?.message}`)
    }
    await admin.disconnect().catch(() => {})
    const phases = phaseList(faults, windowMs)
    const cpu = this.#cpuReport(stages, resources.samples, phases)
    const rate = rateReport({
      rate: profile.rate,
      windowMs,
      targetFraction: profile.targetFraction,
      ...totals,
    })
    rate.limitedBy = rateLimitedBy({
      reached: rate.reached,
      coreShares: [
        cpu.stages.offer?.coreShare ?? null,
        ...processes.map((p) => p.cpu?.offer.coreShare ?? null),
      ],
      machineBusyShare: cpu.stages.offer?.machineBusyShare ?? null,
    })
    if (processes.length)
      cpu.producerProcesses = processes.map((p) => ({
        id: p.id,
        producers: p.views.map((v) => v.id),
        offer: p.cpu?.offer ?? null,
        join: p.cpu?.join ?? null,
      }))
    const flow = this.flow && this.#flowVerdict(producers, phases)
    const throughput = throughputReport({
      series: this.series.snapshot(),
      rates: this.#seriesRates(producers),
      windowMs,
      faults,
    })
    this.#write('throughput.json', {
      ...throughput.series,
      processes: Object.fromEntries(processes.map((p) => [p.id, p.seriesSnapshot])),
    })
    const verdict = verdictOf({
      oracle,
      rate,
      error: this.errors[0] ?? null,
      flow: flow?.verdict ?? null,
      faults,
    })
    const report = {
      verdict,
      rate,
      totals,
      ...(flow && { flow }),
      throughput: throughput.summary,
      latencyMs: this.latency.toJSON(),
      eventLoopDelayMs: {
        p50: loop.percentile(50) / 1e6,
        p99: loop.percentile(99) / 1e6,
        max: loop.max / 1e6,
      },
      resources,
      memory: phaseMemory(resources.samples, phases),
      cpu,
      cluster: { ...setup, ...meter.toJSON() },
      oracle,
      producers: producers.map((p) => ({
        id: p.id,
        kind: p.kind,
        stats: p.stats,
        errors: p.errors,
      })),
      consumers: consumers.map((c) => ({
        member: c.ledger.member,
        delivered: c.delivered,
        crashes: c.crashes,
      })),
      faults: faults.map(({ phase, service, kind, startMs, endMs, outageMs, skipped }) => ({
        phase,
        service,
        kind,
        startMs,
        endMs,
        outageMs,
        skipped,
      })),
      errors: this.errors,
    }
    this.#write('report.json', report)
    this.#seal()
    return report
  }

  /**
   * The run's producers: in this process, or with `producerProcesses` above 1 spread over that
   * many child processes, producer `p<i>` in process `i % producerProcesses`.
   * @param {Function} [laneOf] @param {ClientMeter} meter
   * @returns {{ producers: object[], units: object[], processes: ProducerProcess[] }} `producers`
   *   carry each producer's id, kind, rate, stats, errors and ledger reports, in order; `units` are
   *   what connects, begins, ticks, joins and closes (the producers themselves, or the processes)
   */
  #producers(laneOf, meter) {
    const { profile } = this
    const specs = Array.from({ length: profile.producers }, (_, i) => ({
      id: `p${i}`,
      kind: profile.producerKinds[i % profile.producerKinds.length],
    }))
    if (profile.producerProcesses === 1) {
      const producers = specs.map(
        ({ id, kind }) =>
          new LoadProducer({
            kafka: this.kafka,
            id,
            kind,
            profile,
            topics: this.topics,
            runId: this.runId,
            latency: this.latency,
            clock: this.clock,
            sleep: this.sleep,
            laneOf,
            flow: this.flow,
            meter,
            series: this.series,
          })
      )
      return { producers, units: producers, processes: [] }
    }
    const processes = Array.from(
      { length: profile.producerProcesses },
      (_, n) =>
        new ProducerProcess({
          id: `process-${n}`,
          producers: specs.filter((_, i) => i % profile.producerProcesses === n),
          profile,
          runId: this.runId,
          topics: this.topics,
          latency: this.latency,
          clock: this.clock,
          sleep: this.sleep,
          fork: this.fork,
          series: this.series,
        })
    )
    const byId = new Map(processes.flatMap((p) => p.views.map((v) => [v.id, v])))
    return { producers: specs.map(({ id }) => byId.get(id)), units: processes, processes }
  }

  /**
   * The offer window: ticks every producer on a deadline schedule while the fault schedule runs.
   * `totals.sent` is what this process saw sent by the window's end; the run takes the producers'
   * final count after the join, which a producer process reports only then.
   * @param {object[]} producers see `#producers` @param {object[]} units
   * @param {object} admin @param {object} resources
   * @returns {Promise<{ totals: { offered: number, sent: number, acked: number, failed: number }, faults: object[], windowMs: number }>}
   */
  async #offer(producers, units, admin, resources) {
    const { profile } = this
    const start = this.clock.now()
    const windowMs = profile.durationSeconds * 1000
    const end = start + windowMs
    const tickMs = 1000 / profile.ticksPerSecond
    for (const unit of units) unit.begin(start)
    this.flow?.begin(start)
    this.series.begin(start)
    const window = new AbortController()
    this.brokerCpu?.start(window.signal)
    const fault = FAULT_SCHEDULES[profile.faults]
      .start({
        bouncer: this.bouncer,
        netem: this.netem,
        admin,
        topics: this.topics,
        windowMs,
        windowStart: start,
        clock: this.clock,
        signal: window.signal,
        sleep: this.sleep,
      })
      .catch((error) => {
        this.errors.push(`fault schedule: ${error?.message}`)
        return []
      })
    const sample = (now) => {
      const { heapUsed, rss } = process.memoryUsage()
      resources.maxHeapUsed = Math.max(resources.maxHeapUsed, heapUsed)
      resources.maxRss = Math.max(resources.maxRss, rss)
      const sent = producers.reduce((s, p) => s + p.stats.sent, 0)
      const atMs = Math.round(now - start)
      resources.samples.push({ atMs, heapUsed, rss, sent, ...this.host.read() })
    }
    sample(start)
    let nextSample = start + SAMPLE_MS
    for (let n = 1; ; n++) {
      const now = Math.min(this.clock.now(), end)
      for (const unit of units) unit.tick(now)
      if (now >= nextSample) {
        nextSample += SAMPLE_MS
        sample(now)
      }
      if (now >= end) break
      await this.sleep(Math.max(0, Math.min(end, start + n * tickMs) - this.clock.now()))
    }
    window.abort()
    await this.brokerCpu?.stop()
    const faults = await fault
    const offered = producers.reduce((s, p) => s + Math.floor((windowMs * p.rate) / 1000), 0)
    const sent = producers.reduce((s, p) => s + p.stats.sent, 0)
    return { totals: { offered, sent, acked: 0, failed: 0 }, faults, windowMs }
  }

  /**
   * What the CPU did: this process per stage (the offer window, the join, the drain; producers and
   * consumers share the process, and the drain is the consumers' alone) and per fault phase, the
   * machine's cores and load average, and the broker containers when they were probed.
   * @param {Record<string, object>} stages `HostMeter#read` with `at`, at the end of each stage
   * @param {object[]} samples the window's samples @param {object[]} phases `phaseList`
   * @returns {object}
   */
  #cpuReport(stages, samples, phases) {
    const order = ['start', 'offer', 'join', 'drain']
    const spans = {}
    for (let i = 1; i < order.length; i++) {
      const [from, to] = [stages[order[i - 1]], stages[order[i]]]
      spans[order[i]] = from && to ? HostMeter.between(from, to, to.at - from.at) : null
    }
    const loads = samples.map((s) => s.load1).filter(Number.isFinite)
    return {
      cpus: this.host.cpus,
      loadAverage1m: { max: loads.length ? Math.max(...loads) : null, end: loads.at(-1) ?? null },
      stages: spans,
      phases: phaseCpu(samples, phases).map(({ phase, coreShare, machineBusyShare }) => ({
        phase,
        coreShare,
        machineBusyShare,
      })),
      brokers: this.brokerCpu?.toJSON() ?? null,
    }
  }

  /**
   * Creates the topics, `CREATE_CHUNK` per request, each waiting for its leaders.
   * @param {object} admin
   * @returns {Promise<number>} how long it took, ms
   */
  async #createTopics(admin) {
    const { profile } = this
    const began = this.clock.now()
    for (let i = 0; i < this.topics.length; i += CREATE_CHUNK)
      await admin.createTopics({
        waitForLeaders: true,
        timeout: ADMIN_TIMEOUT_MS,
        topics: this.topics.slice(i, i + CREATE_CHUNK).map((topic) => ({
          topic,
          numPartitions: profile.partitions,
          replicationFactor: profile.replicationFactor,
          configEntries: [
            { name: 'min.insync.replicas', value: String(profile.minInsyncReplicas) },
          ],
        })),
      })
    return this.clock.now() - began
  }

  /**
   * @param {object} admin
   * @returns {Promise<Map<string, string>>} each `topic/partition`'s leader node id before the window
   */
  async #leaders(admin) {
    const { topics } = await admin.fetchTopicMetadata({ topics: this.topics })
    return new Map(
      topics.flatMap(({ name, partitions }) =>
        partitions.map(({ partitionId, leader }) => [`${name}/${partitionId}`, String(leader)])
      )
    )
  }

  /**
   * @param {LoadProducer[]} producers @param {object[]} phases `phaseList`
   * @returns {object} `flowVerdict` over the idempotent and plain producers' lanes
   */
  #flowVerdict(producers, phases) {
    const slowNode = String(FAULT_SCHEDULES[this.profile.faults].nodeId)
    return flowVerdict({ phases, flow: this.flow, rates: laneRates(producers), slowNode })
  }

  /**
   * @param {object[]} producers see `#producers`
   * @returns {Record<string, number>} records a second offered per series group: every producer's
   *   in `all`, the idempotent and plain producers' per leader lane
   */
  #seriesRates(producers) {
    const all = producers.reduce((n, p) => n + p.rate, 0)
    const lanes = this.profile.leaderLanes ? laneRates(producers) : {}
    return { ...lanes, all }
  }

  /**
   * Waits until no member was delivered anything for `drainQuietSeconds`, at most `drainSeconds`.
   * Pacing only: the judgement is the oracle's.
   * @param {LoadConsumer[]} consumers
   * @returns {Promise<void>}
   */
  async #drain(consumers) {
    const { drainQuietSeconds, drainSeconds } = this.profile
    const deadline = this.clock.now() + drainSeconds * 1000
    for (;;) {
      const now = this.clock.now()
      const last = Math.max(...consumers.map((c) => c.lastDeliveryAt))
      if (now - last >= drainQuietSeconds * 1000) return
      if (now >= deadline) {
        this.errors.push(`consumers still receiving after ${drainSeconds} s of drain`)
        return
      }
      await this.sleep(1000)
    }
  }

  /**
   * Describes the group and reads every topic with Apache's tools, streaming each line into the
   * checker and into the gzipped dump.
   * @param {object[]} producers @param {object[]} consumers
   * @returns {Promise<object>} `SequenceChecker#finish`, with `size` and per-topic read facts
   * @throws {Error} when a tool failed or a dump's line count differs from the tool's own count
   */
  async #judge(producers, consumers) {
    const describe = await this.reader.describeGroup(this.groupId)
    fs.writeFileSync(path.join(this.profile.out, `apache/${this.groupId}.describe`), describe.text)
    if (describe.exit !== 0)
      throw new Error(`describe exited ${describe.exit}: ${describe.stderr.slice(0, 200)}`)
    const described = parseDescribe(describe.text, this.groupId)
    const committed = { [this.groupId]: described.committed }
    this.#write('committed.json', { committed, logEnds: described.logEnds })
    const checker = new SequenceChecker({
      producers,
      consumers,
      committed,
      logEnds: described.logEnds,
    })
    const reads = []
    const failures = []
    if (this.profile.topicHeader) {
      await this.#readAll(checker, reads, failures)
      const verdict = { ...checker.finish(), size: checker.size(), reads }
      if (failures.length)
        throw Object.assign(new Error(`read failed: ${failures.join('; ')}`), { verdict })
      return verdict
    }
    const queue = [...this.topics]
    const worker = async () => {
      for (let topic = queue.shift(); topic; topic = queue.shift()) {
        const began = this.clock.now()
        const file = path.join(this.profile.out, `apache/${topic}.dump.gz`)
        const gzip = zlib.createGzip()
        const sink = fs.createWriteStream(file)
        gzip.pipe(sink)
        const result = await this.reader.readTopic(topic, (line) => {
          gzip.write(line + '\n')
          const parsed = parseDumpLine(line)
          if (parsed) checker.observe(topic, parsed.partition, parsed.offset, parsed.key)
          else checker.unparseable(topic, line)
        })
        gzip.end()
        await finished(sink)
        const { exit, lines, processed } = result
        fs.writeFileSync(`${file}.end`, `exit=${exit} lines=${lines} processed=${processed}\n`)
        reads.push({ topic, exit, lines, processed, seconds: (this.clock.now() - began) / 1000 })
        if (exit !== 0 || processed !== lines)
          failures.push(`${topic}: exit ${exit}, ${lines} lines, tool counted ${processed}`)
      }
    }
    await Promise.all(Array.from({ length: READ_CONCURRENCY }, worker))
    const verdict = { ...checker.finish(), size: checker.size(), reads }
    if (failures.length)
      throw Object.assign(new Error(`read failed: ${failures.join('; ')}`), { verdict })
    return verdict
  }

  /**
   * Reads every topic of the run in one console consumer into `apache/all.dump.gz`.
   * @param {SequenceChecker} checker @param {object[]} reads @param {string[]} failures
   * @returns {Promise<void>}
   */
  async #readAll(checker, reads, failures) {
    const began = this.clock.now()
    const file = path.join(this.profile.out, 'apache/all.dump.gz')
    const gzip = zlib.createGzip()
    const sink = fs.createWriteStream(file)
    gzip.pipe(sink)
    const result = await this.reader.readAll(`^${this.runId}-\\d+$`, (line) => {
      gzip.write(line + '\n')
      observeTopicLine(checker, line)
    })
    gzip.end()
    await finished(sink)
    const { exit, lines, processed } = result
    fs.writeFileSync(`${file}.end`, `exit=${exit} lines=${lines} processed=${processed}\n`)
    reads.push({ topic: '*', exit, lines, processed, seconds: (this.clock.now() - began) / 1000 })
    if (exit !== 0 || processed !== lines)
      failures.push(`all topics: exit ${exit}, ${lines} lines, tool counted ${processed}`)
  }

  /** @param {string} name @param {*} value @returns {void} */
  #write(name, value) {
    fs.writeFileSync(path.join(this.profile.out, name), JSON.stringify(value, null, 2) + '\n')
  }

  /** Writes `header.json`: the SHA-256 and size of every other file of the artifact. */
  #seal() {
    const files = {}
    const walk = (dir) => {
      for (const entry of fs
        .readdirSync(dir, { withFileTypes: true })
        .sort((a, b) => (a.name < b.name ? -1 : 1))) {
        const full = path.join(dir, entry.name)
        if (entry.isDirectory()) walk(full)
        else {
          const rel = path.relative(this.profile.out, full)
          if (rel === 'header.json') continue
          const bytes = fs.readFileSync(full)
          files[rel] = {
            sha256: crypto.createHash('sha256').update(bytes).digest('hex'),
            bytes: bytes.length,
          }
        }
      }
    }
    walk(this.profile.out)
    this.#write('header.json', {
      treeSha: this.treeSha,
      node: process.version,
      kafkaVersion: process.env.KAFKA_VERSION ?? null,
      runId: this.runId,
      files,
    })
  }
}

/**
 * @param {object[]} producers the run's in-process load producers
 * @returns {Record<string, number>} records a second the idempotent and plain producers offer per
 *   lane (a transactional producer stalls with its slowest partition by design)
 */
const laneRates = (producers) => {
  const rates = {}
  for (const producer of producers)
    if (producer.kind !== 'transactional')
      for (const lane of producer.lanes) rates[lane.key] = (rates[lane.key] ?? 0) + lane.rate
  return rates
}

/** @returns {object} the client under test, as every process of a run builds it */
const loadKafka = () => {
  const { Kafka, logLevel } = require('../../index')
  return new Kafka({ clientId: 'load', brokers: [...BROKERS], logLevel: logLevel.WARN })
}

/** @returns {import('node:child_process').ChildProcess} one producer process */
const forkProducerChild = () =>
  fork(path.join(__dirname, 'loadProducerChild.js'), [], {
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    serialization: 'advanced',
  })

/**
 * Hands one line of the read of every topic to the checker.
 * @param {SequenceChecker} checker @param {string} line
 * @returns {void}
 */
const observeTopicLine = (checker, line) => {
  const parsed = parseTopicLine(line)
  if (parsed) checker.observe(parsed.topic, parsed.partition, parsed.offset, parsed.key)
  else checker.unparseable('*', line)
}

/**
 * Judges a sealed artifact again from its files alone: checks every file against `header.json`,
 * then replays the gzipped Apache dumps through a fresh `SequenceChecker` with the sealed ledgers.
 * @param {string} dir the artifact
 * @returns {Promise<object>} `SequenceChecker#finish`, with `size`
 * @throws {Error} naming the first file that is missing from the seal or differs from it
 */
const recheck = async (dir) => {
  const header = JSON.parse(fs.readFileSync(path.join(dir, 'header.json'), 'utf8'))
  for (const [file, { sha256 }] of Object.entries(header.files)) {
    const actual = crypto.createHash('sha256').update(fs.readFileSync(path.join(dir, file)))
    if (actual.digest('hex') !== sha256) throw new Error(`${file} differs from the seal`)
  }
  const read = (name) => {
    if (!header.files[name]) throw new Error(`${name} is not sealed`)
    return JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8'))
  }
  const { topics } = read('profile.json')
  const { committed, logEnds } = read('committed.json')
  const checker = new SequenceChecker({
    producers: read('producers.json'),
    consumers: read('consumers.json'),
    committed,
    logEnds,
  })
  const replay = async (name, onLine) => {
    if (!header.files[name]) throw new Error(`${name} is not sealed`)
    const lines = readline.createInterface({
      input: fs.createReadStream(path.join(dir, name)).pipe(zlib.createGunzip()),
      crlfDelay: Infinity,
    })
    for await (const line of lines) onLine(line)
  }
  if (header.files['apache/all.dump.gz'])
    await replay('apache/all.dump.gz', (line) => observeTopicLine(checker, line))
  else
    for (const topic of topics)
      await replay(`apache/${topic}.dump.gz`, (line) => {
        const parsed = parseDumpLine(line)
        if (parsed) checker.observe(topic, parsed.partition, parsed.offset, parsed.key)
        else checker.unparseable(topic, line)
      })
  return { ...checker.finish(), size: checker.size() }
}

/**
 * The CLI: `--recheck <artifact>` replays a sealed artifact (exit 0 when the oracle passes it, 1
 * otherwise); any other arguments run a profile, building the client, the Apache reader and, for a
 * fault schedule, the bouncer and (slow-broker) the netem delay. With thousands of partitions a
 * restart may wait up to 5 minutes for the full ISR.
 * @param {string[]} argv
 * @returns {Promise<number>} the exit code
 */
const main = async (argv) => {
  if (argv[0] === '--recheck' && argv.length === 2) {
    const verdict = await recheck(path.resolve(argv[1]))
    console.log(JSON.stringify(verdict, null, 2))
    return verdict.ok ? 0 : 1
  }
  const profile = parseLoadArgs(argv)
  const kafka = loadKafka()
  const composeFile = path.resolve(
    __dirname,
    '../..',
    process.env.COMPOSE_FILE || 'docker-compose.yml'
  )
  const schedule = FAULT_SCHEDULES[profile.faults]
  let bouncer = null
  let netem = null
  if (profile.faults !== 'none') {
    const { BrokerBouncer, NODES, waitForFullIsr } = require('../../testHelpers/leaderElection')
    bouncer = new BrokerBouncer(
      profile.faults === 'many-topics'
        ? {
            waitForFullIsr: (admin, t) =>
              waitForFullIsr(admin, t, { budgetMs: 300000, delayMs: 2000 }),
          }
        : {}
    )
    if (profile.faults === 'slow-broker') {
      const { NetemDelay } = require('./netemDelay')
      const { port } = NODES[schedule.service]
      netem = new NetemDelay({ container: schedule.service, port, delayMs: schedule.delayMs })
    }
  }
  const brokerCpu = new BrokerCpuProbe({
    stats: async () =>
      (
        await execFileAsync(
          'docker',
          ['stats', '--no-stream', '--format', '{{.Name}}\t{{.CPUPerc}}', ...BROKER_SERVICES],
          { timeout: 15000 }
        )
      ).stdout,
    sleep: realSleep,
  })
  const report = await new LoadRun({
    profile,
    kafka,
    reader: new ApacheReader({ composeFile }),
    bouncer,
    netem,
    brokerCpu,
    treeSha: process.env.LOAD_TREE_SHA ?? null,
  }).run()
  const { verdict, rate, latencyMs } = report
  console.log(
    JSON.stringify(
      {
        verdict,
        rate,
        latencyMs,
        cpu: report.cpu,
        flow: report.flow && { verdict: report.flow.verdict, reasons: report.flow.reasons },
        throughput: report.throughput,
        findings: report.oracle?.findings,
        errors: report.errors,
      },
      null,
      2
    )
  )
  return verdict.exitCode
}

module.exports = {
  ApacheReader,
  LoadConsumer,
  LoadProducer,
  LoadRun,
  ProducerProcess,
  forkProducerChild,
  loadKafka,
  main,
  parseDescribe,
  parseTopicLine,
  realSleep,
  recheck,
}

if (require.main === module) {
  main(process.argv.slice(2)).then(
    (code) => {
      process.exitCode = code
    },
    (error) => {
      console.error(error?.message ?? error)
      process.exitCode = 2
    }
  )
}
