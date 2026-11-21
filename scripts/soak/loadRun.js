#!/usr/bin/env node
'use strict'

/**
 * Load profile driver: offers a target record rate through producers of each kind, consumes it in
 * one group, optionally runs a fault schedule, then reads every partition once with Apache's own
 * console consumer and judges it with the sequence oracle. Separate from the final soak's day plan.
 *
 * Usage: see `loadProfile.USAGE`; `--recheck <artifact>` judges a sealed artifact again offline.
 * A run needs the repository's three-broker Compose cluster
 * (`docker-compose.yml`, or `COMPOSE_FILE`). Writes the artifact to `--out` and sets the exit code
 * from `verdictOf`: 0 pass, 1 product failure, 2 inconclusive, 3 rate not reached.
 *
 * Artifact: `profile.json`, `producers.json`, `consumers.json`, `committed.json`, `faults.json`,
 * `apache/<topic>.dump.gz` with `.end`, `apache/<group>.describe`, `report.json`, and `header.json`,
 * written last, which binds every other file by SHA-256 (the seal).
 */

const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const readline = require('node:readline')
const zlib = require('node:zlib')
const { spawn } = require('node:child_process')
const { monitorEventLoopDelay } = require('node:perf_hooks')
const { finished } = require('node:stream/promises')
const {
  FAULT_SCHEDULES,
  LatencyHistogram,
  OfferSchedule,
  parseLoadArgs,
  rateReport,
  verdictOf,
} = require('./loadProfile')
const { DeliveryLedger, SendLedger, SequenceChecker, parseDumpLine } = require('./sequenceOracle')

const BROKERS = Object.freeze(['localhost:9092', 'localhost:9095', 'localhost:9098'])
const BOOTSTRAP = 'kafka1:29092,kafka2:29095,kafka3:29098'
const TOOLS = '/opt/kafka/bin'
/** How long Apache's console consumer may see no record before it ends a topic's read. */
const READ_IDLE_MS = 10000
/** Topics read at once: each read is one JVM in the broker container. */
const READ_CONCURRENCY = 3
const SAMPLE_MS = 10000
const COMPRESSION = Object.freeze({ none: 0, gzip: 1, snappy: 2, lz4: 3, zstd: 4 })

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
    return this.#run(
      [
        `${TOOLS}/kafka-console-consumer.sh`,
        '--bootstrap-server',
        BOOTSTRAP,
        '--topic',
        topic,
        '--from-beginning',
        '--isolation-level',
        'read_committed',
        '--timeout-ms',
        String(READ_IDLE_MS),
        '--formatter-property',
        'print.partition=true',
        '--formatter-property',
        'print.offset=true',
        '--formatter-property',
        'print.key=true',
        '--formatter-property',
        'print.value=false',
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

/**
 * One load producer: a client producer of one kind with its offer schedule. A transactional
 * producer sends into the open transaction and ends it every `transactionMs`, aborting every
 * `abortEvery`-th and any whose send failed; a transaction that cannot end recreates the producer
 * as a new incarnation.
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
   */
  constructor({ kafka, id, kind, profile, topics, runId, latency, clock, sleep }) {
    Object.assign(this, { kafka, id, kind, profile, topics, runId, latency, clock, sleep })
    this.rate = profile.rate / profile.producers
    this.cap = Math.max(1, Math.ceil(this.rate * profile.unsettledSeconds))
    this.cells = topics.flatMap((topic) =>
      Array.from({ length: profile.partitions }, (_, partition) => ({ topic, partition }))
    )
    this.cursor = 0
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
    this.schedule = new OfferSchedule({ rate: this.rate, start })
    this.lastTxAt = start
  }

  /**
   * Sends what the schedule made due, as far as the unsettled cap allows.
   * @param {number} now
   * @returns {void}
   */
  tick(now) {
    if (this.kind === 'transactional' && !this.#transactionReady(now)) return
    const n = Math.min(this.schedule.due(now), this.cap - this.unsettled)
    if (n <= 0) return
    const { dueAt } = this.schedule.take(n)
    const byTopic = new Map()
    const issued = []
    const per = Math.floor(n / this.cells.length)
    let extra = n % this.cells.length
    for (let k = 0; k < this.cells.length; k++) {
      const cell = this.cells[(this.cursor + k) % this.cells.length]
      const count = per + (extra > 0 ? 1 : 0)
      if (extra > 0) extra--
      if (count === 0) break
      const { first, key } = this.ledger.issue(cell.topic, cell.partition, count)
      issued.push({ ...cell, first, count })
      if (!byTopic.has(cell.topic)) byTopic.set(cell.topic, [])
      const messages = byTopic.get(cell.topic)
      for (let i = 0; i < count; i++)
        messages.push({
          partition: cell.partition,
          key: key(i),
          value: this.values[(first + i) & 63],
        })
    }
    this.cursor = (this.cursor + (n % this.cells.length)) % this.cells.length
    this.unsettled += n
    this.stats.sent += n
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
    const entry = { send, issued, ledger, n, dueAt }
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
   * @param {{ issued: object[], ledger: SendLedger, n: number, dueAt: (i: number) => number }} entry
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
    this.unsettled -= entry.n
    this.stats[status] += entry.n
    if (status === 'acked') {
      const now = this.clock.now()
      for (let i = 0; i < entry.n; i++) this.latency.record(now - entry.dueAt(i))
    } else if (!result.ok) this.#recordError('send', result.error)
  }

  /** @param {string} where @param {Error} error @returns {void} */
  #recordError(where, error) {
    if (this.errors.length < 20) this.errors.push(`${where}: ${error?.name}: ${error?.message}`)
  }
}

/**
 * One member of the load group, recording what it was delivered. Commits are the client's own
 * (auto-commit after each batch is resolved, and on disconnect).
 */
class LoadConsumer {
  /**
   * @param {{ kafka: object, groupId: string, member: string, topics: string[], clock: { now: () => number } }} options
   */
  constructor({ kafka, groupId, member, topics, clock }) {
    Object.assign(this, { kafka, groupId, topics, clock })
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
    await this.consumer.connect()
    await this.consumer.subscribe({ topics: this.topics, fromBeginning: true })
    await this.consumer.run({
      eachBatch: async ({ batch }) => {
        for (const message of batch.messages)
          this.ledger.deliver(batch.topic, batch.partition, Number(message.offset))
        this.delivered += batch.messages.length
        this.lastDeliveryAt = this.clock.now()
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
   * @param {{ rollingBounce: Function }|null} [options.bouncer] a `BrokerBouncer`, for a restart
   *   schedule
   * @param {{ now: () => number }} [options.clock]
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} [options.sleep]
   * @param {(line: string) => void} [options.log]
   * @param {string|null} [options.treeSha] the commit the run is of, for the seal
   */
  constructor({
    profile,
    kafka,
    reader,
    bouncer = null,
    clock = performance,
    sleep = realSleep,
    log = (line) => console.log(line),
    treeSha = null,
  }) {
    Object.assign(this, { profile, kafka, reader, bouncer, clock, sleep, log, treeSha })
    this.runId = `load-${Date.now()}`
    this.topics = Array.from({ length: profile.topics }, (_, i) => `${this.runId}-${i}`)
    this.groupId = `${this.runId}-group`
    this.latency = new LatencyHistogram()
    this.errors = []
  }

  /**
   * Sets up, offers, drains, reads and judges; writes and seals the artifact.
   * @returns {Promise<object>} the report, with `verdict`
   */
  async run() {
    const { profile } = this
    fs.mkdirSync(path.join(profile.out, 'apache'), { recursive: true })
    this.#write('profile.json', { ...profile, runId: this.runId, topics: this.topics })
    const admin = this.kafka.admin()
    const producers = Array.from(
      { length: profile.producers },
      (_, i) =>
        new LoadProducer({
          kafka: this.kafka,
          id: `p${i}`,
          kind: profile.producerKinds[i % profile.producerKinds.length],
          profile,
          topics: this.topics,
          runId: this.runId,
          latency: this.latency,
          clock: this.clock,
          sleep: this.sleep,
        })
    )
    const consumers = Array.from(
      { length: profile.consumers },
      (_, i) =>
        new LoadConsumer({
          kafka: this.kafka,
          groupId: this.groupId,
          member: `c${i}`,
          topics: this.topics,
          clock: this.clock,
        })
    )
    const resources = { samples: [], maxHeapUsed: 0, maxRss: 0 }
    const loop = monitorEventLoopDelay({ resolution: 20 })
    let totals = { offered: 0, sent: 0, acked: 0, failed: 0 }
    let faults = []
    let oracle = null
    let windowMs = profile.durationSeconds * 1000
    try {
      await admin.connect()
      await admin.createTopics({
        waitForLeaders: true,
        topics: this.topics.map((topic) => ({
          topic,
          numPartitions: profile.partitions,
          replicationFactor: profile.replicationFactor,
          configEntries: [
            { name: 'min.insync.replicas', value: String(profile.minInsyncReplicas) },
          ],
        })),
      })
      for (const consumer of consumers) await consumer.start()
      for (const producer of producers) await producer.connect()
      loop.enable()
      const result = await this.#offer(producers, admin, resources)
      ;({ faults, windowMs } = result)
      totals = result.totals
      for (const producer of producers)
        if (!(await producer.join(profile.joinSeconds * 1000)))
          this.errors.push(`${producer.id}: sends still unsettled after ${profile.joinSeconds} s`)
      for (const producer of producers) {
        totals.acked += producer.stats.acked + producer.stats.aborted
        totals.failed += producer.stats.failed
      }
      await this.#drain(consumers)
    } catch (error) {
      this.errors.push(`driver: ${error?.stack ?? error}`)
    } finally {
      loop.disable()
      for (const client of [...producers, ...consumers]) {
        try {
          await (client.close?.() ?? client.stop())
        } catch (error) {
          this.errors.push(`close: ${error?.message}`)
        }
      }
    }
    const producerReports = producers.flatMap((p) => p.ledgers.map((l) => l.toJSON()))
    const consumerReports = consumers.map((c) => c.ledger.toJSON())
    this.#write('producers.json', producerReports)
    this.#write('consumers.json', consumerReports)
    this.#write('faults.json', faults)
    try {
      oracle = await this.#judge(producerReports, consumerReports)
    } catch (error) {
      oracle = error.verdict ?? null
      this.errors.push(`read: ${error?.message}`)
    }
    try {
      await admin.deleteTopics({ topics: this.topics })
    } catch (error) {
      this.log(`topic cleanup failed: ${error?.message}`)
    }
    await admin.disconnect().catch(() => {})
    const rate = rateReport({
      rate: profile.rate,
      windowMs,
      targetFraction: profile.targetFraction,
      ...totals,
    })
    const verdict = verdictOf({ oracle, rate, error: this.errors[0] ?? null })
    const report = {
      verdict,
      rate,
      totals,
      latencyMs: this.latency.toJSON(),
      eventLoopDelayMs: {
        p50: loop.percentile(50) / 1e6,
        p99: loop.percentile(99) / 1e6,
        max: loop.max / 1e6,
      },
      resources,
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
      faults: faults.map(({ service, kind, outageMs }) => ({ service, kind, outageMs })),
      errors: this.errors,
    }
    this.#write('report.json', report)
    this.#seal()
    return report
  }

  /**
   * The offer window: ticks every producer on a deadline schedule while the fault schedule runs.
   * @param {LoadProducer[]} producers @param {object} admin @param {object} resources
   * @returns {Promise<{ totals: { offered: number, sent: number, acked: number, failed: number }, faults: object[], windowMs: number }>}
   */
  async #offer(producers, admin, resources) {
    const { profile } = this
    const start = this.clock.now()
    const windowMs = profile.durationSeconds * 1000
    const end = start + windowMs
    const tickMs = 1000 / profile.ticksPerSecond
    for (const producer of producers) producer.begin(start)
    const window = new AbortController()
    const fault = FAULT_SCHEDULES[profile.faults]
      .start({
        bouncer: this.bouncer,
        admin,
        topics: this.topics,
        windowMs,
        signal: window.signal,
        sleep: this.sleep,
      })
      .catch((error) => {
        this.errors.push(`fault schedule: ${error?.message}`)
        return []
      })
    let nextSample = start + SAMPLE_MS
    for (let n = 1; ; n++) {
      const now = Math.min(this.clock.now(), end)
      for (const producer of producers) producer.tick(now)
      if (now >= nextSample) {
        nextSample += SAMPLE_MS
        const { heapUsed, rss } = process.memoryUsage()
        resources.maxHeapUsed = Math.max(resources.maxHeapUsed, heapUsed)
        resources.maxRss = Math.max(resources.maxRss, rss)
        const sent = producers.reduce((s, p) => s + p.stats.sent, 0)
        resources.samples.push({ atMs: Math.round(now - start), heapUsed, rss, sent })
      }
      if (now >= end) break
      await this.sleep(Math.max(0, Math.min(end, start + n * tickMs) - this.clock.now()))
    }
    window.abort()
    const faults = await fault
    const offered = producers.reduce((s, p) => s + Math.floor((windowMs * p.rate) / 1000), 0)
    const sent = producers.reduce((s, p) => s + p.stats.sent, 0)
    return { totals: { offered, sent, acked: 0, failed: 0 }, faults, windowMs }
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
  for (const topic of topics) {
    const name = `apache/${topic}.dump.gz`
    if (!header.files[name]) throw new Error(`${name} is not sealed`)
    const lines = readline.createInterface({
      input: fs.createReadStream(path.join(dir, name)).pipe(zlib.createGunzip()),
      crlfDelay: Infinity,
    })
    for await (const line of lines) {
      const parsed = parseDumpLine(line)
      if (parsed) checker.observe(topic, parsed.partition, parsed.offset, parsed.key)
      else checker.unparseable(topic, line)
    }
  }
  return { ...checker.finish(), size: checker.size() }
}

/**
 * The CLI: `--recheck <artifact>` replays a sealed artifact (exit 0 when the oracle passes it, 1
 * otherwise); any other arguments run a profile, building the client, the Apache reader and, for a
 * restart schedule, the bouncer.
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
  const { Kafka, logLevel } = require('../../index')
  const kafka = new Kafka({ clientId: 'load', brokers: [...BROKERS], logLevel: logLevel.WARN })
  const composeFile = path.resolve(
    __dirname,
    '../..',
    process.env.COMPOSE_FILE || 'docker-compose.yml'
  )
  const bouncer =
    profile.faults === 'rolling-restarts'
      ? new (require('../../testHelpers/leaderElection').BrokerBouncer)()
      : null
  const report = await new LoadRun({
    profile,
    kafka,
    reader: new ApacheReader({ composeFile }),
    bouncer,
    treeSha: process.env.LOAD_TREE_SHA ?? null,
  }).run()
  const { verdict, rate, latencyMs } = report
  console.log(
    JSON.stringify(
      { verdict, rate, latencyMs, findings: report.oracle?.findings, errors: report.errors },
      null,
      2
    )
  )
  return verdict.exitCode
}

module.exports = { ApacheReader, LoadConsumer, LoadProducer, LoadRun, main, parseDescribe, recheck }

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
