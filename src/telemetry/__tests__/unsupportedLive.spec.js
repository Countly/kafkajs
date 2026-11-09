const crypto = require('crypto')
const net = require('net')
const path = require('path')
const { performance } = require('perf_hooks')
const { Kafka, logLevel, KafkaJSTelemetryError } = require('../../../index')
const { ordinaryOwnership } = require('../../../scripts/e2e/telemetry/semantics')
const {
  ShareTrace,
  confirmedDecisions,
  heldAcquisitions,
} = require('../../../scripts/e2e/telemetry/shareEvidence')
const { setShareGroupConfigs } = require('../../../testHelpers/shareFixtures')
const { runCase } = require('../../../scripts/e2e/telemetry/evidence')
const { TelemetryWireObserver } = require('../../../testHelpers/telemetryFixtures')
const { readLog } = require('../../../testHelpers/rawLog')
const {
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: needs the base broker fixture (docker-compose.yml, no reporter plugin) and the telemetry
// CI lanes. A failure always fails; the only self-skip is the absence of the outer opt-in.
const live = process.env.TELEMETRY_E2E ? test : test.skip

const PORTS = [9092, 9095, 9098]
const KINDS = ['producer', 'consumer', 'admin', 'share']
const RECORDS = 1000
const PARTITIONS = 3
const VALUE_BYTES = 200
const QUIET_MS = 3000
const clock = { now: () => performance.now() }

/** Plaintext IPv4 socket for the telemetry-disabled auxiliary admin. */
const ipv4Factory = ({ host, port, onConnect }) => {
  const socket = net.connect({ host, port, family: 4 }, onConnect)
  socket.setKeepAlive(true, 60000)
  return socket
}

/**
 * @param {Array<function():Promise<*>>} tasks started together
 * @returns {Promise<void>} when all settled
 * @throws {*} the single failure or an AggregateError of all
 */
const settle = async (tasks) => {
  const results = await Promise.allSettled(tasks.map((task) => task()))
  const errors = results.filter((r) => r.status === 'rejected').map((r) => r.reason)
  if (errors.length === 1) throw errors[0]
  if (errors.length) throw new AggregateError(errors, 'Several telemetry operations failed')
}

/**
 * @param {string[]} values identities
 * @returns {string} SHA-256 of their sorted, newline-joined list
 */
const digestOf = (values) =>
  crypto
    .createHash('sha256')
    .update([...values].sort().join('\n'))
    .digest('hex')

/**
 * @param {string} kind client kind
 * @param {boolean} oldShare whether the broker predates the share capability
 * @returns {string} the telemetry error reason an enabled client's getter gives after connect
 */
const refusedReason = (kind, oldShare) =>
  kind === 'share' && oldShare ? 'not-connected' : 'unsupported'

/**
 * The ordinary foreground workload and its three independent oracles: a durable log read straight
 * from each leader, raw committed next offsets read by a telemetry-disabled admin, and the
 * handler invocations in the order the consumer entered them. Expected identities are generated
 * before anything is produced, never from output.
 */
class OrdinaryCorpus {
  /**
   * @param {object} options corpus identity
   * @param {string} options.arm unique arm name used in keys, topic and group
   * @param {string[]} options.brokers seed `host:port` list
   */
  constructor({ arm, brokers }) {
    this.arm = arm
    this.brokers = brokers
    this.topic = `${arm}-topic`
    this.group = `${arm}-group`
    this.ids = Array.from({ length: RECORDS }, (_, i) => `${arm}-${String(i).padStart(4, '0')}`)
    this.ends = Object.fromEntries(
      Array.from({ length: PARTITIONS }, (_, p) => [
        p,
        String(this.ids.filter((_id, i) => i % PARTITIONS === p).length),
      ])
    )
    this.handled = []
    this.processedNext = {}
    this.committedNext = {}
  }

  /**
   * @param {number} index record number
   * @returns {Buffer} exactly 200 deterministic bytes that carry the identity
   */
  value(index) {
    return Buffer.from(`${this.ids[index]}|`.padEnd(VALUE_BYTES, 'x'))
  }

  /**
   * Creates the 3-partition RF3 topic with minimum in-sync replicas 2 and reads that config back.
   * @param {object} admin telemetry-disabled admin
   * @returns {Promise<void>} when the topic exists with the configuration
   */
  async create(admin) {
    const created = await admin.createTopics({
      topics: [
        {
          topic: this.topic,
          numPartitions: PARTITIONS,
          replicationFactor: 3,
          configEntries: [{ name: 'min.insync.replicas', value: '2' }],
        },
      ],
      waitForLeaders: true,
    })
    expect(created).toBe(true)
    const { resources } = await admin.describeConfigs({
      resources: [{ type: 2, name: this.topic, configNames: ['min.insync.replicas'] }],
    })
    expect(resources[0].configEntries.map((e) => [e.configName, e.configValue])).toEqual([
      ['min.insync.replicas', '2'],
    ])
  }

  /**
   * One explicit acknowledged send of all records to known partitions.
   * @param {object} producer idempotent producer
   * @returns {Promise<void>} when the broker acknowledged every record
   */
  async produce(producer) {
    await producer.send({
      topic: this.topic,
      acks: -1,
      messages: this.ids.map((key, i) => ({
        key,
        value: this.value(i),
        partition: i % PARTITIONS,
      })),
    })
  }

  /**
   * Consumes exactly the corpus, records each handler entry, commits each partition's processed
   * next offset explicitly and stops cleanly; an extra delivery stays visible in `handled`.
   * @param {object} consumer connected ordinary classic consumer
   * @param {function(number,function():Promise<boolean>):Promise<void>} untilDone bounded waiter
   * @returns {Promise<void>} once all records were handled and committed
   */
  async consume(consumer, untilDone) {
    await consumer.subscribe({ topic: this.topic, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        const entry = {
          partition,
          offset: message.offset,
          key: message.key.toString(),
          ordinal: this.handled.length + 1,
        }
        this.handled.push(entry)
        const next = String(BigInt(message.offset) + 1n)
        this.processedNext[partition] = next
        if (next === this.ends[partition] || this.handled.length % 100 === 0) {
          await consumer.commitOffsets([{ topic: this.topic, partition, offset: next }])
          this.committedNext[partition] = next
        }
      },
    })
    await untilDone(30000, async () =>
      Object.entries(this.ends).every(([partition, end]) => this.committedNext[partition] === end)
    )
    await consumer.stop()
  }

  /**
   * Reads and compares all three oracles; none uses the consumer's own received list as the log.
   * @param {object} admin telemetry-disabled admin for the raw commit read
   * @returns {Promise<object>} compact evidence of the three independent sets
   */
  async verify(admin) {
    const log = await readLog({ brokers: this.brokers, topic: this.topic })
    const logged = []
    const rawEnd = {}
    for (const [partition, { endOffset, records }] of log) {
      rawEnd[partition] = endOffset
      records.forEach((record, position) => {
        expect(record.offset).toBe(String(position))
        const index = this.ids.indexOf(record.key.toString())
        expect(index).toBeGreaterThanOrEqual(0)
        expect(index % PARTITIONS).toBe(partition)
        expect(record.value.equals(this.value(index))).toBe(true)
        logged.push(record.key.toString())
      })
    }
    expect([...logged].sort()).toEqual([...this.ids].sort())
    expect(rawEnd).toEqual(this.ends)
    const fetched = await admin.fetchOffsets({
      groupId: this.group,
      topics: [this.topic],
      resolveOffsets: false,
    })
    const rows = fetched[0]?.partitions ?? fetched
    const rawCommitted = Object.fromEntries(rows.map((r) => [r.partition, String(r.offset)]))
    expect(rawCommitted).toEqual(this.processedNext)
    expect(rawCommitted).toEqual(rawEnd)
    expect(this.handled).toHaveLength(RECORDS)
    expect(new Set(this.handled.map((h) => `${h.partition}:${h.offset}`)).size).toBe(RECORDS)
    expect(this.handled.map((h) => h.ordinal)).toEqual(this.ids.map((_, i) => i + 1))
    for (let partition = 0; partition < PARTITIONS; partition++) {
      const offsets = this.handled.filter((h) => h.partition === partition).map((h) => h.offset)
      expect(offsets).toEqual(Array.from({ length: offsets.length }, (_, i) => String(i)))
    }
    expect(digestOf(this.handled.map((h) => h.key))).toBe(digestOf(this.ids))
    return {
      inputs: {
        expected: this.ids.map((key, i) => ({
          key,
          valueHex: this.value(i).toString('hex'),
          partition: i % PARTITIONS,
        })),
        log: [...log].flatMap(([partition, { records }]) =>
          records.map((r) => ({
            partition,
            offset: r.offset,
            key: r.key.toString(),
            valueHex: r.value.toString('hex'),
          }))
        ),
        handled: this.handled,
        commits: fetched,
        ends: rawEnd,
      },
      count: RECORDS,
      expectedSha256: digestOf(this.ids),
      logSha256: digestOf(logged),
      handlerSha256: digestOf(this.handled.map((h) => h.key)),
      handlerEntries: this.handled.length,
      rawCommitted,
      processedNext: this.processedNext,
      rawEnd,
    }
  }
}

/**
 * L6: with no 71/72 advertised, the four enabled factories stay usable and quiet, and a stable
 * ordinary workload has exact durable-log, raw-commit and handler sets.
 */
class UnsupportedCase {
  #tasks = []
  #closing = null

  /**
   * @param {object} options case state
   * @param {AbortSignal} options.signal the case phase scope
   * @param {string} options.line broker line
   * @param {string} options.directory absolute evidence directory
   * @param {string} options.stem unique prefix
   * @param {object} options.attempt mutable evidence of the running case
   */
  constructor({ signal, line, directory, stem, attempt }) {
    this.signal = signal
    this.line = line
    this.directory = directory
    this.stem = stem
    this.attempt = attempt
    this.on = {}
    this.off = {}
    this.observers = {}
  }

  /** @param {function():Promise<void>|void} task release attempted by `close` @returns {void} */
  defer(task) {
    this.#tasks.push(task)
  }

  /**
   * @returns {Promise<void>} once every release was attempted
   * @throws {Error|AggregateError} failures in release order; the live run is tainted first
   */
  close() {
    this.#closing ??= (async () => {
      const errors = []
      while (this.#tasks.length) {
        try {
          await this.#tasks.pop()()
        } catch (error) {
          errors.push(error)
        }
      }
      if (errors.length) {
        liveRun.taint(errors[0])
        throw errors.length === 1 ? errors[0] : new AggregateError(errors, 'Cleanup failed')
      }
    })()
    return this.#closing
  }

  /**
   * @param {number} ms bound of this one operation
   * @param {string} label failure description
   * @param {function(AbortSignal):*} op the operation
   * @returns {Promise<*>} its result
   */
  within(ms, label, op) {
    return bounded(this.signal, ms, op, { label })
  }

  /**
   * @param {number} ms budget
   * @param {function():Promise<boolean>} check condition polled every 100 ms
   * @returns {Promise<void>} once the condition holds
   */
  until(ms, check) {
    return pollUntil(this.signal, ms, check, { delayMs: 100, label: 'The ordinary workload' })
  }

  /**
   * Builds one client of a kind on its own observed base-fixture socket factory.
   * @param {string} kind producer, consumer, admin or share
   * @param {boolean} enabled whether broker telemetry is requested
   * @returns {object} the public client
   */
  build(kind, enabled) {
    const suffix = enabled ? '' : '-off'
    const owner = kind + suffix
    const observer = new TelemetryWireObserver({
      directory: this.directory,
      line: this.line,
      clock,
      mode: 'base',
      owner,
    })
    this.observers[owner] = observer
    this.attempt.observers[owner] = observer
    const trace = kind === 'share' ? new ShareTrace() : null
    this.traces ??= {}
    this.traces[owner] = trace
    const kafka = new Kafka({
      clientId: `${this.stem}-${owner}`,
      brokers: PORTS.map((port) => `localhost:${port}`),
      socketFactory: (options) => {
        const socket = observer.socketFactory(options)
        return trace ? trace.wrap(socket) : socket
      },
      logLevel: logLevel.NOTHING,
    })
    const common = { enableMetricsPush: enabled }
    const client =
      kind === 'producer'
        ? kafka.producer({ ...common, idempotent: true })
        : kind === 'admin'
          ? kafka.admin(common)
          : kind === 'share'
            ? kafka.shareConsumer({
                ...common,
                groupId: `${this.stem}-share-group${suffix}`,
                acknowledgementMode: 'explicit',
                maxRecords: 10,
                maxWaitTimeInMs: 250,
              })
            : kafka.consumer({
                ...common,
                groupId: enabled ? `${this.stem}-group` : `${this.stem}-off-group`,
                sessionTimeout: 30000,
                heartbeatInterval: 3000,
                maxWaitTimeInMs: 500,
                readUncommitted: false,
              })
    this.defer(() => observer.assertClean({ allowUnanswered: observer.cancellationKeys() }))
    this.defer(() => observer.dispose())
    this.defer(() => observer.closeClient(client))
    ;(enabled ? this.on : this.off)[kind] = client
    return client
  }

  /**
   * @param {object} client public client
   * @returns {Promise<string>} the telemetry error reason a zero-timeout getter rejects with
   */
  async reasonOf(client) {
    const error = await client.clientInstanceId({ timeout: 0 }).then(
      () => null,
      (e) => e
    )
    expect(error).toBeInstanceOf(KafkaJSTelemetryError)
    return error.reason
  }

  /**
   * @param {object} client public client
   * @returns {Promise<object>} a connected status or the typed refusal's identifying fields
   */
  async connectOutcome(client) {
    try {
      await this.within(15000, 'The client connect', () => client.connect())
      return { status: 'connected' }
    } catch (error) {
      return {
        status: 'rejected',
        name: error?.name,
        message: error?.message,
        reason: error?.reason,
        apiKey: error?.apiKey,
        shareVersion: error?.shareVersion,
      }
    }
  }

  /**
   * @returns {Promise<void>} when the unsupported-broker scenario and its evidence are complete
   */
  async run() {
    const results = (this.attempt.record.results = {})
    const aux = new Kafka({
      clientId: `${this.stem}-aux`,
      brokers: PORTS.map((port) => `localhost:${port}`),
      socketFactory: ipv4Factory,
      logLevel: logLevel.NOTHING,
    }).admin({ enableMetricsPush: false, apiTimeout: 3000 })
    this.defer(() => aux.disconnect())
    await this.within(10000, 'The auxiliary admin connect', () => aux.connect())
    const cluster = await this.within(5000, 'The topology check', () =>
      aux.describeCluster({ timeout: 3000 })
    )
    expect(cluster.brokers.map((b) => [b.nodeId, b.host, b.port]).sort()).toEqual(
      PORTS.map((port, index) => [index + 1, 'localhost', port])
    )
    for (const kind of KINDS) {
      this.build(kind, true)
      this.build(kind, false)
    }
    const oldShare = ['4.0.2', '4.1.2'].includes(this.line)
    for (const kind of KINDS) {
      expect(await this.reasonOf(this.on[kind])).toBe('not-connected')
      expect(await this.reasonOf(this.off[kind])).toBe('disabled')
      for (const owner of [kind, `${kind}-off`])
        expect(this.observers[owner].readTranscript()).toEqual([])
    }

    const outcomes = {}
    const disabledOutcomes = {}
    await settle(
      KINDS.flatMap((kind) => [
        async () => {
          outcomes[kind] = await this.connectOutcome(this.on[kind])
        },
        async () => {
          disabledOutcomes[kind] = await this.connectOutcome(this.off[kind])
        },
      ])
    )
    this.attempt.record.outcomes = outcomes
    this.attempt.record.disabledOutcomes = disabledOutcomes
    const reasons = {}
    for (const kind of KINDS) {
      expect(outcomes[kind]).toEqual(disabledOutcomes[kind])
      const refused = kind === 'share' && oldShare
      if (refused)
        expect(outcomes[kind]).toMatchObject({
          status: 'rejected',
          name: 'KafkaJSShareGroupsNotSupportedError',
          reason: this.line === '4.0.2' ? 'api-not-advertised' : 'api-version-range',
          apiKey: this.line === '4.0.2' ? 76 : 78,
          shareVersion: 0,
        })
      else expect(outcomes[kind]).toEqual({ status: 'connected' })
    }

    for (const observer of Object.values(this.observers)) observer.confirmForegroundBootstrap()
    const corpus = new OrdinaryCorpus({
      arm: this.stem,
      brokers: [`localhost:${PORTS[0]}`],
    })
    await this.within(15000, 'The corpus topic', () => corpus.create(aux))
    await this.within(15000, 'The acknowledged send', () => corpus.produce(this.on.producer))
    await this.within(40000, 'The ordinary consume', (scope) =>
      corpus.consume(this.on.consumer, (ms, check) =>
        pollUntil(scope, ms, check, { delayMs: 100, label: 'All records handled' })
      )
    )
    await this.within(10000, 'The supported admin work', () => this.on.admin.describeCluster())
    await this.within(10000, 'The opt-out admin work', () => this.off.admin.describeCluster())
    results.corpus = await this.within(20000, 'The three oracles', () => corpus.verify(aux))

    const disabledCorpus = new OrdinaryCorpus({
      arm: `${this.stem}-off`,
      brokers: [`localhost:${PORTS[0]}`],
    })
    await this.within(15000, 'The disabled ordinary workload', async () => {
      await disabledCorpus.create(aux)
      await disabledCorpus.produce(this.off.producer)
      await disabledCorpus.consume(this.off.consumer, (ms, check) =>
        pollUntil(this.signal, ms, check, { delayMs: 100, label: 'Disabled handlers' })
      )
    })
    results.disabledCorpus = await this.within(10000, 'Disabled independent ledgers', () =>
      disabledCorpus.verify(aux)
    )
    if (!oldShare) results.share = await this.shareWork(aux)
    for (const owner of ['share', 'share-off']) this.observers[owner].confirmForegroundFetchSetup()
    results.ownership = {}
    for (const kind of KINDS) {
      const roles = (owner) =>
        ordinaryOwnership({
          records: this.observers[owner].readTranscript(),
          sockets: this.observers[owner].socketOwnership(),
        })
      const on = roles(kind)
      const off = roles(`${kind}-off`)
      expect([...new Set(on.map((r) => r.split(':').at(-1)))].sort()).toEqual(
        [...new Set(off.map((r) => r.split(':').at(-1)))].sort()
      )
      results.ownership[kind] = { on, off }
    }
    const adverts = (owner) => this.observers[owner].advertisements()
    const firstAdvert = Math.min(
      ...KINDS.flatMap((kind) =>
        this.observers[kind]
          .readTranscript()
          .filter((r) => r.apiKey === 18 && r.response?.errorCode === 0)
          .map((r) => r.response.responseAt)
      )
    )
    const wait = firstAdvert + QUIET_MS - clock.now()
    if (wait > 0)
      await this.within(
        wait + 2000,
        'The quiet window',
        () => new Promise((r) => setTimeout(r, wait))
      )
    for (const kind of KINDS) {
      reasons[kind] = refusedReason(kind, oldShare)
      expect(await this.reasonOf(this.on[kind])).toBe(reasons[kind])
      expect(await this.reasonOf(this.off[kind])).toBe('disabled')
      for (const owner of [kind, `${kind}-off`]) {
        const observer = this.observers[owner]
        expect(adverts(owner).length).toBeGreaterThan(0)
        expect(adverts(owner).every((a) => a.ranges[71] === null && a.ranges[72] === null)).toBe(
          true
        )
        expect(observer.subscriptions()).toEqual([])
        expect(observer.telemetrySockets()).toEqual([])
        expect(observer.readTranscript().some((r) => r.apiKey === 71 || r.apiKey === 72)).toBe(
          false
        )
        observer.assertHealthy()
      }
    }
    results.reasons = reasons
    results.quietFrom = firstAdvert
  }

  /**
   * Requires actual capable-share acquisition, handlers and successful broker acknowledgements
   * under both telemetry options; the older broker refusal path never calls this method.
   * @param {object} aux disabled administration client
   * @returns {Promise<object>} raw per-member acknowledgement and handler evidence
   */
  async shareWork(aux) {
    const topic = `${this.stem}-share-work`
    const keys = Array.from({ length: 10 }, (_, i) => `${this.stem}-share-${i}`)
    await aux.createTopics({
      topics: [{ topic, numPartitions: 1, replicationFactor: 3 }],
      waitForLeaders: true,
    })
    await this.on.producer.send({
      topic,
      acks: -1,
      messages: keys.map((key) => ({ key, value: key, partition: 0 })),
    })
    const metadata = await aux.fetchTopicMetadata({ topics: [topic] })
    const topicId = metadata.topics.find((t) => t.name === topic).topicId
    const results = {}
    for (const owner of ['share', 'share-off']) {
      const group = `${this.stem}-share-group${owner.endsWith('-off') ? '-off' : ''}`
      await setShareGroupConfigs(aux, group, { 'share.auto.offset.reset': 'earliest' })
      const client = owner.endsWith('-off') ? this.off.share : this.on.share
      const handled = []
      const settlements = []
      this.defer(client.on(client.events.ACKNOWLEDGE, ({ payload }) => settlements.push(payload)))
      await client.subscribe({ topics: [topic] })
      await client.run({
        eachBatch: async ({ batch, acknowledge }) => {
          for (const message of batch.messages) {
            handled.push({
              topic: batch.topic,
              partition: batch.partition,
              key: message.key.toString(),
              offset: message.offset,
            })
            acknowledge(message, 'accept')
          }
        },
      })
      await pollUntil(
        this.signal,
        10000,
        () =>
          confirmedDecisions(this.traces[owner].snapshot(), group).filter((r) => r.type === 1)
            .length === 10,
        { delayMs: 50, label: 'Supported share acknowledgements' }
      )
      await client.stop()
      expect(handled.map((r) => r.key)).toEqual(keys)
      const log = await readLog({ brokers: [`localhost:${PORTS[0]}`], topic })
      results[owner] = {
        group,
        topic,
        topicId,
        metadata,
        held: heldAcquisitions(this.traces[owner].snapshot(), group),
        settlements: settlements.map(({ error, ...event }) => ({
          ...event,
          error: error ? { name: error.name, reason: error.reason } : null,
        })),
        keys,
        handled,
        frames: this.traces[owner].snapshot(),
        log: [...log.entries()].flatMap(([partition, { records }]) =>
          records.map((r) => ({
            topic,
            partition,
            key: r.key.toString(),
            offset: r.offset,
            valueHex: r.value.toString('hex'),
          }))
        ),
      }
    }
    return results
  }

  /**
   * Closes every client, then hands the case record to the shared persistence and verification.
   * @returns {void}
   */
  finishRecord() {
    this.attempt.record = {
      wireVersions: {},
      run: { stem: this.stem, line: this.line, project: process.env.COMPOSE_PROJECT_NAME },
      outcomes: this.attempt.record.outcomes,
      disabledOutcomes: this.attempt.record.disabledOutcomes,
      reasons: this.attempt.record.results.reasons,
      corpus: this.attempt.record.results.corpus,
      results: this.attempt.record.results,
      oracles: {
        corpus: this.attempt.record.results.corpus.inputs,
        disabledCorpus: this.attempt.record.results.disabledCorpus.inputs,
        share: this.attempt.record.results.share,
      },
      pushes: [],
      allowed: { errors: {}, faultedSockets: {} },
    }
  }
}

const BUDGET = 58000
live(
  'unsupported telemetry',
  liveCase(BUDGET, (scope) =>
    runCase('telemetry-unsupported', async (cleanup, publish, attempt) => {
      const line = process.env.KAFKA_VERSION
      const directory = process.env.TELEMETRY_EVIDENCE_DIR
      if (!line) throw new Error('KAFKA_VERSION must name the broker line')
      if (!directory || !path.isAbsolute(directory))
        throw new Error('TELEMETRY_EVIDENCE_DIR must name the absolute run directory')
      const scenario = new UnsupportedCase({
        signal: scope,
        line,
        directory,
        stem: attempt.record.run.stem,
        attempt,
      })
      cleanup.defer(() => scenario.close())
      await bounded(
        scope,
        BUDGET - 15000,
        (phase) => {
          scenario.signal = phase
          return scenario.run()
        },
        { label: 'The unsupported telemetry scenario' }
      )
      attempt.stage = 'verification'
      scenario.finishRecord()
      publish({
        name: 'telemetry-unsupported',
        directory,
        observers: attempt.observers,
        record: attempt.record,
      })
    })
  ),
  caseTimeout(BUDGET)
)
