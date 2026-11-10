const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const path = require('path')
const { performance } = require('perf_hooks')
const { Kafka, logLevel, KafkaJSTelemetryError } = require('../../../index')
const {
  ShareTrace,
  confirmedDecisions,
  acquisitionStart,
  heldAcquisitions,
} = require('../../../scripts/e2e/telemetry/shareEvidence')
const { runCase } = require('../../../scripts/e2e/telemetry/evidence')
const { runShareEnvelope } = require('../../../scripts/e2e/telemetry/shareEnvelope')
const {
  TelemetryWireObserver,
  withTelemetryResource,
} = require('../../../testHelpers/telemetryFixtures')
const { createChaosSocketFactory, FrameFilter } = require('../../../testHelpers/chaosSocket')
const { readLog } = require('../../../testHelpers/rawLog')
const { setShareGroupConfigs, verifierMember } = require('../../../testHelpers/shareFixtures')
const {
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: the configured fixture (docker-compose.telemetry.yml) and, for the no-plugin control, the
// base fixture (docker-compose.yml). A failure always fails; the only self-skip is the outer opt-in.
const live = process.env.TELEMETRY_E2E ? test : test.skip

const ROOT = path.resolve(__dirname, '../../..')
const PORTS = { configured: [19092, 19095, 19098], base: [9092, 9095, 9098] }
const RECORDS = 1000
const PARTITIONS = 3
const VALUE_BYTES = 200
const SHARE_RECORDS = 10
const SHARE_ACCEPTED = 4
const LOCK_MS = 15000
const TELEMETRY_APIS = [71, 72]
const clock = { now: () => performance.now() }

/** The F04/F06 finite seams that pin original outcome identity, which a separate live process cannot compare. */
const STRICT_SEAMS = Object.freeze([
  [
    'src/telemetry/__tests__/failureIsolation.spec.js',
    'private failure preserves foreground identity',
  ],
  [
    'src/telemetry/__tests__/failureIsolation.spec.js',
    '%s retains the unique sentinel and receiver',
  ],
  [
    'src/telemetry/__tests__/capability.spec.js',
    'distinguishes runtime refusal from a successful empty resource selection',
  ],
])

/** Plaintext IPv4 socket for unobserved fixture clients. */
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
 * @returns {{promise:Promise<*>,resolve:function(*):void}} a promise and its resolver
 */
const deferred = () => {
  let resolve
  const promise = new Promise((r) => (resolve = r))
  return { promise, resolve }
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
 * @param {string} file absolute path
 * @returns {string} SHA-256 of the file
 */
const fileSha = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')

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
    await untilDone(25000, async () =>
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
 * Chaos sockets for one observed client, with a passive frame classifier on each: the first
 * request frame of API 71 or 72 marks that very socket object as the telemetry transport. A fault
 * is armed on the classified object only, never on an endpoint, and every patch is removed again.
 */
class TelemetryFaults {
  /** @param {object} observer the wire observer whose real sockets the chaos layer wraps */
  constructor(observer) {
    const inner = createChaosSocketFactory((options) => observer.socketFactory(options))
    this.records = []
    this.factory = (options) => this.track(inner(options))
  }

  /**
   * Records the socket's issue ordinal and starts classifying its request frames.
   * @param {object} socket the chaos socket just issued
   * @returns {object} the same socket
   */
  track(socket) {
    const record = {
      ordinal: this.records.length + 1,
      socket,
      telemetry: false,
      closed: false,
      attempts: [],
    }
    const classifier = new FrameFilter({
      headerBytes: 12,
      decide: (header) => {
        if (header.length >= 12 && TELEMETRY_APIS.includes(header.readInt16BE(4))) {
          record.telemetry = true
          record.attempts.push({
            apiKey: header.readInt16BE(4),
            correlation: header.readInt32BE(8),
            at: clock.now(),
          })
        }
        return false
      },
    })
    const write = socket.write
    socket.write = function (data, encoding) {
      classifier.push(Buffer.from(data, encoding))
      return write.call(this, data, encoding)
    }
    record.restore = () => {
      delete socket.write
    }
    socket.on('close', () => {
      record.closed = true
    })
    this.records.push(record)
    return socket
  }

  /** @returns {object[]} open sockets proven by an actual 71/72 frame to be the telemetry transport */
  telemetry() {
    return this.records.filter((r) => r.telemetry && !r.closed)
  }

  /** @returns {object[]} open sockets with no 71/72 frame, the normal-work transports */
  normal() {
    return this.records.filter((r) => !r.telemetry && !r.closed)
  }

  /**
   * Arms one fault on exactly the given classified socket.
   * @param {object} record a record from `telemetry()`
   * @param {'drop'|'blackhole'|'late'} mode the fault
   * @returns {void}
   */
  arm(record, mode) {
    if (!record.telemetry) throw new Error('Refusing to fault an unclassified socket')
    if (mode === 'drop') record.socket.dropResponses(1, { apiKey: 72 })
    else if (mode === 'blackhole') record.socket.blackhole()
    else record.socket.delay({ read: 6000 })
  }

  /** @returns {void} removes every classifier patch, whatever state the sockets are in */
  restore() {
    for (const record of this.records) record.restore()
  }
}

/**
 * L8: foreground outcomes with telemetry off, on, and failing only on its own transport, each in
 * its own bounded subarm with exact durable-log, raw-commit and handler sets, plus the share
 * accepted-prefix and rest-release oracle on the configured fixture.
 */
class OutcomeCase {
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
    this.mode = process.env.COMPOSE_FILE === 'docker-compose.telemetry.yml' ? 'configured' : 'base'
    this.brokers = PORTS[this.mode].map((port) => `localhost:${port}`)
    this.results = { arms: {}, faults: {} }
    this.faulted = {}
    this.pushes = []
    this.shapes = {}
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
   * Polls a lookup until it returns a truthy value.
   * @param {string} label failure description
   * @param {number} ms poll budget
   * @param {function():*} lookup the value, or falsy when not yet
   * @returns {Promise<*>} the truthy lookup result
   */
  async find(label, ms, lookup) {
    let found
    await pollUntil(this.signal, Math.max(1, ms), async () => (found = await lookup()), {
      delayMs: 100,
      label: `Waiting for ${label}`,
    })
    return found
  }

  /**
   * Builds an observed client of one kind. Faulted arms also route its sockets through the chaos
   * layer. Release order: disconnect, dispose, require clean, restore the classifier patches.
   * @param {string} owner observer owner key
   * @param {string} kind producer, consumer or share
   * @param {object} options arm description: `enabled`, `faults` and a `group` for consumers
   * @returns {object} `{ client, observer, faults }`
   */
  build(owner, kind, { enabled, faults, group }) {
    const observer = new TelemetryWireObserver({
      completeStream: true,
      directory: this.directory,
      line: this.line,
      clock,
      mode: this.mode,
      owner,
    })
    this.attempt.observers[owner] = observer
    const chaos = faults ? new TelemetryFaults(observer) : null
    const trace = kind === 'share' ? new ShareTrace() : null
    const kafka = new Kafka({
      clientId: `${this.stem}-${owner}`,
      brokers: this.brokers,
      socketFactory: (options) => {
        const socket = chaos ? chaos.factory(options) : observer.socketFactory(options)
        return trace ? trace.wrap(socket) : socket
      },
      logLevel: logLevel.NOTHING,
    })
    const common = { enableMetricsPush: enabled }
    const client =
      kind === 'producer'
        ? kafka.producer({ ...common, idempotent: true })
        : kind === 'share'
          ? kafka.shareConsumer({
              ...common,
              groupId: group,
              acknowledgementMode: 'explicit',
              maxRecords: SHARE_RECORDS,
              maxWaitTimeInMs: 250,
            })
          : kafka.consumer({
              ...common,
              groupId: group,
              sessionTimeout: 30000,
              heartbeatInterval: 3000,
              maxWaitTimeInMs: 500,
              readUncommitted: false,
            })
    if (chaos) this.defer(() => chaos.restore())
    this.defer(() => observer.assertFunctional({ allowUnanswered: observer.cancellationKeys() }))
    this.defer(() => observer.dispose())
    this.defer(() => observer.closeClient(client))
    return { client, observer, faults: chaos, trace }
  }

  /**
   * Waits for a telemetry client's UUID and its first accepted normal Push, so a fault starts
   * from an established stream.
   * @param {object} member `{ client, observer }`
   * @returns {Promise<string>} the client instance UUID
   */
  async establish({ client, observer }) {
    const uuid = await client.clientInstanceId({ timeout: 5000 })
    await this.find('an established telemetry stream', 8000, () =>
      observer.accepted({ uuid }).find((p) => p.metricsBytes > 0)
    )
    return uuid
  }

  /**
   * Faults the classified telemetry transport of one member, then returns what to verify later.
   * @param {object} member `{ client, observer, faults }`
   * @param {string} owner observer owner key
   * @param {'drop'|'blackhole'|'late'} mode the fault
   * @param {string} uuid the member's client instance UUID
   * @returns {object} the fault record with its socket, ordinal and pre-fault evidence
   */
  inject(member, owner, mode, uuid) {
    const [telemetry] = member.faults.telemetry()
    expect(member.faults.telemetry()).toHaveLength(1)
    const normal = member.faults.normal()
    expect(normal.length).toBeGreaterThan(0)
    expect(normal.every((r) => r.socket !== telemetry.socket)).toBe(true)
    const fault = {
      owner,
      mode,
      uuid,
      record: telemetry,
      ordinal: telemetry.ordinal,
      normalOpen: normal,
      checkpoint: member.observer.checkpoint(),
      injectedAt: clock.now(),
    }
    this.faulted[owner] = [...(this.faulted[owner] ?? []), telemetry.ordinal]
    member.observer.declareFault(telemetry.ordinal)
    member.faults.arm(telemetry, mode)
    return fault
  }

  /**
   * A telemetry-only failure must retire only its own socket, resume on a new one with the same
   * UUID and a new accepted normal Push, and leave every normal-work socket open.
   * @param {object} member `{ observer }`
   * @param {object} fault the record from `inject`
   * @returns {Promise<object>} compact recovery evidence
   */
  async recover(member, fault) {
    const { observer } = member
    const attempted = await this.find(`the post-injection ${fault.mode} Push attempt`, 8000, () =>
      fault.record.attempts.find((r) => r.apiKey === 72 && r.at >= fault.injectedAt)
    )
    const injected =
      fault.mode === 'blackhole'
        ? null
        : await this.find(`the post-injection ${fault.mode} Push`, 8000, () =>
            observer
              .readTranscript({ after: fault.checkpoint })
              .find(
                (r) =>
                  r.apiKey === 72 &&
                  !r.terminating &&
                  r.socketId === fault.ordinal &&
                  r.correlation === attempted.correlation
              )
          )
    if (fault.mode === 'drop')
      await this.find(
        'the actual response drop',
        8000,
        () => fault.record.socket.responseRule.count === 0
      )
    await this.find(`the ${fault.mode} socket retirement`, 15000, () => fault.record.closed)
    const get = await this.find(`the replacement Get after ${fault.mode}`, 15000, () =>
      observer
        .subscriptions({ uuid: fault.uuid, after: fault.checkpoint })
        .find(
          (r) =>
            r.uuid === fault.uuid && r.socketId !== fault.ordinal && r.response?.errorCode === 0
        )
    )
    const push = await this.find(`a new accepted Push after ${fault.mode}`, 15000, () =>
      observer
        .accepted({ uuid: fault.uuid, after: get.index })
        .find((p) => p.socketId === get.socketId && p.metricsBytes > 0)
    )
    await observer.decodeAccepted(push)
    this.pushes.push({ owner: fault.owner, requestKey: push.requestKey })
    expect(fault.normalOpen.every((r) => !r.closed && !r.socket.destroyed)).toBe(true)
    return {
      injectedPush: injected?.requestKey ?? null,
      attempted,
      injectedAt: fault.injectedAt,
      checkpoint: fault.checkpoint,
      droppedCorrelation: fault.mode === 'drop' ? injected.correlation : null,
      owner: fault.owner,
      mode: fault.mode,
      faultedSocket: fault.ordinal,
      replacementSocket: get.socketId,
      replacementGet: get.requestKey,
      recoveredPush: push.requestKey,
      uuid: fault.uuid,
    }
  }

  /**
   * Runs one bounded subarm: its own corpus, producer and consumer, serial with every other arm.
   * The parent budget must hold the whole envelope before anything is issued.
   * @param {string} name arm name
   * @param {number} envelopeMs the arm's frozen envelope
   * @param {{enabled:boolean,fault?:'drop'|'blackhole'|'late'}} arm what the arm exercises
   * @returns {Promise<object>} the arm's three-oracle summary
   */
  async arm(name, envelopeMs, { enabled, fault }) {
    if (this.deadline - clock.now() < envelopeMs) {
      const error = new Error(`The remaining budget cannot hold the ${name} subarm`)
      liveRun.taint(error)
      throw error
    }
    const armName = `${this.stem}-${name}`
    const corpus = new OrdinaryCorpus({ arm: armName, brokers: [this.brokers[0]] })
    const producer = this.build(`${name}-producer`, 'producer', { enabled, faults: !!fault })
    const consumer = this.build(`${name}-consumer`, 'consumer', {
      enabled,
      faults: !!fault,
      group: corpus.group,
    })
    const members = { producer, consumer }
    const body = async (scope) => {
      await bounded(scope, 8000, () =>
        settle([
          () => corpus.create(this.aux),
          () => producer.client.connect(),
          () => consumer.client.connect(),
        ])
      )
      const uuids = {}
      if (enabled && this.mode === 'configured')
        for (const kind of ['producer', 'consumer'])
          uuids[kind] = await this.establish(members[kind])
      const faults = []
      if (fault)
        for (const kind of ['producer', 'consumer'])
          faults.push(this.inject(members[kind], `${name}-${kind}`, fault, uuids[kind]))
      await bounded(scope, 15000, () => corpus.produce(producer.client))
      await bounded(scope, 30000, (inner) =>
        corpus.consume(consumer.client, (ms, check) =>
          pollUntil(inner, ms, check, { delayMs: 100, label: 'All records handled' })
        )
      )
      this.results.arms[name] = await corpus.verify(this.aux)
      this.shapes[name] = {
        rawCommitted: this.results.arms[name].rawCommitted,
        rawEnd: this.results.arms[name].rawEnd,
        perPartition: Object.fromEntries(
          Array.from({ length: PARTITIONS }, (_, p) => [
            p,
            corpus.handled.filter((h) => h.partition === p).length,
          ])
        ),
      }
      if (fault)
        this.results.faults[name] = await Promise.all(
          faults.map((f) => this.recover(members[f.owner.split('-').at(-1)], f))
        )
      if (this.mode === 'configured' && enabled && !fault)
        for (const kind of ['producer', 'consumer']) {
          expect(await members[kind].client.clientInstanceId({ timeout: 0 })).toBe(uuids[kind])
          members[kind].observer.assertHealthy()
        }
      if (!enabled || this.mode === 'base') await this.expectQuiet(members, enabled)
    }
    const failures = []
    try {
      await bounded(this.signal, envelopeMs - 2000, body, { label: `The ${name} subarm` })
    } catch (error) {
      failures.push(error)
    }
    try {
      await bounded(this.signal, 2000, () =>
        settle([
          () => producer.observer.closeClient(producer.client, { timeout: 1500 }),
          () => consumer.observer.closeClient(consumer.client, { timeout: 1500 }),
        ])
      )
    } catch (error) {
      liveRun.taint(error)
      failures.push(error)
    }
    if (failures.length === 1) throw failures[0]
    if (failures.length)
      throw new AggregateError(failures, `The ${name} subarm and its close failed`)
    return this.results.arms[name]
  }

  /**
   * An opted-out client, or any client on the no-plugin fixture, has no telemetry transport.
   * @param {object} members producer and consumer members
   * @param {boolean} enabled whether the arm requested telemetry
   * @returns {Promise<void>} when every applicable absence was asserted
   */
  async expectQuiet(members, enabled) {
    for (const { observer } of Object.values(members)) {
      expect(observer.subscriptions()).toEqual([])
      expect(observer.readTranscript().some((r) => TELEMETRY_APIS.includes(r.apiKey))).toBe(false)
      observer.assertHealthy()
    }
    if (enabled)
      await expect(members.producer.client.clientInstanceId({ timeout: 0 })).rejects.toBeInstanceOf(
        KafkaJSTelemetryError
      )
  }

  /**
   * Share accepted-prefix and rest-release arm on the configured fixture: a member accepts a valid
   * prefix and holds the rest, whose release must reach another member exactly while the accepted
   * prefix stays terminal, with one telemetry response dropped on the holder's own transport.
   * @returns {Promise<object>} evidence of the durable log, holder handler and verifier delivery
   */
  async shareArm() {
    const previous = this.signal
    this.shareOwned = []
    try {
      return await runShareEnvelope({
        signal: previous,
        remainingMs: this.deadline - clock.now(),
        bounded,
        taint: (error) => liveRun.taint(error),
        work: (scope) => {
          this.signal = scope
          return this.shareWork()
        },
        cleanup: () => settle(this.shareOwned),
      })
    } finally {
      this.signal = previous
    }
  }

  /**
   * Executes fault/recovery before acquisition, then the accepted-prefix and release episode.
   * @returns {Promise<object>} independently replayable raw acknowledgement and delivery ledgers
   */
  async shareWork() {
    const topic = `${this.stem}-share-topic`
    const group = `${this.stem}-share-group`
    const keys = Array.from(
      { length: SHARE_RECORDS },
      (_, i) => `${this.stem}-share-${String(i).padStart(4, '0')}`
    )
    const value = (i) => Buffer.from(`${keys[i]}|`.padEnd(VALUE_BYTES, 'x'))
    const plain = new Kafka({
      clientId: `${this.stem}-share-aux`,
      brokers: this.brokers,
      socketFactory: ipv4Factory,
      logLevel: logLevel.NOTHING,
    })
    const feeder = plain.producer({ enableMetricsPush: false, idempotent: true })
    this.shareOwned.push(() => feeder.disconnect({ timeout: 1500 }))
    this.defer(() => feeder.disconnect())
    expect(
      await this.within(10000, 'The share topic', () =>
        this.aux.createTopics({
          topics: [{ topic, numPartitions: 1, replicationFactor: 3 }],
          waitForLeaders: true,
        })
      )
    ).toBe(true)
    await this.within(10000, 'The share feed', async () => {
      await feeder.connect()
      await feeder.send({
        topic,
        acks: -1,
        messages: keys.map((key, i) => ({ key, value: value(i), partition: 0 })),
      })
    })
    await this.within(10000, 'The share group configuration', () =>
      setShareGroupConfigs(this.aux, group, {
        'share.auto.offset.reset': 'earliest',
        'share.record.lock.duration.ms': LOCK_MS,
      })
    )
    const metadata = await this.aux.fetchTopicMetadata({ topics: [topic] })
    const topicId = metadata.topics.find((t) => t.name === topic).topicId
    const holder = this.build('share', 'share', { enabled: true, faults: true, group })
    this.shareOwned.push(() => holder.observer.closeClient(holder.client, { timeout: 1500 }))
    await this.within(10000, 'The holder connect', () => holder.client.connect())
    const uuid = await this.establish(holder)
    const fault = this.inject(holder, 'share', 'drop', uuid)
    const recovery = await this.recover(holder, fault)
    const handled = []
    let acquiredAt
    const settlements = []
    this.defer(
      holder.client.on(holder.client.events.ACKNOWLEDGE, ({ payload }) => settlements.push(payload))
    )
    const entered = deferred()
    const gate = deferred()
    this.defer(() => gate.resolve())
    await this.within(10000, 'The holder run', async () => {
      await holder.client.subscribe({ topics: [topic] })
      await holder.client.run({
        eachBatch: async ({ batch, acknowledge }) => {
          acquiredAt ??= clock.now()
          for (const message of batch.messages)
            handled.push({
              topic: batch.topic,
              partition: batch.partition,
              key: message.key.toString(),
              offset: message.offset,
            })
          for (const message of batch.messages.slice(0, SHARE_ACCEPTED))
            acknowledge(message, 'accept')
          entered.resolve(batch.messages.length)
          await gate.promise
        },
      })
    })
    expect(await this.within(15000, 'The held batch', () => entered.promise)).toBe(SHARE_RECORDS)
    await this.within(8000, 'The holder close', () =>
      holder.observer.closeClient(holder.client, { timeout: 2000 })
    )
    gate.resolve()
    const delivered = await this.within(15000, 'The verifier member', (scope) =>
      verifierMember(group, topic, { kafka: plain, signal: scope, windowMs: 3000 })
    )
    const observedAt = clock.now()
    const verified = delivered.map((d) => JSON.parse(d.identity))
    const verifiedKeys = verified.map((tuple) => Buffer.from(tuple[3], 'hex').toString())
    expect([...verifiedKeys].sort()).toEqual(keys.slice(SHARE_ACCEPTED))
    expect(delivered.every((d) => d.deliveryCount >= 2)).toBe(true)
    expect(handled).toEqual(keys.map((key, i) => ({ topic, partition: 0, offset: String(i), key })))
    const log = await readLog({ brokers: [this.brokers[0]], topic })
    const logged = [...log.values()].flatMap(({ records }) => records.map((r) => r.key.toString()))
    expect([...logged].sort()).toEqual(keys)
    return {
      recovery,
      inputs: {
        topic,
        topicId,
        metadata,
        held: heldAcquisitions(holder.trace.snapshot(), group),
        group,
        acquiredAt: acquisitionStart(holder.trace.snapshot(), group),
        handlerAt: acquiredAt,
        expected: keys.map((key, i) => ({ key, valueHex: value(i).toString('hex') })),
        handled,
        log: [...log.entries()].flatMap(([partition, { records }]) =>
          records.map((r) => ({
            topic,
            partition,
            key: r.key.toString(),
            offset: r.offset,
            valueHex: r.value.toString('hex'),
          }))
        ),
        frames: holder.trace.snapshot(),
        settlements: settlements.map(({ error, ...event }) => ({
          ...event,
          error: error
            ? {
                name: error.name,
                reason: error.reason,
                retriable: error.retriable,
                topic: error.topic,
                partition: error.partition,
              }
            : null,
        })),
        confirmed: confirmedDecisions(holder.trace.snapshot(), group),
        redelivered: verifiedKeys.map((key, i) => ({
          topic: verified[i][0],
          partition: verified[i][1],
          offset: verified[i][2],
          valueHex: verified[i][4],
          key,
          deliveryCount: delivered[i].deliveryCount,
          at: observedAt,
        })),
      },
      keys: keys.length,
      accepted: SHARE_ACCEPTED,
      redelivered: verifiedKeys.sort(),
      lockMs: LOCK_MS,
      faultedSocket: fault.ordinal,
    }
  }

  /**
   * The share capability outcome on a broker with no telemetry plugin.
   * @returns {Promise<object>} the typed refusal or the connected status, with N/A dimensions
   */
  async shareRefusal() {
    const client = this.build('share', 'share', {
      enabled: true,
      faults: false,
      group: `${this.stem}-share-group`,
    }).client
    const outcome = await client.connect().then(
      () => ({ status: 'connected' }),
      (error) => ({
        status: 'rejected',
        name: error?.name,
        reason: error?.reason,
        apiKey: error?.apiKey,
        shareVersion: error?.shareVersion,
      })
    )
    if (['4.0.2', '4.1.2'].includes(this.line))
      expect(outcome).toMatchObject({
        status: 'rejected',
        name: 'KafkaJSShareGroupsNotSupportedError',
        reason: this.line === '4.0.2' ? 'api-not-advertised' : 'api-version-range',
        apiKey: this.line === '4.0.2' ? 76 : 78,
        shareVersion: 0,
      })
    else expect(outcome).toEqual({ status: 'connected' })
    return outcome
  }

  /**
   * Runs every subarm serially inside their frozen envelopes.
   * @param {number} phaseMs the whole phase budget
   * @returns {Promise<void>} when all applicable subarms and comparisons are complete
   */
  async run(phaseMs) {
    this.deadline = clock.now() + phaseMs
    const kafka = new Kafka({
      clientId: `${this.stem}-aux`,
      brokers: this.brokers,
      socketFactory: ipv4Factory,
      logLevel: logLevel.NOTHING,
    })
    this.aux = kafka.admin({ enableMetricsPush: false, apiTimeout: 3000 })
    this.defer(() => this.aux.disconnect())
    await this.within(10000, 'The auxiliary admin connect', () => this.aux.connect())
    const cluster = await this.within(5000, 'The topology check', () =>
      this.aux.describeCluster({ timeout: 3000 })
    )
    expect(cluster.brokers.map((b) => [b.nodeId, b.host, b.port]).sort()).toEqual(
      PORTS[this.mode].map((port, index) => [index + 1, 'localhost', port])
    )
    const sequence = async () => {
      await this.arm('control', 20000, { enabled: false })
      await this.arm('optin', 20000, { enabled: true })
      if (this.mode === 'configured') {
        await this.arm('drop', 25000, { enabled: true, fault: 'drop' })
        await this.arm('blackhole', 30000, { enabled: true, fault: 'blackhole' })
        await this.arm('late', 30000, { enabled: true, fault: 'late' })
        this.results.share = await this.shareArm()
      } else {
        this.results.inapplicable = {
          drop: 'absent telemetry transport',
          blackhole: 'absent telemetry transport',
          late: 'absent telemetry transport',
        }
        this.results.share = await this.shareRefusal()
      }
      for (const name of Object.keys(this.shapes).filter((n) => n !== 'control'))
        expect(this.shapes[name]).toEqual(this.shapes.control)
    }
    if (this.mode === 'configured')
      await withTelemetryResource(
        this.aux,
        {
          name: this.stem,
          entries: {
            metrics: 'org.apache.kafka.',
            'interval.ms': '1000',
            match: `client_id=^${this.stem}-.*$`,
          },
        },
        sequence,
        { signal: this.signal }
      )
    else await sequence()
    this.results.seams = STRICT_SEAMS.map(([file, title]) => {
      expect(fs.readFileSync(path.join(ROOT, file), 'utf8')).toContain(title)
      return { file, title, lane: 'strict seam, unit lane' }
    })
  }

  /**
   * @returns {void} hands the case record to the shared persistence and verification
   */
  finishRecord() {
    const base = {
      wireVersions: this.mode === 'configured' ? { 71: 0, 72: 0 } : {},
      run: { stem: this.stem, line: this.line, project: process.env.COMPOSE_PROJECT_NAME },
      mode: this.mode,
      results: this.results,
      pushes: this.pushes,
      allowed: { errors: {}, faultedSockets: this.faulted },
    }
    if (this.mode === 'configured') {
      const manifestFile = path.join(ROOT, 'scripts/e2e/telemetry/fixtures/manifest.json')
      const manifest = JSON.parse(fs.readFileSync(manifestFile, 'utf8'))
      base.run.composeSha256 = fileSha(path.join(ROOT, 'docker-compose.telemetry.yml'))
      base.fixture = {
        manifestSha256: fileSha(manifestFile),
        reporterJar: manifest.lines[this.line].reporterJar,
      }
    }
    this.attempt.record = base
  }
}

const BUDGET = 188000
live(
  'foreground outcomes',
  liveCase(BUDGET, (scope) =>
    runCase('telemetry-outcomes', async (cleanup, publish, attempt) => {
      const line = process.env.KAFKA_VERSION
      const directory = process.env.TELEMETRY_EVIDENCE_DIR
      if (!line) throw new Error('KAFKA_VERSION must name the broker line')
      if (!directory || !path.isAbsolute(directory))
        throw new Error('TELEMETRY_EVIDENCE_DIR must name the absolute run directory')
      const scenario = new OutcomeCase({
        signal: scope,
        line,
        directory,
        stem: attempt.record.run.stem,
        attempt,
      })
      cleanup.defer(() => scenario.close())
      const phase = BUDGET - 30000
      await bounded(
        scope,
        phase,
        (inner) => {
          scenario.signal = inner
          return scenario.run(phase)
        },
        { label: 'The foreground outcomes scenario' }
      )
      attempt.stage = 'verification'
      scenario.finishRecord()
      publish({
        name: 'telemetry-outcomes',
        directory,
        observers: attempt.observers,
        record: attempt.record,
      })
    })
  ),
  caseTimeout(BUDGET)
)
