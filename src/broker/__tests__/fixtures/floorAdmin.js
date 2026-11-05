const { inspect } = require('node:util')
const { randomBytes } = require('node:crypto')
// eslint-disable-next-line n/no-unsupported-features/node-builtins -- test-only timer ownership
const { createHook } = require('node:async_hooks')
const Broker = require('../../index')
const defaultSocketFactory = require('../../../network/socketFactory')
const ISOLATION_LEVEL = require('../../../protocol/isolationLevel')
const COORDINATOR_TYPES = require('../../../protocol/coordinatorTypes')
const ConfigResourceTypes = require('../../../protocol/configResourceTypes')
const {
  KafkaJSError,
  KafkaJSTimeout,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../../../errors')
const { Kafka, AlterConfigOpTypes, logLevel } = require('../../../../index')
const FloorCapabilities = require('../../../../testHelpers/floorCapabilities')
const {
  fenceUntilSettled,
  describeScramUntilApplied,
} = require('../../../../testHelpers/adminCatchUp')
const { createChaosSocketFactory } = require('../../../../testHelpers/chaosSocket')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const { readLog, endOffsets, committedRecords } = require('../../../../testHelpers/rawLog')
const { instrument } = require('../../../../scripts/soak/soak')
const {
  createConnectionPool,
  newLogger,
  plainTextBrokers,
  saslBrokers,
  saslConnectionOpts,
  wireVersionsOf,
  writeEvidence,
} = require('testHelpers')
const { CleanupStack } = require('../../../producer/__tests__/fixtures/liveProbe')
const {
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')

/** The evidence file of the floor core. */
const EVIDENCE = 'floor-core'
/** The client id every case's clients carry; a client-metrics subscription matches it. */
const CLIENT_ID = 'e2e-floor-admin'
/** The cases `FloorAdminCases#run` accepts, in the order the suite runs them. */
const METHODS = [
  'offsets',
  'diagnostics',
  'abortFence',
  'cluster',
  'logDirs',
  'reassignment',
  'security',
  'connection',
]
/** The inclusive budget the eight cases share, cleanup included. */
const SUITE_MS = 540000
/** The cleanup time held back across the suite: 60000 ms, split evenly over the cases. */
const CASE_RESERVE_MS = 60000 / METHODS.length
/** The most group ids tried when looking for three that sit on two coordinators. */
const MAX_GROUP_CANDIDATES = 100
/** The idle limit of the reopening case, and the gap between its sends. */
const IDLE_MS = 5000
const SEND_GAP_MS = 20000
/**
 * The cluster retries of a producer, which takes them from the Kafka options: its coordinator
 * lookups run on the cluster's retrier, and the `retry` producer option does not reach it. On a
 * fresh cluster the first transactional lookup is answered "coordinator not available" until the
 * broker has created `__transaction_state`, which takes seconds; the delays here add up to 13 s.
 * An idempotent producer also refuses to be built without retries.
 * @see https://github.com/apache/kafka/blob/4.0.2/core/src/main/scala/kafka/server/KafkaApis.scala
 */
const PRODUCER_RETRY = {
  retries: 10,
  initialRetryTime: 100,
  multiplier: 2,
  maxRetryTime: 2000,
  factor: 0,
}
/** The error codes the cases read from broker truth. */
const PRODUCER_FENCED = 90
const GROUP_ID_NOT_FOUND = 69
const RESOURCE_NOT_FOUND = 91

/**
 * Walks an error's cause chain, once per link.
 * @param {*} error the failure of a public call
 * @returns {Error[]} the error and its causes, outermost first; empty for a non-object
 */
const chainOf = (error) => {
  const chain = []
  for (let link = error; link && typeof link === 'object' && !chain.includes(link);) {
    chain.push(link)
    link = link.cause
  }
  return chain
}

/**
 * @param {*} error the failure of a public call
 * @returns {*} the typed capability refusal in its cause chain, else the error itself, so a wrong
 *   class still reaches the class assertion
 */
const refusalOf = (error) =>
  chainOf(error).find((link) => link instanceof KafkaJSServerDoesNotSupportApiKey) ?? error

/** @param {*} error a failure @returns {*} the deepest cause of its chain */
const deepest = (error) => chainOf(error).at(-1) ?? error

/**
 * Turns a rejection into a value, so a case can assert on the failure itself.
 * @param {Promise<*>} promise the call under test
 * @returns {Promise<*>} what it rejected with, or `null` when it resolved
 */
const rejectionOf = (promise) =>
  promise.then(
    () => null,
    (error) => error
  )

/** The features no case moves: `metadata.version` has its own downgrade rules, `kraft.version` its own path. */
const FIXED_FEATURES = new Set(['metadata.version', 'kraft.version'])

/**
 * Picks a feature update the controller can validate that differs from the finalized level: a safe
 * downgrade by one level where the supported range allows it, else an upgrade by one.
 * @param {{ finalizedFeatures: object, supportedFeatures: object }} features a `describeFeatures` answer
 * @returns {{ name: string, level: number, target: number, upgradeType: string }|undefined}
 *   the update, with the level it starts from; undefined when no feature has another legal level
 */
const differentLevel = ({ finalizedFeatures, supportedFeatures }) => {
  const options = Object.keys(finalizedFeatures)
    .sort()
    .filter((name) => Object.hasOwn(supportedFeatures, name) && !FIXED_FEATURES.has(name))
    .flatMap((name) => {
      const { maxVersionLevel: level } = finalizedFeatures[name]
      const { minVersion, maxVersion } = supportedFeatures[name]
      return [
        level > minVersion && { name, level, target: level - 1, upgradeType: 'safe-downgrade' },
        level < maxVersion && { name, level, target: level + 1, upgradeType: 'upgrade' },
      ].filter(Boolean)
    })
  return options.find(({ upgradeType }) => upgradeType === 'safe-downgrade') ?? options[0]
}

/**
 * Every encoding in which a credential could reach a log, a request record or a buffer dump.
 * @param {string|Buffer} secret the credential
 * @returns {string[]} the markers, none empty
 */
const markersOf = (secret) => {
  const bytes = Buffer.from(secret)
  return [
    bytes.toString('utf8'),
    bytes.toString('hex'),
    bytes.toString('base64'),
    bytes.toString('hex').match(/../g).join(' '),
    [...bytes].join(','),
  ]
}

/**
 * Follows the timers created from `start` on, by identity, so a case can be held to the timers it
 * made itself rather than to the process-wide count, which other work moves too.
 */
class TimerLedger {
  /** Builds the hook; it does nothing until `start`. */
  constructor() {
    /** @type {Map<number, object>} the pending timers by async id */
    this.pending = new Map()
    this.hook = createHook({
      init: (id, type, _trigger, resource) => {
        if (type === 'Timeout') this.pending.set(id, resource)
      },
      destroy: (id) => this.pending.delete(id),
    })
  }

  /** @returns {void} */
  start() {
    this.hook.enable()
  }

  /**
   * Stops following timers. A cleared or fired timer is dropped by the hook after the turn that
   * ended it, so the caller yields one turn first.
   * @returns {object[]} the timers started since `start` that neither fired nor were cleared
   */
  stop() {
    this.hook.disable()
    const retained = [...this.pending.values()]
    this.pending.clear()
    return retained
  }
}

/**
 * The inclusive wall-clock budget the floor administration cases share. The clock starts at the
 * first case and never restarts, so a later case gets only what the earlier ones left.
 */
class SuiteBudget {
  /**
   * @param {object} [options]
   * @param {number} [options.totalMs] the whole suite's budget, cleanup included
   * @param {number} [options.reserveMs] the least a case may start with, one cleanup reserve
   */
  constructor({ totalMs = SUITE_MS, reserveMs = CASE_RESERVE_MS } = {}) {
    this.totalMs = totalMs
    this.reserveMs = reserveMs
    this.due = null
  }

  /**
   * Hands out what is left of the suite budget.
   * @returns {number} milliseconds left until the shared deadline
   * @throws {Error} when no more than one cleanup reserve is left; nothing was mutated yet
   */
  take() {
    this.due ??= performance.now() + this.totalMs
    const left = this.due - performance.now()
    if (left <= this.reserveMs) throw new Error('The shared administration budget is exhausted')
    return Math.floor(left)
  }
}

/**
 * One case's bounded environment: its absolute deadline and cleanup reserve, the public clients it
 * owns, independent raw observations, and the secret-safe evidence it writes. Every operation is
 * a lazy thunk run under `bounded`, so nothing starts after the deadline or an abort.
 */
class FloorProbe {
  /**
   * @param {object} options
   * @param {AbortSignal} options.signal the case's scope, aborted when its budget ends
   * @param {number} options.budgetMs the inclusive time this case may use, cleanup included
   * @param {number} options.reserveMs the part of it held back for cleanup
   * @throws {Error} when the budget does not exceed the reserve, or the run is already tainted
   */
  constructor({ signal, budgetMs, reserveMs }) {
    liveRun.assertClean()
    signal.throwIfAborted()
    if (!Number.isSafeInteger(budgetMs) || budgetMs <= reserveMs) {
      throw new Error('The case budget must exceed its cleanup reserve')
    }
    this.signal = signal
    this.reserveMs = reserveMs
    this.due = performance.now() + budgetMs
    this.cleanup = new CleanupStack()
    this.cleaning = false
    this.requests = []
    this.logs = []
    this.sockets = []
    this.markers = []
    this.removers = []
    this.timers = null
    this.factory = defaultSocketFactory()
    this.logger = newLogger()
    this.resources = createResourceProbe({
      sockets: () => ({
        created: this.sockets.length,
        destroyed: this.sockets.filter((socket) => socket.destroyed).length,
      }),
      activeResources: true,
    })
  }

  /**
   * Starts holding the case to the timers it creates: `close` fails the case on any still pending.
   * @returns {void}
   */
  trackTimers() {
    this.timers = new TimerLedger()
    this.timers.start()
  }

  /** @returns {number} milliseconds left inside the case's inclusive deadline */
  remaining() {
    return Math.max(0, this.due - performance.now())
  }

  /**
   * Notes the resources the case starts from, before it mutates anything.
   * @returns {Promise<void>}
   */
  async begin() {
    this.baseline = await this.resources.snapshot()
  }

  /**
   * Runs one lazy operation inside the time left before the cleanup reserve.
   * @template T
   * @param {() => Promise<T>} work started only when the case may still start work
   * @returns {Promise<T>} what `work` returned
   * @throws {Error} `work`'s failure, an abort, a tainted run or no time left; no retry or wrapping
   */
  invoke(work) {
    liveRun.assertClean()
    this.signal.throwIfAborted()
    const allowance = this.remaining() - this.reserveMs
    if (allowance <= 0) throw new Error('The case has no time left before its cleanup reserve')
    return bounded(this.signal, allowance, work)
  }

  /**
   * The socket factory of every client and raw broker of the case, so each socket is counted.
   * @param {object} options standard socket-factory options
   * @returns {import('net').Socket} the real socket
   */
  socket(options) {
    const socket = this.factory(options)
    this.sockets.push(socket)
    return socket
  }

  /**
   * Creates, owns and connects a public client; its disconnect is registered before it connects.
   * Admin and consumer clients make one attempt per call; a producer gets `PRODUCER_RETRY`.
   * @param {'admin'|'consumer'|'producer'} kind the client factory
   * @param {object} [clientOptions] the factory's options
   * @param {object} [kafkaOptions] `Kafka` constructor overrides
   * @returns {Promise<object>} the connected client
   * @throws {Error} the construction or connection failure
   */
  client(kind, clientOptions = {}, kafkaOptions = {}) {
    return this.invoke(async () => {
      const kafka = new Kafka({
        clientId: CLIENT_ID,
        brokers: plainTextBrokers(),
        retry: kind === 'producer' ? PRODUCER_RETRY : { retries: 0 },
        ...kafkaOptions,
        logLevel: logLevel.DEBUG,
        logCreator: () => (entry) => this.logs.push(entry),
        socketFactory: (options) => this.socket(options),
      })
      const client = kafka[kind](clientOptions)
      this.cleanup.add(`${kind} disconnect`, () => client.disconnect())
      this.removers.push(
        client.on(client.events.REQUEST, ({ payload }) => this.requests.push(payload))
      )
      await client.connect()
      return client
    })
  }

  /**
   * Starts the case's main admin, the one that creates topics.
   * @returns {Promise<object>} the connected admin
   */
  async start() {
    this.admin = await this.client('admin', {
      apiTimeout: Math.min(30000, this.remaining() - this.reserveMs),
    })
    return this.admin
  }

  /**
   * Opens a raw broker on one address, apart from every client; its disconnect is registered first.
   * @param {string} host the broker's host
   * @param {number} port the broker's port
   * @param {object} [poolOptions] connection-pool overrides, such as `connectionsMaxIdle`
   * @returns {Promise<Broker>} the connected broker
   */
  async raw(host, port, poolOptions = {}) {
    const broker = new Broker({
      connectionPool: createConnectionPool({
        host,
        port: Number(port),
        socketFactory: (options) => this.socket(options),
        ...poolOptions,
      }),
      logger: this.logger,
    })
    this.cleanup.add('raw broker disconnect', () => broker.disconnect())
    await this.invoke(() => broker.connect())
    return broker
  }

  /** @returns {Promise<Broker>} the case's raw broker on the first seed, opened once */
  async seed() {
    if (!this.seedBroker) {
      const [host, port] = plainTextBrokers()[0].split(':')
      this.seedBroker = await this.raw(host, port)
    }
    return this.seedBroker
  }

  /**
   * Reads a partition's leader and replicas from the cluster's own metadata.
   * @param {string} topic an existing topic
   * @param {number} [partition] the partition
   * @returns {Promise<{ nodeId: number, host: string, port: number, replicas: number[] }>} the
   *   leader's identity and address, and the partition's replicas in preference order
   */
  async leaderNode(topic, partition = 0) {
    const metadata = await this.invoke(() => this.admin.fetchTopicMetadata({ topics: [topic] }))
    const { leader, replicas } = metadata.topics[0].partitions.find(
      (entry) => entry.partitionId === partition
    )
    const seed = await this.seed()
    const { brokers } = await this.invoke(() => seed.metadata([topic]))
    const { host, port } = brokers.find(({ nodeId }) => nodeId === leader)
    return { nodeId: leader, host, port, replicas }
  }

  /**
   * Opens a raw broker on a partition's leader.
   * @param {string} topic an existing topic
   * @param {number} [partition] the partition
   * @returns {Promise<{ broker: Broker, nodeId: number, replicas: number[] }>} the connected leader
   *   and the partition's replicas
   */
  async leader(topic, partition = 0) {
    const { nodeId, host, port, replicas } = await this.leaderNode(topic, partition)
    return { broker: await this.raw(host, port), nodeId, replicas }
  }

  /**
   * Creates a uniquely named topic; its deletion is registered before it is created.
   * @param {object} [options]
   * @param {string} [options.name] a suffix for the name
   * @param {number} [options.partitions] the partition count
   * @param {number} [options.replicationFactor] the replication factor
   * @returns {Promise<string>} the topic's name
   */
  async topic({ name = 'data', partitions = 1, replicationFactor = 3 } = {}) {
    const topic = `e2e-p9d-floor-${name}-${randomBytes(6).toString('hex')}`
    this.cleanup.add('delete topic', () => this.admin.deleteTopics({ topics: [topic] }))
    await this.invoke(() =>
      this.admin.createTopics({
        topics: [{ topic, numPartitions: partitions, replicationFactor }],
      })
    )
    return topic
  }

  /**
   * Opens a transaction and registers, before its first send, the one way this case settles it: an
   * `abort` ends it through its producer when it is still active; a `fence` leaves it to the case's
   * admin, which fences the transactional id.
   * @param {object} producer a connected transactional producer
   * @param {string} transactionalId the producer's transactional id
   * @param {'abort'|'fence'} [settle] who ends the transaction at cleanup
   * @returns {Promise<object>} the open transaction
   */
  async transaction(producer, transactionalId, settle = 'abort') {
    const transaction = await this.invoke(() => producer.transaction())
    if (settle === 'fence') {
      this.cleanup.add('fence transaction', () =>
        this.admin.fenceProducers({ transactionalIds: [transactionalId] })
      )
    } else {
      this.cleanup.add('abort transaction', () =>
        transaction.isActive() ? transaction.abort() : undefined
      )
    }
    return transaction
  }

  /**
   * Polls an independent readback until it holds, inside the case's deadline.
   * @param {() => Promise<boolean>} predicate the readback
   * @param {number} [delayMs] the pause between reads
   * @param {number} [limitMs] the most this wait may take, within the case's deadline
   * @returns {Promise<void>}
   * @throws {Error} the readback's failure, an abort or the deadline
   */
  poll(predicate, delayMs = 250, limitMs = Infinity) {
    return this.invoke(() =>
      pollUntil(this.signal, Math.min(limitMs, this.remaining() - this.reserveMs), predicate, {
        delayMs,
      })
    )
  }

  /**
   * Reads partition 0 of a topic straight from its leader.
   * @param {string} topic the topic
   * @param {number} [isolationLevel] the read's isolation level
   * @returns {Promise<{ endOffset: string, records: object[], abortedTransactions: object[] }>}
   *   the raw log, control and aborted records included under read-uncommitted
   */
  async physical(topic, isolationLevel = ISOLATION_LEVEL.READ_UNCOMMITTED) {
    const log = await this.invoke(() =>
      readLog({ brokers: plainTextBrokers(), topic, isolationLevel, maxIdleFetches: 100 })
    )
    return log.get(0)
  }

  /**
   * The values a read-committed reader is given, once the last stable offset reached the end.
   * @param {string} topic the topic
   * @returns {Promise<string[]>} the committed-visible values of partition 0, in order
   */
  async committed(topic) {
    const brokers = plainTextBrokers()
    await this.poll(async () => {
      const [high, stable] = await Promise.all([
        endOffsets({ brokers, topic }),
        endOffsets({ brokers, topic, isolationLevel: ISOLATION_LEVEL.READ_COMMITTED }),
      ])
      return stable.get(0) === high.get(0)
    })
    const log = await this.physical(topic, ISOLATION_LEVEL.READ_COMMITTED)
    return committedRecords(log).map(({ value }) => String(value))
  }

  /**
   * Requires every item of a result to have succeeded before its success fields are used.
   * @param {object|object[]} result one admin item or a list of them
   * @returns {void}
   * @throws {Error} on a failed item
   */
  items(result) {
    for (const item of Array.isArray(result) ? result : [result]) {
      if (item.errorCode !== 0 || item.error) {
        throw new Error('An administration item failed', { cause: item.error })
      }
    }
  }

  /**
   * Keeps a credential out of the case's evidence and diagnostics.
   * @param {string|Buffer} secret the credential
   * @returns {void}
   */
  guard(secret) {
    this.markers.push(...markersOf(secret))
  }

  /**
   * @returns {boolean} whether the case's logs, request records or errors hold a guarded credential
   * @param {Error[]} [errors] failures to inspect as well
   */
  leaked(errors = []) {
    const text = inspect(
      { logs: this.logs, requests: this.requests, errors },
      { depth: null, maxArrayLength: null }
    )
    return this.markers.some((marker) => text.includes(marker))
  }

  /**
   * Writes the case's evidence line; call it only after the case's assertions.
   * @param {string} name the case id, `F10` to `F17`
   * @param {object} facts safe scalar and array facts
   * @returns {void}
   * @throws {Error} when a fact holds a guarded credential
   */
  record(name, facts) {
    const text = JSON.stringify(facts)
    if (this.markers.some((marker) => text.includes(marker)))
      throw new Error('Secret evidence value')
    writeEvidence(EVIDENCE, {
      case: name,
      wireVersions: wireVersionsOf(this.requests),
      config: { clientId: CLIENT_ID },
      ...facts,
    })
  }

  /**
   * Runs every restoration in reverse order of registration, then checks the case left no socket
   * behind, and, once `trackTimers` was called, none of its own timers. Each restoration is
   * attempted whatever an earlier one did.
   * @returns {Promise<Error[]>} the failures, none when everything was restored
   */
  async close() {
    this.cleaning = true
    const errors = await this.cleanup.closeAll({ budgetMs: Math.max(1, this.remaining()) })
    for (const remove of this.removers.splice(0)) remove()
    await new Promise((resolve) => setImmediate(resolve))
    const after = await this.resources.snapshot()
    if (this.baseline && after.sockets.open !== this.baseline.sockets.open) {
      const error = new Error('The case retained sockets')
      liveRun.taint(error)
      errors.push(error)
    }
    const owned = this.timers?.stop() ?? []
    if (owned.length) {
      const count = (snapshot = {}) => snapshot.activeResources?.Timeout ?? 0
      const error = new Error(
        `The case retained ${owned.length} of its own timers (process-wide Timeout count ${count(this.baseline)} before, ${count(after)} after)`
      )
      liveRun.taint(error)
      errors.push(error)
    }
    if (this.remaining() <= 0) {
      const error = new Error('The case cleanup exceeded its inclusive deadline')
      liveRun.taint(error)
      errors.push(error)
    }
    return errors
  }
}

/**
 * Holds the transport-ready notification of every dial made while `holding`, so the client's own
 * connection timeout ends the attempt. The socket still reaches Kafka; its destruction is the
 * client's.
 */
class HeldDial {
  /** @param {Function} transport the factory that makes the real sockets */
  constructor(transport) {
    this.transport = transport
    this.holding = false
    this.attempts = []
    this.sockets = []
    this.factory = this.factory.bind(this)
  }

  /**
   * @param {object} options the public socket-factory options
   * @returns {import('net').Socket} a real socket, withheld when this dial is held
   */
  factory(options) {
    const held = this.holding
    const startedAt = performance.now()
    const socket = this.transport({
      ...options,
      onConnect: () => {
        if (!held) options.onConnect()
      },
    })
    this.sockets.push(socket)
    if (held) {
      const destroy = socket.destroy.bind(socket)
      let counted = false
      socket.destroy = (...params) => {
        if (!counted) {
          counted = true
          this.attempts.push(performance.now() - startedAt)
        }
        return destroy(...params)
      }
    }
    return socket
  }
}

/**
 * The floor's administration and connection cases, one public method each. A case is built from
 * actual public calls, compares real negotiated ranges with the recorded probe, asserts typed
 * refusals with both ranges, and writes its evidence only after every assertion.
 */
class FloorAdminCases {
  /**
   * @param {object} options
   * @param {AbortSignal} options.signal the case's scope
   * @param {number} options.budgetMs the inclusive time of this case, cleanup included
   * @throws {Error} on a missing floor, a tainted run or too small a budget
   */
  constructor({ signal, budgetMs }) {
    this.floor = process.env.KAFKA_VERSION
    if (!this.floor) throw new Error('KAFKA_VERSION names the floor under test')
    this.probe = new FloorProbe({ signal, budgetMs, reserveMs: CASE_RESERVE_MS })
    this.id = randomBytes(8).toString('hex')
    this.unsupported = []
  }

  /**
   * Runs one named case under the probe; restoration waits for `close`.
   * @param {string} method one of `METHODS`
   * @returns {Promise<void>}
   * @throws {Error} an unknown case, or the case's own failure, kept as the primary error
   */
  async run(method) {
    if (!METHODS.includes(method)) throw new Error('Unknown floor administration case')
    try {
      await this.probe.begin()
      await this[method]()
    } catch (error) {
      this.primary = error
      throw error
    }
  }

  /**
   * Attempts every restoration and disconnect. A case failure stays the primary error, with the
   * restoration failures attached to it; without one, restoration failures are thrown together.
   * @returns {Promise<void>}
   * @throws {AggregateError} when the case passed but a restoration failed
   */
  async close() {
    const errors = await this.probe.close()
    if (this.primary !== undefined) {
      if (this.primary !== null && typeof this.primary === 'object') {
        this.primary.cleanupErrors = errors
      }
      return
    }
    if (errors.length) throw new AggregateError(errors, 'The floor case cleanup failed')
  }

  /**
   * Observes the real negotiation of the named APIs and builds the oracle for this case.
   * @param {string[]} names the APIs the case uses
   * @returns {Promise<object>} the safe range facts, one entry per API
   * @throws {Error} on a missing, stale or nonoverlapping range
   */
  async capabilities(names) {
    const seed = await this.probe.seed()
    const facts = FloorCapabilities.fromBroker(seed, names, this.floor)
    const table = (key) => Object.fromEntries(names.map((name) => [name, facts[name][key]]))
    this.oracle = new FloorCapabilities({
      recorded: table('recorded'),
      observed: table('offered'),
      implemented: table('implemented'),
    })
    return facts
  }

  /**
   * Asserts one typed refusal and keeps its record for the evidence line.
   * @param {*} error what the public call rejected with
   * @param {string} api the API whose option is unavailable
   * @param {number} required the version the option needs
   * @param {string} option the option the refusal names
   * @returns {void}
   * @throws {Error} unless it is the typed refusal naming the option and both ranges
   */
  refuse(error, api, required, option) {
    const typed = refusalOf(error)
    this.oracle.unsupported(typed, api, required, option)
    const client = this.oracle.implemented[api]
    this.unsupported.push({
      api,
      option,
      required,
      name: typed.name,
      message: typed.message,
      client: [Math.min(...client), Math.max(...client)],
      offered: this.oracle.observed[api],
    })
  }

  /**
   * @param {string} api an API name
   * @returns {number[]} the wire versions of its requests the case's clients sent, in order
   */
  versionsOf(api) {
    return this.probe.requests.filter(({ apiName }) => apiName === api).map((r) => r.apiVersion)
  }

  /**
   * Requires that every request of an API the case sent used exactly the negotiated version.
   * @param {string} api an API name
   * @param {number} version the version the real negotiation selects
   * @returns {void}
   * @throws {Error} when no request was sent or one used another version
   */
  expectNegotiated(api, version) {
    const versions = this.versionsOf(api)
    expect(versions.length).toBeGreaterThan(0)
    expect(new Set(versions)).toEqual(new Set([version]))
  }

  /**
   * F10: three groups on two coordinators round trip committed offsets through the negotiated
   * OffsetFetch, and one group's offset is deleted through OffsetDelete v0.
   * @returns {Promise<void>}
   * @throws {Error} on any offset, coordinator, version or physical-corpus mismatch
   */
  async offsets() {
    const p = this.probe
    const facts = await this.capabilities(['OffsetDelete', 'OffsetFetch'])
    const admin = await p.start()
    const topic = await p.topic({ name: 'offsets' })
    const producer = await p.client('producer', { idempotent: true, linger: 0 })
    const corpus = []
    for (let batch = 0; batch < 5; batch++) {
      const messages = Array.from({ length: 20 }, (_, index) => ({
        value: `offset-${batch * 20 + index}`,
      }))
      corpus.push(...messages.map(({ value }) => value))
      const [sent] = await p.invoke(() => producer.send({ topic, messages }))
      expect(sent.baseOffset).toBe(String(batch * 20))
    }
    const log = await p.physical(topic)
    expect(log.endOffset).toBe('100')
    expect(log.records.map(({ value }) => String(value))).toEqual(corpus)

    const seed = await p.seed()
    const buckets = new Map()
    for (let index = 0; index < MAX_GROUP_CANDIDATES; index++) {
      const groupId = `e2e-floor-offsets-${this.id}-${index}`
      const { coordinator } = await p.invoke(() =>
        seed.findGroupCoordinator({ groupId, coordinatorType: COORDINATOR_TYPES.GROUP })
      )
      if (!buckets.has(coordinator.nodeId) && buckets.size === 2) continue
      if (!buckets.has(coordinator.nodeId)) buckets.set(coordinator.nodeId, [])
      const ids = buckets.get(coordinator.nodeId)
      if (ids.length < 2) ids.push(groupId)
      if (buckets.size === 2 && [...buckets.values()].flat().length >= 3) break
    }
    expect(buckets.size).toBe(2)
    const [first, second] = [...buckets.values()]
    const groups = [first[0], second[0], first[1] ?? second[1]]
    expect(groups.every((id) => typeof id === 'string')).toBe(true)
    p.cleanup.add('delete groups', async () => {
      const rows = await admin.deleteGroups(groups)
      for (const row of rows) {
        if (![0, GROUP_ID_NOT_FOUND].includes(row.errorCode)) p.items(row)
      }
    })

    const committed = ['0', '20', '50']
    const rawOffsetOf = async (groupId) =>
      (
        await p.invoke(() =>
          admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false })
        )
      )[0].partitions[0].offset
    const rawCommittedPositions = []
    for (const [index, groupId] of groups.entries()) {
      await p.invoke(() =>
        admin.setOffsets({
          groupId,
          topic,
          partitions: [{ partition: 0, offset: committed[index] }],
        })
      )
      rawCommittedPositions.push(await rawOffsetOf(groupId))
    }
    expect(rawCommittedPositions).toEqual(committed)

    p.requests.length = 0
    const rows = await p.invoke(() =>
      admin.fetchOffsetsForGroups({
        groups: groups.map((groupId) => ({ groupId, topics: [{ topic, partitions: [0] }] })),
        requireStable: true,
      })
    )
    p.items(rows)
    for (const row of rows) for (const entry of row.topics) p.items(entry.partitions)
    const groupIdsReturned = rows.map(({ groupId }) => groupId)
    expect([...groupIdsReturned].sort()).toEqual([...groups].sort())
    expect(new Set(groupIdsReturned).size).toBe(groups.length)
    const offsetOf = new Map(rows.map((row) => [row.groupId, row.topics[0].partitions[0].offset]))
    const fetchedOffsets = groups.map((groupId) => offsetOf.get(groupId))
    expect(fetchedOffsets).toEqual(committed)
    const fetches = p.requests.filter(({ apiName }) => apiName === 'OffsetFetch')
    expect(fetches).toHaveLength(2)
    expect(new Set(fetches.map(({ broker }) => broker)).size).toBe(2)
    for (const { apiVersion } of fetches) {
      expect(apiVersion).toBeGreaterThanOrEqual(8)
      expect(apiVersion).toBeLessThanOrEqual(facts.OffsetFetch.selected)
    }

    const deleted = await p.invoke(() =>
      admin.deleteOffsets({ groupId: groups[2], topics: [{ topic, partitions: [0] }] })
    )
    const deletedPartition = deleted.topics[0].partitions[0]
    expect(deletedPartition.errorCode).toBe(0)
    expect(await rawOffsetOf(groups[2])).toBe('-1')
    expect(await rawOffsetOf(groups[0])).toBe('0')
    expect(await rawOffsetOf(groups[1])).toBe('20')
    this.expectNegotiated('OffsetDelete', facts.OffsetDelete.selected)

    p.record('F10', {
      groupCount: groups.length,
      coordinatorBrokers: fetches.map(({ broker }) => broker),
      committedOffsets: rawCommittedPositions,
      fetchedOffsets,
      offsetFetchVersions: fetches.map(({ apiVersion }) => apiVersion),
      offsetDeleteVersion: facts.OffsetDelete.selected,
      deletedCode: deletedPartition.errorCode,
      offsetAfterDelete: '-1',
      physicalCount: log.records.length,
      ranges: facts,
    })
  }

  /**
   * F11: an open transaction is found by DescribeProducers, DescribeTransactions and
   * ListTransactions at the negotiated versions, and the pattern filter is refused with both ranges.
   * @returns {Promise<void>}
   * @throws {Error} on a mismatch between the three observations or an untyped refusal
   */
  async diagnostics() {
    const p = this.probe
    const facts = await this.capabilities([
      'DescribeProducers',
      'DescribeTransactions',
      'ListTransactions',
    ])
    const admin = await p.start()
    const topic = await p.topic({ name: 'diagnostics' })
    const transactionalId = `e2e-floor-diagnostics-${this.id}`
    const producer = await p.client('producer', {
      transactionalId,
      idempotent: true,
      maxInFlightRequests: 1,
    })
    const transaction = await p.transaction(producer, transactionalId)
    await p.invoke(() => transaction.send({ topic, messages: [{ value: 'open' }] }))

    const description = (
      await p.invoke(() => admin.describeTransactions({ transactionalIds: [transactionalId] }))
    )[0]
    p.items(description)
    expect(description.state).toBe('Ongoing')
    expect(description.topics).toContainEqual({ topic, partitions: [0] })
    const seed = await p.seed()
    const { coordinator } = await p.invoke(() =>
      seed.findGroupCoordinator({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
    )
    this.expectNegotiated('DescribeTransactions', facts.DescribeTransactions.selected)
    const described = p.requests.filter(({ apiName }) => apiName === 'DescribeTransactions')
    expect(described.map(({ broker }) => broker)).toEqual([
      `${coordinator.host}:${coordinator.port}`,
    ])

    const producers = await p.invoke(() =>
      admin.describeProducers({ topicPartitions: [{ topic, partitions: [0] }] })
    )
    p.items(producers[0].partitions[0])
    const state = producers[0].partitions[0].activeProducers.find(
      ({ producerId }) => producerId === description.producerId
    )
    expect(state).toMatchObject({
      producerId: description.producerId,
      producerEpoch: description.producerEpoch,
    })
    this.expectNegotiated('DescribeProducers', facts.DescribeProducers.selected)

    const listed = await p.invoke(() =>
      admin.listTransactions({
        states: ['Ongoing'],
        producerIds: [description.producerId],
        durationFilter: 0,
      })
    )
    expect(listed.transactions).toContainEqual({
      transactionalId,
      producerId: description.producerId,
      state: 'Ongoing',
    })
    this.expectNegotiated('ListTransactions', facts.ListTransactions.selected)

    const mark = p.requests.length
    const refusal = await rejectionOf(
      p.invoke(() =>
        admin.listTransactions({
          states: ['Ongoing'],
          transactionalIdPattern: `^${transactionalId}$`,
        })
      )
    )
    this.refuse(refusal, 'ListTransactions', 2, 'transactionalIdPattern')
    const patternFrames = p.requests
      .slice(mark)
      .filter(({ apiName }) => apiName === 'ListTransactions')
    expect(patternFrames).toHaveLength(0)

    p.record('F11', {
      producerId: description.producerId,
      producerEpoch: description.producerEpoch,
      transactionState: description.state,
      coordinatorBroker: described[0].broker,
      describeTransactionsVersion: facts.DescribeTransactions.selected,
      describeProducersVersion: facts.DescribeProducers.selected,
      listTransactionsVersion: facts.ListTransactions.selected,
      patternFrames: patternFrames.length,
      unsupported: this.unsupported,
      ranges: facts,
    })
  }

  /**
   * F12: a physical abort through WriteTxnMarkers v1, read from independent leader connections, and
   * a fence that makes the original producer's next send fail with broker truth.
   * @returns {Promise<void>}
   * @throws {Error} on a physical, committed-visible, handler or fencing mismatch
   */
  async abortFence() {
    const p = this.probe
    const facts = await this.capabilities([
      'WriteTxnMarkers',
      'DescribeProducers',
      'DescribeTransactions',
    ])
    const admin = await p.start()
    const topic = await p.topic({ name: 'abort' })
    const transactionalId = `e2e-floor-abort-${this.id}`
    const producer = await p.client('producer', {
      transactionalId,
      idempotent: true,
      maxInFlightRequests: 1,
    })
    const warmup = await p.transaction(producer, transactionalId)
    await p.invoke(() => warmup.send({ topic, messages: [{ value: 'committed' }] }))
    await p.invoke(() => warmup.commit())
    const open = await p.transaction(producer, transactionalId, 'fence')
    await p.invoke(() => open.send({ topic, messages: [{ value: 'aborted' }] }))

    const description = (
      await p.invoke(() => admin.describeTransactions({ transactionalIds: [transactionalId] }))
    )[0]
    p.items(description)
    expect(description.state).toBe('Ongoing')
    const { nodeId: leader } = await p.leaderNode(topic)
    const producers = await p.invoke(() =>
      admin.describeProducers({ topicPartitions: [{ topic, partitions: [0] }], brokerId: leader })
    )
    p.items(producers[0].partitions[0])
    const state = producers[0].partitions[0].activeProducers.find(
      ({ producerId }) => producerId === description.producerId
    )
    expect(state).toMatchObject({
      producerId: description.producerId,
      producerEpoch: description.producerEpoch,
    })
    const hidden = (
      await p.invoke(() => admin.fetchTopicOffsets(topic, { isolationLevel: 'read_committed' }))
    )[0].high
    const visibleEnd = (await p.physical(topic)).endOffset
    expect(BigInt(hidden)).toBeLessThan(BigInt(visibleEnd))

    expect(
      await p.invoke(() =>
        admin.abortTransaction({
          topic,
          partition: 0,
          producerId: state.producerId,
          producerEpoch: state.producerEpoch,
          coordinatorEpoch: state.coordinatorEpoch,
        })
      )
    ).toBeUndefined()
    this.expectNegotiated('WriteTxnMarkers', facts.WriteTxnMarkers.selected)
    const plain = await p.client('producer')
    await p.invoke(() => plain.send({ topic, messages: [{ value: 'sentinel' }] }))

    const committedVisibleIds = await p.committed(topic)
    const physicalIds = (await p.physical(topic)).records
      .filter(({ batchContext }) => !batchContext.isControlBatch)
      .map(({ value }) => String(value))
    expect(physicalIds).toEqual(['committed', 'aborted', 'sentinel'])
    expect(committedVisibleIds).toEqual(['committed', 'sentinel'])

    const handlerIds = []
    const consumer = await p.client('consumer', {
      groupId: `e2e-floor-visible-${this.id}`,
      readUncommitted: false,
    })
    p.cleanup.add('stop consumer', () => consumer.stop())
    await p.invoke(() => consumer.subscribe({ topic, fromBeginning: true }))
    await p.invoke(() =>
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => handlerIds.push(String(message.value)),
      })
    )
    await p.poll(async () => handlerIds.includes('sentinel'))
    expect(handlerIds).toEqual(['committed', 'sentinel'])

    const fencedId = `e2e-floor-fence-${this.id}`
    const victim = await p.client('producer', {
      transactionalId: fencedId,
      idempotent: true,
      maxInFlightRequests: 1,
    })
    const doomed = await p.transaction(victim, fencedId, 'fence')
    await p.invoke(() => doomed.send({ topic, messages: [{ value: 'fenced' }] }))
    const before = (
      await p.invoke(() => admin.describeTransactions({ transactionalIds: [fencedId] }))
    )[0]
    p.items(before)
    const [fenced] = await fenceUntilSettled(p, admin, fencedId)
    p.items(fenced)
    expect(
      fenced.producerId !== before.producerId || fenced.producerEpoch > before.producerEpoch
    ).toBe(true)
    const refusal = await rejectionOf(
      p.invoke(() => doomed.send({ topic, messages: [{ value: 'must-refuse' }] }))
    )
    const fencedSendCause = deepest(refusal)
    expect(fencedSendCause?.code).toBe(PRODUCER_FENCED)
    const successor = await p.client('producer', {
      transactionalId: fencedId,
      idempotent: true,
      maxInFlightRequests: 1,
    })
    const recovered = await p.transaction(successor, fencedId)
    await p.invoke(() => recovered.send({ topic, messages: [{ value: 'recovered' }] }))
    await p.invoke(() => recovered.commit())

    p.record('F12', {
      producerId: state.producerId,
      producerEpoch: state.producerEpoch,
      coordinatorEpoch: state.coordinatorEpoch,
      leaderBroker: leader,
      writeTxnMarkersVersion: facts.WriteTxnMarkers.selected,
      physicalIds,
      committedIds: committedVisibleIds,
      handlerIds,
      fencedCode: fencedSendCause.code,
      ranges: facts,
    })
  }

  /**
   * F13: a preferred election with real replicas, a validate-only feature update, the fenced
   * listing, a client-metrics resource through its config APIs, and the refused topic filter.
   * @returns {Promise<void>}
   * @throws {Error} on any election, feature, resource or refusal mismatch
   */
  async cluster() {
    const p = this.probe
    const facts = await this.capabilities([
      'ElectLeaders',
      'UpdateFeatures',
      'DescribeCluster',
      'ListConfigResources',
    ])
    const admin = await p.start()
    const seed = await p.seed()
    const before = await p.invoke(() => admin.describeCluster({ includeFencedBrokers: true }))
    expect(before.clusterId.length).toBeGreaterThan(0)
    expect(before.brokers.map(({ fenced }) => fenced)).toEqual([false, false, false])
    this.expectNegotiated('DescribeCluster', facts.DescribeCluster.selected)

    const topic = await p.topic({ name: 'election' })
    const { nodeId: initialLeader, replicas } = await p.leaderNode(topic)
    const preferred = replicas.find((nodeId) => nodeId !== initialLeader)
    const reordered = [preferred, ...replicas.filter((nodeId) => nodeId !== preferred)]
    expect(
      await p.invoke(() =>
        admin.alterPartitionReassignments({
          topics: [{ topic, partitionAssignment: [{ partition: 0, replicas: reordered }] }],
        })
      )
    ).toBeUndefined()
    const metadataOf = async () =>
      (await p.invoke(() => admin.fetchTopicMetadata({ topics: [topic] }))).topics[0].partitions[0]
    await p.poll(async () => (await metadataOf()).replicas.join() === reordered.join())
    const staged = await metadataOf()
    expect(staged.leader).toBe(initialLeader)
    expect(staged.leader).not.toBe(preferred)
    const elected = await p.invoke(() =>
      admin.electLeaders({
        electionType: 'preferred',
        topicPartitions: [{ topic, partitions: [0] }],
      })
    )
    expect(elected).toHaveLength(1)
    expect(elected[0].topic).toBe(topic)
    expect(elected[0].partitions).toHaveLength(1)
    expect(elected[0].partitions[0]).toMatchObject({ partition: 0, errorCode: 0 })
    await p.poll(async () => (await metadataOf()).leader === preferred)
    this.expectNegotiated('ElectLeaders', facts.ElectLeaders.selected)

    const featuresBeforeValidation = await p.invoke(() => admin.describeFeatures())
    const change = differentLevel(featuresBeforeValidation)
    expect(change).toBeDefined()
    const { name: featureName } = change
    const levelOf = (features) => features.finalizedFeatures[featureName].maxVersionLevel
    p.cleanup.add('restore feature level', async () => {
      const level = levelOf(await admin.describeFeatures())
      if (level === change.level) return
      p.items(
        await admin.updateFeatures({
          features: [
            {
              name: featureName,
              maxVersionLevel: change.level,
              upgradeType: level > change.level ? 'unsafe-downgrade' : 'upgrade',
            },
          ],
        })
      )
    })
    const validated = await p.invoke(() =>
      admin.updateFeatures({
        validateOnly: true,
        features: [
          { name: featureName, maxVersionLevel: change.target, upgradeType: change.upgradeType },
        ],
      })
    )
    expect(validated).toHaveLength(1)
    expect(validated[0]).toMatchObject({ feature: featureName, errorCode: 0 })
    expect(validated[0].error).toBeFalsy()
    this.expectNegotiated('UpdateFeatures', facts.UpdateFeatures.selected)
    const featuresAfterValidation = await p.invoke(() => admin.describeFeatures())
    expect(levelOf(featuresAfterValidation)).toBe(change.level)
    // KRaft reports the metadata log offset as the epoch, so any metadata write moves it.
    const withoutEpoch = ({ finalizedFeaturesEpoch, ...features }) => features
    expect(withoutEpoch(featuresAfterValidation)).toEqual(withoutEpoch(featuresBeforeValidation))

    const type = ConfigResourceTypes.CLIENT_METRICS
    const createdResourceName = `e2e-floor-metrics-${this.id}`
    const entries = [
      { name: 'interval.ms', value: '1000' },
      { name: 'match', value: `client_id=${CLIENT_ID}` },
    ]
    const alter = (operation) =>
      admin.incrementalAlterConfigs({
        resources: [
          {
            type,
            name: createdResourceName,
            configEntries: entries.map((configEntry) =>
              operation === AlterConfigOpTypes.SET
                ? { ...configEntry, operation }
                : { name: configEntry.name, operation }
            ),
          },
        ],
      })
    p.cleanup.add('remove metrics subscription', () => alter(AlterConfigOpTypes.DELETE))
    p.items((await p.invoke(() => alter(AlterConfigOpTypes.SET))).resources)
    const namesOf = async () => {
      const listed = await admin.listConfigResources({ resourceTypes: [type] })
      const legacy = await seed.listConfigResources({})
      return {
        configResourceNames: listed.resources.map(({ name }) => name).sort(),
        legacyMetricsResourceNames: legacy.configResources
          .map(({ resourceName }) => resourceName)
          .sort(),
      }
    }
    let names
    await p.poll(async () => {
      names = await namesOf()
      return (
        names.configResourceNames.includes(createdResourceName) &&
        names.legacyMetricsResourceNames.includes(createdResourceName)
      )
    })
    expect(names.configResourceNames).toEqual(names.legacyMetricsResourceNames)
    const described = await p.invoke(() =>
      admin.describeConfigs({
        resources: [
          { type, name: createdResourceName, configNames: entries.map(({ name }) => name) },
        ],
      })
    )
    p.items(described.resources)
    for (const { name, value } of entries) {
      expect(described.resources[0].configEntries).toContainEqual(
        expect.objectContaining({ configName: name, configValue: value })
      )
    }
    p.items((await p.invoke(() => alter(AlterConfigOpTypes.DELETE))).resources)
    await p.poll(async () => {
      names = await namesOf()
      return (
        !names.configResourceNames.includes(createdResourceName) &&
        !names.legacyMetricsResourceNames.includes(createdResourceName)
      )
    })
    expect(names.configResourceNames).toEqual(names.legacyMetricsResourceNames)
    this.expectNegotiated('ListConfigResources', facts.ListConfigResources.selected)

    const mark = p.requests.length
    const refusal = await rejectionOf(
      p.invoke(() => admin.listConfigResources({ resourceTypes: [ConfigResourceTypes.TOPIC] }))
    )
    this.refuse(refusal, 'ListConfigResources', 1, 'resourceTypes')
    const unsupportedFrames = p.requests
      .slice(mark)
      .filter(({ apiName }) => apiName === 'ListConfigResources')
    expect(unsupportedFrames).toHaveLength(0)

    p.record('F13', {
      clusterId: before.clusterId,
      fencedBrokerCount: before.brokers.filter(({ fenced }) => fenced).length,
      electedLeader: preferred,
      leaderBeforeElection: staged.leader,
      electionVersion: facts.ElectLeaders.selected,
      featureName,
      featureLevel: change.level,
      featureValidatedLevel: change.target,
      featureUpdateVersion: facts.UpdateFeatures.selected,
      featuresUnchanged: true,
      metricsResourcePresent: true,
      metricsResourceAbsentAfterDeletion: true,
      unsupportedFrames: unsupportedFrames.length,
      unsupported: this.unsupported,
      ranges: facts,
    })
  }

  /**
   * F14: a written replica's current directory, size and offset lag agree between the public log
   * directory reads and the broker's own raw answer, and no cordon flag is invented.
   * @returns {Promise<void>}
   * @throws {Error} on a directory, size or lag mismatch, or a non-null cordon flag
   */
  async logDirs() {
    const p = this.probe
    const facts = await this.capabilities(['DescribeLogDirs'])
    const admin = await p.start()
    const topic = await p.topic({ name: 'logdirs' })
    const producer = await p.client('producer', { idempotent: true, linger: 0 })
    const corpus = Array.from({ length: 10 }, (_, index) => `logdir-${index}`)
    await p.invoke(() => producer.send({ topic, messages: corpus.map((value) => ({ value })) }))
    const log = await p.physical(topic)
    expect(log.records.map(({ value }) => String(value))).toEqual(corpus)

    const { broker, nodeId } = await p.leader(topic)
    const raw = await p.invoke(() =>
      broker.describeLogDirs({ topics: [{ topic, partitions: [0] }] })
    )
    const rawHolders = raw.logDirs.filter((dir) =>
      dir.topics.some((entry) => entry.topic === topic)
    )
    expect(rawHolders).toHaveLength(1)
    const independentlyObservedDirectory = rawHolders[0].path
    const [rawPartition] = rawHolders[0].topics[0].partitions

    const rows = await p.invoke(() =>
      admin.describeLogDirs({ brokers: [nodeId], topicPartitions: [{ topic, partitions: [0] }] })
    )
    p.items(rows)
    const publicHolders = rows[0].logDirs.filter((dir) =>
      dir.topics.some((entry) => entry.topic === topic)
    )
    expect(publicHolders.map(({ path }) => path)).toEqual([independentlyObservedDirectory])
    const [publicPartition] = publicHolders[0].topics[0].partitions
    expect(publicPartition).toMatchObject({
      partition: 0,
      size: String(rawPartition.size),
      offsetLag: String(rawPartition.offsetLag),
      isFuture: false,
    })
    expect(BigInt(publicPartition.size)).toBeGreaterThan(0n)

    const [replica] = await p.invoke(() =>
      admin.describeReplicaLogDirs({ replicas: [{ topic, partition: 0, brokerId: nodeId }] })
    )
    p.items(replica)
    const currentDirectory = replica.currentLogDir
    expect(currentDirectory).toBe(independentlyObservedDirectory)
    expect(replica.currentOffsetLag).toBe(String(rawPartition.offsetLag))
    expect(replica.futureLogDir).toBeNull()

    const everyDirectory = await p.invoke(() => admin.describeLogDirs())
    const isCordonedValues = everyDirectory.flatMap(({ logDirs }) =>
      logDirs.map(({ isCordoned }) => isCordoned)
    )
    expect(isCordonedValues.length).toBeGreaterThan(0)
    expect(isCordonedValues.every((value) => value === null)).toBe(true)
    const versions = this.versionsOf('DescribeLogDirs')
    expect(versions.length).toBeGreaterThan(0)
    for (const version of versions)
      expect(version).toBeLessThanOrEqual(facts.DescribeLogDirs.selected)

    p.record('F14', {
      leaderBroker: nodeId,
      currentLogDir: currentDirectory,
      partitionSizeBytes: String(rawPartition.size),
      partitionOffsetLag: String(rawPartition.offsetLag),
      physicalCount: log.records.length,
      cordonValuesCount: isCordonedValues.length,
      describeLogDirsVersions: versions,
      ranges: facts,
    })
  }

  /**
   * F15: a reassignment option and an offset spec the floor lacks are refused with both ranges and
   * no frame, while the supported specs return real log boundaries.
   * @returns {Promise<void>}
   * @throws {Error} on a missing refusal, a sent frame or a wrong boundary
   */
  async reassignment() {
    const p = this.probe
    const facts = await this.capabilities(['AlterPartitionReassignments', 'ListOffsets'])
    const admin = await p.start()
    const topic = await p.topic({ name: 'specs' })
    const producer = await p.client('producer', { idempotent: true, linger: 0 })
    await p.invoke(() =>
      producer.send({
        topic,
        messages: Array.from({ length: 20 }, (_, index) => ({ value: `spec-${index}` })),
      })
    )
    expect(
      await p.invoke(() =>
        admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: '10' }] })
      )
    ).toBeUndefined()
    const { broker, replicas } = await p.leader(topic)

    let mark = p.requests.length
    const reassignmentRefusal = await rejectionOf(
      p.invoke(() =>
        admin.alterPartitionReassignments({
          allowReplicationFactorChange: false,
          topics: [{ topic, partitionAssignment: [{ partition: 0, replicas }] }],
        })
      )
    )
    this.refuse(
      reassignmentRefusal,
      'AlterPartitionReassignments',
      1,
      'allowReplicationFactorChange'
    )
    const reassignmentFrames = p.requests
      .slice(mark)
      .filter(({ apiName }) => apiName === 'AlterPartitionReassignments')
    expect(reassignmentFrames).toHaveLength(0)

    const rawLogStart = String(
      (
        await p.invoke(() =>
          broker.listOffsets({
            replicaId: -1,
            isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
            topics: [{ topic, partitions: [{ partition: 0, timestamp: -2 }] }],
          })
        )
      ).responses[0].partitions[0].offset
    )
    expect(rawLogStart).toBe('10')
    mark = p.requests.length
    const [local] = await p.invoke(() => admin.fetchTopicOffsetsByTimestamp(topic, 'earliestLocal'))
    const [tiered] = await p.invoke(() => admin.fetchTopicOffsetsByTimestamp(topic, 'latestTiered'))
    const plainEarliestLocal = local.offset
    const plainLatestTiered = tiered.offset
    expect(plainEarliestLocal).toBe(rawLogStart)
    expect(plainLatestTiered).toBe('-1')
    const specVersions = p.requests
      .slice(mark)
      .filter(({ apiName }) => apiName === 'ListOffsets')
      .map(({ apiVersion }) => apiVersion)
    expect(specVersions).toEqual([facts.ListOffsets.selected, facts.ListOffsets.selected])

    mark = p.requests.length
    const specRefusal = await rejectionOf(
      p.invoke(() => admin.fetchTopicOffsetsByTimestamp(topic, 'earliestPendingUpload'))
    )
    this.refuse(specRefusal, 'ListOffsets', 11, 'earliestPendingUpload')
    const unsupportedFrames = p.requests
      .slice(mark)
      .filter(({ apiName }) => apiName === 'ListOffsets')
    expect(unsupportedFrames).toHaveLength(0)

    p.record('F15', {
      logStartOffset: rawLogStart,
      earliestLocalOffset: plainEarliestLocal,
      latestTieredOffset: plainLatestTiered,
      listOffsetsVersions: specVersions,
      reassignmentFrames: reassignmentFrames.length,
      unsupportedFrames: unsupportedFrames.length,
      unsupported: this.unsupported,
      ranges: facts,
    })
  }

  /**
   * F16: client quotas and SCRAM credentials round trip through their public administration, a
   * credential authenticates until it is deleted, and no credential appears in any diagnostic.
   * @returns {Promise<void>}
   * @throws {Error} on a quota or credential mismatch, a missing typed authentication failure or a leak
   */
  async security() {
    const p = this.probe
    const facts = await this.capabilities(['DescribeClientQuotas', 'AlterClientQuotas'])
    const admin = await p.start()
    const user = `e2e-floor-user-${this.id}`
    const clientId = `e2e-floor-client-${this.id}`
    const quotas = [
      { entity: [{ entityType: 'user', entityName: user }], value: 65536 },
      { entity: [{ entityType: 'client-id', entityName: clientId }], value: 131072 },
      {
        entity: [
          { entityType: 'user', entityName: user },
          { entityType: 'client-id', entityName: clientId },
        ],
        value: 196608,
      },
    ]
    const key = 'producer_byte_rate'
    const alterQuota = ({ validateOnly = false, remove = false } = {}) =>
      admin.alterClientQuotas({
        ...(validateOnly ? { validateOnly } : {}),
        entries: quotas.map(({ entity, value }) => ({
          entity,
          ops: [remove ? { key, remove } : { key, value }],
        })),
      })
    const readQuota = ({ entity }) =>
      p.invoke(() =>
        admin.describeClientQuotas({
          components: entity.map(({ entityType, entityName }) => ({
            entityType,
            matchType: 'exact',
            match: entityName,
          })),
          strict: true,
        })
      )
    // The controller commits a change before every broker applies it, so a read right after it
    // may still answer the old state.
    const readQuotaUntil = async (quota, settled) => {
      let rows
      await p.poll(async () => settled((rows = await readQuota(quota))))
      return rows
    }
    p.cleanup.add('remove quotas', async () => p.items(await alterQuota({ remove: true })))
    p.items(await p.invoke(() => alterQuota({ validateOnly: true })))
    for (const quota of quotas) expect(await readQuota(quota)).toEqual([])
    p.items(await p.invoke(() => alterQuota()))
    const quotaReadback = []
    const byType = (entity) => [...entity].sort((a, b) => a.entityType.localeCompare(b.entityType))
    for (const quota of quotas) {
      const rows = await readQuotaUntil(quota, (read) => read.length > 0)
      expect(rows).toHaveLength(1)
      expect(byType(rows[0].entity)).toEqual(byType(quota.entity))
      expect(rows[0].values).toEqual({ [key]: quota.value })
      quotaReadback.push(rows[0].values[key])
    }
    expect(quotaReadback).toEqual(quotas.map(({ value }) => value))
    p.items(await p.invoke(() => alterQuota({ remove: true })))
    for (const quota of quotas) {
      expect(await readQuotaUntil(quota, (read) => read.length === 0)).toEqual([])
    }

    const { ssl } = saslConnectionOpts()
    const previous = process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
    p.cleanup.add('restore buffer diagnostics', () => {
      if (previous === undefined) delete process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
      else process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = previous
    })
    const failures = []
    const closeFailures = []
    const authenticated = []
    const refused = []
    const isSaslFailure = (error) =>
      chainOf(error).some(
        ({ name, type }) =>
          name === 'KafkaJSSASLAuthenticationError' || type === 'SASL_AUTHENTICATION_FAILED'
      )
    for (const mechanism of ['SCRAM-SHA-256', 'SCRAM-SHA-512']) {
      const scramUser = `e2e-floor-scram-${this.id}-${mechanism.slice(-3)}`
      const password = randomBytes(32).toString('hex')
      p.guard(password)
      const deletions = [{ user: scramUser, mechanism }]
      p.cleanup.add('remove SCRAM credential', async () => {
        for (const row of await admin.alterUserScramCredentials({ deletions })) {
          if (![0, RESOURCE_NOT_FOUND].includes(row.errorCode)) p.items(row)
        }
      })
      p.items(
        await p.invoke(() =>
          admin.alterUserScramCredentials({
            upsertions: [{ user: scramUser, mechanism, iterations: 4096, password }],
          })
        )
      )
      const [described] = await describeScramUntilApplied(p, admin, scramUser)
      p.items(described)
      expect(described.credentials).toEqual([{ mechanism, iterations: 4096 }])
      const authenticate = async (buffers) => {
        process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = buffers
        const client = new Kafka({
          clientId: CLIENT_ID,
          brokers: saslBrokers(),
          ssl,
          sasl: { mechanism: mechanism.toLowerCase(), username: scramUser, password },
          retry: { retries: 0 },
          logLevel: logLevel.DEBUG,
          logCreator: () => (entry) => p.logs.push(entry),
          socketFactory: (options) => p.socket(options),
        }).admin()
        let failure = null
        try {
          await client.connect()
          await client.listTopics()
        } catch (error) {
          failure = error
          failures.push(error)
        }
        await client.disconnect().catch((error) => closeFailures.push(error))
        return failure
      }
      for (const buffers of ['0', '1']) {
        await p.poll(async () => (await authenticate(buffers)) === null)
      }
      authenticated.push(mechanism)
      p.items(await p.invoke(() => admin.alterUserScramCredentials({ deletions })))
      for (const buffers of ['0', '1']) {
        let failure
        await p.poll(async () => {
          failure = await authenticate(buffers)
          return failure !== null && isSaslFailure(failure)
        })
        expect(isSaslFailure(failure)).toBe(true)
      }
      refused.push(mechanism)
    }
    expect(closeFailures).toEqual([])
    const leakedSecret = p.leaked(failures)
    expect(leakedSecret).toBe(false)

    p.record('F16', {
      quotaEntityCount: quotas.length,
      quotaReadback,
      scramAuthenticatedMechanisms: authenticated,
      scramRefusedMechanisms: refused,
      leakedSecret,
      ranges: facts,
    })
  }

  /**
   * F17: setup, idle and stalled-call bounds over real sockets: a held dial ends inside its budget
   * and a ready connection resets it, an idle connection is reopened afresh while a held long poll
   * is kept, and a stalled administrative call ends at its deadline and leaves no late write.
   * @returns {Promise<void>}
   * @throws {Error} on a bound, socket, delivery, commit or resource mismatch
   */
  async connection() {
    const p = this.probe
    p.trackTimers()
    const facts = await this.capabilities(['ApiVersions', 'Fetch'])
    await p.start()
    const attempts = await this.heldDial()
    const idle = await this.idleReopening()
    const heldLongPollWasReaped = await this.heldLongPoll()
    const stalled = await this.stalledCall()
    p.record('F17', {
      connectionTimeoutMs: 500,
      connectionTimeoutMaxMs: 3000,
      heldAttemptMs: attempts,
      reopenedSocketIds: idle.socketIds,
      apiVersionsRequests: idle.apiVersions,
      acknowledgedIds: idle.acknowledged,
      handlerIds: idle.delivered,
      finalCommitOffset: idle.committed,
      heldLongPollWasReaped,
      stalledCallMs: stalled.elapsed,
      stalledWritesAfterDeadline: stalled.late,
      ranges: facts,
    })
  }

  /**
   * A dial held for one budget with no retry ends inside it; once a connection is ready, the next
   * held dial starts from the base budget again.
   * @returns {Promise<number[]>} the two held attempts' durations, in milliseconds
   * @throws {Error} on a missing failure, a wrong attempt count or a duration outside its bound
   */
  async heldDial() {
    const p = this.probe
    const dial = new HeldDial((options) => p.socket(options))
    const kafka = new Kafka({
      brokers: [plainTextBrokers()[0]],
      logLevel: logLevel.NOTHING,
      socketFactory: dial.factory,
      connectionTimeout: 500,
      connectionTimeoutMax: 3000,
      retry: { retries: 0 },
    })
    const admin = kafka.admin()
    p.cleanup.add('held dial admin disconnect', () => admin.disconnect({ timeout: 5000 }))
    const failed = () => rejectionOf(p.invoke(() => admin.connect()))
    dial.holding = true
    expect(await failed()).toBeInstanceOf(KafkaJSError)
    expect(dial.attempts).toHaveLength(1)
    dial.holding = false
    await p.invoke(() => admin.connect())
    expect(await p.invoke(() => admin.listTopics())).toEqual(expect.any(Array))
    dial.holding = true
    for (const socket of dial.sockets.filter(({ destroyed }) => !destroyed)) {
      socket.destroy(new Error('transport interrupted'))
    }
    await new Promise((resolve) => setImmediate(resolve))
    expect(await failed()).toBeInstanceOf(KafkaJSError)
    dial.holding = false
    expect(dial.attempts).toHaveLength(2)
    for (const duration of dial.attempts) {
      expect(duration).toBeGreaterThanOrEqual(400)
      expect(duration).toBeLessThanOrEqual(750)
    }
    await p.invoke(() => admin.connect())
    expect(await p.invoke(() => admin.listTopics())).toEqual(expect.any(Array))
    return dial.attempts.map(Math.round)
  }

  /**
   * Four records sent one send period apart: the idle socket closes between sends, each send
   * negotiates a fresh connection, and the physical, committed and handler sets stay exact.
   * @returns {Promise<{ socketIds: number[], apiVersions: number, acknowledged: string[],
   *   delivered: string[], committed: string }>} the sockets each send used and the three sets
   * @throws {Error} on a retained or reused socket, a crash, a rebalance or a replay mismatch
   */
  async idleReopening() {
    const p = this.probe
    const brokers = plainTextBrokers()
    const topic = await p.topic({ name: 'idle' })
    const groupId = `${topic}-group`
    const box = instrument(
      {
        brokers,
        clientId: 'floor-idle-producer',
        connectionsMaxIdle: IDLE_MS,
        logLevel: logLevel.NOTHING,
      },
      { activeResources: false, socketIdentities: true }
    )
    const kafka = new Kafka({
      brokers,
      clientId: 'floor-idle-member',
      connectionsMaxIdle: IDLE_MS,
      logLevel: logLevel.NOTHING,
    })
    const producer = box.kafka.producer({ idempotent: true })
    const consumer = kafka.consumer({
      groupId,
      groupProtocol: 'classic',
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTimeInMs: 500,
    })
    const delivered = []
    const commits = []
    const apiVersions = []
    const crashes = []
    let rebalances = 0
    p.cleanup.add('idle producer disconnect', () => producer.disconnect())
    p.cleanup.add('idle consumer disconnect', () => consumer.disconnect())
    p.cleanup.add('idle consumer stop', () => consumer.stop())
    const removers = [
      producer.on(producer.events.REQUEST, ({ payload }) => {
        if (payload.apiName === 'ApiVersions') apiVersions.push(payload)
      }),
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload)),
      consumer.on(consumer.events.REBALANCING, () => {
        rebalances++
      }),
    ]
    p.cleanup.add('idle listeners removed', () => removers.forEach((remove) => remove()))
    await p.invoke(() => producer.connect())
    await p.invoke(() => consumer.connect())
    await p.invoke(() => consumer.subscribe({ topic, fromBeginning: true }))
    await p.invoke(() =>
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
          delivered.push(message.value.toString())
          const offset = String(BigInt(message.offset) + 1n)
          await consumer.commitOffsets([{ topic, partition, offset }])
          commits.push(offset)
        },
      })
    )
    const acknowledged = []
    const retired = new Set()
    const socketIds = []
    for (let index = 0; index < 4; index++) {
      const startedAt = performance.now()
      const [sent] = await p.invoke(() =>
        producer.send({ topic, messages: [{ partition: 0, value: String(index) }] })
      )
      acknowledged.push(`${index}@${sent.baseOffset}`)
      await p.poll(async () => commits.length === index + 1, 50, 10000)
      const used = await box.probe.snapshot()
      const usedIds = used.sockets.live.map(({ id }) => id)
      expect(usedIds.length).toBeGreaterThan(0)
      expect(usedIds.every((id) => !retired.has(id))).toBe(true)
      socketIds.push(...usedIds)
      await p.poll(async () => (await box.probe.snapshot()).sockets.open === 0, 50, 7000)
      for (const id of usedIds) retired.add(id)
      expect((await box.probe.snapshot()).queueDepth).toBe(0)
      if (index < 3) {
        const wait = Math.max(0, SEND_GAP_MS - (performance.now() - startedAt))
        await p.invoke(() => delay(p.signal, wait))
      }
    }
    expect(apiVersions.length).toBeGreaterThanOrEqual(4)
    expect(crashes).toEqual([])
    expect(rebalances).toBe(0)
    expect(delivered).toEqual(['0', '1', '2', '3'])
    const log = await p.physical(topic)
    expect(log.records.map(({ value }) => String(value))).toEqual(['0', '1', '2', '3'])
    expect(acknowledged).toEqual(log.records.map(({ value, offset }) => `${value}@${offset}`))
    const [{ partitions }] = await p.invoke(() =>
      p.admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false })
    )
    expect(partitions[0].offset).toBe(commits.at(-1))
    expect(commits.at(-1)).toBe(String(BigInt(log.records.at(-1).offset) + 1n))
    return {
      socketIds,
      apiVersions: apiVersions.length,
      acknowledged,
      delivered,
      committed: partitions[0].offset,
    }
  }

  /**
   * A fetch waiting on the broker is not an idle connection: it stays pending past the idle limit on
   * the pool's dedicated Fetch socket, which stays open, while the primary socket, idle since its
   * setup exchange, is reaped; the fetch then completes normally for exactly the requested partition.
   * @returns {Promise<boolean>} whether the held Fetch socket was closed early; false on success
   * @throws {Error} when the fetch settles early, its socket is replaced or closed, the idle primary
   *   socket is not reaped, or the fetch fails
   */
  async heldLongPoll() {
    const p = this.probe
    const topic = await p.topic({ name: 'held' })
    const { host, port } = await p.leaderNode(topic)
    const dialed = []
    const broker = await p.raw(host, port, {
      connectionsMaxIdle: IDLE_MS,
      socketFactory: (options) => {
        const socket = p.socket(options)
        dialed.push(socket)
        return socket
      },
    })
    expect(dialed).toHaveLength(1)
    const [primary] = dialed
    let settled = false
    const outcome = p
      .invoke(() =>
        broker.fetch({
          minBytes: 1,
          maxWaitTime: 10000,
          topics: [{ topic, partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1048576 }] }],
        })
      )
      .then(
        (response) => ({ response }),
        (error) => ({ error })
      )
      .finally(() => {
        settled = true
      })
    await p.invoke(() => delay(p.signal, IDLE_MS + 1500))
    expect(settled).toBe(false)
    expect(dialed).toHaveLength(2)
    const fetchSocket = dialed[1]
    expect(primary.destroyed).toBe(true)
    const heldLongPollWasReaped = fetchSocket.destroyed
    expect(heldLongPollWasReaped).toBe(false)
    const { response, error } = await outcome
    expect(error).toBeUndefined()
    expect(response.errorCode).toBe(0)
    expect(response.responses).toHaveLength(1)
    expect(response.responses[0].topicName).toBe(topic)
    expect(response.responses[0].partitions).toHaveLength(1)
    expect(response.responses[0].partitions[0]).toMatchObject({ partition: 0, errorCode: 0 })
    return heldLongPollWasReaped
  }

  /**
   * An administrative call on a blackholed connection rejects at its own deadline, writes nothing
   * once it did, and a later explicit reconnect succeeds.
   * @returns {Promise<{ elapsed: number, late: number }>} the call's duration in milliseconds and
   *   the number of writes made after its deadline
   * @throws {Error} on a wrong error, a late write or a failed reconnect
   */
  async stalledCall() {
    const p = this.probe
    const chaos = createChaosSocketFactory((options) => p.socket(options))
    const writes = []
    const kafka = new Kafka({
      brokers: [plainTextBrokers()[0]],
      logLevel: logLevel.NOTHING,
      retry: { retries: 0 },
      socketFactory: (options) => {
        const socket = chaos(options)
        const write = socket.write.bind(socket)
        socket.write = (...args) => {
          writes.push(performance.now())
          return write(...args)
        }
        return socket
      },
    })
    const admin = kafka.admin({ apiTimeout: 500 })
    p.cleanup.add('stalled admin disconnect', () => admin.disconnect({ timeout: 5000 }))
    await p.invoke(() => admin.connect())
    const lift = chaos.arm({}, (socket) => socket.blackhole())
    const startedAt = performance.now()
    const failure = await rejectionOf(p.invoke(() => admin.listTopics()))
    const deadlineAt = performance.now()
    const elapsed = Math.round(deadlineAt - startedAt)
    expect(failure).toBeInstanceOf(KafkaJSTimeout)
    expect(elapsed).toBeLessThan(2000)
    await p.invoke(() => delay(p.signal, 1000))
    const late = writes.filter((at) => at > deadlineAt).length
    expect(late).toBe(0)
    lift()
    await p.invoke(() => admin.disconnect({ timeout: 5000 }))
    await p.invoke(() => admin.connect())
    expect(await p.invoke(() => admin.listTopics())).toEqual(expect.any(Array))
    return { elapsed, late }
  }
}

/**
 * Runs one floor administration case inside the shared suite budget: take what is left, build the
 * case under that deadline, run it, and always restore.
 * @param {SuiteBudget} budget the suite's shared budget
 * @param {string} method one of the case names `FloorAdminCases#run` accepts
 * @returns {Promise<void>}
 * @throws {Error} a shortage of budget before any mutation, or the case's failure
 */
const runFloorCase = async (budget, method) => {
  const budgetMs = budget.take()
  await liveCase(budgetMs, async (signal) => {
    const cases = new FloorAdminCases({ signal, budgetMs })
    try {
      await cases.run(method)
    } finally {
      await cases.close()
    }
  })()
}

module.exports = {
  FloorAdminCases,
  HeldDial,
  SuiteBudget,
  SUITE_MS,
  caseTimeout,
  TimerLedger,
  deepest,
  differentLevel,
  markersOf,
  refusalOf,
  rejectionOf,
  runFloorCase,
}
