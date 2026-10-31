#!/usr/bin/env node
'use strict'

/**
 * Soak driver: runs a client for a stretch of time against a cluster, reads what it holds on to
 * every 30 s, and checks the readings against the bounds of `bounds.js`.
 *
 * Usage: node --expose-gc scripts/soak/soak.js --profile
 *          <idle|member-churn|regex-topic-churn|outage|first-run|producer-churn|consumer-churn>
 *          [--phase-seconds <n>]
 *   (= npm run test:kafka -- "node --expose-gc scripts/soak/soak.js --profile first-run" for the
 *   15-minute run, `npm run soak:first-run`)
 * Writes one JSON line per sample and per quiet point to `.tmp/e2e/soak.jsonl` through
 * `writeEvidence` (`.tmp/e2e/producer-churn.jsonl` for the producer-churn profile), prints the
 * verdict, and sets the exit code: 0 when every bound held, 1 when one broke or the run failed. It
 * ends by disconnecting what it built and does not call `process.exit`, so a run that leaves a
 * handle behind does not end, which is also a finding.
 *
 * Profiles, over finite populations: one topic of 6 partitions and two member slots. A profile of
 * one phase runs 7 minutes, which leaves six readings after the warm-up.
 *   idle              one member polling an idle topic, a message a second
 *   member-churn      two members, one of them replaced by a new consumer every 15 s, rebalancing
 *                     cooperatively (`cooperativeSticky`)
 *   regex-topic-churn a regex subscriber while 50 distinct topic names are created and deleted
 *   outage            one member while a broker is stopped for a third of the phase and restarted
 *   first-run         5 minutes of idle, of member churn and of outage, the 15-minute screen
 *   producer-churn    5 minutes each of steady production, rolling broker restarts and buffer
 *                     pressure from one idempotent producer; see `ProducerChurn`
 *   consumer-churn    a 3-minute warm-up of one short pass of each of the four shapes below, then
 *                     5 minutes each of member churn, regex-topic
 *                     churn, pause and topic recreation, and a mostly idle 1000-partition
 *                     assignment, judged by an independent log, broker-offset and handler check;
 *                     see `ConsumerChurnRun`
 * Every phase ends by disconnecting its clients, also when it fails; the readings that must be zero
 * are taken there.
 * `--phase-seconds n` shortens every phase to n seconds: a smoke run of the driver, not of memory.
 *
 * Development-only. The first run is a short-run screen, never proof of bounded long-run memory.
 * The gauges read internals: the clusters a `Kafka` builds, their request queues, and the
 * operation ledgers and reset obligations of its `operations` store.
 */
const fs = require('node:fs')
const path = require('node:path')
const { Kafka, KafkaJSBufferExhaustedError, logLevel, PartitionAssigners } = require('../../index')
const Broker = require('../../src/broker')
const ConnectionPool = require('../../src/network/connectionPool')
const defaultSocketFactory = require('../../src/network/socketFactory')
const ISOLATION_LEVEL = require('../../src/protocol/isolationLevel')
const { createLogger } = require('../../src/loggers')
const LoggerConsole = require('../../src/loggers/console')
const { Clients } = require('../clients')
const { createChaosSocketFactory } = require('../../testHelpers/chaosSocket')
const { assertProbe } = require('../../testHelpers/producerProbe')
const { endOffsets, readLog } = require('../../testHelpers/rawLog')
const { createResourceProbe } = require('../../testHelpers/resourceProbe')
const { evidencePath, writeEvidence } = require('../../testHelpers/e2eEvidence')
const {
  MIN_PHASE_SAMPLES,
  PHASE_WARMUP_MS,
  SAMPLE_MS,
  WARMUP_MS,
  evaluate,
  evaluatePhases,
} = require('./bounds')
const {
  LATENCY_SAMPLE_EVERY,
  STATUS,
  ObservationJournal,
  ObservedLog,
  OfferLedger,
  decodeId,
  encodeValue,
  mergeDurability,
  sequenceOf,
} = require('./churnLedger')
const { ClosingProbe } = require('./closingProbe')
const { ConsumerChurnOracle } = require('./consumerChurn')

const PARTITIONS = 6
const MEMBER_CHURN_MS = 15000
const OUTAGE_SERVICE = 'kafka3'
const TOPIC_NAMES = 50
/**
 * How long a quiet point waits for the sockets the clients closed to go. An obsolete connection
 * drains in the background for up to the client's `requestTimeout` (30 s by default) after a
 * disconnect, so the wait outlasts that before a socket counts as left behind.
 */
const SETTLE_MS = 31000
const PHASE_MS = 300000
/** A profile of one phase: the warm-up, then six readings and a margin for their collection. */
const STANDALONE_MS = 420000

/**
 * The producer-churn workload. It offers `rate` public sends a second, never more than
 * `maxUnsettled` unsettled at once, through one idempotent producer whose buffer is small enough
 * for the pressure phase to fill and whose largest request admits the closing probe's record.
 */
const CHURN = Object.freeze({
  rate: 2000,
  maxUnsettled: 8192,
  steadyBytes: 200,
  pressureBytes: 1024,
  pressureDelayMs: 5000,
  partitions: PARTITIONS,
  minInsyncReplicas: 2,
  /** The least the offered rate may fall to in the steady phase, as a fraction of `rate`. */
  targetFraction: 0.95,
  /** Offered ids kept beyond the offered sends, for the sends of recovery and the capture. */
  idSlack: 4096,
  recoveryMs: 60000,
  recoveryRetryMs: 500,
  joinMs: 150000,
  flushMs: 150000,
  pollMs: 20,
  minLatencySamples: 20000,
  /** The fewest heap samples each phase keeps after its warm-up. */
  minHeapSamples: MIN_PHASE_SAMPLES,
  /** One offered id in this many is timed from send to first sighting. */
  latencySampleEvery: LATENCY_SAMPLE_EVERY,
  producer: Object.freeze({
    idempotent: true,
    bufferMemory: 4194304,
    maxBlockMs: 2000,
    maxRequestSize: 8388608,
    deliveryTimeout: 120000,
  }),
})
/** The nodes of the outage phase, in rolling order. */
const CHURN_SERVICES = Object.freeze(['kafka1', 'kafka2', 'kafka3'])
/** The kind of outage each `BrokerBouncer` step is. */
const OUTAGE_KINDS = Object.freeze({ stop: 'clean', kill: 'hard' })

/** The phases of each profile: how long each runs and which workload it is. */
const PROFILES = {
  idle: [{ name: 'idle', workload: 'idle', ms: STANDALONE_MS }],
  'member-churn': [{ name: 'member-churn', workload: 'memberChurn', ms: STANDALONE_MS }],
  'regex-topic-churn': [
    { name: 'regex-topic-churn', workload: 'regexTopicChurn', ms: STANDALONE_MS },
  ],
  outage: [{ name: 'outage', workload: 'outage', ms: STANDALONE_MS }],
  'producer-churn': [
    { name: 'steady', workload: 'producerSteady', ms: PHASE_MS },
    { name: 'outage', workload: 'producerOutage', ms: PHASE_MS },
    { name: 'buffer-pressure', workload: 'producerPressure', ms: PHASE_MS },
  ],
  'consumer-churn': [
    { name: 'member-churn', workload: 'consumerMemberChurn', ms: PHASE_MS },
    { name: 'regex-topics', workload: 'consumerRegexTopics', ms: PHASE_MS },
    { name: 'pause-recreate', workload: 'consumerPauseRecreate', ms: PHASE_MS },
    { name: 'mostly-idle', workload: 'consumerMostlyIdle', ms: PHASE_MS },
  ],
  'first-run': [
    { name: 'idle', workload: 'idle', ms: PHASE_MS },
    { name: 'member-churn', workload: 'memberChurn', ms: PHASE_MS },
    { name: 'outage', workload: 'outage', ms: PHASE_MS },
  ],
}

const USAGE = `usage: node --expose-gc scripts/soak/soak.js --profile <${Object.keys(PROFILES).join('|')}> [--phase-seconds <n>]`

/**
 * @param {string[]} argv
 * @returns {{ profile: string, phaseSeconds: number|null }} `phaseSeconds` shortens every phase to
 *   that long, for a smoke run that checks the driver and proves nothing about memory
 * @throws {Error} the usage line, for a missing or unknown profile or any other argument
 */
const parseArgs = (argv) => {
  const [flag, profile, ...rest] = argv
  const shortened = rest[0] === '--phase-seconds'
  const phaseSeconds = shortened ? Number(rest[1]) : null
  const valid =
    flag === '--profile' &&
    Boolean(PROFILES[profile]) &&
    rest.length === (shortened ? 2 : 0) &&
    (!shortened || (Number.isInteger(phaseSeconds) && phaseSeconds >= 1))

  if (!valid) {
    throw new Error(USAGE)
  }

  return { profile, phaseSeconds }
}

/**
 * @param {number} ms
 * @param {AbortSignal} [signal] ends the wait early; the timer is cleared either way
 * @returns {Promise<void>}
 */
const sleep = (ms, signal) =>
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
 * @param {Array<import('events').EventEmitter>} emitters
 * @returns {{ eventNames: () => string[], listenerCount: (name: string) => number }} the listeners
 *   of all of them as one emitter's, so the probe counts them together
 */
const together = (emitters) => ({
  eventNames: () => [...new Set(emitters.flatMap((emitter) => emitter.eventNames()))],
  listenerCount: (name) =>
    emitters.reduce((total, emitter) => total + emitter.listenerCount(name), 0),
})

/**
 * @param {object} kafka a `Kafka`
 * @param {string} name `createCluster` or `operations`
 * @returns {{ symbol: symbol, value: * }} the key and the value of the private member of `kafka`
 *   with that name
 * @throws {Error} when this version of the client has no such member
 */
const privateOf = (kafka, name) => {
  const symbol = Object.getOwnPropertySymbols(kafka).find(
    ({ description }) => description === `private:Kafka:${name}`
  )
  if (!symbol) {
    throw new Error(
      `The soak driver reads Kafka's private ${name}, which this client does not have`
    )
  }

  return { symbol, value: kafka[symbol] }
}

/**
 * Builds a client whose sockets and clusters are counted, and the probe that reads them. The driver
 * holds no socket the client destroyed and no cluster the client let go (a cluster is held through
 * a `WeakRef`), so what it measures is not inflated by what it keeps.
 * @param {object} options `Kafka` options; a `socketFactory` among them is the one counted
 * @param {object} [readings]
 * @param {boolean} [readings.activeResources=true] count the event loop's resources by type; a run
 *   that shares its process with others, as a spec does, leaves it off
 * @returns {{ kafka: object, probe: ReturnType<typeof createResourceProbe>,
 *   socketFactory: Function, listeners: () => Record<string, Record<string, number>> }}
 *   `socketFactory` is the counted factory the client was given: a connection made through it,
 *   such as a raw reader's, is measured like the client's own. `listeners` names the listeners of
 *   each tracked emitter by event, so a swapped listener shows where a total would not.
 */
const instrument = (
  { socketFactory = defaultSocketFactory(), ...options },
  { activeResources = true } = {}
) => {
  const open = new Set()
  let created = 0
  const clusters = new Set()
  const counted = (args) => {
    const socket = socketFactory(args)
    created += 1
    open.add(socket)
    return socket
  }
  const kafka = new Kafka({ ...options, socketFactory: counted })

  const { symbol, value: createCluster } = privateOf(kafka, 'createCluster')
  kafka[symbol] = (...args) => {
    const cluster = createCluster(...args)
    clusters.add(new WeakRef(cluster))
    return cluster
  }
  const { value: operations } = privateOf(kafka, 'operations')

  const openSockets = () => {
    for (const socket of open) {
      if (socket.destroyed) open.delete(socket)
    }
    return [...open]
  }
  const liveClusters = () => {
    const live = []
    for (const ref of clusters) {
      const cluster = ref.deref()
      if (cluster === undefined) clusters.delete(ref)
      else live.push(cluster)
    }
    return live
  }
  const queues = () =>
    liveClusters()
      .flatMap(({ brokerPool }) => [...Object.values(brokerPool.brokers), brokerPool.seedBroker])
      .filter(Boolean)
      .flatMap(({ connectionPool }) => connectionPool.pool.map(({ requestQueue }) => requestQueue))
  const unique = (items) => [...new Set(items)]

  const probe = createResourceProbe({
    emitters: {
      get requestQueues() {
        return together(unique(queues()))
      },
      get sockets() {
        return together(openSockets())
      },
    },
    sockets: () => ({ created, destroyed: created - openSockets().length }),
    queueDepth: () =>
      unique(queues()).reduce(
        (total, queue) => total + queue.pending.length + queue.inflight.size,
        0
      ),
    ledgerSize: () => [...operations.values()].reduce((total, ledger) => total + ledger.size(), 0),
    obligationSize: () =>
      [...operations.values()]
        .flatMap((ledger) => [...ledger.attached])
        .reduce((total, obligations) => total + obligations.size(), 0),
    activeResources,
    heap: typeof globalThis.gc === 'function',
  })

  const named = (emitter) =>
    Object.fromEntries(
      emitter.eventNames().map((name) => [String(name), emitter.listenerCount(name)])
    )
  const listeners = () => ({
    requestQueues: named(together(unique(queues()))),
    sockets: named(together(openSockets())),
  })

  return { kafka, probe, socketFactory: counted, listeners }
}

/**
 * What a workload is handed.
 * @typedef {object} WorkloadContext
 * @property {object} kafka the `Kafka` the clients are built from
 * @property {string} topic the topic of the run
 * @property {string} groupId the consumer group of the run
 * @property {ProducerChurn} [producerChurn] the owner of the producer-churn phases
 * @property {{ runPhase: (name: string, ms: number) => Promise<void> }} [consumerChurn] the owner of the
 *   consumer-churn phases
 */

/**
 * The workloads. Each runs for `ms` milliseconds and resolves once every client it built is
 * disconnected. When it fails, it rejects with that failure after the same disconnects; a failed
 * disconnect never hides the failure of the workload, and does not stop the others.
 */
const WORKLOADS = {
  /**
   * Runs one measured phase of the consumer-churn profile through its owner, which keeps the clients
   * public and disconnects them before it returns.
   * @param {WorkloadContext & { consumerChurn: { runPhase: Function } }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves once the phase's clients are drained and disconnected.
   * @throws {Error} For workload or cleanup failure.
   */
  consumerMemberChurn: (context, ms) => context.consumerChurn.runPhase('member-churn', ms),
  /** Runs the regex-topic phase of the consumer-churn profile; see `consumerMemberChurn`. */
  consumerRegexTopics: (context, ms) => context.consumerChurn.runPhase('regex-topics', ms),
  /** Runs the pause and recreation phase of the consumer-churn profile; see `consumerMemberChurn`. */
  consumerPauseRecreate: (context, ms) => context.consumerChurn.runPhase('pause-recreate', ms),
  /** Runs the mostly idle phase of the consumer-churn profile; see `consumerMemberChurn`. */
  consumerMostlyIdle: (context, ms) => context.consumerChurn.runPhase('mostly-idle', ms),
  /**
   * Runs the steady producer phase through its owned lifecycle.
   * @param {WorkloadContext & { producerChurn: ProducerChurn }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves after drain, admission probe and disconnect.
   * @throws {Error} For workload or cleanup failure.
   */
  producerSteady(context, ms) {
    return context.producerChurn.runPhase('steady', ms)
  },
  /**
   * Runs the producer outage phase and restores every broker before returning.
   * @param {WorkloadContext & { producerChurn: ProducerChurn }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves after recovery and disconnect.
   * @throws {Error} For workload, restore or cleanup failure.
   */
  producerOutage(context, ms) {
    return context.producerChurn.runPhase('outage', ms)
  },
  /**
   * Runs buffer pressure through delayed writes, then clears the delay and drains.
   * @param {WorkloadContext & { producerChurn: ProducerChurn }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves after recovery and disconnect.
   * @throws {Error} For workload, positive-control or cleanup failure.
   */
  producerPressure(context, ms) {
    return context.producerChurn.runPhase('buffer-pressure', ms)
  },
  /**
   * One member polling the topic while a message arrives every second.
   * @param {WorkloadContext} context
   * @param {number} ms how long to run, in milliseconds
   * @returns {Promise<void>}
   * @throws {Error} the failure of a connect, subscribe, run or send, after the clients are
   *   disconnected; when only a disconnect fails, that failure
   */
  async idle({ kafka, topic, groupId }, ms) {
    await Clients.use(kafka, async (clients) => {
      const producer = clients.producer()
      const consumer = clients.consumer({ groupId })
      await producer.connect()
      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: false })
      await consumer.run({ eachMessage: async () => {} })

      const deadline = Date.now() + ms
      for (let key = 0; Date.now() < deadline; key++) {
        await producer.send({ topic, messages: [{ key: String(key % PARTITIONS), value: 'x' }] })
        await sleep(1000)
      }
    })
  },

  /**
   * Two members of one group, one slot's consumer replaced by a new one every 15 s, cooperatively.
   * @param {WorkloadContext} context
   * @param {number} ms how long to run, in milliseconds
   * @returns {Promise<void>}
   * @throws {Error} the failure of a connect, subscribe, run, send or replacement, after every
   *   client built so far, a replacement half built among them, is disconnected; when only a
   *   disconnect fails, that failure
   */
  async memberChurn({ kafka, topic, groupId }, ms) {
    await Clients.use(kafka, async (clients) => {
      const producer = clients.producer()
      await producer.connect()
      const start = async () => {
        const consumer = clients.consumer({
          groupId,
          partitionAssigners: [PartitionAssigners.cooperativeSticky],
        })
        await consumer.connect()
        await consumer.subscribe({ topic, fromBeginning: false })
        await consumer.run({ eachMessage: async () => {} })
        return consumer
      }
      const slots = [await start(), await start()]

      const deadline = Date.now() + ms
      for (let round = 0; Date.now() < deadline; round++) {
        await producer.send({ topic, messages: [{ key: String(round % PARTITIONS), value: 'x' }] })
        await sleep(Math.min(MEMBER_CHURN_MS, Math.max(deadline - Date.now(), 0)))
        const slot = round % slots.length
        await clients.release(slots[slot])
        slots[slot] = await start()
      }
    })
  },

  /**
   * A regex subscriber while 50 distinct names are created, written to and deleted in turn.
   * @param {WorkloadContext} context `topic` is not used: the topics are `soak-regex-<n>`
   * @param {number} ms how long to run, in milliseconds
   * @returns {Promise<void>}
   * @throws {Error} the failure of a connect, subscribe, run, topic creation, send or deletion,
   *   after the admin, the producer and the consumer are disconnected; when only a disconnect
   *   fails, that failure
   */
  async regexTopicChurn({ kafka, groupId }, ms) {
    await Clients.use(kafka, async (clients) => {
      const admin = clients.admin()
      const producer = clients.producer()
      const consumer = clients.consumer({ groupId })
      await admin.connect()
      await producer.connect()
      await consumer.connect()
      await consumer.subscribe({ topics: [/^soak-regex-/], fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      const deadline = Date.now() + ms
      for (let round = 0; Date.now() < deadline; round++) {
        const topic = `soak-regex-${round % TOPIC_NAMES}`
        await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true })
        await producer.send({ topic, messages: [{ value: 'x' }] })
        await sleep(1000)
        await admin.deleteTopics({ topics: [topic] })
      }
    })
  },

  /**
   * The idle workload while a broker is stopped for the middle third of the phase. Both branches
   * settle before this does, and the broker is started again even when stopping it throws.
   * @param {WorkloadContext} context
   * @param {number} ms how long to run, in milliseconds
   * @returns {Promise<void>}
   * @throws {Error} the failure of the idle workload, else of stopping the broker, else of starting
   *   it again, each after the clients are disconnected and the restart attempted
   */
  async outage(context, ms) {
    const { stopBroker, startBroker } = require('../../testHelpers/leaderElection')
    const third = Math.floor(ms / 3)
    const outage = async () => {
      await sleep(third)
      try {
        stopBroker(OUTAGE_SERVICE)
        await sleep(third)
      } catch (error) {
        try {
          startBroker(OUTAGE_SERVICE)
        } catch {}
        throw error
      }
      startBroker(OUTAGE_SERVICE)
    }

    const [idle, broker] = await Promise.allSettled([WORKLOADS.idle(context, ms), outage()])
    for (const result of [idle, broker]) {
      if (result.status === 'rejected') {
        throw result.reason
      }
    }
  },
}

/**
 * Creates the topic of a run, over three replicas, and waits for its leaders.
 * @param {object} kafka a `Kafka`
 * @param {string} topic
 * @returns {Promise<void>}
 * @throws {Error} the failure of the creation, after the admin is disconnected
 */
const createTopic = (kafka, topic) =>
  Clients.use(kafka, async (clients) => {
    const admin = clients.admin()
    await admin.connect()
    await admin.createTopics({
      topics: [{ topic, numPartitions: PARTITIONS, replicationFactor: 3 }],
      waitForLeaders: true,
    })
  })

/** The bytes one Fetch may return for a partition: 1 MiB. */
const PARTITION_MAX_BYTES = 1048576

/** @type {{ now: () => number }} the monotonic clock of a run; a spec supplies its own */
const REAL_CLOCK = Object.freeze({ now: () => performance.now() })

/**
 * Connects one broker through `socketFactory`, with no client around it: no retry, no metadata
 * cache, no group.
 * @param {{ host: string, port: number, socketFactory: Function }} options
 * @returns {Promise<Broker>} connected
 */
const openRawBroker = async ({ host, port, socketFactory }) => {
  const logger = createLogger({ level: logLevel.NOTHING, logCreator: LoggerConsole })
  const broker = new Broker({
    connectionPool: new ConnectionPool({
      host,
      port,
      logger,
      socketFactory,
      connectionTimeout: 3000,
      requestTimeout: 30000,
    }),
    logger,
  })
  await broker.connect()
  return broker
}

/**
 * The live reader of the producer-churn soak: one raw reader role that follows the topic from
 * offset 0 with `Broker#fetch`, a connection to each partition leader and one to ask for metadata,
 * all opened through the counted socket factory so what it leaves open is measured. It joins no
 * group and commits nothing. Each partition has its own cursor, which moves only past a record
 * actually read; after a leadership or connection error the connections concerned are dropped and
 * the routing is fetched again. Cursors survive `close`, so one reader serves every phase. It hands
 * each data record on in order and keeps no record itself.
 */
class RawProducerReader {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses, `host:port`, asked in turn for metadata
   * @param {string} options.topic
   * @param {(line: object) => void} options.record where the reader's routing lines go
   * @param {Function} [options.socketFactory] the counted factory of `instrument`
   * @param {(partition: number, record: object) => void} [options.onRecord] given every data
   *   record, in partition order, before the cursor passes it
   * @param {number} [options.partitions=PARTITIONS]
   * @param {(options: object) => Promise<object>} [options.openBroker] connects one broker; a spec
   *   supplies a fake
   * @param {{ now: () => number }} [options.clock]
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} [options.sleep]
   * @param {number} [options.maxWaitTime=100] how long a Fetch waits at the broker for data, ms
   * @param {number} [options.retryMs=200] the pause after an error, ms
   * @param {number} [options.drainMs=120000] how long `drain` may take, ms
   */
  constructor({
    brokers,
    topic,
    record,
    socketFactory = defaultSocketFactory(),
    onRecord = () => {},
    partitions = PARTITIONS,
    openBroker = openRawBroker,
    clock = REAL_CLOCK,
    sleep: pause = sleep,
    maxWaitTime = 100,
    retryMs = 200,
    drainMs = 120000,
  }) {
    Object.assign(this, {
      brokers,
      topic,
      record,
      onRecord,
      partitions,
      openBroker,
      clock,
      pause,
      maxWaitTime,
      retryMs,
      drainMs,
    })
    this.cursor = new Float64Array(partitions)
    this.leaders = new Int32Array(partitions).fill(-1)
    this.addresses = new Map()
    this.connections = new Map()
    this.seed = null
    this.routed = false
    this.sockets = new Set()
    this.creating = false
    this.refetches = 0
    this.records = 0
    this.lastRoute = ''
    this.factory = (args) => {
      this.creating = true
      try {
        const socket = socketFactory(args)
        for (const own of this.sockets) {
          if (own.destroyed) this.sockets.delete(own)
        }
        this.sockets.add(socket)
        return socket
      } finally {
        this.creating = false
      }
    }
  }

  /**
   * @param {object} socket a socket of the counted factory
   * @returns {boolean} whether the reader made it, or is making it now
   */
  owns(socket) {
    return this.creating || this.sockets.has(socket)
  }

  /**
   * @returns {Map<number, number>} the next offset to read of each partition
   */
  cursors() {
    return new Map(Array.from(this.cursor, (offset, partition) => [partition, offset]))
  }

  /**
   * Opens the connections the routing needs.
   * @returns {Promise<void>}
   * @throws {Error} when no seed broker answers or the topic has no metadata
   */
  start() {
    return this.#route()
  }

  /**
   * One round: fetches from every partition leader at once, waiting at most `maxWaitTime` for data,
   * and hands each record read on. A retriable error, such as a leader that moved or a node that
   * is down, drops the connections concerned, schedules a new routing and pauses `retryMs`.
   * @returns {Promise<number>} how many records were read
   * @throws {Error} for an error that is not retriable
   */
  async poll() {
    let failed = false
    try {
      if (!this.routed) await this.#route()
    } catch (error) {
      if (error?.retriable !== true) throw error
      failed = true
    }
    let read = 0
    if (!failed) {
      const byNode = new Map()
      this.leaders.forEach((node, partition) => {
        if (node < 0) failed = true
        else byNode.set(node, [...(byNode.get(node) ?? []), partition])
      })
      const rounds = await Promise.all([...byNode].map(([node, ids]) => this.#fetch(node, ids)))
      for (const round of rounds) {
        read += round.read
        failed ||= round.failed
      }
    }
    if (failed) {
      this.routed = false
      await this.pause(this.retryMs)
    }
    return read
  }

  /**
   * Polls until every cursor has reached its frozen end.
   * @param {Map<number, string>} ends the end offset of each partition
   * @returns {Promise<void>}
   * @throws {Error} naming the first partition still short when `drainMs` runs out
   */
  async drain(ends) {
    const deadline = this.clock.now() + this.drainMs
    for (;;) {
      const short = [...ends].find(([partition, end]) => this.cursor[partition] < Number(end))
      if (!short) return
      if (this.clock.now() >= deadline) {
        throw new Error(
          `The reader is at ${this.cursor[short[0]]} of partition ${short[0]}, below its frozen end ${short[1]}`
        )
      }
      await this.poll()
    }
  }

  /**
   * Disconnects every connection the reader holds; the cursors stay.
   * @returns {Promise<void>}
   * @throws {Error} the first failed disconnect, after all were tried
   */
  async close() {
    const held = [this.seed, ...this.connections.values()].filter(Boolean)
    this.seed = null
    this.connections.clear()
    this.routed = false
    const results = await Promise.allSettled(held.map((broker) => broker.disconnect()))
    this.sockets.clear()
    const failed = results.find(({ status }) => status === 'rejected')
    if (failed) throw failed.reason
  }

  /**
   * @private
   * @param {number} node
   * @param {number[]} partitions the partitions it leads
   * @returns {Promise<{ read: number, failed: boolean }>}
   * @throws {Error} for an error that is not retriable
   */
  async #fetch(node, partitions) {
    try {
      const broker = await this.#connection(node)
      const { responses } = await broker.fetch({
        isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        maxWaitTime: this.maxWaitTime,
        topics: [
          {
            topic: this.topic,
            partitions: partitions.map((partition) => ({
              partition,
              fetchOffset: String(this.cursor[partition]),
              maxBytes: PARTITION_MAX_BYTES,
            })),
          },
        ],
      })
      let read = 0
      for (const { partitions: answered } of responses) {
        for (const { partition, messages } of answered) read += this.#take(partition, messages)
      }
      return { read, failed: false }
    } catch (error) {
      if (error?.retriable !== true) throw error
      this.refetches += 1
      await this.connections
        .get(node)
        ?.disconnect()
        .catch(() => {})
      this.connections.delete(node)
      return { read: 0, failed: true }
    }
  }

  /**
   * Hands the data records of one partition on and moves its cursor past each.
   * @private
   * @param {number} partition
   * @param {object[]} messages
   * @returns {number} how many data records it passed
   */
  #take(partition, messages) {
    let taken = 0
    for (const message of messages) {
      const offset = Number(message.offset)
      if (offset < this.cursor[partition] || message.batchContext?.isControlBatch) continue
      this.onRecord(partition, message)
      this.cursor[partition] = offset + 1
      taken += 1
    }
    this.records += taken
    return taken
  }

  /**
   * @private
   * @param {number} node
   * @returns {Promise<object>} the connection to the node, opened when it has none
   */
  async #connection(node) {
    let broker = this.connections.get(node)
    if (!broker) {
      broker = await this.#open(this.addresses.get(node))
      this.connections.set(node, broker)
    }
    return broker
  }

  /**
   * @private
   * @param {{ host: string, port: number }} address
   * @returns {Promise<object>}
   */
  #open({ host, port }) {
    return this.openBroker({ host, port, socketFactory: this.factory })
  }

  /**
   * Asks the first seed broker that answers for the topic's leaders, and keeps that connection.
   * @private
   * @returns {Promise<void>}
   * @throws {Error} the last retriable failure when no seed answers; any other failure at once
   */
  async #route() {
    let failure
    for (const address of this.brokers) {
      const [seedHost, seedPort] = address.split(':')
      let seed
      try {
        seed = await this.#open({ host: seedHost, port: Number(seedPort) })
        const { brokers, topicMetadata } = await seed.metadata([this.topic])
        const metadata = topicMetadata.find(({ topic }) => topic === this.topic)
        if (!metadata || metadata.topicErrorCode) {
          throw Object.assign(new Error(`No metadata for ${this.topic}`), { retriable: true })
        }
        this.addresses = new Map(brokers.map(({ nodeId, host, port }) => [nodeId, { host, port }]))
        this.leaders.fill(-1)
        for (const { partitionId, leader } of metadata.partitionMetadata) {
          if (partitionId < this.partitions) this.leaders[partitionId] = leader
        }
        await this.seed?.disconnect().catch(() => {})
        this.seed = seed
        this.routed = true
        const leaders = Array.from(this.leaders).join(',')
        if (leaders !== this.lastRoute) {
          this.lastRoute = leaders
          this.record({ kind: 'reader', event: 'route', leaders })
        }
        return
      } catch (error) {
        await seed?.disconnect().catch(() => {})
        if (error?.retriable !== true) throw error
        failure = error
      }
    }
    throw failure ?? new Error('The reader has no seed broker')
  }
}

/**
 * Delays what the workload's own producer writes, to fill its buffer, and takes the delay off
 * again. The reader's sockets are left alone, so what it measures is not itself delayed.
 */
class WriteDelay {
  /**
   * @param {object} options
   * @param {ReturnType<typeof createChaosSocketFactory>} options.chaos the factory the client's
   *   sockets come from
   * @param {{ owns: (socket: object) => boolean }} options.reader whose sockets are spared
   */
  constructor({ chaos, reader }) {
    this.chaos = chaos
    this.reader = reader
    this.disarm = null
  }

  /**
   * Delays every write of every open and later client socket.
   * @param {number} ms
   * @returns {void}
   */
  delayWrites(ms) {
    this.disarm?.()
    this.disarm = this.chaos.arm({}, (socket) => {
      if (!this.reader.owns(socket)) socket.delay({ write: ms })
    })
  }

  /**
   * Takes the delay off every socket, so later writes pass at once, and forgets destroyed sockets.
   * @returns {void}
   */
  clear() {
    this.disarm?.()
    this.disarm = null
    for (const socket of this.chaos.sockets) {
      if (!socket.destroyed && !this.reader.owns(socket)) socket.delay({ write: 0 })
    }
    this.chaos.sockets = this.chaos.sockets.filter(({ destroyed }) => !destroyed)
  }
}

/**
 * The producer-churn soak: one idempotent producer offers `rate` public sends a second to one
 * six-partition topic for three phases, while one raw reader reads the log, and what is left is
 * judged by four independent readings.
 *
 * - **steady**: the load alone, which has to hold its offered rate.
 * - **outage**: a clean, then a hard, rolling restart of the brokers, one node at a time, each
 *   restored before the next, and after each restoration a normal send that must be acknowledged.
 * - **buffer-pressure**: every write is delayed 5 s so the producer's buffer fills; a typed
 *   `KafkaJSBufferExhaustedError` must refuse sends, and once the delay is off a normal send must
 *   be acknowledged.
 *
 * Every phase ends the same way: the offering stops, every call is joined, the producer is
 * flushed, the closing admission probe runs, the reader catches up, and the producer, the admin and
 * the reader disconnect. The readings: the resource bounds of `Soak` (the sockets, listeners by
 * name, queues and timers, and the heap, which `evaluatePhases` judges inside each phase and across
 * the quiet points, since the buffer-pressure phase holds a full buffer by design); the controls of
 * each phase; the closing probe of each phase; and
 * the durability of the log, read once through the reader's own journal and once again from the
 * quiet log. Each acknowledged id must be in the log exactly once, an id of unknown outcome at most
 * once, an id refused before sending never, and no record may repeat a `(partition, pid, epoch,
 * sequence)`. `ok` is the conjunction of all four.
 *
 * Observer storage is allocated once in `prepare`, before the baseline reading, and never grows.
 * Nothing resends an unknown outcome: a record the application would send again would have a new id.
 */
class ProducerChurn {
  /**
   * @param {object} options
   * @param {object} options.kafka a `Kafka` whose sockets are counted (`instrument`)
   * @param {{ snapshot: () => Promise<object> }} options.probe the resource probe of `instrument`
   * @param {string[]} options.brokers seed addresses, `host:port`
   * @param {string} options.topic the workload topic, made by `prepare`
   * @param {(line: object) => void} options.record where every evidence line goes
   * @param {Array<{ name: string, workload: string, ms: number }>} options.phases
   * @param {{ rollingBounce: Function }} options.bouncer a `BrokerBouncer`
   * @param {object} options.reader a `RawProducerReader`; `onRecord` is set here
   * @param {object} options.admissionProbe a `ClosingProbe`: `prepare`, `install`, `restore`,
   *   `capture`, `run` and `retained`
   * @param {{ delayWrites: (ms: number) => void, clear: () => void }} options.faults
   * @param {() => Record<string, Record<string, number>>} [options.listeners] the listeners of each
   *   tracked emitter by event name
   * @param {{ append: Function, replay: Function, close: Function }} [options.journal] what the
   *   reader saw; a file under `.tmp/e2e` by default
   * @param {{ now: () => number }} [options.clock]
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} [options.sleep]
   * @param {{ endOffsets: Function, readLog: Function }} [options.rawLog]
   * @param {Record<string, Function>} [options.workloads=WORKLOADS]
   * @param {object} [options.soak] `sampleMs`, `warmupMs` (how long into each phase its heap
   *   samples start to count; `PHASE_WARMUP_MS` by default) and `settleMs` for `Soak`
   * @param {Partial<typeof CHURN> & { requiredKinds?: string[] }} [options.settings] overrides of
   *   `CHURN`, and which kinds of outage the outage phase must have made
   */
  constructor({
    kafka,
    probe,
    brokers,
    topic,
    record,
    phases,
    bouncer,
    reader,
    admissionProbe,
    faults,
    listeners = () => ({}),
    journal,
    clock = REAL_CLOCK,
    sleep: pause = sleep,
    rawLog = { endOffsets, readLog },
    workloads = WORKLOADS,
    soak = {},
    settings = {},
  }) {
    Object.assign(this, {
      kafka,
      brokers,
      topic,
      record,
      phases,
      bouncer,
      reader,
      admissionProbe,
      faults,
      listeners,
      journal,
      clock,
      pause,
      rawLog,
      workloads,
      soak: { warmupMs: PHASE_WARMUP_MS, ...soak },
    })
    this.settings = { requiredKinds: ['clean', 'hard'], ...CHURN, ...settings }
    this.measure = {
      snapshot: async () => ({
        ...(await probe.snapshot()),
        named: this.listeners(),
        unhandled: this.unhandled,
      }),
    }
    this.tag = (Math.random() * 0xffffffff) >>> 0
    this.reader.onRecord = (partition, message) => this.#observe(partition, message)
    this.ledger = null
    this.observed = null
    this.producer = null
    this.admin = null
    this.bytes = this.settings.steadyBytes
    this.unsettled = 0
    this.unhandled = 0
    this.capacityReached = false
    this.firstUnexpected = null
    this.prepared = false
    this.closed = false
    this.connected = false
    this.captured = false
    this.startedAt = 0
    this.phase = null
    this.phaseResults = {}
    this.quietPoints = []
    this.wire = {}
    this.outcome = null
    this.stopWire = () => {}
    this.onUnhandled = () => {
      this.unhandled += 1
    }
  }

  /**
   * Allocates every observer, installs the probe, and makes the topics. Everything the run keeps
   * is made here, before the baseline reading.
   * @returns {Promise<void>}
   * @throws {Error} when called twice, or the topics cannot be made
   */
  async prepare() {
    if (this.prepared) throw new Error('The producer churn is already prepared')
    this.prepared = true
    const { rate, idSlack, partitions } = this.settings
    const totalMs = this.phases.reduce((total, { ms }) => total + ms, 0)
    const capacity = Math.ceil((rate * totalMs) / 1000) + idSlack
    this.ledger = new OfferLedger({
      capacity,
      partitions,
      sampleEvery: this.settings.latencySampleEvery,
    })
    this.observed = new ObservedLog({ capacity })
    this.record({
      kind: 'config',
      config: {
        ...this.settings,
        phases: this.phases,
        ledgerCapacity: capacity,
        replicationFactor: 3,
        brokers: this.brokers,
        topic: this.topic,
      },
    })
    this.journal ??= openJournal()
    this.startedAt = this.clock.now()
    process.on('unhandledRejection', this.onUnhandled)
    this.admissionProbe.install()
    await Clients.use(this.kafka, async (clients) => {
      const admin = clients.admin()
      await admin.connect()
      await admin.createTopics({
        topics: [
          {
            topic: this.topic,
            numPartitions: partitions,
            replicationFactor: 3,
            configEntries: [
              { name: 'min.insync.replicas', value: String(this.settings.minInsyncReplicas) },
            ],
          },
        ],
        waitForLeaders: true,
      })
    })
    await this.admissionProbe.prepare(this.kafka)
  }

  /**
   * Runs the whole profile: `prepare`, then the phases of `Soak` with this object as their owner,
   * the durability reading before the teardown reading, and the verdict.
   * @returns {Promise<{ ok: boolean, violations: string[] }>} the verdict, also recorded as the
   *   run's one `verdict` line; the resource verdict of `Soak` is recorded as `resource-verdict`
   * @throws {Error} the failure of a phase, after its clients are disconnected and a failed
   *   verdict is recorded; or a profile that names a workload that is not registered
   */
  async run() {
    const unregistered = this.phases.find(
      ({ workload }) => typeof this.workloads[workload] !== 'function'
    )
    if (unregistered) {
      throw new Error(
        `The phase ${unregistered.name} names the workload ${unregistered.workload}, which is not registered`
      )
    }
    try {
      await this.prepare()
      const soak = new Soak({
        kafka: this.kafka,
        probe: this.measure,
        phases: this.phases,
        judge: (run) => evaluatePhases({ ...run, minSamples: this.settings.minHeapSamples }),
        context: {
          kafka: this.kafka,
          topic: this.topic,
          groupId: `${this.topic}-group`,
          producerChurn: this,
        },
        record: (line) => this.#recordSoak(line),
        workloads: this.workloads,
        beforeTeardown: async () => {
          this.outcome = await this.finish()
        },
        ...this.soak,
      })
      const resources = await soak.run()
      await this.close()
      await this.pause(1)
      const violations = [
        ...this.outcome.violations,
        ...this.#quietPointViolations(),
        ...(this.unhandled === 0 ? [] : [`unhandled rejections: ${this.unhandled} after close`]),
        ...resources.violations.map((line) => `resources: ${line}`),
      ]
      const verdict = {
        ok: violations.length === 0,
        violations,
        resources,
        controls: this.outcome.controls,
        durability: this.outcome.durability,
        latency: this.outcome.latency,
        counts: this.outcome.counts,
        wireVersions: this.wire,
      }
      this.record({ kind: 'verdict', ...verdict })
      return verdict
    } catch (error) {
      this.record({ kind: 'verdict', ok: false, violations: [`run failed: ${error.message}`] })
      throw error
    } finally {
      await this.close()
    }
  }

  /**
   * One phase: connects the producer and the reader, offers load for `ms` while the phase's own
   * fault runs, reaches the quiet point, and disconnects. Whatever fails, the fault is taken off,
   * the node is restored, the background work is joined and every client is disconnected, and the
   * first failure is the one thrown.
   * @param {'steady'|'outage'|'buffer-pressure'} name
   * @param {number} ms how long to offer load
   * @returns {Promise<void>}
   * @throws {Error} the first failure of the work, the quiet point or the cleanup
   */
  async runPhase(name, ms) {
    if (!this.prepared || this.closed) {
      throw new Error(`The phase ${name} has no prepared, open producer churn to run in`)
    }
    this.#begin(name, ms)
    const work = new AbortController()
    const reading = new AbortController()
    let failure
    const guard = (task) =>
      task().catch((error) => {
        failure ??= error
        work.abort(error)
        reading.abort(error)
      })
    const tasks = []
    const spawn = (task) => tasks.push(guard(task))
    let following = Promise.resolve()
    try {
      await this.#open(name)
      following = guard(() => this.#follow(reading.signal))
      await this.#work(name, { until: this.clock.now() + ms, signal: work.signal, spawn })
      work.abort()
      await Promise.all(tasks)
      if (failure) throw failure
      await this.quiet(name)
      reading.abort()
      await following
      if (failure) throw failure
      const ends = await this.rawLog.endOffsets({ brokers: this.brokers, topic: this.topic })
      await this.reader.drain(ends)
    } catch (error) {
      failure ??= error
    }
    work.abort()
    reading.abort()
    await Promise.allSettled([...tasks, following])
    const cleanup = await this.#disconnect()
    this.#end()
    if (failure) {
      failure.cleanupErrors = cleanup
      throw failure
    }
    if (cleanup.length > 0) throw cleanup[0]
  }

  /**
   * Offers sends at the configured rate until `until`, on a deadline schedule: the n-th send is
   * due at `n / rate` after the start, so a late tick is made up and its lateness is recorded as
   * slip. A tick that finds `maxUnsettled` sends unsettled offers none and counts backpressure.
   * @param {number} until a time of the clock
   * @param {AbortSignal} [signal] ends the loop early
   * @returns {Promise<void>}
   */
  async offerUntil(until, signal) {
    const { rate, maxUnsettled } = this.settings
    const start = this.clock.now()
    const stats = this.phase
    let slot = 0
    while (this.clock.now() < until && !signal?.aborted) {
      const now = this.clock.now()
      const due = Math.floor(((now - start) * rate) / 1000) + 1
      if (slot < due) {
        const slip = Math.max(0, now - (start + (slot * 1000) / rate))
        stats.slip.maxMs = Math.max(stats.slip.maxMs, slip)
        stats.slip.totalMs += slip
        stats.slip.ticks += 1
      }
      for (; slot < due; slot++) {
        if (this.unsettled >= maxUnsettled) stats.backpressure += 1
        else if (this.#offer(this.bytes) === null) return
      }
      await this.pause(Math.max(1, start + (slot * 1000) / rate - this.clock.now()), signal)
    }
  }

  /**
   * The quiet point of a phase, with the producer still connected: every call joined, the producer
   * flushed, and the closing admission probe run. A probe that fails is recorded for the verdict,
   * it does not stop the phase.
   * @param {string} name the phase
   * @returns {Promise<void>}
   * @throws {Error} when calls are still unsettled after `joinMs` or the flush fails
   */
  async quiet(name) {
    const began = this.clock.now()
    const { joinMs, flushMs } = this.settings
    if (!(await this.#waitFor(() => this.unsettled === 0, joinMs))) {
      throw new Error(`${name}: ${this.unsettled} calls are still unsettled after ${joinMs} ms`)
    }
    await this.producer.flush({ timeout: flushMs })
    try {
      this.phase.probe = await this.admissionProbe.run({ producer: this.producer, label: name })
    } catch (error) {
      this.phase.probeError = error.message
    }
    this.phase.quietMs = this.clock.now() - began
    this.record({
      kind: 'quiet',
      phase: name,
      quietMs: this.phase.quietMs,
      probe: this.phase.probe,
      probeError: this.phase.probeError,
    })
  }

  /**
   * The durability reading and the controls, once every phase is over: freezes each partition's end
   * offset, drains the live reader to it, merges the reader's journal by id, then reads the quiet
   * log independently and merges that too.
   * @returns {Promise<{ ok: boolean, violations: string[], durability: object, controls: object,
   *   latency: object, counts: object }>} every violation, not the first
   */
  async finish() {
    const violations = []
    let ends = null
    try {
      ends = await this.rawLog.endOffsets({ brokers: this.brokers, topic: this.topic })
      await this.reader.start()
      try {
        await this.reader.drain(ends)
      } finally {
        await this.reader.close()
      }
    } catch (error) {
      violations.push(`durability: the reader did not reach the frozen ends: ${error.message}`)
    }
    const cursors = this.reader.cursors()
    for (const [partition, end] of ends ?? []) {
      const cursor = cursors.get(partition) ?? 0
      if (cursor < Number(end)) {
        violations.push(
          `durability: the final cursor ${cursor} of partition ${partition} is below the frozen end ${end}`
        )
      }
    }

    this.observed.reset()
    this.journal.replay((...entry) => this.observed.add(...entry))
    const live = mergeDurability(this.ledger, this.observed, 'live reader')
    violations.push(...live.violations)
    const { lastAckId, lastAckOffset } = this.ledger
    lastAckId.forEach((id, partition) => {
      if (
        id >= 0 &&
        !(this.observed.seen[id] > 0 && cursors.get(partition) > lastAckOffset[partition])
      ) {
        violations.push(
          `durability: the last acknowledged record of partition ${partition} (id ${id}, offset ${lastAckOffset[partition]}) was not reached by the reader`
        )
      }
    })
    const quiet = await this.#readQuiet(ends, violations)

    const latency = this.ledger.latency()
    const controls = this.#controls(violations, latency)
    const counts = { offered: this.ledger.offered, ...this.ledger.counts }
    this.record({ kind: 'control', controls, latency })
    this.record({ kind: 'oracle', live, quiet, counts })
    return {
      ok: violations.length === 0,
      violations,
      durability: { live, quiet },
      controls,
      latency,
      counts,
    }
  }

  /**
   * Disconnects whatever is still open, restores the probe, takes the listener off the process and
   * closes the journal; safe to call again.
   * @returns {Promise<void>}
   * @throws {Error} the first failure, after every step was tried
   */
  async close() {
    if (this.closed) return
    this.closed = true
    const failures = await this.#disconnect()
    for (const step of [
      () => this.stopWire(),
      () => process.off('unhandledRejection', this.onUnhandled),
      () => this.admissionProbe.restore(),
      () => this.journal?.close(),
    ]) {
      try {
        step()
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length > 0) throw failures[0]
  }

  /**
   * @private
   * @param {string} name
   * @param {number} ms
   * @returns {void}
   */
  #begin(name, ms) {
    this.phase = {
      name,
      ms,
      startedAt: this.clock.now(),
      before: { offered: this.ledger.offered, ...this.ledger.counts },
      refusals: {},
      backpressure: 0,
      slip: { maxMs: 0, totalMs: 0, ticks: 0 },
      outages: [],
      recoveries: [],
      pressureRecovery: null,
      probe: null,
      probeError: null,
      afterDisconnect: null,
      quietMs: null,
    }
    this.bytes = this.settings.steadyBytes
  }

  /**
   * Closes the books of the current phase and records them.
   * @private
   * @returns {void}
   */
  #end() {
    const { phase, ledger } = this
    const delta = (key) =>
      (key === 'offered' ? ledger.offered : ledger.counts[key]) - phase.before[key]
    const refused = Object.values(phase.refusals).reduce((total, count) => total + count, 0)
    phase.offered = delta('offered')
    phase.ack = delta('ack')
    phase.unknown = delta('unknown')
    phase.unsent = delta('unsent')
    phase.unexpected = delta('unexpected')
    phase.admitted = phase.offered - refused
    phase.offeredPerSecond = phase.offered / (phase.ms / 1000)
    phase.slip.meanMs = phase.slip.ticks === 0 ? 0 : phase.slip.totalMs / phase.slip.ticks
    phase.endedAt = this.clock.now()
    this.phaseResults[phase.name] = phase
    const { name, before, probe, ...rest } = phase
    this.record({ kind: 'phase', phase: name, ...rest })
  }

  /**
   * @private
   * @param {string} name
   * @returns {Promise<void>}
   */
  async #open(name) {
    if (this.producer === null) {
      this.producer = this.kafka.producer({ ...this.settings.producer })
      this.stopWire = this.producer.on(this.producer.events.REQUEST, ({ payload }) => {
        this.wire[payload.apiName] = Math.max(this.wire[payload.apiName] ?? 0, payload.apiVersion)
      })
    }
    await this.producer.connect()
    this.connected = true
    await this.reader.start()
    if (name === 'outage') {
      this.admin = this.kafka.admin()
      await this.admin.connect()
    }
    if (!this.captured) {
      const status = await this.admissionProbe.capture(() => this.#offer(this.settings.steadyBytes))
      if (status !== STATUS.ACK) throw new Error('The first send of the run was not acknowledged')
      this.captured = true
    }
  }

  /**
   * Runs what a phase adds to the load.
   * @private
   * @param {string} name
   * @param {{ until: number, signal: AbortSignal, spawn: Function }} context
   * @returns {Promise<void>}
   */
  async #work(name, { until, signal, spawn }) {
    if (name === 'outage') {
      spawn(() => this.#roll(signal))
    } else if (name === 'buffer-pressure') {
      this.bytes = this.settings.pressureBytes
      this.faults.delayWrites(this.settings.pressureDelayMs)
    }
    try {
      await this.offerUntil(until, signal)
    } finally {
      if (name === 'buffer-pressure') {
        this.faults.clear()
        this.bytes = this.settings.steadyBytes
      }
    }
    if (name === 'buffer-pressure') this.phase.pressureRecovery = await this.#recover('pressure')
  }

  /**
   * The outage phase's fault: a clean and then a hard rolling restart, node by node, each node
   * restored and answered by a normal send before the next goes down. It stops when `signal` aborts.
   * @private
   * @param {AbortSignal} signal
   * @returns {Promise<void>}
   * @throws {Error} a failed step, after its node was restored
   */
  async #roll(signal) {
    for (const hard of [false, true]) {
      for (const service of CHURN_SERVICES) {
        if (signal.aborted) return
        let outages
        try {
          outages = await this.bouncer.rollingBounce({
            hard,
            admin: this.admin,
            topics: [this.topic],
            services: [service],
            signal,
          })
        } catch (error) {
          if (signal.aborted && error === signal.reason) return
          throw error
        }
        for (const { kind, outageMs } of outages) {
          this.phase.outages.push({ service, kind: OUTAGE_KINDS[kind] ?? kind, outageMs })
          const recovery = await this.#recover(`${service} ${kind}`)
          if (recovery) this.phase.recoveries.push(recovery)
        }
      }
    }
  }

  /**
   * Keeps the reader polling until `signal` aborts.
   * @private
   * @param {AbortSignal} signal
   * @returns {Promise<void>}
   * @throws {Error} a reader error that is not retriable
   */
  async #follow(signal) {
    while (!signal.aborted) {
      if ((await this.reader.poll()) === 0) await this.pause(this.settings.pollMs, signal)
    }
  }

  /**
   * Sends normal sends until one is acknowledged: the proof that the producer works again after a
   * restoration or after the pressure is off.
   * @private
   * @param {string} label
   * @returns {Promise<{ label: string, attempts: number }|null>} null when none was acknowledged
   *   within `recoveryMs`
   */
  async #recover(label) {
    const deadline = this.clock.now() + this.settings.recoveryMs
    for (let attempts = 1; ; attempts++) {
      const sent = this.#offer(this.settings.steadyBytes)
      if (sent !== null && (await sent) === STATUS.ACK) return { label, attempts }
      if (sent === null || this.clock.now() >= deadline) return null
      await this.pause(this.settings.recoveryRetryMs)
    }
  }

  /**
   * Makes one public send and settles its id with the outcome.
   * @private
   * @param {number} bytes the size of the value
   * @returns {Promise<number>|null} the status it ended with; null when the ledger is full
   */
  #offer(bytes) {
    const id = this.ledger.offer(this.clock.now())
    if (id < 0) {
      this.capacityReached = true
      return null
    }
    this.unsettled += 1
    let call
    try {
      call = this.producer.send({
        topic: this.topic,
        messages: [{ value: encodeValue(bytes, id, this.tag) }],
        acks: -1,
      })
    } catch (error) {
      call = Promise.reject(error)
    }
    return call.then(
      (metadata) => this.#acknowledged(id, metadata),
      (error) => this.#refused(id, error)
    )
  }

  /**
   * @private
   * @param {number} id
   * @param {Array<{ partition: number, baseOffset: string }>} [metadata]
   * @returns {number}
   */
  #acknowledged(id, metadata) {
    const [landed] = metadata ?? []
    this.ledger.settle(
      id,
      STATUS.ACK,
      landed && { partition: landed.partition, offset: Number(landed.baseOffset) }
    )
    this.unsettled -= 1
    return STATUS.ACK
  }

  /**
   * Classifies a rejected send: refused by the full buffer, or annotated `unsent` or `unknown` by
   * the producer; anything else is unexpected and fails the run.
   * @private
   * @param {number} id
   * @param {Error} error
   * @returns {number}
   */
  #refused(id, error) {
    let status = STATUS.UNEXPECTED
    if (error instanceof KafkaJSBufferExhaustedError) {
      status = STATUS.UNSENT
      this.phase.refusals[error.reason] = (this.phase.refusals[error.reason] ?? 0) + 1
    } else if (error?.outcome === 'unsent') {
      status = STATUS.UNSENT
    } else if (error?.outcome === 'unknown') {
      status = STATUS.UNKNOWN
    } else {
      this.firstUnexpected ??= `${error?.name}: ${error?.message}`
    }
    this.ledger.settle(id, status)
    this.unsettled -= 1
    return status
  }

  /**
   * Takes a record the live reader read.
   * @private
   * @param {number} partition
   * @param {object} message a decoded record
   * @returns {void}
   */
  #observe(partition, message) {
    const id = decodeId(message.value, this.tag)
    const now = this.clock.now()
    this.journal.append(
      id,
      partition,
      Number(message.batchContext.producerId),
      message.batchContext.producerEpoch,
      sequenceOf(message),
      Number(message.offset),
      now - this.startedAt
    )
    this.ledger.observed(id, now)
  }

  /**
   * @private
   * @param {() => boolean} done
   * @param {number} budgetMs
   * @returns {Promise<boolean>} whether `done` held within the budget
   */
  async #waitFor(done, budgetMs) {
    const deadline = this.clock.now() + budgetMs
    while (!done()) {
      if (this.clock.now() >= deadline) return false
      await this.pause(this.settings.pollMs)
    }
    return true
  }

  /**
   * Takes the delay off and disconnects the producer, the admin and the reader, trying every one,
   * then reads what the producer still retains.
   * @private
   * @returns {Promise<Error[]>} what failed
   */
  async #disconnect() {
    const failures = []
    const attempt = async (step) => {
      try {
        await step()
      } catch (error) {
        failures.push(error)
      }
    }
    await attempt(() => this.faults.clear())
    if (this.connected) {
      this.connected = false
      await attempt(() => this.producer.disconnect())
    }
    if (this.admin) {
      const { admin } = this
      this.admin = null
      await attempt(() => admin.disconnect())
    }
    await attempt(() => this.reader.close())
    if (this.phase && this.captured && this.phase.afterDisconnect === null) {
      try {
        this.phase.afterDisconnect = this.admissionProbe.retained()
      } catch (error) {
        failures.push(error)
      }
    }
    return failures
  }

  /**
   * @private
   * @param {string[]} violations
   * @param {Map<number, string>|null} ends
   * @returns {Promise<object|null>} the merge of the quiet log, or null when it could not be read
   */
  async #readQuiet(ends, violations) {
    try {
      const log = await this.rawLog.readLog({ brokers: this.brokers, topic: this.topic })
      this.observed.reset()
      for (const [partition, { endOffset, records }] of log) {
        if (ends && ends.has(partition) && String(endOffset) !== String(ends.get(partition))) {
          violations.push(
            `durability: partition ${partition} ends at ${endOffset} in the quiet read, ${ends.get(partition)} when frozen`
          )
        }
        for (const message of records) {
          if (message.batchContext.isControlBatch) continue
          this.observed.add(
            decodeId(message.value, this.tag),
            partition,
            Number(message.batchContext.producerId),
            message.batchContext.producerEpoch,
            sequenceOf(message)
          )
        }
      }
      const merged = mergeDurability(this.ledger, this.observed, 'quiet read')
      violations.push(...merged.violations)
      return merged
    } catch (error) {
      violations.push(`durability: the quiet read failed: ${error.message}`)
      return null
    }
  }

  /**
   * Judges every phase's controls, its closing probe and the latency samples.
   * @private
   * @param {string[]} violations receives each failed control
   * @param {ReturnType<OfferLedger['latency']>} latency
   * @returns {object} what each phase measured, for the evidence
   */
  #controls(violations, latency) {
    const { rate, targetFraction, requiredKinds, minLatencySamples } = this.settings
    const phases = this.phaseResults
    for (const { name } of this.phases) {
      const phase = phases[name]
      if (!phase) {
        violations.push(`${name}: the phase did not finish`)
        continue
      }
      if (phase.unexpected > 0) {
        violations.push(
          `sends: ${phase.unexpected} rejections in ${name} carry no outcome, the first ${this.firstUnexpected}`
        )
      }
      if (phase.probeError || !phase.probe) {
        violations.push(`admission probe (${name}): ${phase.probeError ?? 'it did not run'}`)
      } else {
        try {
          assertProbe(phase.probe)
        } catch (error) {
          violations.push(`admission probe (${name}): ${error.message.split('\n')[0].trim()}`)
        }
      }
      const retained = phase.afterDisconnect
      for (const field of ['pins', 'registrations', 'settlePromises']) {
        if (retained && retained[field] !== 0) {
          violations.push(
            `after disconnect (${name}): the producer still holds ${retained[field]} ${field}`
          )
        }
      }
    }
    const steady = phases.steady
    if (steady) {
      if (steady.offeredPerSecond < rate * targetFraction || steady.backpressure > 0) {
        violations.push(
          `steady: offered ${Math.round(steady.offeredPerSecond)} a second, target ${rate}, ` +
            `${steady.backpressure} ticks at the unsettled bound, slip up to ${Math.round(steady.slip.maxMs)} ms`
        )
      }
    }
    const outage = phases.outage
    if (outage) {
      const kinds = new Set(outage.outages.map(({ kind }) => kind))
      if (outage.outages.length === 0) {
        violations.push('outage: no broker outage was observed')
      } else if (outage.recoveries.length !== outage.outages.length) {
        violations.push(
          `outage: ${outage.outages.length} restorations, ${outage.recoveries.length} followed by an acknowledged send`
        )
      }
      for (const kind of requiredKinds) {
        if (!kinds.has(kind)) violations.push(`outage: no ${kind} outage was observed`)
      }
    }
    const pressure = phases['buffer-pressure']
    if (pressure) {
      const refused = (pressure.refusals.timeout ?? 0) + (pressure.refusals['queue-full'] ?? 0)
      if (refused === 0) {
        violations.push(
          'buffer-pressure: the full buffer refused no send with KafkaJSBufferExhaustedError'
        )
      }
      if (!pressure.pressureRecovery) {
        violations.push(
          'buffer-pressure: no normal send was acknowledged after the delay was cleared'
        )
      }
    }
    if (this.capacityReached)
      violations.push('sends: the offered-id table filled before the run ended')
    if (this.ledger.counts.pending > 0 || this.unsettled > 0) {
      violations.push(
        `sends: ${this.ledger.counts.pending} offered ids and ${this.unsettled} calls are unsettled`
      )
    }
    if (latency.samples < minLatencySamples || latency.invalid > 0) {
      violations.push(
        `latency: ${latency.samples} samples, ${minLatencySamples} are needed, and ${latency.invalid} are negative`
      )
    }
    return Object.fromEntries(
      Object.entries(phases).map(([name, { probe, ...rest }]) => [
        name,
        { ...rest, before: undefined },
      ])
    )
  }

  /**
   * Keeps the quiet points of `Soak`'s lines, then passes each line on; the resource verdict is
   * renamed, since the run's one verdict is this object's.
   * @private
   * @param {object} line
   * @returns {void}
   */
  #recordSoak(line) {
    if (line.snapshot?.named && ['baseline', 'phase-end', 'teardown'].includes(line.kind)) {
      const when =
        { baseline: 'baseline', teardown: 'after disconnect' }[line.kind] ?? `after ${line.phase}`
      this.quietPoints.push({
        when,
        named: line.snapshot.named,
        unhandled: line.snapshot.unhandled,
      })
    }
    this.record(line.kind === 'verdict' ? { ...line, kind: 'resource-verdict' } : line)
  }

  /**
   * Holds each quiet point to the baseline listener by listener, and to no unhandled rejection.
   * @private
   * @returns {string[]}
   */
  #quietPointViolations() {
    const [baseline, ...points] = this.quietPoints
    const expected = this.phases.length + 2
    if (!baseline || points.length !== expected - 1) {
      return [
        `listeners: ${this.quietPoints.length} of ${expected} quiet points carried a listener reading`,
      ]
    }
    const violations = []
    for (const { when, named, unhandled } of points) {
      for (const group of new Set([...Object.keys(baseline.named), ...Object.keys(named)])) {
        const before = baseline.named[group] ?? {}
        const now = named[group] ?? {}
        for (const event of new Set([...Object.keys(before), ...Object.keys(now)])) {
          if ((before[event] ?? 0) !== (now[event] ?? 0)) {
            violations.push(
              `listeners ${when}: ${now[event] ?? 0} on ${group} '${event}', ${before[event] ?? 0} at the start`
            )
          }
        }
      }
      if (unhandled !== 0) violations.push(`unhandled rejections ${when}: ${unhandled}`)
    }
    return violations
  }
}

/**
 * @returns {ObservationJournal} a journal in the evidence directory, next to the evidence file
 */
const openJournal = () => {
  const file = evidencePath('producer-churn').replace(/\.jsonl$/, '.journal')
  fs.mkdirSync(path.dirname(file), { recursive: true })
  return new ObservationJournal({ file })
}

/**
 * One soak run: the phases of a profile, the readings, and the verdict.
 */
class Soak {
  /**
   * @param {object} options
   * @param {object} options.kafka the client the workloads use
   * @param {ReturnType<typeof createResourceProbe>} options.probe
   * @param {Array<{ name: string, workload: keyof typeof WORKLOADS, ms: number }>} options.phases
   * @param {WorkloadContext} options.context handed to every workload
   * @param {number} [options.sampleMs=SAMPLE_MS]
   * @param {number} [options.warmupMs=WARMUP_MS] how long the heap series is left to settle; for a
   *   `judge` that reads a phase on its own, how long into each phase
   * @param {(run: object) => { ok: boolean, violations: string[] }} [options.judge=evaluate] holds
   *   the readings to the bounds: it is given `baseline`, `phaseEnds`, `samples`, `teardown` and
   *   `warmupMs`, and each sample names its phase and how long into it it was taken
   * @param {number} [options.settleMs=SETTLE_MS] how long a quiet point waits for closed sockets to go
   * @param {(record: object) => void} [options.record=writeEvidence('soak', ...)] where every line goes
   * @param {Record<string, Function>} [options.workloads=WORKLOADS]
   * @param {() => Promise<void>} [options.beforeTeardown] runs once every phase has ended, before
   *   the sockets are awaited and the teardown reading is taken
   */
  constructor({
    kafka,
    probe,
    phases,
    context,
    sampleMs = SAMPLE_MS,
    warmupMs = WARMUP_MS,
    judge = evaluate,
    settleMs = SETTLE_MS,
    record = (line) => writeEvidence('soak', line),
    workloads = WORKLOADS,
    beforeTeardown = async () => {},
  }) {
    Object.assign(this, {
      kafka,
      probe,
      phases,
      context,
      sampleMs,
      warmupMs,
      judge,
      settleMs,
      record,
      workloads,
      beforeTeardown,
    })
    this.samples = []
    this.phaseEnds = []
    this.sampling = false
    this.wake = () => {}
  }

  /**
   * Runs every phase, reads the quiet points and judges the run. The baseline is read once the
   * sockets the setup closed are gone, so it counts only what the clients will leave behind.
   * @returns {Promise<ReturnType<typeof evaluate>>} the verdict of `judge`; the lines were
   *   recorded as they were taken
   * @throws {Error} the failure of a workload, after its sampler is stopped
   */
  async run() {
    const startedAt = Date.now()
    await this.settle()
    const baseline = await this.probe.snapshot()
    this.record({ kind: 'baseline', snapshot: baseline })

    for (const { name, workload, ms } of this.phases) {
      await this.runPhase({ name, workload, ms }, startedAt)
    }

    await this.beforeTeardown()
    await this.settle()
    const teardown = await this.probe.snapshot()
    this.record({ kind: 'teardown', snapshot: teardown })

    const verdict = this.judge({
      baseline,
      phaseEnds: this.phaseEnds,
      samples: this.samples,
      teardown,
      warmupMs: this.warmupMs,
    })
    this.record({ kind: 'verdict', ...verdict })
    return verdict
  }

  /**
   * Runs one workload with the sampler beside it, then takes the quiet reading.
   * @private
   * @param {{ name: string, workload: string, ms: number }} phase
   * @param {number} startedAt when the run began, for the sample times
   * @returns {Promise<void>}
   * @throws {Error} the failure of the workload, or a failed sample or evidence record, after the
   *   phase's workload has ended and its clients are disconnected
   */
  async runPhase({ name, workload, ms }, startedAt) {
    const phaseStartedAt = Date.now()
    this.sampling = true
    const sampler = (async () => {
      while (this.sampling) {
        await this.nap()
        if (this.sampling) {
          const snapshot = await this.probe.snapshot()
          const now = Date.now()
          const sample = {
            phase: name,
            atMs: now - startedAt,
            phaseMs: now - phaseStartedAt,
            snapshot,
          }
          this.samples.push(sample)
          this.record({ kind: 'sample', ...sample })
        }
      }
    })()
    sampler.catch(() => {})

    try {
      await this.workloads[workload](this.context, ms)
    } finally {
      this.sampling = false
      this.wake()
      await sampler
    }

    await this.settle()
    const snapshot = await this.probe.snapshot()
    this.phaseEnds.push({ phase: name, snapshot })
    this.record({ kind: 'phase-end', phase: name, snapshot })
  }

  /**
   * Waits one sample interval, or until `wake()` ends the wait, and leaves no timer behind.
   * @private
   * @returns {Promise<void>}
   */
  nap() {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, this.sampleMs)
      this.wake = () => {
        clearTimeout(timer)
        resolve()
      }
    })
  }

  /**
   * Waits for the sockets the clients closed to be gone.
   * @private
   * @returns {Promise<void>}
   */
  async settle() {
    const deadline = Date.now() + this.settleMs
    while (Date.now() < deadline && (await this.probe.snapshot()).sockets.open > 0) {
      await sleep(100)
    }
  }
}

/**
 * @param {string} profile a key of `PROFILES`
 * @param {number|null} phaseSeconds
 * @returns {Array<{ name: string, workload: string, ms: number }>} the profile's phases, every one
 *   shortened to `phaseSeconds` for a smoke run
 */
const phasesOf = (profile, phaseSeconds) =>
  PROFILES[profile].map((phase) =>
    phaseSeconds === null ? phase : { ...phase, ms: phaseSeconds * 1000 }
  )

/**
 * @param {ReturnType<typeof createChaosSocketFactory>} chaos
 * @returns {Function} a socket factory that makes chaos sockets and first forgets the destroyed
 *   ones, so the list of sockets a long outage reconnects through does not grow
 */
const forgettingDestroyed = (chaos) => (args) => {
  chaos.sockets = chaos.sockets.filter(({ destroyed }) => !destroyed)
  return chaos(args)
}

/**
 * Builds the producer-churn owner for a real cluster: the counted client over a chaos socket
 * factory, the reader on the same counted factory, the brokers' bouncer and the closing probe. Every
 * evidence line goes to `.tmp/e2e/producer-churn.jsonl`.
 * @param {object} options
 * @param {string[]} options.brokers
 * @param {number|null} options.phaseSeconds a smoke run's phase length; its sampling is then
 *   shortened and the latency and outage-kind requirements of a full run are not asked
 * @param {Array<{ name: string, workload: string, ms: number }>} options.phases
 * @returns {ProducerChurn}
 */
const buildProducerChurn = ({ brokers, phaseSeconds, phases }) => {
  const { BrokerBouncer } = require('../../testHelpers/leaderElection')
  const smoke = phaseSeconds !== null
  const topic = `soak-churn-${Date.now()}`
  const chaos = createChaosSocketFactory(defaultSocketFactory())
  const { kafka, probe, socketFactory, listeners } = instrument({
    clientId: 'soak',
    brokers,
    logLevel: logLevel.NOTHING,
    socketFactory: forgettingDestroyed(chaos),
  })
  const record = (line) => writeEvidence('producer-churn', line)
  const reader = new RawProducerReader({ brokers, topic, record, socketFactory })

  return new ProducerChurn({
    kafka,
    probe,
    brokers,
    topic,
    record,
    phases,
    listeners,
    reader,
    bouncer: new BrokerBouncer(),
    faults: new WriteDelay({ chaos, reader }),
    admissionProbe: new ClosingProbe({
      brokers,
      clientId: 'soak',
      topic: `${topic}-probe`,
      capacity: CHURN.producer.bufferMemory,
      maxRequestSize: CHURN.producer.maxRequestSize,
      budgetMs: CHURN.producer.deliveryTimeout,
    }),
    soak: smoke ? { sampleMs: Math.max(1000, (phaseSeconds * 1000) / 6), warmupMs: 0 } : {},
    settings: smoke ? { minLatencySamples: 1, minHeapSamples: 2, requiredKinds: [] } : {},
  })
}

/**
 * Builds the owner of the consumer-churn profile: a public client whose sockets the probe counts,
 * the journal on disk and the four phase workloads. Nothing here imports the client's sources or
 * reads its private members.
 * @param {object} options
 * @param {string[]} options.brokers
 * @param {number|null} options.phaseSeconds a smoke run's phase length; such a run is never acceptance
 * @returns {import('./consumerChurnRun').ConsumerChurnRun}
 */
const buildConsumerChurn = ({ brokers, phaseSeconds }) => {
  const {
    ChurnJournal,
    GapSampler,
    PublicConsumerProbe,
    ChurnClientLog,
    consumerChurnPlan,
    publicSocketFactory,
  } = require('./consumerChurn')
  const { ChurnWorkloads, ConsumerChurnRun } = require('./consumerChurnRun')
  const v8 = require('node:v8')
  const heap = async () => {
    globalThis.gc()
    await sleep(100)
    globalThis.gc()
    return v8.getHeapStatistics().used_heap_size
  }
  const probe = new PublicConsumerProbe({ heap })
  const file = evidencePath('consumer-churn').replace(/\.jsonl$/, '.journal')
  fs.mkdirSync(path.dirname(file), { recursive: true })
  const clientLog = new ChurnClientLog({
    file: evidencePath('consumer-churn').replace(/\.jsonl$/, '.clientlog'),
  })
  const kafka = new Kafka({
    clientId: 'soak-consumer-churn',
    brokers,
    logLevel: logLevel.WARN,
    logCreator: clientLog.creator(),
    socketFactory: probe.wrap(publicSocketFactory()),
  })
  const workloads = new ChurnWorkloads({
    kafka,
    probe,
    tag: `cc${Date.now().toString(36)}`,
    clientLog,
  })
  return new ConsumerChurnRun({
    workloads,
    scope: workloads.groups,
    probe,
    journal: new ChurnJournal({ file }),
    clientLog,
    record: (line) => writeEvidence('consumer-churn', line),
    plan: consumerChurnPlan(phaseSeconds),
    clock: REAL_CLOCK,
    sleep,
    gaps: new GapSampler(phaseSeconds === null ? {} : { minimum: 1 }),
    heap,
  })
}

/**
 * Runs a profile against the cluster `brokers` names.
 * @param {string} profile a key of `PROFILES`
 * @param {object} [options]
 * @param {string[]} [options.brokers=['localhost:9092', 'localhost:9095', 'localhost:9098']]
 * @param {number|null} [options.phaseSeconds=null] the length of every phase, for a smoke run:
 *   samples are then taken six to a phase and the whole series counts, so the verdict is of the
 *   driver and not of memory
 * @param {(options: object) => { run: () => Promise<{ ok: boolean }>, close: () => Promise<void> }} [options.churn=buildProducerChurn]
 *   builds the owner of the `producer-churn` profile; a spec supplies its own
 * @param {(options: object) => { run: () => Promise<{ ok: boolean }>, close: () => Promise<void> }} [options.consumerChurn=buildConsumerChurn]
 *   builds the owner of the `consumer-churn` profile
 * @returns {Promise<{ ok: boolean }>} the verdict: `evaluate`'s for a consumer profile, the
 *   combined verdict of `ProducerChurn` for `producer-churn`
 * @throws {Error} unless the process runs with `--expose-gc`
 */
const main = async (
  profile,
  {
    brokers = ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    phaseSeconds = null,
    churn = buildProducerChurn,
    consumerChurn = buildConsumerChurn,
  } = {}
) => {
  if (typeof globalThis.gc !== 'function') {
    throw new Error('The soak reads the heap after a collection: run node with --expose-gc')
  }

  const phases = phasesOf(profile, phaseSeconds)
  if (profile === 'producer-churn') {
    const owner = churn({ brokers, phaseSeconds, phases })
    try {
      return await owner.run()
    } finally {
      await owner.close()
    }
  }

  if (profile === 'consumer-churn') {
    const owner = consumerChurn({ brokers, phaseSeconds, phases })
    try {
      return await owner.run()
    } finally {
      await owner.close()
    }
  }

  const { kafka, probe } = instrument({ clientId: 'soak', brokers, logLevel: logLevel.NOTHING })
  const stamp = Date.now()
  const topic = `soak-${stamp}`
  await createTopic(kafka, topic)

  const smoke = phaseSeconds !== null
  const soak = new Soak({
    kafka,
    probe,
    phases,
    context: { kafka, topic, groupId: `${topic}-group` },
    ...(smoke && { sampleMs: Math.max(1000, (phaseSeconds * 1000) / 6), warmupMs: 0 }),
  })
  return soak.run()
}

if (require.main === module) {
  const { profile, phaseSeconds } = parseArgs(process.argv.slice(2))
  main(profile, { phaseSeconds }).then(
    (verdict) => {
      console.log(JSON.stringify(verdict))
      process.exitCode = verdict.ok ? 0 : 1
    },
    (error) => {
      console.error(error)
      process.exitCode = 1
    }
  )
}

module.exports = {
  CHURN,
  ConsumerChurnOracle,
  buildConsumerChurn,
  buildProducerChurn,
  forgettingDestroyed,
  PROFILES,
  WORKLOADS,
  ProducerChurn,
  RawProducerReader,
  Soak,
  WriteDelay,
  createTopic,
  parseArgs,
  privateOf,
  instrument,
  main,
}
