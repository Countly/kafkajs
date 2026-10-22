#!/usr/bin/env node
'use strict'

/**
 * Soak driver: runs a client for a stretch of time against a cluster, reads what it holds on to
 * every 30 s, and checks the readings against the bounds of `bounds.js`.
 *
 * Usage: node --expose-gc scripts/soak/soak.js --profile <idle|member-churn|regex-topic-churn|outage|first-run>
 *          [--phase-seconds <n>]
 *   (= npm run test:kafka -- "node --expose-gc scripts/soak/soak.js --profile first-run" for the
 *   15-minute run, `npm run soak:first-run`)
 * Writes one JSON line per sample and per quiet point to `.tmp/e2e/soak.jsonl` through
 * `writeEvidence`, prints the verdict, and sets the exit code: 0 when every bound held, 1 when one
 * broke or the run failed. It ends by disconnecting what it built and does not call
 * `process.exit`, so a run that leaves a handle behind does not end, which is also a finding.
 *
 * Profiles, over finite populations: one topic of 6 partitions and two member slots. A profile of
 * one phase runs 7 minutes, which leaves six readings after the warm-up.
 *   idle              one member polling an idle topic, a message a second
 *   member-churn      two members, one of them replaced by a new consumer every 15 s, rebalancing
 *                     cooperatively (`cooperativeSticky`)
 *   regex-topic-churn a regex subscriber while 50 distinct topic names are created and deleted
 *   outage            one member while a broker is stopped for a third of the phase and restarted
 *   first-run         5 minutes of idle, of member churn and of outage, the 15-minute screen
 * Every phase ends by disconnecting its clients, also when it fails; the readings that must be zero
 * are taken there.
 * `--phase-seconds n` shortens every phase to n seconds: a smoke run of the driver, not of memory.
 *
 * Development-only. The first run is a short-run screen, never proof of bounded long-run memory.
 * The gauges read internals: the clusters a `Kafka` builds, their request queues, and the
 * operation ledgers and reset obligations of its `operations` store.
 */
const { Kafka, logLevel, PartitionAssigners } = require('../../index')
const defaultSocketFactory = require('../../src/network/socketFactory')
const { Clients } = require('../clients')
const { createResourceProbe } = require('../../testHelpers/resourceProbe')
const { writeEvidence } = require('../../testHelpers/e2eEvidence')
const { SAMPLE_MS, WARMUP_MS, evaluate } = require('./bounds')

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

/** The phases of each profile: how long each runs and which workload it is. */
const PROFILES = {
  idle: [{ name: 'idle', workload: 'idle', ms: STANDALONE_MS }],
  'member-churn': [{ name: 'member-churn', workload: 'memberChurn', ms: STANDALONE_MS }],
  'regex-topic-churn': [
    { name: 'regex-topic-churn', workload: 'regexTopicChurn', ms: STANDALONE_MS },
  ],
  outage: [{ name: 'outage', workload: 'outage', ms: STANDALONE_MS }],
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

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
 * @returns {{ kafka: object, probe: ReturnType<typeof createResourceProbe> }}
 */
const instrument = (
  { socketFactory = defaultSocketFactory(), ...options },
  { activeResources = true } = {}
) => {
  const open = new Set()
  let created = 0
  const clusters = new Set()
  const kafka = new Kafka({
    ...options,
    socketFactory: (args) => {
      const socket = socketFactory(args)
      created += 1
      open.add(socket)
      return socket
    },
  })

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

  return { kafka, probe }
}

/**
 * What a workload is handed.
 * @typedef {object} WorkloadContext
 * @property {object} kafka the `Kafka` the clients are built from
 * @property {string} topic the topic of the run
 * @property {string} groupId the consumer group of the run
 */

/**
 * The workloads. Each runs for `ms` milliseconds and resolves once every client it built is
 * disconnected. When it fails, it rejects with that failure after the same disconnects; a failed
 * disconnect never hides the failure of the workload, and does not stop the others.
 */
const WORKLOADS = {
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
   * @param {number} [options.warmupMs=WARMUP_MS]
   * @param {number} [options.settleMs=SETTLE_MS] how long a quiet point waits for closed sockets to go
   * @param {(record: object) => void} [options.record=writeEvidence('soak', ...)] where every line goes
   * @param {Record<string, Function>} [options.workloads=WORKLOADS]
   */
  constructor({
    kafka,
    probe,
    phases,
    context,
    sampleMs = SAMPLE_MS,
    warmupMs = WARMUP_MS,
    settleMs = SETTLE_MS,
    record = (line) => writeEvidence('soak', line),
    workloads = WORKLOADS,
  }) {
    Object.assign(this, {
      kafka,
      probe,
      phases,
      context,
      sampleMs,
      warmupMs,
      settleMs,
      record,
      workloads,
    })
    this.samples = []
    this.phaseEnds = []
    this.sampling = false
    this.wake = () => {}
  }

  /**
   * Runs every phase, reads the quiet points and judges the run. The baseline is read once the
   * sockets the setup closed are gone, so it counts only what the clients will leave behind.
   * @returns {Promise<ReturnType<typeof evaluate>>} the verdict; the lines were recorded as they
   *   were taken
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

    await this.settle()
    const teardown = await this.probe.snapshot()
    this.record({ kind: 'teardown', snapshot: teardown })

    const verdict = evaluate({
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
    this.sampling = true
    const sampler = (async () => {
      while (this.sampling) {
        await this.nap()
        if (this.sampling) {
          const snapshot = await this.probe.snapshot()
          const sample = { atMs: Date.now() - startedAt, snapshot }
          this.samples.push(sample)
          this.record({ kind: 'sample', phase: name, ...sample })
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
 * Runs a profile against the cluster `brokers` names.
 * @param {string} profile a key of `PROFILES`
 * @param {object} [options]
 * @param {string[]} [options.brokers=['localhost:9092', 'localhost:9095', 'localhost:9098']]
 * @param {number|null} [options.phaseSeconds=null] the length of every phase, for a smoke run:
 *   samples are then taken six to a phase and the whole series counts, so the verdict is of the
 *   driver and not of memory
 * @returns {Promise<ReturnType<typeof evaluate>>}
 * @throws {Error} unless the process runs with `--expose-gc`
 */
const main = async (
  profile,
  { brokers = ['localhost:9092', 'localhost:9095', 'localhost:9098'], phaseSeconds = null } = {}
) => {
  if (typeof globalThis.gc !== 'function') {
    throw new Error('The soak reads the heap after a collection: run node with --expose-gc')
  }

  const { kafka, probe } = instrument({ clientId: 'soak', brokers, logLevel: logLevel.NOTHING })
  const stamp = Date.now()
  const topic = `soak-${stamp}`
  await createTopic(kafka, topic)

  const smoke = phaseSeconds !== null
  const soak = new Soak({
    kafka,
    probe,
    phases: PROFILES[profile].map((phase) =>
      smoke ? { ...phase, ms: phaseSeconds * 1000 } : phase
    ),
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
  PROFILES,
  WORKLOADS,
  Soak,
  createTopic,
  parseArgs,
  privateOf,
  instrument,
  main,
}
