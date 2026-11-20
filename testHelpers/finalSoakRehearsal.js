'use strict'
/**
 * Broker-free rehearsal preload for the final soak preflight and driver, loaded with
 * `NODE_OPTIONS=--require <this file>` so every process the driver spawns loads it too.
 *
 * It replaces the package's `Kafka` with a subclass that builds every real client (so the real
 * option validation and construction run) and swaps only the client's network calls for in-memory
 * answers that check the arguments the soak passes. The private cluster the preflight reads
 * ApiVersions ranges through stays the product's own, served by a three-node `ScriptedCluster` on
 * the in-process socket seam, as is the raw reader's own broker connection. Any real dial or name
 * lookup throws, so the rehearsal cannot pass or fail by the host's network. With
 * `FINAL_SOAK_REHEARSAL_WARP=<k>` the harness clock (`MonotonicClock`) and the harness's own timers
 * run k times faster from the shared `FINAL_SOAK_REHEARSAL_ANCHOR`, so a smoke schedule runs in a
 * fraction of its length with every process on one clock; the clients' own timeouts stay on real
 * time.
 *
 * Environment: `KAFKA_BROKERS` (three `host:port`, the scripted nodes 1–3);
 * `TELEMETRY_EVIDENCE_DIR` and `FINAL_SOAK_REHEARSAL_PUSH=1` make a telemetry-enabled client's
 * connect append one decoded push line for node 1, as the broker reporter would. The consumer
 * worker's assignment read is replaced, since nothing joins a group.
 */
const crypto = require('node:crypto')
const dns = require('node:dns')
const fs = require('node:fs')
const net = require('node:net')
const path = require('node:path')

// Nothing here may reach a real network: a dial or a name lookup fails loudly instead of passing or
// failing by whatever the host's resolver and ports happen to do.
const offline = (what) => {
  throw new Error(`rehearsal: real network use (${what})`)
}
net.Socket.prototype.connect = function (...args) {
  // `net.connect` hands over its normalized `[options, callback]` pair as the first argument.
  const target = Array.isArray(args[0]) ? args[0][0] : args[0]
  offline(`connect ${target?.host ?? target?.path ?? target}:${target?.port ?? args[1] ?? ''}`)
}
dns.lookup = (host) => offline(`lookup ${host}`)
dns.promises.lookup = async (host) => offline(`lookup ${host}`)

// The scripted cluster's fixtures name the helpers by the bare id Jest maps; resolve it the same way.
const Module = require('node:module')
const resolve = Module._resolveFilename
Module._resolveFilename = function (request, ...rest) {
  return resolve.call(
    this,
    request === 'testHelpers' ? path.join(__dirname, 'index.js') : request,
    ...rest
  )
}
const kafkajs = require('../index')
const CONFIG_RESOURCE_TYPES = require('../src/protocol/configResourceTypes')
const CONFIG_SOURCE = require('../src/protocol/configSource')
const { createScriptedCluster } = require('./scriptedCluster')
Module._resolveFilename = resolve

const RealKafka = kafkajs.Kafka
const NODES = (process.env.KAFKA_BROKERS ?? 'kafka1:9092,kafka2:9092,kafka3:9092')
  .split(',')
  .map((address, i) => {
    const at = address.lastIndexOf(':')
    return { id: i + 1, host: address.slice(0, at), port: Number(address.slice(at + 1)) }
  })
const FEATURES = {
  supportedFeatures: [{ name: 'share.version', minVersion: 0, maxVersion: 2 }],
  finalizedFeatures: [{ name: 'share.version', minVersionLevel: 1, maxVersionLevel: 1 }],
  finalizedFeaturesEpoch: '5',
}

/** @returns {object} a cluster answering the bootstrap and cluster-admin APIs with share.version */
const scripted = () => {
  const wire = createScriptedCluster({ nodes: NODES, profile: 'clusterAdmin' })
  wire.answer('ApiVersions', () => ({ ...wire.apiVersionsAnswer(), ...FEATURES }))
  return wire
}

/** @param {boolean} ok @param {string} what @throws {TypeError} when the argument is malformed */
const check = (ok, what) => {
  if (!ok) throw new TypeError(`rehearsal: malformed ${what}`)
}
const isTopics = (topics) =>
  Array.isArray(topics) &&
  topics.length > 0 &&
  topics.every((t) => typeof t === 'string' || t instanceof RegExp)
const isPartitions = (list) =>
  Array.isArray(list) &&
  list.every((e) => typeof e.topic === 'string' && Array.isArray(e.partitions ?? []))

/** Per-process topic table: name → { partitions, id }, a new id for each creation. */
const topics = new Map()
const idOf = (name) =>
  topics.get(name)?.id ??
  crypto.createHash('sha256').update(name).digest().subarray(0, 16).toString('base64url')

/** @param {string} name @returns {object} the metadata the admin answers for one topic */
const metadataOf = (name) => {
  const count = topics.get(name)?.partitions ?? (/-(main|txn)$/.test(name) ? 1 : 2)
  return {
    name,
    topicId: idOf(name),
    partitions: Array.from({ length: count }, (_, partitionId) => ({
      partitionErrorCode: 0,
      partitionId,
      leader: 1,
      replicas: [1, 2, 3],
      isr: [1, 2, 3],
      offlineReplicas: [],
    })),
  }
}

let seq = 0
/** Appends one decoded push line for a telemetry-enabled client, as the node 1 reporter would. */
const push = (clientId) => {
  const dir = process.env.TELEMETRY_EVIDENCE_DIR
  if (process.env.FINAL_SOAK_REHEARSAL_PUSH !== '1' || !dir) return
  const lines = path.join(dir, 'exports-1.jsonl')
  const prior = fs.existsSync(lines) ? fs.readFileSync(lines, 'utf8').split('\n').length - 1 : 0
  seq = Math.max(seq, prior) + 1
  const line = {
    node: 1,
    seq,
    clientId,
    clientInstanceId: crypto.randomUUID(),
    parse: 'ok',
    truncated: false,
    metricsTotal: 4,
    terminating: false,
    dataSha256: crypto.createHash('sha256').update(clientId).digest('hex'),
  }
  fs.appendFileSync(lines, JSON.stringify(line) + '\n')
}

const done = async () => {}

/** @returns {object} admin network calls */
const adminCalls = () => {
  const configs = new Map()
  return {
    connect: done,
    disconnect: done,
    createTopics: async ({ topics: list, waitForLeaders }) => {
      check(Array.isArray(list) && typeof waitForLeaders === 'boolean', 'createTopics')
      for (const t of list) {
        check(
          typeof t.topic === 'string' &&
            Number.isInteger(t.numPartitions) &&
            Number.isInteger(t.replicationFactor) &&
            (t.configEntries ?? []).every(
              (e) => typeof e.name === 'string' && typeof e.value === 'string'
            ),
          'createTopics topic'
        )
        topics.set(t.topic, {
          partitions: t.numPartitions,
          id: crypto.randomBytes(16).toString('base64url'),
        })
      }
      return true
    },
    deleteTopics: async ({ topics: list }) => {
      check(Array.isArray(list) && list.every((t) => typeof t === 'string'), 'deleteTopics')
      for (const t of list) topics.delete(t)
    },
    listTopics: async () => [...topics.keys()],
    fetchTopicOffsets: async (topic) => {
      check(typeof topic === 'string', 'fetchTopicOffsets')
      return metadataOf(topic).partitions.map(({ partitionId }) => ({
        partition: partitionId,
        offset: '0',
        high: '0',
        low: '0',
      }))
    },
    fetchTopicMetadata: async ({ topics: list } = {}) => {
      check(Array.isArray(list) && list.every((t) => typeof t === 'string'), 'fetchTopicMetadata')
      return { topics: list.map(metadataOf) }
    },
    fetchOffsets: async ({ groupId, topics: list }) => {
      check(typeof groupId === 'string' && isTopics(list), 'fetchOffsets')
      return list.map((topic) => ({
        topic,
        partitions: metadataOf(topic).partitions.map(({ partitionId }) => ({
          partition: partitionId,
          offset: '-1',
          metadata: null,
        })),
      }))
    },
    describeCluster: async () => ({
      brokers: NODES.map(({ id, host, port }) => ({ nodeId: id, host, port })),
      controller: 1,
      clusterId: 'rehearsal',
    }),
    describeFeatures: async ({ brokerId } = {}) => {
      check(brokerId === undefined || Number.isInteger(brokerId), 'describeFeatures')
      const map = (list, fields) => Object.fromEntries(list.map((f) => [f.name, fields(f)]))
      return {
        supportedFeatures: map(FEATURES.supportedFeatures, ({ minVersion, maxVersion }) => ({
          minVersion,
          maxVersion,
        })),
        finalizedFeatures: map(FEATURES.finalizedFeatures, (f) => ({
          minVersionLevel: f.minVersionLevel,
          maxVersionLevel: f.maxVersionLevel,
        })),
        finalizedFeaturesEpoch: FEATURES.finalizedFeaturesEpoch,
      }
    },
    incrementalAlterConfigs: async ({ resources }) => {
      check(Array.isArray(resources), 'incrementalAlterConfigs')
      for (const r of resources) {
        check(Number.isInteger(r.type) && typeof r.name === 'string', 'config resource')
        for (const e of r.configEntries)
          configs.set(`${r.type}/${r.name}/${e.name}`, String(e.value))
      }
      return {
        resources: resources.map((r) => ({
          errorCode: 0,
          errorMessage: null,
          resourceType: r.type,
          resourceName: r.name,
        })),
      }
    },
    describeConfigs: async ({ resources }) => ({
      resources: resources.map((r) => ({
        resourceType: r.type,
        resourceName: r.name,
        errorCode: 0,
        configEntries: (r.configNames ?? []).map((configName) => ({
          configName,
          configValue: configs.get(`${r.type}/${r.name}/${configName}`) ?? null,
          configSource:
            r.type === CONFIG_RESOURCE_TYPES.GROUP ? CONFIG_SOURCE.GROUP_CONFIG : undefined,
        })),
      })),
    }),
    deleteGroups: async (groups) => {
      check(Array.isArray(groups), 'deleteGroups')
      return []
    },
  }
}

/** @returns {object} producer network calls; offsets per topic-partition */
const producerCalls = (clientId, options) => {
  const offsets = new Map()
  const send = async ({ topic, acks, messages }) => {
    check(
      typeof topic === 'string' && acks === -1 && Array.isArray(messages) && messages.length > 0,
      'send'
    )
    return messages.map((m) => {
      check(Buffer.isBuffer(m.value) || typeof m.value === 'string', 'message value')
      const key = `${topic}|${m.partition ?? 0}`
      const offset = offsets.get(key) ?? 0n
      offsets.set(key, offset + 1n)
      return {
        topicName: topic,
        partition: m.partition ?? 0,
        errorCode: 0,
        baseOffset: String(offset),
      }
    })
  }
  return {
    connect: async () => {
      if (options?.enableMetricsPush) push(clientId)
    },
    disconnect: done,
    send,
    transaction: async () => {
      check(typeof options?.transactionalId === 'string', 'transaction on a transactional producer')
      let open = true
      const closing = async () => {
        check(open, 'transaction already ended')
        open = false
      }
      return { send, commit: closing, abort: closing, isActive: () => open }
    },
  }
}

/** @returns {object} consumer network calls */
const consumerCalls = (clientId, options) => ({
  connect: async () => {
    if (options?.enableMetricsPush) push(clientId)
  },
  disconnect: done,
  subscribe: async ({ topics: list }) => check(isTopics(list), 'subscribe'),
  run: async ({ eachMessage } = {}) => check(typeof eachMessage === 'function', 'run'),
  stop: done,
  pause: (list) => check(isPartitions(list), 'pause'),
  resume: (list) => check(isPartitions(list), 'resume'),
  commitOffsets: async (list) =>
    check(
      Array.isArray(list) &&
        list.every(
          (e) =>
            typeof e.topic === 'string' &&
            Number.isInteger(e.partition) &&
            /^\d+$/.test(String(e.offset))
        ),
      'commitOffsets'
    ),
})

/** The package's `Kafka`, with real clients whose network calls answer in memory. */
class RehearsalKafka extends RealKafka {
  /** @param {object} options the soak's `Kafka` options */
  constructor(options) {
    super({ ...options, socketFactory: scripted().socketFactory })
    this.rehearsalClientId = options.clientId
  }

  /** @returns {object} the real admin with in-memory network calls */
  admin(options) {
    return { ...super.admin(options), ...adminCalls() }
  }

  /** @returns {object} the real producer with in-memory network calls */
  producer(options) {
    return { ...super.producer(options), ...producerCalls(this.rehearsalClientId, options) }
  }

  /** @returns {object} the real consumer with in-memory network calls */
  consumer(options) {
    return { ...super.consumer(options), ...consumerCalls(this.rehearsalClientId, options) }
  }

  /** @returns {object} a copy of the real (frozen) share consumer with in-memory network calls */
  shareConsumer(options) {
    return { ...super.shareConsumer(options), ...consumerCalls(this.rehearsalClientId, options) }
  }
}

kafkajs.Kafka = RehearsalKafka

// The in-memory clients never join a group, so no partition is ever assigned: every consumer
// member reads as holding the main topic's one partition, which is what the readiness check and
// the membership waves look for.
// Only the harness clock and the harness's own timers run faster: a client's connection and request
// bounds stay on real time, so a CPU stall cannot expire them on an in-memory network.
const { MonotonicClock } = require('../scripts/soak/finalSoakScheduler')
const warp = Number(process.env.FINAL_SOAK_REHEARSAL_WARP ?? 1)
if (warp > 1) {
  MonotonicClock.rate = warp
  MonotonicClock.anchor = BigInt(process.env.FINAL_SOAK_REHEARSAL_ANCHOR)
}
const {
  ConsumerWorker,
  ShareWorker,
  ReaderWorker,
  openRawBroker,
} = require('../scripts/soak/finalSoakWorkers')
ConsumerWorker.prototype.assignedKeys = function () {
  const { main } = this.runtime.config.topics
  return [{ topic: main, topicId: idOf(main), partition: 0 }]
}
// A regex or recreated topic is seen by a real group within its metadata refresh; here no group
// exists to see it, so these waits are answered at once instead of running to their deadline.
ConsumerWorker.prototype.onRegexChurn = async () => ({ accepted: true })
ConsumerWorker.prototype.onRetired = async () => ({ retired: true })
ConsumerWorker.prototype.onRecreated = async () => ({ assigned: true })
// No record is ever delivered here, so no member could show the recovery witness a real one gives.
ConsumerWorker.prototype.onRecovered = async () => ({ ok: true })
ShareWorker.prototype.onRecovered = async () => ({ ok: true })
// No share session exists here, so no fetch answer can show the member is ready to acquire.
ShareWorker.prototype.fetchAnswered = () => true
// The raw reader dials its own broker connection, outside any `Kafka`: it reads through a scripted
// cluster where nothing was ever written, so each scan archives its raw exchanges and ends at 0.
let readerWire = null
ReaderWorker.prototype.openBroker = (address, onExchange) => {
  readerWire ??= scripted().answer('ListOffsets', ({ topics: list }) => ({
    topics: list.map(({ name, partitions }) => ({
      name,
      partitions: partitions.map(({ partitionIndex }) => ({
        partitionIndex,
        errorCode: 0,
        timestamp: '-1',
        offset: '0',
        leaderEpoch: 0,
      })),
    })),
  }))
  return openRawBroker(address, onExchange, () => readerWire.socketFactory)
}
