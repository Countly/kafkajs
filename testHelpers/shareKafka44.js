const net = require('net')
const { Kafka, logLevel } = require('../index')
const Decoder = require('../src/protocol/decoder')
const { RESPONSE_HEADER_V1, responseHeaderVersion } = require('../src/protocol/headers')
const CONFIG_RESOURCE_TYPES = require('../src/protocol/configResourceTypes')
const CONFIG_SOURCE = require('../src/protocol/configSource')
const ALTER_CONFIG_OP_TYPES = require('../src/protocol/alterConfigOpTypes')
const sleep = require('../src/utils/sleep')
const { plainTextBrokers, secureRandom } = require('./index')
const { readRequestHeader } = require('./fakeSocket')
const shareFetch = require('./wire/shareFetchV2')
const shareAcknowledge = require('./wire/shareAcknowledgeV2')
const {
  setShareGroupConfigs,
  resetShareGroupConfigs,
  shareTopicName,
  waitForShareFixtureAbsence,
} = require('./shareFixtures')
const { bounded, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
const { CleanupStack } = require('../src/producer/__tests__/fixtures/liveProbe')

/** The feature a Kafka 4.4 share cluster must have finalized before a dead-letter scenario runs. */
const SHARE_VERSION_FEATURE = 'share.version'

/** The finalized level of `share.version` that enables share dead-letter queues. */
const SHARE_VERSION_LEVEL = 2

/** The share APIs a wire tap records: ShareGroupHeartbeat, ShareFetch and ShareAcknowledge. */
const SHARE_API_KEYS = Object.freeze([76, 78, 79])

const DLQ_GROUP_TOPIC = 'errors.deadletterqueue.topic.name'
const DLQ_GROUP_COPY = 'errors.deadletterqueue.copy.record.enable'
const DLQ_TOPIC_ENABLE = 'errors.deadletterqueue.group.enable'
const DLQ_CLUSTER_PREFIX = 'errors.deadletterqueue.topic.name.prefix'
const DLQ_CLUSTER_AUTO_CREATE = 'errors.deadletterqueue.auto.create.topics.enable'

/**
 * Proves the cluster finalized `share.version` at exactly 2 before a live workload starts. The
 * level is read from the broker's own ApiVersions answer through the admin client; nothing is
 * upgraded here, because the cluster setup owns that and a failed upgrade must stop the run.
 * @param {{ admin: object }} options a connected admin client
 * @returns {Promise<void>}
 * @throws {Error} whatever `describeFeatures` throws; or when `share.version` is finalized at
 *   any other level, or not reported at all, naming the observed level
 */
const prepareShareKafka44 = async ({ admin }) => {
  const { finalizedFeatures } = await admin.describeFeatures()
  const level = finalizedFeatures?.[SHARE_VERSION_FEATURE]?.maxVersionLevel ?? 0
  if (level !== SHARE_VERSION_LEVEL) {
    throw new Error(
      `${SHARE_VERSION_FEATURE} is finalized at level ${level}, not ${SHARE_VERSION_LEVEL}: the Kafka 4.4 share setup did not upgrade it`
    )
  }
}

/**
 * Sets configs on one resource with a single IncrementalAlterConfigs call and resolves once a
 * DescribeConfigs answer shows every value from the expected source.
 * @param {object} admin a connected admin client
 * @param {{ type: number, name: string }} resource the resource to change
 * @param {Record<string, string|boolean>} kv config names and values
 * @param {number} source the `CONFIG_SOURCE` the readback must show for every entry
 * @param {{ timeout?: number, interval?: number }} [options] readback budget and pause, in ms
 * @returns {Promise<void>}
 * @throws {Error} when the broker refuses the change, or a value is not read back in time
 */
const setResourceConfigs = async (
  admin,
  resource,
  kv,
  source,
  { timeout = 10000, interval = 250 } = {}
) => {
  const wanted = Object.entries(kv).map(([name, value]) => [name, String(value)])
  const configEntries = wanted.map(([name, value]) => ({
    name,
    operation: ALTER_CONFIG_OP_TYPES.SET,
    value,
  }))
  const { resources } = await admin.incrementalAlterConfigs({
    resources: [{ ...resource, configEntries }],
  })
  const [result] = resources
  if (result.errorCode !== 0) {
    throw new Error(
      `Resource ${resource.type}/${resource.name} refused ${wanted.map(([name]) => name).join(', ')}: error ${result.errorCode} ${result.errorMessage}`
    )
  }

  const deadline = Date.now() + timeout
  for (;;) {
    const read = await admin.describeConfigs({
      resources: [{ ...resource, configNames: wanted.map(([name]) => name) }],
    })
    const row = read.resources.find(
      ({ resourceType, resourceName }) =>
        resourceType === resource.type && resourceName === resource.name
    )
    const entries = new Map((row?.configEntries ?? []).map((entry) => [entry.configName, entry]))
    const unread = wanted
      .filter(([name, value]) => {
        const entry = entries.get(name)
        return !(entry && entry.configValue === value && entry.configSource === source)
      })
      .map(([name, value]) => `${name}=${value}`)
    if (unread.length === 0) return
    if (Date.now() >= deadline) {
      throw new Error(
        `Resource ${resource.type}/${resource.name} did not read back ${unread.join(', ')} within ${timeout} ms`
      )
    }
    await sleep(interval)
  }
}

/**
 * Configures share dead-letter routing for one group through the existing configuration APIs:
 * the group's destination topic and copy flag, the enable flag on the destination topic, and the
 * two cluster settings that gate which topics a broker accepts as a destination. Each change is
 * read back before the call resolves, so a member that joins afterwards starts with it in force.
 *
 * The destination topic must exist and its name must start with the prefix `dlqTopicPrefix`
 * (the broker default is `dlq.`). The call registers no cleanup: the caller deletes the group
 * overrides, the destination topic and, if it set them, the cluster overrides with
 * {@link resetShareDlqClusterConfigs}.
 * @param {object} options
 * @param {object} options.admin a connected admin client
 * @param {string} options.groupId the share group that dead-letters
 * @param {string} options.sourceTopic the topic the group consumes; it can never be its own destination
 * @param {string} options.dlqTopic the existing destination topic
 * @param {boolean} options.copyRecord whether the broker copies the original key and value
 * @param {string} [options.dlqTopicPrefix='dlq.'] the cluster-wide destination name prefix
 * @returns {Promise<void>}
 * @throws {Error} when `dlqTopic` is `sourceTopic`, or when the broker refuses or does not read
 *   back any of the changes
 */
const configureShareDlq = async ({
  admin,
  groupId,
  sourceTopic,
  dlqTopic,
  copyRecord,
  dlqTopicPrefix = 'dlq.',
}) => {
  if (dlqTopic === sourceTopic) {
    throw new Error(`Topic ${sourceTopic} cannot be its own dead-letter destination`)
  }

  await setResourceConfigs(
    admin,
    { type: CONFIG_RESOURCE_TYPES.BROKER, name: '' },
    { [DLQ_CLUSTER_PREFIX]: dlqTopicPrefix, [DLQ_CLUSTER_AUTO_CREATE]: false },
    CONFIG_SOURCE.DYNAMIC_DEFAULT_BROKER_CONFIG
  )
  await setResourceConfigs(
    admin,
    { type: CONFIG_RESOURCE_TYPES.TOPIC, name: dlqTopic },
    { [DLQ_TOPIC_ENABLE]: true },
    CONFIG_SOURCE.TOPIC_CONFIG
  )
  await setShareGroupConfigs(admin, groupId, {
    [DLQ_GROUP_TOPIC]: dlqTopic,
    [DLQ_GROUP_COPY]: copyRecord,
  })
}

/**
 * Removes the two cluster-wide dead-letter overrides that {@link configureShareDlq} set.
 * @param {object} admin a connected admin client
 * @returns {Promise<void>}
 * @throws {Error} when the broker refuses the change
 */
const resetShareDlqClusterConfigs = async (admin) => {
  const { resources } = await admin.incrementalAlterConfigs({
    resources: [
      {
        type: CONFIG_RESOURCE_TYPES.BROKER,
        name: '',
        configEntries: [DLQ_CLUSTER_PREFIX, DLQ_CLUSTER_AUTO_CREATE].map((name) => ({
          name,
          operation: ALTER_CONFIG_OP_TYPES.DELETE,
        })),
      },
    ],
  })
  if (resources[0].errorCode !== 0) {
    throw new Error(`Cluster dead-letter reset refused: error ${resources[0].errorCode}`)
  }
}

/**
 * Splits a byte stream into whole length-prefixed Kafka frames.
 */
class FrameSplitter {
  /** Starts with no partial frame. */
  constructor() {
    this.carry = Buffer.alloc(0)
  }

  /**
   * @param {Buffer} chunk the next bytes of the stream
   * @returns {Buffer[]} copies of the frames this chunk completed, each with its 4-byte length
   */
  push(chunk) {
    let bytes = this.carry.length === 0 ? chunk : Buffer.concat([this.carry, chunk])
    const frames = []
    while (bytes.length >= 4 && bytes.length >= 4 + bytes.readInt32BE(0)) {
      const end = 4 + bytes.readInt32BE(0)
      frames.push(Buffer.from(bytes.subarray(0, end)))
      bytes = bytes.subarray(end)
    }
    this.carry = Buffer.from(bytes)
    return frames
  }
}

/**
 * One request and the broker's answer to it, as they crossed one socket.
 * @typedef {object} WireExchange
 * @property {string} label the client that wrote the request
 * @property {number} socket order in which that tap opened the socket
 * @property {number} apiKey
 * @property {number} apiVersion
 * @property {number} correlationId
 * @property {Buffer} request the request body, without its header
 * @property {Buffer|null} response the response body, without its header; null until it arrives
 * @property {number} requestedAt `performance.now()` when the request was written
 * @property {number|null} answeredAt `performance.now()` when the answer arrived; null until then
 */

/**
 * Records the share requests of every client it serves together with the broker's answer bytes,
 * for a live scenario to decode with an independent reader. It passes every byte through
 * unchanged: the connection and the order of writes are the client's own.
 */
class ShareWireTap {
  /**
   * @param {object} [options]
   * @param {number[]} [options.apiKeys] the APIs to record; the three share APIs by default,
   *   with the response header read for the version each request used
   * @param {Function} [options.connect] opens one socket from a Kafka socket factory's options;
   *   replaced by a fake in the broker-free checks
   */
  constructor({
    apiKeys = SHARE_API_KEYS,
    connect = ({ host, port, onConnect }) => net.connect({ host, port }, onConnect),
  } = {}) {
    this.apiKeys = new Set(apiKeys)
    this.connect = connect
    this.sockets = 0
    /** @type {WireExchange[]} */
    this.exchanges = []
  }

  /**
   * @param {string} label names the client the sockets belong to
   * @returns {Function} a Kafka `socketFactory`
   */
  factory(label) {
    return (options) => this.wrap(label, this.connect(options))
  }

  /**
   * @param {string} label names the owning client
   * @param {object} socket the real socket
   * @returns {object} the same socket, observed
   */
  wrap(label, socket) {
    const ordinal = ++this.sockets
    const writer = new FrameSplitter()
    const reader = new FrameSplitter()
    const pending = new Map()
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => {
      const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data)
      for (const frame of writer.push(bytes)) {
        const header = readRequestHeader(frame)
        if (!this.apiKeys.has(header.apiKey)) continue
        const exchange = {
          label,
          socket: ordinal,
          apiKey: header.apiKey,
          apiVersion: header.apiVersion,
          correlationId: header.correlationId,
          request: frame.subarray(header.byteLength),
          response: null,
          requestedAt: performance.now(),
          answeredAt: null,
        }
        this.exchanges.push(exchange)
        pending.set(header.correlationId, exchange)
      }
      return write(data, ...rest)
    }
    socket.on('data', (chunk) => {
      for (const frame of reader.push(chunk)) {
        const exchange = pending.get(frame.readInt32BE(4))
        if (!exchange) continue
        pending.delete(exchange.correlationId)
        const header = new Decoder(frame.subarray(8))
        if (responseHeaderVersion(exchange.apiKey, exchange.apiVersion) === RESPONSE_HEADER_V1) {
          header.readTaggedFields()
        }
        exchange.response = frame.subarray(8 + header.offset)
        exchange.answeredAt = performance.now()
      }
    })
    return socket
  }

  /**
   * @param {number} apiKey the API to select
   * @param {string} [label] restricts the selection to one client
   * @returns {WireExchange[]} the recorded exchanges of that API, oldest first
   */
  of(apiKey, label) {
    return this.exchanges.filter(
      (exchange) => exchange.apiKey === apiKey && (label === undefined || exchange.label === label)
    )
  }
}

/**
 * @param {WireExchange} exchange a recorded ShareFetch (78) or ShareAcknowledge (79) exchange
 * @returns {object} its request, decoded independently of the client's own codec
 * @throws {Error} when the exchange is another API or its body does not parse
 */
const readShareRequest = ({ apiKey, request }) => {
  if (apiKey !== 78 && apiKey !== 79) throw new Error(`API ${apiKey} is not a share fetch`)
  return (apiKey === 78 ? shareFetch : shareAcknowledge).readRequest(request)
}

/**
 * Lists, for every offset, the acknowledge types the client wrote for it, in request order. A
 * batch with one type applies it to every offset it covers; otherwise the types line up with the
 * offsets. Offsets are exact decimal strings.
 * @param {object[]} requests decoded ShareFetch or ShareAcknowledge requests, oldest first
 * @param {string} topicId the topic to read
 * @param {number} partition the partition to read
 * @returns {Map<string, number[]>} acknowledge types by decimal offset
 */
const acknowledgeTypesByOffset = (requests, topicId, partition) => {
  const types = new Map()
  for (const request of requests) {
    for (const topic of request.topics.filter((entry) => entry.topicId === topicId)) {
      for (const row of topic.partitions.filter((entry) => entry.partitionIndex === partition)) {
        for (const { firstOffset, lastOffset, acknowledgeTypes } of row.acknowledgementBatches) {
          for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
            const type =
              acknowledgeTypes.length === 1
                ? acknowledgeTypes[0]
                : acknowledgeTypes[Number(offset - BigInt(firstOffset))]
            const key = String(offset)
            types.set(key, [...(types.get(key) ?? []), type])
          }
        }
      }
    }
  }
  return types
}

/**
 * Where one partition appears in a client's ShareFetch requests, as indexes into the given list:
 * the first request carrying acknowledgements for it, the requests that name it in the forgotten
 * set, and the last request that lists it. This is what shows whether a partition left the
 * session in the very request that carried its last acknowledgements.
 * @param {object[]} requests decoded ShareFetch requests of one client, oldest first
 * @param {string} topicId the topic to read
 * @param {number} partition the partition to read
 * @returns {{ firstAcknowledged: number, forgotten: number[], lastListed: number }} indexes, -1
 *   when absent
 */
const partitionLifecycle = (requests, topicId, partition) => {
  const lifecycle = { firstAcknowledged: -1, forgotten: [], lastListed: -1 }
  requests.forEach((request, index) => {
    for (const topic of request.topics.filter((entry) => entry.topicId === topicId)) {
      for (const row of topic.partitions.filter((entry) => entry.partitionIndex === partition)) {
        lifecycle.lastListed = index
        if (lifecycle.firstAcknowledged === -1 && row.acknowledgementBatches.length > 0) {
          lifecycle.firstAcknowledged = index
        }
      }
    }
    for (const topic of request.forgottenTopicsData.filter((entry) => entry.topicId === topicId)) {
      if (topic.partitions.includes(partition)) lifecycle.forgotten.push(index)
    }
  })
  return lifecycle
}

/**
 * The partitions of one topic that a member's heartbeat answers assigned, as each answer that
 * carried an assignment stated them; answers with no assignment change nothing.
 * @param {object[]} answers decoded ShareGroupHeartbeat answers of one member, oldest first
 * @param {string} topicId the topic to read
 * @returns {number[][]} the sorted partitions of every distinct stated assignment, in order
 */
const assignedPartitions = (answers, topicId) => {
  const history = []
  for (const { assignment } of answers) {
    if (!assignment) continue
    const partitions = assignment.topicPartitions
      .filter((entry) => entry.topicId === topicId)
      .flatMap((entry) => entry.partitions)
      .sort((a, b) => a - b)
    const last = history[history.length - 1]
    if (!last || last.join() !== partitions.join()) history.push(partitions)
  }
  return history
}

/** The Metadata API, recorded next to the share APIs so a scenario can see a metadata refresh. */
const METADATA_API_KEY = 3

/** Fixed budgets, in ms, of the fixture's own steps. */
const STEP_MS = 15000
const CLEANUP_MS = 40000

/**
 * The shared body of a live Kafka 4.4 share scenario: one admin, one wire tap that sees every
 * client of the scenario, and a cleanup stack that deletes what the scenario created and proves
 * it gone. A scenario creates topics, groups, a producer and consumers through it, so nothing is
 * left behind whatever the outcome.
 */
class ShareKafka44Fixture {
  /**
   * @param {string} scenario lower-case kebab-case scenario name, part of every topic name
   */
  constructor(scenario) {
    this.scenario = scenario
    this.nonce = secureRandom(4)
    this.tap = new ShareWireTap({ apiKeys: [...SHARE_API_KEYS, METADATA_API_KEY] })
    this.stack = new CleanupStack()
    this.groups = []
    this.topics = []
    this.admin = null
  }

  /**
   * @param {string} label names the client; its sockets are tagged with it in the tap
   * @param {object} [config] extra Kafka options
   * @returns {Kafka} a client whose sockets go through the tap
   */
  client(label, config = {}) {
    return new Kafka({
      clientId: `${label}-${this.nonce}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.tap.factory(label),
      ...config,
    })
  }

  /**
   * Runs `work`, then every cleanup, whatever `work` did.
   * @param {() => Promise<void>} work the scenario body
   * @returns {Promise<void>}
   * @throws {Error} the body's failure first, else the first cleanup failure
   */
  run(work) {
    return this.stack.run(work, { budgetMs: CLEANUP_MS })
  }

  /**
   * Connects the admin client and proves the cluster finalized `share.version` at 2. Registers the
   * last cleanup: the proof that every owned group and topic is gone, then the admin's close.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async open(scope) {
    this.admin = this.client('admin').admin()
    this.stack.add('prove fixtures gone and disconnect admin', async (signal) => {
      try {
        await waitForShareFixtureAbsence(this.admin, this.groups, this.topics, {
          signal,
          unrelated: [],
        })
      } finally {
        await this.admin.disconnect()
      }
    })
    await bounded(scope, STEP_MS, () => this.admin.connect())
    await bounded(scope, STEP_MS, () => prepareShareKafka44({ admin: this.admin }))
  }

  /**
   * Creates a topic whose name starts with the share scenario prefix and registers its deletion.
   * @param {AbortSignal} scope the case scope
   * @param {object} [options]
   * @param {number} [options.partitions=1] partition count, ignored with `assignment`
   * @param {number[][]} [options.assignment] replica ids per partition, which fixes the leaders
   * @param {string} [options.name] the whole topic name, for a name the broker constrains
   * @returns {Promise<string>} the topic name
   */
  async topic(scope, { partitions = 1, assignment, name } = {}) {
    const topic = name ?? shareTopicName(this.scenario)
    this.topics.push(topic)
    this.stack.add(`delete topic ${topic}`, (signal) => this.deleteTopic(topic, signal))
    await bounded(scope, STEP_MS, () =>
      this.admin.createTopics({
        waitForLeaders: true,
        timeout: 10000,
        topics: [
          assignment
            ? {
                topic,
                replicaAssignment: assignment.map((replicas, partition) => ({
                  partition,
                  replicas,
                })),
              }
            : { topic, numPartitions: partitions, replicationFactor: 1 },
        ],
      })
    )
    return topic
  }

  /**
   * @param {string} topic an owned topic
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   */
  async deleteTopic(topic, signal) {
    await bounded(signal, 10000, () =>
      this.admin.deleteTopics({ topics: [topic], timeout: 8000 })
    ).catch((error) => {
      if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
    })
  }

  /**
   * Registers a group id for deletion and, when `configs` is given, sets those group configs and
   * waits for their readback before any member can join.
   * @param {AbortSignal} scope the case scope
   * @param {string} topic an owned topic, which names the group
   * @param {Record<string, string|number|boolean>} [configs] GROUP configs to set
   * @returns {Promise<string>} the group id
   */
  async group(scope, topic, configs) {
    const group = `${topic}-group`
    this.groups.push(group)
    this.stack.add(`delete group ${group}`, (signal) => this.deleteGroup(group, signal))
    if (configs) {
      this.stack.add(`reset group configs of ${group}`, (signal) =>
        resetShareGroupConfigs(this.admin, group, Object.keys(configs), { signal })
      )
      await bounded(scope, STEP_MS, () => setShareGroupConfigs(this.admin, group, configs))
    }
    return group
  }

  /**
   * @param {string} group an owned group id
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>} once the broker no longer lists the group
   */
  async deleteGroup(group, signal) {
    await pollUntil(
      signal,
      15000,
      async () => {
        try {
          await this.admin.deleteGroups([group])
          return true
        } catch (error) {
          const code = error.groups?.[0]?.error?.code
          if (code === 69) return true
          if (code === 68) return false
          throw error
        }
      },
      { label: `Deleting group ${group}` }
    )
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<object>} a connected producer that never creates topics, closed on cleanup
   */
  async producer(scope) {
    const producer = this.client('producer').producer({ allowAutoTopicCreation: false })
    this.stack.add('disconnect producer', () => producer.disconnect())
    await bounded(scope, STEP_MS, () => producer.connect())
    return producer
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @param {string} label names the member in the tap
   * @param {object} config shareConsumer options; `groupId` is required
   * @param {string[]} topics the topics to subscribe
   * @param {object} handlers the `run` options, `eachMessage` or `eachBatch`
   * @returns {Promise<{ consumer: object, crashes: Error[], acknowledgements: object[] }>} a
   *   running member and what its events reported; closed on cleanup
   */
  async member(scope, label, config, topics, handlers) {
    const consumer = this.client(label).shareConsumer({
      acquireMode: 'record_limit',
      maxRecords: 100,
      maxWaitTimeInMs: 250,
      ...config,
    })
    const member = { label, consumer, crashes: [], acknowledgements: [] }
    const { events } = consumer
    const removers = [
      consumer.on(events.CRASH, ({ payload }) => member.crashes.push(payload.error)),
      consumer.on(events.ACKNOWLEDGE, ({ payload }) => member.acknowledgements.push(payload)),
    ]
    this.stack.add(`remove ${label} listeners`, () => removers.forEach((remove) => remove()))
    this.stack.add(`disconnect ${label}`, () => consumer.disconnect({ timeout: 8000 }))
    await bounded(scope, STEP_MS, () => consumer.connect())
    await bounded(scope, STEP_MS, () => consumer.subscribe({ topics }))
    await bounded(scope, STEP_MS, () => consumer.run(handlers))
    return member
  }
}

module.exports = {
  SHARE_VERSION_LEVEL,
  SHARE_API_KEYS,
  prepareShareKafka44,
  configureShareDlq,
  resetShareDlqClusterConfigs,
  ShareWireTap,
  ShareKafka44Fixture,
  METADATA_API_KEY,
  readShareRequest,
  acknowledgeTypesByOffset,
  partitionLifecycle,
  assignedPartitions,
}
