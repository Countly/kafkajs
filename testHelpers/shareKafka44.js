const net = require('net')
const Decoder = require('../src/protocol/decoder')
const CONFIG_RESOURCE_TYPES = require('../src/protocol/configResourceTypes')
const CONFIG_SOURCE = require('../src/protocol/configSource')
const ALTER_CONFIG_OP_TYPES = require('../src/protocol/alterConfigOpTypes')
const sleep = require('../src/utils/sleep')
const { readRequestHeader } = require('./fakeSocket')
const { setShareGroupConfigs } = require('./shareFixtures')

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
 */

/**
 * Records the share requests of every client it serves together with the broker's answer bytes,
 * for a live scenario to decode with an independent reader. It passes every byte through
 * unchanged: the connection and the order of writes are the client's own.
 */
class ShareWireTap {
  /**
   * @param {object} [options]
   * @param {number[]} [options.apiKeys] the APIs to record; the three share APIs by default
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
        header.readTaggedFields()
        exchange.response = frame.subarray(8 + header.offset)
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

module.exports = {
  SHARE_VERSION_LEVEL,
  SHARE_API_KEYS,
  prepareShareKafka44,
  configureShareDlq,
  resetShareDlqClusterConfigs,
  ShareWireTap,
}
