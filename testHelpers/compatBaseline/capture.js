/**
 * Baseline capture of five surfaces as they stand in the captured tree: the bytes of a
 * non-idempotent Produce request, the order a non-idempotent send resolves its RecordMetadata in,
 * the retriable flag of every protocol error code, the retrier's attempts and delays, and the
 * public instrumentation events with what a listener receives.
 *
 * `node testHelpers/compatBaseline/capture.js` writes the five JSON files beside this module from
 * the tree it runs on, the same bytes every time. The specs that guard those surfaces import the
 * drivers below and compare the live tree against the committed files, so each scenario has one
 * definition. The files record the tree they were captured from; regenerate them only to adopt a
 * deliberate change to one of these surfaces, never to make a failing comparison pass.
 */

// The specs run under jest, where NODE_ENV is 'test' and the retrier loads its test defaults; a
// capture run must load the same ones, so this precedes every require of src/.
if (require.main === module) {
  process.env.NODE_ENV = 'test'
}

const fs = require('fs')
const path = require('path')
const Broker = require('../../src/broker')
const createProducer = require('../../src/producer')
const createConsumer = require('../../src/consumer')
const createAdmin = require('../../src/admin')
const InstrumentationEventEmitter = require('../../src/instrumentation/emitter')
const producerEvents = require('../../src/producer/instrumentationEvents')
const consumerEvents = require('../../src/consumer/instrumentationEvents')
const adminEvents = require('../../src/admin/instrumentationEvents')
const createRetry = require('../../src/retry')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { errorCodes, createErrorFromCode } = require('../../src/protocol/error')
const { Types: CompressionTypes, Codecs } = require('../../src/protocol/message/compression')
const { createLogger, LEVELS } = require('../../src/loggers')
const LoggerConsole = require('../../src/loggers/console')

const FIXTURE_FILES = {
  nonIdempotentRequests: 'nonIdempotentRequests.json',
  nonIdempotentMetadataOrder: 'nonIdempotentMetadataOrder.json',
  errorRetriable: 'errorRetriable.json',
  retryTimeline: 'retryTimeline.json',
  instrumentationEvents: 'instrumentationEvents.json',
}

const TOPIC_A = 'baseline-topic-a'
const TOPIC_B = 'baseline-topic-b'
const TOPIC_A_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const TOPIC_B_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const BASE_TIMESTAMP = 1700000000000

/**
 * The ApiVersions answer every recording broker gives, in the decoded v3+ shape: Apache Kafka
 * 4.3.1's Produce range and no finalized features.
 */
const API_VERSIONS_RESPONSE = {
  errorCode: 0,
  apiVersions: [
    { apiKey: apiKeys.Produce, minVersion: 3, maxVersion: 13 },
    { apiKey: apiKeys.ApiVersions, minVersion: 0, maxVersion: 4 },
  ],
  throttleTime: 0,
  clientSideThrottleTime: 0,
  supportedFeatures: [],
  finalizedFeaturesEpoch: '-1',
  finalizedFeatures: [],
  zkMigrationReady: false,
}

/**
 * Stands in for gzip while a case runs: zlib's output is not guaranteed identical across the zlib
 * builds Node versions bundle, and the gzip header's OS byte differs between platforms, so a real
 * gzip body would tie the fixture to one machine. What the producer controls - the attributes byte
 * and the exact record bytes it hands the codec - is still in the captured request.
 */
const GZIP_MARKER = Buffer.from('baseline-gzip:')
const STAND_IN_GZIP = {
  compress: async (encoder) => Buffer.concat([GZIP_MARKER, encoder.buffer]),
  decompress: async (buffer) => buffer.subarray(GZIP_MARKER.length),
}

/** `src/retry/defaults.js` outside tests, as an explicit configuration. */
const PRODUCTION_RETRY_DEFAULTS = {
  maxRetryTime: 30000,
  initialRetryTime: 300,
  factor: 0.2,
  multiplier: 2,
  retries: 5,
}
const RANDOM_SEED = 0x5eed

/** @returns {object} a logger that prints nothing */
const silentLogger = () => createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole })

/**
 * @param {number} partition
 * @param {number} index position within the partition; also offsets the timestamp
 * @param {object} [extra] fields to add, e.g. headers
 * @returns {object} a message with every field the record encoder writes fixed
 */
const message = (partition, index, extra = {}) => ({
  partition,
  key: `key-${partition}-${index}`,
  value: `value-${partition}-${index}`,
  timestamp: BASE_TIMESTAMP + index,
  ...extra,
})

/** One topic with known id, one partition led by node 1, two records, one with a header. */
const ONE_TOPIC = {
  leaders: { [TOPIC_A]: { 0: 1 } },
  topicIds: { [TOPIC_A]: TOPIC_A_ID },
  topicMessages: [
    {
      topic: TOPIC_A,
      messages: [message(0, 0, { headers: { 'trace-id': 'baseline' } }), message(0, 1)],
    },
  ],
}

/**
 * The `idempotent: false` sends whose Produce request bytes are captured.
 * @type {{ case: string, input: { leaders: object, topicIds: object, acks: number, compression: number, topicMessages: object[] } }[]}
 */
const NON_IDEMPOTENT_CASES = [
  {
    case: 'acksAllOneTopic',
    input: { ...ONE_TOPIC, acks: -1, compression: CompressionTypes.None },
  },
  {
    case: 'acksLeaderOneTopic',
    input: { ...ONE_TOPIC, acks: 1, compression: CompressionTypes.None },
  },
  {
    case: 'acksNoneOneTopic',
    input: { ...ONE_TOPIC, acks: 0, compression: CompressionTypes.None },
  },
  { case: 'gzipOneTopic', input: { ...ONE_TOPIC, acks: -1, compression: CompressionTypes.GZIP } },
  {
    case: 'topicIdUnknown',
    input: { ...ONE_TOPIC, topicIds: {}, acks: -1, compression: CompressionTypes.None },
  },
  {
    case: 'twoTopicsTwoBrokers',
    input: {
      leaders: { [TOPIC_A]: { 0: 1, 1: 2 }, [TOPIC_B]: { 0: 1 } },
      topicIds: { [TOPIC_A]: TOPIC_A_ID, [TOPIC_B]: TOPIC_B_ID },
      acks: -1,
      compression: CompressionTypes.None,
      topicMessages: [
        { topic: TOPIC_A, messages: [message(0, 0), message(1, 0)] },
        { topic: TOPIC_B, messages: [message(0, 0)] },
      ],
    },
  },
]

/** Two topics whose leaders resolve out of topic order, so a broker-grouped order is visible. */
const METADATA_ORDER_CASE = {
  case: 'leadersOutOfTopicOrder',
  input: {
    leaders: { [TOPIC_A]: { 0: 2, 1: 1 }, [TOPIC_B]: { 0: 1 } },
    topicIds: { [TOPIC_A]: TOPIC_A_ID, [TOPIC_B]: TOPIC_B_ID },
    acks: -1,
    compression: CompressionTypes.None,
    topicMessages: [
      { topic: TOPIC_A, messages: [message(0, 0), message(1, 0)] },
      { topic: TOPIC_B, messages: [message(0, 0)] },
    ],
  },
}

/**
 * The retrier runs whose attempts, delays and outcome are captured.
 * @type {{ case: string, config: object, succeedOnAttempt: number|null, nonRetriableOnAttempt: number|null }[]}
 */
const RETRY_CASES = [
  { case: 'defaultsExhausted', config: {}, succeedOnAttempt: null, nonRetriableOnAttempt: null },
  {
    case: 'productionDefaultsExhausted',
    config: PRODUCTION_RETRY_DEFAULTS,
    succeedOnAttempt: null,
    nonRetriableOnAttempt: null,
  },
  {
    case: 'succeedsOnThirdAttempt',
    config: { retries: 3, initialRetryTime: 100, factor: 0, multiplier: 3, maxRetryTime: 1000 },
    succeedOnAttempt: 3,
    nonRetriableOnAttempt: null,
  },
  {
    case: 'bailsOnNonRetriable',
    config: { retries: 2, initialRetryTime: 10, maxRetryTime: 20 },
    succeedOnAttempt: null,
    nonRetriableOnAttempt: 2,
  },
]

/**
 * The connection-pool surface `Broker` uses, answering without a socket: ApiVersions with the
 * pinned table, and a Produce by recording the body this client encoded and answering success for
 * every partition the broker was asked to write.
 */
class RecordingConnectionPool {
  /**
   * @param {object} options
   * @param {number} options.nodeId
   * @param {object[]} options.requests where every recorded request is pushed
   */
  constructor({ nodeId, requests }) {
    this.nodeId = nodeId
    this.requests = requests
    this.host = 'localhost'
    this.port = 9092 + nodeId
    this.sasl = null
    this.generation = 0
    this.connectionTimeout = 1000
    /** The topic data of the Produce request in flight, set by `RecordingBroker#produce`. */
    this.topicData = null
  }

  /** @returns {boolean} always connected: there is no socket to open */
  isConnected() {
    return true
  }

  /** @returns {boolean} always authenticated: the recording pool never negotiates SASL */
  isAuthenticated() {
    return true
  }

  /** @returns {Promise<{ authenticate: () => Promise<void> }>} a connection whose authentication is a no-op */
  async getConnection() {
    return { authenticate: async () => {} }
  }

  /** @returns {Promise<object>} an empty connection; requests are answered by `send` */
  async getConnectionByRequest() {
    return {}
  }

  /** Accepts the negotiated versions and ignores them: the table is pinned in `API_VERSIONS_RESPONSE`. */
  setVersions() {}

  /** Releases nothing: the pool holds no socket. */
  async destroy() {}

  /**
   * @param {{ request: object, response: { parse: Function } }} protocolRequest what `Broker`
   *   sends
   * @returns {Promise<object|null>} what the request's own `response.parse` makes of the decoded
   *   success answer, as a connection returns it; `null` for a request that expects none
   *   (`acks: 0`)
   */
  async send({ request, response }) {
    if (request.apiKey === apiKeys.ApiVersions) {
      return API_VERSIONS_RESPONSE
    }

    const { buffer } = await request.encode()
    this.requests.push({
      nodeId: this.nodeId,
      apiKey: request.apiKey,
      apiVersion: request.apiVersion,
      bodyHex: buffer.toString('hex'),
    })

    if (request.expectResponse && !request.expectResponse()) {
      return null
    }

    return response.parse({
      topics: this.topicData.map(({ topic, partitions }) => ({
        topicName: topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
          baseOffset: String(this.nodeId * 1000 + partition),
          logAppendTime: '-1',
          logStartOffset: '0',
        })),
      })),
    })
  }
}

/** A real `Broker` that tells its recording pool which topic data the next Produce carries. */
class RecordingBroker extends Broker {
  /**
   * @param {object} request the `Broker#produce` arguments
   * @returns {Promise<object>}
   */
  async produce(request) {
    this.connectionPool.topicData = request.topicData
    return super.produce(request)
  }
}

/**
 * The cluster surface a producer's send path reads, over a fixed leader layout and real brokers
 * whose pools record instead of connecting.
 */
class RecordingCluster {
  /**
   * @param {object} options
   * @param {{ [topic: string]: { [partition: string]: number } }} options.leaders the node leading each partition
   * @param {{ [topic: string]: string }} options.topicIds a topic absent here has no known id
   */
  constructor({ leaders, topicIds }) {
    this.leaders = leaders
    this.topicIds = topicIds
    this.requests = []
    this.brokers = new Map()
    this.targetTopics = new Set()
  }

  /** Connects nothing: brokers are created connected, on first use. */
  async connect() {}

  /** Disconnects nothing: the recording brokers hold no sockets. */
  async disconnect() {}

  /** @returns {boolean} always connected */
  isConnected() {
    return true
  }

  /** Leaves the fixed leader layout as it is. */
  async refreshMetadata() {}

  /** Leaves the fixed leader layout as it is. */
  async refreshMetadataIfNecessary() {}

  /** @param {string[]} topics the topics the producer will send to; remembered, not fetched */
  async addMultipleTargetTopics(topics) {
    topics.forEach((topic) => this.targetTopics.add(topic))
  }

  /**
   * @param {string} topic
   * @returns {string|undefined} the topic id given at construction, `undefined` when unknown
   */
  topicId(topic) {
    return this.topicIds[topic]
  }

  /** @returns {{ partitionId: number, leader: number }[]} */
  findTopicPartitionMetadata(topic) {
    return Object.entries(this.leaders[topic] || {}).map(([partitionId, leader]) => ({
      partitionId: Number(partitionId),
      leader,
    }))
  }

  /** `Cluster#findLeaderForPartitions` over the fixed layout: leader id to partition ids. */
  findLeaderForPartitions(topic, partitions) {
    const metadata = this.findTopicPartitionMetadata(topic)
    return partitions.reduce((result, id) => {
      const partitionId = parseInt(id, 10)
      const { leader } = metadata.find((entry) => entry.partitionId === partitionId)
      return { ...result, [leader]: [...(result[leader] || []), partitionId] }
    }, {})
  }

  /**
   * @param {{ nodeId: number|string }} options
   * @returns {Promise<RecordingBroker>} connected, one per node
   */
  async findBroker({ nodeId }) {
    const id = Number(nodeId)
    if (!this.brokers.has(id)) {
      const broker = new RecordingBroker({
        connectionPool: new RecordingConnectionPool({ nodeId: id, requests: this.requests }),
        logger: silentLogger(),
        nodeId: id,
      })
      await broker.connect()
      this.brokers.set(id, broker)
    }

    return this.brokers.get(id)
  }

  /** @returns {object[]} every recorded request by node id, in send order within a node */
  recordedRequests() {
    return [...this.requests].sort((a, b) => a.nodeId - b.nodeId)
  }
}

/**
 * Sends one case through a real `idempotent: false` producer over recording brokers, with the
 * gzip stand-in registered for the duration. Runs must not overlap in one process: it patches the
 * process-wide gzip codec slot and restores it in `finally`.
 * @param {{ input: object }} testCase
 * @returns {Promise<{ cluster: RecordingCluster, metadata: object[] }>}
 */
const sendNonIdempotent = async ({ input }) => {
  const { leaders, topicIds, acks, compression, topicMessages } = input
  const cluster = new RecordingCluster({ leaders, topicIds })
  const producer = createProducer({ cluster, logger: silentLogger(), idempotent: false })
  const gzip = Codecs[CompressionTypes.GZIP]
  Codecs[CompressionTypes.GZIP] = () => STAND_IN_GZIP

  try {
    await producer.connect()
    const metadata = await producer.sendBatch({ acks, compression, topicMessages })
    await producer.disconnect()
    return { cluster, metadata }
  } finally {
    Codecs[CompressionTypes.GZIP] = gzip
  }
}

/**
 * @param {{ case: string, input: object }} testCase one of `NON_IDEMPOTENT_CASES`
 * @returns {Promise<{ case: string, input: object, requests: object[] }>}
 */
const captureNonIdempotentRequests = async (testCase) => {
  const { cluster } = await sendNonIdempotent(testCase)
  return { case: testCase.case, input: testCase.input, requests: cluster.recordedRequests() }
}

/**
 * @param {{ case: string, input: object }} testCase `METADATA_ORDER_CASE`
 * @returns {Promise<{ case: string, input: object, metadata: object[] }>}
 */
const captureMetadataOrder = async (testCase) => {
  const { metadata } = await sendNonIdempotent(testCase)
  return { case: testCase.case, input: testCase.input, metadata }
}

/** @returns {{ code: number, type: string, retriable: boolean }[]} every row of the error table */
const errorRetriableTable = () =>
  errorCodes.map(({ code, type, retriable }) => ({ code, type, retriable }))

/**
 * mulberry32: a small deterministic generator, so the retrier's jitter is the same every run.
 * @param {number} seed
 * @returns {() => number} values in [0, 1)
 */
const seededRandom = (seed) => {
  let state = seed
  return () => {
    state = (state + 0x6d2b79f5) | 0
    let t = Math.imul(state ^ (state >>> 15), 1 | state)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

/**
 * Runs the retrier once over a function that fails with a retriable REQUEST_TIMED_OUT (7) until
 * `succeedOnAttempt`, or with a non-retriable INVALID_RECORD (87) on `nonRetriableOnAttempt`,
 * recording every attempt and every delay it schedules. `Math.random` is seeded and `setTimeout`
 * records its delay and runs on the next tick, so a five-minute schedule takes milliseconds; both
 * are restored afterwards. Runs must not overlap.
 * @param {{ case: string, config: object, succeedOnAttempt: number|null, nonRetriableOnAttempt: number|null }} testCase
 * @returns {Promise<object>}
 */
const captureRetryTimeline = async (testCase) => {
  const { config, succeedOnAttempt, nonRetriableOnAttempt } = testCase
  const attempts = []
  const delays = []
  const realSetTimeout = global.setTimeout
  const realRandom = Math.random
  Math.random = seededRandom(RANDOM_SEED)
  global.setTimeout = (fn, ms) => {
    delays.push(ms)
    return realSetTimeout(fn, 0)
  }

  try {
    const outcome = await createRetry(config)(async (bail, retryCount, retryTime) => {
      attempts.push({ retryCount, retryTime })
      const attempt = retryCount + 1
      if (attempt === succeedOnAttempt) {
        return 'done'
      }
      throw createErrorFromCode(attempt === nonRetriableOnAttempt ? 87 : 7)
    }).then(
      () => ({ resolved: true }),
      (e) => ({ resolved: false, name: e.name, type: e.type, retryCount: e.retryCount ?? null })
    )

    return { ...testCase, outcome, attempts, delays }
  } finally {
    global.setTimeout = realSetTimeout
    Math.random = realRandom
  }
}

/**
 * What each client kind exposes through `on()`: the module naming its public events, and a factory
 * for a client over a stub cluster and the given emitter. Nothing connects, so the stub only
 * answers the one call a consumer makes while it is built.
 */
const EVENT_SOURCES = {
  producer: {
    module: producerEvents,
    create: (instrumentationEmitter) =>
      createProducer({ cluster: {}, logger: silentLogger(), instrumentationEmitter }),
  },
  consumer: {
    module: consumerEvents,
    create: (instrumentationEmitter) =>
      createConsumer({
        cluster: { onTopicRecreated: () => () => {} },
        groupId: 'baseline-group',
        logger: silentLogger(),
        instrumentationEmitter,
      }),
  },
  admin: {
    module: adminEvents,
    create: (instrumentationEmitter) =>
      createAdmin({ cluster: {}, logger: silentLogger(), instrumentationEmitter }),
  },
}

/**
 * Drives every public instrumentation event through the real `on()` of a client built over its own
 * emitter, as `Kafka#producer`, `#consumer` and `#admin` build them (a shared emitter would let one
 * client's listener rewrite the `type` another client's listener sees), and records what the
 * listener received. Each event is emitted with the payload `{ probe: true }` under its internal
 * name, which differs from the public one only for the network and cluster events.
 * @returns {{ producer: object[], consumer: object[], admin: object[], listener: Record<string, { type: string, keys: string[] }> }}
 *   per client kind the `{ publicName, internalName }` pairs sorted by public name; `listener` maps
 *   each public name to the `type` and the sorted own keys of the event its listener received
 */
const captureInstrumentationEvents = () => {
  const names = {}
  const listener = {}

  for (const [kind, { module: events, create }] of Object.entries(EVENT_SOURCES)) {
    const emitter = new InstrumentationEventEmitter()
    const client = create(emitter)
    const received = {}
    names[kind] = Object.values(events.events)
      .sort()
      .map((publicName) => ({ publicName, internalName: events.unwrap(publicName) }))

    for (const { publicName } of names[kind]) {
      client.on(publicName, (event) => {
        received[publicName] = event
      })
    }
    for (const { publicName, internalName } of names[kind]) {
      emitter.emit(internalName, { probe: true })
      listener[publicName] = {
        type: received[publicName].type,
        keys: Object.keys(received[publicName]).sort(),
      }
    }
  }

  return { ...names, listener }
}

/** @returns {Promise<object>} the five fixtures, keyed like `FIXTURE_FILES` */
const captureAll = async () => {
  const nonIdempotentRequests = []
  for (const testCase of NON_IDEMPOTENT_CASES) {
    nonIdempotentRequests.push(await captureNonIdempotentRequests(testCase))
  }

  const retryTimeline = []
  for (const testCase of RETRY_CASES) {
    retryTimeline.push(await captureRetryTimeline(testCase))
  }

  return {
    nonIdempotentRequests,
    nonIdempotentMetadataOrder: await captureMetadataOrder(METADATA_ORDER_CASE),
    errorRetriable: errorRetriableTable(),
    retryTimeline,
    instrumentationEvents: captureInstrumentationEvents(),
  }
}

/** @param {object} captured what `captureAll` resolved */
const writeFixtures = (captured) => {
  for (const [key, file] of Object.entries(FIXTURE_FILES)) {
    fs.writeFileSync(path.join(__dirname, file), `${JSON.stringify(captured[key], null, 2)}\n`)
  }
}

module.exports = {
  NON_IDEMPOTENT_CASES,
  METADATA_ORDER_CASE,
  RETRY_CASES,
  PRODUCTION_RETRY_DEFAULTS,
  captureNonIdempotentRequests,
  captureMetadataOrder,
  errorRetriableTable,
  captureRetryTimeline,
  captureInstrumentationEvents,
}

if (require.main === module) {
  captureAll()
    .then(writeFixtures)
    .catch((e) => {
      console.error(e)
      process.exit(1)
    })
}
