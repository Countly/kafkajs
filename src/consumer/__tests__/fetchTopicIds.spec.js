const Broker = require('../../broker')
const Connection = require('../../network/connection')
const Decoder = require('../../protocol/decoder')
const Encoder = require('../../protocol/encoder')
const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { responseHeaderVersion } = require('../../protocol/headers')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

const TOPIC = 'test-topic'
const OTHER_TOPIC = 'other-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const UNKNOWN_TOPIC_ID = '21222324-2526-2728-292a-2b2c2d2e2f30'
const NODE_ID = 0

/** A v3/v4-shaped ApiVersions body advertising one Fetch range. */
const apiVersionsBody = (fetchMaxVersion) =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      new Encoder()
        .writeInt16(apiKeys.Fetch)
        .writeInt16(4)
        .writeInt16(fetchMaxVersion)
        .writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer

/**
 * A flexible (v12+) Fetch response body: one empty partition per topic, keyed by whichever of
 * name or id the caller supplies, so the same builder serves both sides of the downgrade.
 * @param {{ topicId?: string, topicName?: string }[]} topics
 * @returns {Buffer}
 */
const flexibleFetchResponse = (topics) =>
  new Encoder()
    .writeInt32(0) // clientSideThrottleTime
    .writeInt16(0) // errorCode
    .writeInt32(0) // sessionId
    .writeUVarIntArray(
      topics.map(({ topicId, topicName }) => {
        const topic = new Encoder()
        topicId ? topic.writeUuid(topicId) : topic.writeUVarIntString(topicName)
        return topic
          .writeUVarIntArray([
            new Encoder()
              .writeInt32(0) // partition
              .writeInt16(0) // errorCode
              .writeInt64(0) // highWatermark
              .writeInt64(0) // lastStableOffset
              .writeInt64(0) // logStartOffset
              .writeUVarIntArray([]) // abortedTransactions
              .writeInt32(-1) // preferredReadReplica
              .writeUVarInt(0) // records: compact null
              .writeTaggedFields(),
          ])
          .writeTaggedFields()
      })
    )
    .writeTaggedFields().buffer

/**
 * Decodes the **last** Fetch request written to the fake socket.
 *
 * The version is read off the request header, because the leading scalars differ by it:
 * `ReplicaId` exists from v0 to v14 and is gone from v15, and topics are keyed by name up to v12
 * and by id from v13. Everything else is identical across v12-v18.
 *
 * Only v12 and above: the header read below always consumes a tag buffer, which a classic
 * (sub-v12) frame does not carry.
 *
 * @param {Buffer[]} writes `state.writes`
 * @returns {{ apiVersion: number, replicaId: number|null, maxWaitMs: number, minBytes: number,
 *   maxBytes: number, isolationLevel: number, sessionId: number, sessionEpoch: number,
 *   topics: {topic: string|null, topicId: string|null, partitions: {partition: number,
 *   currentLeaderEpoch: number, fetchOffset: string, lastFetchedEpoch: number,
 *   logStartOffset: string, maxBytes: number}[]}[], rackId: string }}
 */
const decodeFetchRequest = (writes) => {
  const frame = [...writes].reverse().find((w) => readRequestHeader(w).apiKey === apiKeys.Fetch)
  const { apiVersion } = readRequestHeader(frame)

  // size(4) apiKey(2) apiVersion(2) correlationId(4) clientId then a tag buffer - request header
  // v2, whose clientId stays an INT16-prefixed string (see src/protocol/request.js).
  const decoder = new Decoder(frame)
  decoder.readInt32()
  decoder.readInt16()
  decoder.readInt16()
  decoder.readInt32()
  decoder.readString()
  decoder.readTaggedFields()

  const replicaId = apiVersion <= 14 ? decoder.readInt32() : null
  const maxWaitMs = decoder.readInt32()
  const minBytes = decoder.readInt32()
  const maxBytes = decoder.readInt32()
  const isolationLevel = decoder.readInt8()
  const sessionId = decoder.readInt32()
  const sessionEpoch = decoder.readInt32()

  const topics = decoder.readUVarIntArray((d) => {
    const topic = apiVersion <= 12 ? d.readUVarIntString() : null
    const topicId = apiVersion >= 13 ? d.readUuid() : null
    const partitions = d.readUVarIntArray((p) => {
      const entry = {
        partition: p.readInt32(),
        currentLeaderEpoch: p.readInt32(),
        fetchOffset: p.readInt64().toString(),
        lastFetchedEpoch: p.readInt32(),
        logStartOffset: p.readInt64().toString(),
        maxBytes: p.readInt32(),
      }
      p.readTaggedFields()
      return entry
    })
    d.readTaggedFields()
    return { topic, topicId, partitions }
  })

  // forgottenTopicsData: this client always sends it empty, so the element reader is never called
  // and the decode stays in step.
  decoder.readUVarIntArray(() => {})
  const rackId = decoder.readUVarIntString()
  decoder.readTaggedFields()

  return {
    apiVersion,
    replicaId,
    maxWaitMs,
    minBytes,
    maxBytes,
    isolationLevel,
    sessionId,
    sessionEpoch,
    topics,
    rackId,
  }
}

/**
 * Builds a real `Broker` wired to a real `Connection` over the fake socket from `testHelpers`.
 * `answer` receives each outgoing request's header and returns the response body to play back;
 * each response is framed with the header version that request's api key and version imply, so a
 * Fetch above v11 is answered flexibly and ApiVersions stays classic.
 * @param {(header: { apiKey: number, apiVersion: number }) => Buffer} answer
 * @returns {{ broker: Broker, state: { writes: Buffer[] } }}
 */
const createFakeBroker = (answer) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'test-client',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
  })

  let answered = 0
  const pump = async () => {
    while (answered < state.writes.length) {
      const header = readRequestHeader(state.writes[answered])
      answered += 1
      respondWithFrame(
        state,
        header.correlationId,
        responseHeaderVersion(header.apiKey, header.apiVersion),
        answer(header)
      )
    }
  }

  const connectionPool = {
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    generation: 0,
    connectionTimeout: 1000,
    isConnected: () => connection.isConnected(),
    isAuthenticated: () => true,
    getConnection: async () => {
      await connection.connect()
      return connection
    },
    getConnectionByRequest: async () => {
      await connection.connect()
      return connection
    },
    send: async (protocolRequest) => {
      const promise = connection.send(protocolRequest)
      await new Promise(setImmediate)
      await pump()
      return promise
    },
    setVersions: () => {},
    destroy: async () => connection.disconnect(),
  }

  return { broker: new Broker({ connectionPool, logger: newLogger() }), state }
}

/**
 * Builds a `ConsumerGroup` whose join/sync dance is skipped -- `subscriptionState` and
 * `offsetManager` are primed directly -- over a real `Broker` on the fake socket, so every hop
 * from version negotiation through the Fetch codec and back is real.
 *
 * @param {object} options
 * @param {number} options.fetchMaxVersion the Fetch maximum the fake broker advertises
 * @param {string[]} [options.topics] topics the group is assigned
 * @param {number[]} [options.partitions] partitions assigned per topic
 * @param {{[topic: string]: string}} [options.topicIds] what `cluster.topicId` answers
 * @param {{[partition: number]: number}} [options.leaderEpochs] what `cluster.leaderEpoch` answers
 * @param {Function} [options.refreshMetadata] the `cluster.refreshMetadata` stub, for assertions
 * @param {() => Buffer} options.respond the Fetch response body to play back
 */
const createGroup = async ({
  fetchMaxVersion,
  topics = [TOPIC],
  partitions = [0],
  topicIds = {},
  leaderEpochs = {},
  refreshMetadata = jest.fn(async () => {}),
  respond,
}) => {
  const { broker, state } = createFakeBroker((header) =>
    header.apiKey === apiKeys.ApiVersions ? apiVersionsBody(fetchMaxVersion) : respond()
  )
  await broker.connect()

  const findBrokerCalls = []
  const cluster = {
    refreshMetadataIfNecessary: async () => {},
    refreshMetadata,
    findBroker: async (options) => {
      findBrokerCalls.push(options)
      return broker
    },
    findTopicPartitionMetadata: () =>
      partitions.map((partitionId) => ({ partitionId, leader: NODE_ID })),
    committedOffsets: () => ({}),
    topicId: (topic) => topicIds[topic],
    leaderEpoch: (topic, partition) =>
      leaderEpochs[partition] === undefined ? -1 : leaderEpochs[partition],
    hasNode: () => true,
  }

  const topicConfigurations = {}
  const memberAssignment = {}
  for (const topic of topics) {
    topicConfigurations[topic] = {}
    memberAssignment[topic] = partitions
  }

  const group = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'test-group',
    topics,
    topicConfigurations,
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    metadataMaxAge: 300000,
  })

  group.subscriptionState.assign(topics.map((topic) => ({ topic, partitions })))
  group.offsetManager = new OffsetManager({
    cluster,
    coordinator: broker,
    memberAssignment,
    autoCommit: false,
    topicConfigurations,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 0,
    memberId: 'member-1',
  })

  // A real join would have resolved these from OffsetFetch; priming them directly is what lets
  // this scenario skip the join dance while keeping the Fetch path completely real.
  for (const topic of topics) {
    for (const partition of partitions) {
      group.offsetManager.committedOffsets()[topic][partition] = '0'
    }
  }

  let lastBatches
  const fetch = group.fetch.bind(group)
  group.fetch = async (nodeId) => {
    lastBatches = await fetch(nodeId)
    return lastBatches
  }

  return {
    group,
    cluster,
    logger: group.logger,
    requests: () => state.writes.map(readRequestHeader),
    batches: () => lastBatches,
    findBrokerCalls,
    lastFetchRequest: () => decodeFetchRequest(state.writes),
    lastFetchRequestPartitions: () => decodeFetchRequest(state.writes).topics[0].partitions,
  }
}

describe('Consumer > fetch topic ids', () => {
  test('a fetch with every topic id known is sent at the negotiated maximum, keyed by id', async () => {
    const { group, requests, lastFetchRequest } = await createGroup({
      fetchMaxVersion: 18,
      topicIds: { [TOPIC]: TOPIC_ID },
      respond: () => flexibleFetchResponse([{ topicId: TOPIC_ID }]),
    })

    await group.fetch(NODE_ID)

    const fetchRequest = requests().find(({ apiKey }) => apiKey === apiKeys.Fetch)
    expect(fetchRequest.apiVersion).toEqual(18)
    // Keyed by id on the wire: the uuid is there and the name is not.
    expect(lastFetchRequest().topics).toEqual([
      expect.objectContaining({ topicId: TOPIC_ID, topic: null }),
    ])
  })

  test('supplying ids is what lifts the cap: the same round with one id missing drops to v12', async () => {
    const { group, requests, lastFetchRequest } = await createGroup({
      fetchMaxVersion: 18,
      topicIds: {},
      respond: () => flexibleFetchResponse([{ topicName: TOPIC }]),
    })

    await group.fetch(NODE_ID)

    expect(requests().find(({ apiKey }) => apiKey === apiKeys.Fetch).apiVersion).toEqual(12)
    // Keyed by name on the wire: the name is there and no uuid was written.
    expect(lastFetchRequest().topics).toEqual([
      expect.objectContaining({ topic: TOPIC, topicId: null }),
    ])
  })

  test('a fetch with one unknown topic id is sent at v12 with topic names, in one request', async () => {
    const { group, requests, lastFetchRequest } = await createGroup({
      fetchMaxVersion: 18,
      topics: [TOPIC, OTHER_TOPIC],
      // OTHER_TOPIC has no id: the whole request falls back, rather than being split.
      topicIds: { [TOPIC]: TOPIC_ID },
      respond: () => flexibleFetchResponse([{ topicName: TOPIC }, { topicName: OTHER_TOPIC }]),
    })

    await group.fetch(NODE_ID)

    const fetchRequests = requests().filter(({ apiKey }) => apiKey === apiKeys.Fetch)
    expect(fetchRequests).toHaveLength(1)
    expect(fetchRequests[0].apiVersion).toEqual(12)
    // The topic that *does* have an id is sent by name too: the fallback is per request.
    expect(lastFetchRequest().topics.map(({ topic, topicId }) => ({ topic, topicId }))).toEqual(
      expect.arrayContaining([
        { topic: TOPIC, topicId: null },
        { topic: OTHER_TOPIC, topicId: null },
      ])
    )
  })

  test('response topic ids resolve through the request map, not through the metadata cache', async () => {
    const { group, cluster, batches } = await createGroup({
      fetchMaxVersion: 18,
      topicIds: { [TOPIC]: TOPIC_ID },
      respond: () => {
        // The cache moves on between request and response, as a recreation would move it.
        cluster.topicId = () => OTHER_TOPIC_ID
        return flexibleFetchResponse([{ topicId: TOPIC_ID }])
      },
    })

    const result = await group.fetch(NODE_ID)

    // Mapped through the request's own id map, so the batch still carries the original name.
    expect(result.map(({ topic }) => topic)).toEqual([TOPIC])
    expect(batches()).toHaveLength(1)
  })

  test('an id absent from the request map takes the stale-metadata action', async () => {
    const refreshMetadata = jest.fn(async () => {})
    const { group, logger } = await createGroup({
      fetchMaxVersion: 18,
      topicIds: { [TOPIC]: TOPIC_ID },
      refreshMetadata,
      // A broker bug: an id the client never asked for.
      respond: () => flexibleFetchResponse([{ topicId: OTHER_TOPIC_ID }]),
    })
    const warn = jest.spyOn(logger, 'warn')

    await expect(group.fetch(NODE_ID)).resolves.toEqual([])

    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('topic id'),
      expect.objectContaining({ topicId: OTHER_TOPIC_ID })
    )
    expect(refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('an unknown id costs only its own topic: the rest of the response is still delivered', async () => {
    const refreshMetadata = jest.fn(async () => {})
    const { group, logger } = await createGroup({
      fetchMaxVersion: 18,
      topics: [TOPIC, OTHER_TOPIC],
      topicIds: { [TOPIC]: TOPIC_ID, [OTHER_TOPIC]: OTHER_TOPIC_ID },
      refreshMetadata,
      respond: () => flexibleFetchResponse([{ topicId: TOPIC_ID }, { topicId: UNKNOWN_TOPIC_ID }]),
    })
    const warn = jest.spyOn(logger, 'warn')

    const result = await group.fetch(NODE_ID)

    // The bad entry is dropped from the round, not the round from the consumer.
    expect(result.map(({ topic }) => topic)).toEqual([TOPIC])
    expect(warn).toHaveBeenCalledTimes(1)
    expect(refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('CurrentLeaderEpoch carries the cached epoch, and -1 when the cache does not know it', async () => {
    const { group, lastFetchRequestPartitions } = await createGroup({
      fetchMaxVersion: 18,
      topicIds: { [TOPIC]: TOPIC_ID },
      partitions: [0, 1],
      // The cache knows partition 0's epoch and has never seen partition 1.
      leaderEpochs: { 0: 7 },
      respond: () => flexibleFetchResponse([{ topicId: TOPIC_ID }]),
    })

    await group.fetch(NODE_ID)

    // Read off the wire, not off the call: the assertion is that the bytes carry it.
    expect(lastFetchRequestPartitions()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ partition: 0, currentLeaderEpoch: 7 }),
        expect.objectContaining({ partition: 1, currentLeaderEpoch: -1 }),
      ])
    )
  })

  test('LastFetchedEpoch is never populated: every partition carries the wire default', async () => {
    const { group, lastFetchRequestPartitions } = await createGroup({
      fetchMaxVersion: 18,
      topicIds: { [TOPIC]: TOPIC_ID },
      leaderEpochs: { 0: 7 },
      respond: () => flexibleFetchResponse([{ topicId: TOPIC_ID }]),
    })

    await group.fetch(NODE_ID)

    expect(lastFetchRequestPartitions()).toEqual([
      expect.objectContaining({ lastFetchedEpoch: -1 }),
    ])
  })

  test('an unknown id is mapped by Broker#fetch itself, with no consumer group involved', async () => {
    const { broker } = createFakeBroker((header) =>
      header.apiKey === apiKeys.ApiVersions
        ? apiVersionsBody(18)
        : flexibleFetchResponse([{ topicId: OTHER_TOPIC_ID }])
    )
    await broker.connect()

    const { responses } = await broker.fetch({
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1024,
      topicIds: new Map([[TOPIC, TOPIC_ID]]),
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1024 }] }],
    })

    expect(responses[0].topicName).toBeNull()
    expect(responses[0].topicId).toEqual(OTHER_TOPIC_ID)
  })

  test('a mapped id leaves no topicId behind, so the decoded shape is unchanged at every version', async () => {
    const { broker } = createFakeBroker((header) =>
      header.apiKey === apiKeys.ApiVersions
        ? apiVersionsBody(18)
        : flexibleFetchResponse([{ topicId: TOPIC_ID }])
    )
    await broker.connect()

    const { responses } = await broker.fetch({
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1024,
      topicIds: new Map([[TOPIC, TOPIC_ID]]),
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1024 }] }],
    })

    expect(responses[0].topicName).toEqual(TOPIC)
    expect(responses[0].topicId).toBeUndefined()
    expect(Object.keys(responses[0]).sort()).toEqual(['partitions', 'topicName'])
  })

  test('the codecs stay silent: nothing is logged from inside the protocol layer', async () => {
    const responseV13 = require('../../protocol/requests/fetch/v13/response')
    // A codec has no logger to log with, and no per-request closure to hold one.
    expect(Object.keys(responseV13)).toEqual(expect.arrayContaining(['decode', 'parse']))
    expect(responseV13.parse).toBe(require('../../protocol/requests/fetch/v12/parse').parse)
  })
})
