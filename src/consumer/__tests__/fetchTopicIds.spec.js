const Encoder = require('../../protocol/encoder')
const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger, readRequestHeader } = require('testHelpers')
// The wire decoder and the fake broker are shared with the other fetch specs: two copies of a
// hand-written request decoder would drift the first time the request layout changes.
const { apiVersionsBody, decodeFetchRequest, createFakeBroker } = require('./fixtures/fetchHarness')

const TOPIC = 'test-topic'
const OTHER_TOPIC = 'other-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const UNKNOWN_TOPIC_ID = '21222324-2526-2728-292a-2b2c2d2e2f30'
const NODE_ID = 0

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
    header.apiKey === apiKeys.ApiVersions ? apiVersionsBody(fetchMaxVersion, false) : respond()
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
        ? apiVersionsBody(18, false)
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
        ? apiVersionsBody(18, false)
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

  test('the codecs stay silent: no Fetch codec can log, and none holds per-request state', () => {
    // Read as source, because a logger call added to a codec is only visible in the code: the
    // decision to warn about a topic id the request never asked for belongs to the caller.
    const fs = require('fs')
    const path = require('path')
    const dir = path.join(__dirname, '../../protocol/requests/fetch')
    const codecs = [12, 13, 14, 15, 16, 17, 18].flatMap((version) =>
      ['request.js', 'response.js'].map((file) => path.join(dir, `v${version}`, file))
    )

    for (const file of codecs) {
      const source = fs
        .readFileSync(file, 'utf8')
        .replace(/\/\*[\s\S]*?\*\//g, '')
        .replace(/\/\/.*$/gm, '')
      expect({ file, mentionsLogger: /logger/i.test(source) }).toEqual({
        file,
        mentionsLogger: false,
      })
    }

    const responseV13 = require('../../protocol/requests/fetch/v13/response')
    expect(responseV13.parse).toBe(require('../../protocol/requests/fetch/v12/parse').parse)
  })
})
