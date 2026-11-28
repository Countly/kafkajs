const Encoder = require('../../protocol/encoder')
const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSNonRetriableError } = require('../../errors')
const { newLogger, readRequestHeader } = require('testHelpers')
// The wire decoder and the fake broker are shared with the other fetch specs: two copies of a
// hand-written request decoder would drift the first time the request layout changes.
const {
  apiVersionsBody,
  decodeFetchRequest,
  createFakeBroker,
  trackFixtures,
} = require('./fixtures/fetchHarness')

trackFixtures()

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
 * @param {number} [sessionId=0] the Fetch session the broker answers under
 * @returns {Buffer}
 */
const flexibleFetchResponse = (topics, sessionId = 0) =>
  new Encoder()
    .writeInt32(0) // clientSideThrottleTime
    .writeInt16(0) // errorCode
    .writeInt32(sessionId)
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
 * @param {boolean} [options.fetchSessions=false] the group's `fetchSessions` option
 * @param {(header: {apiVersion: number}) => Buffer} options.respond the Fetch response body to
 *   play back, given the request header
 */
const createGroup = async ({
  fetchMaxVersion,
  topics = [TOPIC],
  partitions = [0],
  topicIds = {},
  leaderEpochs = {},
  refreshMetadata = jest.fn(async () => {}),
  fetchSessions = false,
  respond,
}) => {
  const { broker, state } = createFakeBroker((header) =>
    header.apiKey === apiKeys.ApiVersions
      ? apiVersionsBody(fetchMaxVersion, false)
      : respond(header)
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
    fetchSessions,
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

  describe('a topic whose id is unknown', () => {
    const REPEATED = 'Partition has produced consecutive fetch errors'

    // A request keyed by name (v12) gets both topics back by name, one keyed by id gets only the
    // topic it asked for: each round is answered in the shape it was sent.
    const answerBoth =
      (sessionId = 0) =>
      (header) =>
        header.apiVersion >= 13
          ? flexibleFetchResponse([{ topicId: TOPIC_ID }], sessionId)
          : flexibleFetchResponse([{ topicName: TOPIC }, { topicName: OTHER_TOPIC }], sessionId)

    const repeatedWarnings = (warn) => warn.mock.calls.filter(([message]) => message === REPEATED)

    test('a round where one topic has no id sends the others keyed by id and leaves it out', async () => {
      const { group, requests, lastFetchRequest } = await createGroup({
        fetchMaxVersion: 18,
        topics: [TOPIC, OTHER_TOPIC],
        topicIds: { [TOPIC]: TOPIC_ID },
        fetchSessions: true,
        respond: answerBoth(7),
      })

      await group.fetch(NODE_ID)

      const fetchRequests = requests().filter(({ apiKey }) => apiKey === apiKeys.Fetch)
      expect(fetchRequests.map(({ apiVersion }) => apiVersion)).toEqual([18])
      // Read off the wire: one topic, keyed by its uuid, no name written.
      expect(lastFetchRequest().topics.map(({ topic, topicId }) => ({ topic, topicId }))).toEqual([
        { topic: null, topicId: TOPIC_ID },
      ])
      const cells = [...group.fetchSessions.active(NODE_ID).partitions.values()]
      expect(cells.map(({ topic, topicId, partition }) => ({ topic, topicId, partition }))).toEqual(
        [{ topic: TOPIC, topicId: TOPIC_ID, partition: 0 }]
      )
    })

    test('a topic left out while a sibling keeps fetching is refreshed and reported', async () => {
      const refreshMetadata = jest.fn(async () => {})
      const { group, logger, requests } = await createGroup({
        fetchMaxVersion: 18,
        topics: [TOPIC, OTHER_TOPIC],
        topicIds: { [TOPIC]: TOPIC_ID },
        refreshMetadata,
        respond: answerBoth(),
      })
      // Long enough that every round below falls inside one refresh interval.
      group.leaderlessRefreshInterval = 60000
      const warn = jest.spyOn(logger, 'warn')

      const delivered = []
      for (let round = 1; round <= 4; round++) {
        delivered.push((await group.fetch(NODE_ID)).map(({ topic }) => topic))
      }
      expect(repeatedWarnings(warn)).toEqual([])
      delivered.push((await group.fetch(NODE_ID)).map(({ topic }) => topic))

      expect(delivered).toEqual([[TOPIC], [TOPIC], [TOPIC], [TOPIC], [TOPIC]])
      expect(
        requests()
          .filter(({ apiKey }) => apiKey === apiKeys.Fetch)
          .map(({ apiVersion }) => apiVersion)
      ).toEqual([18, 18, 18, 18, 18])
      expect(refreshMetadata).toHaveBeenCalledTimes(1)
      expect(repeatedWarnings(warn)).toEqual([
        [
          REPEATED,
          expect.objectContaining({
            topic: OTHER_TOPIC,
            partition: 0,
            errorCode: 100,
            consecutiveRounds: 5,
          }),
        ],
      ])
    })

    test('a round where no topic has an id sends no Fetch, refreshes and reports', async () => {
      const refreshMetadata = jest.fn(async () => {})
      const { group, logger, requests } = await createGroup({
        fetchMaxVersion: 18,
        topicIds: {},
        refreshMetadata,
        respond: () => flexibleFetchResponse([{ topicName: TOPIC }]),
      })
      group.leaderlessRefreshInterval = 60000
      // The idle sleep between rounds is not under test: each round returns at once.
      jest.spyOn(group.idleWait, 'wait').mockResolvedValue(undefined)
      const warn = jest.spyOn(logger, 'warn')

      const rounds = []
      for (let round = 1; round <= 5; round++) rounds.push(await group.fetch(NODE_ID))

      expect(rounds).toEqual([[], [], [], [], []])
      expect(requests().filter(({ apiKey }) => apiKey === apiKeys.Fetch)).toEqual([])
      expect(refreshMetadata).toHaveBeenCalledTimes(1)
      expect(repeatedWarnings(warn)).toEqual([
        [
          REPEATED,
          expect.objectContaining({
            topic: TOPIC,
            partition: 0,
            errorCode: 100,
            consecutiveRounds: 5,
          }),
        ],
      ])
    })

    test('a node whose last partitions are left out still forgets them in its fetch session', async () => {
      const topicIds = { [TOPIC]: TOPIC_ID }
      const { group, lastFetchRequest } = await createGroup({
        fetchMaxVersion: 18,
        topicIds,
        fetchSessions: true,
        respond: (header) =>
          header.apiVersion >= 13 && lastFetchRequest().topics.length === 0
            ? flexibleFetchResponse([], 7)
            : flexibleFetchResponse(
                [header.apiVersion >= 13 ? { topicId: TOPIC_ID } : { topicName: TOPIC }],
                7
              ),
      })

      await group.fetch(NODE_ID)
      expect(lastFetchRequest()).toMatchObject({ apiVersion: 18, sessionId: 0, sessionEpoch: 0 })

      // The id is gone, as after a rebootstrap cleared the cache.
      delete topicIds[TOPIC]
      await group.fetch(NODE_ID)

      expect(lastFetchRequest()).toMatchObject({
        apiVersion: 18,
        sessionId: 7,
        sessionEpoch: 1,
        topics: [],
        forgottenTopics: [{ topic: null, topicId: TOPIC_ID, partitions: [0] }],
      })
    })

    // The current metadata is fresh, so only the refresh asked for the left-out topic learns the
    // refusal; the retrier hands a non-retriable code over wrapped, its type kept.
    test.each([
      ['code 29 while a sibling fetches', 29, { [TOPIC]: TOPIC_ID }],
      ['code 17 while a sibling fetches', 17, { [TOPIC]: TOPIC_ID }],
      ['code 29 with no topic left to fetch', 29, {}],
    ])('a refresh refused for good fails the round: %s', async (_, code, topicIds) => {
      const cause = createErrorFromCode(code)
      const refusal = new KafkaJSNonRetriableError(cause, { cause })
      const refreshMetadata = jest.fn(async () => {
        throw refusal
      })
      const { group, requests } = await createGroup({
        fetchMaxVersion: 18,
        topics: [TOPIC, OTHER_TOPIC],
        topicIds,
        refreshMetadata,
        respond: () => flexibleFetchResponse([{ topicId: TOPIC_ID }]),
      })
      jest.spyOn(group.idleWait, 'wait').mockResolvedValue(undefined)

      await expect(group.fetch(NODE_ID)).rejects.toBe(refusal)

      expect(refusal.type).toBe(cause.type)
      expect(refreshMetadata).toHaveBeenCalledTimes(1)
      expect(
        requests()
          .filter(({ apiKey }) => apiKey === apiKeys.Fetch)
          .map(({ apiVersion }) => apiVersion)
      ).toEqual(topicIds[TOPIC] ? [18] : [])
    })

    test('a refresh refusal that can clear is waited out, and the id it brings puts the topic back', async () => {
      const topicIds = { [TOPIC]: TOPIC_ID }
      const refreshMetadata = jest
        .fn()
        .mockRejectedValueOnce(createErrorFromCode(3))
        .mockImplementationOnce(async () => {
          topicIds[OTHER_TOPIC] = OTHER_TOPIC_ID
        })
      const { group, requests, lastFetchRequest } = await createGroup({
        fetchMaxVersion: 18,
        topics: [TOPIC, OTHER_TOPIC],
        topicIds,
        refreshMetadata,
        respond: () =>
          flexibleFetchResponse(lastFetchRequest().topics.map(({ topicId }) => ({ topicId }))),
      })
      // Every round is due for its own refresh.
      group.leaderlessRefreshInterval = 0

      const delivered = []
      for (let round = 1; round <= 3; round++) {
        delivered.push((await group.fetch(NODE_ID)).map(({ topic }) => topic).sort())
      }

      expect(delivered).toEqual([[TOPIC], [TOPIC], [OTHER_TOPIC, TOPIC]])
      expect(refreshMetadata).toHaveBeenCalledTimes(2)
      expect(
        requests()
          .filter(({ apiKey }) => apiKey === apiKeys.Fetch)
          .map(({ apiVersion }) => apiVersion)
      ).toEqual([18, 18, 18])
      expect(lastFetchRequest().topics.map(({ topic, topicId }) => ({ topic, topicId }))).toEqual(
        expect.arrayContaining([
          { topic: null, topicId: TOPIC_ID },
          { topic: null, topicId: OTHER_TOPIC_ID },
        ])
      )
      expect(lastFetchRequest().topics).toHaveLength(2)
    })
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
