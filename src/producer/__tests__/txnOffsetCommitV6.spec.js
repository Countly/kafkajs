const createProducer = require('../index')
const Cluster = require('../../cluster')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const apiKeys = require('../../protocol/requests/apiKeys')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')
const { NO_TOPIC_ID } = require('../../protocol/requests/topicIds')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSInvariantViolation,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProducerClosedError,
  KafkaJSProtocolError,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const { newLogger, connectionOpts } = require('testHelpers')

const V5_VECTOR = require('../../protocol/requests/txnOffsetCommit/fixtures/java/txnOffsetCommitRequest-v5-groupMetadata.json')

const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'
const REPLACEMENT_ID_A = '31323334-3536-3738-393a-3b3c3d3e3f40'
const FOREIGN_ID = '41424344-4546-4748-494a-4b4c4d4e4f50'
const ALL_IDS = new Map([
  ['topic-a', ID_A],
  ['topic-b', ID_B],
])

const GROUP_ID_NOT_FOUND = 69
const UNKNOWN_TOPIC_ID = 100
const STALE_MEMBER_EPOCH = 113
const COORDINATOR_LOAD_IN_PROGRESS = 14

/** What a transactional offset commit sends: offsets beyond 2^53, and every kind of metadata. */
const OFFSETS = [
  {
    topic: 'topic-a',
    partitions: [
      { partition: 0, offset: '9007199254740993', leaderEpoch: 3, metadata: null },
      { partition: 1, offset: '10', leaderEpoch: -1, metadata: 'résumé ✓' },
    ],
  },
  { topic: 'topic-b', partitions: [{ partition: 4, offset: '5', metadata: '' }] },
]

const ARGUMENTS = {
  transactionalId: 'transactional-a',
  groupId: 'group-a',
  producerId: '1000',
  producerEpoch: 3,
  topics: OFFSETS,
  generationId: 7,
  memberId: 'member-a',
  groupInstanceId: 'instance-a',
}

/**
 * Reads a TxnOffsetCommit body back, keyed by topic name below v6 and by topic id from it, so a
 * case can check what went on the wire without trusting the encoder for its own verdict.
 * @param {Buffer} body the request body, without its header
 * @param {number} version 3 or higher: the flexible layout
 * @returns {object} the fields, each topic under `key`
 */
const readRequest = (body, version) => {
  const decoder = new Decoder(body)
  const read = {
    transactionalId: decoder.readUVarIntString(),
    groupId: decoder.readUVarIntString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    generationId: decoder.readInt32(),
    memberId: decoder.readUVarIntString(),
    groupInstanceId: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray((topic) => {
      const entry = {
        key: version >= 6 ? topic.readUuid() : topic.readUVarIntString(),
        partitions: topic.readUVarIntArray((partition) => {
          const found = {
            partition: partition.readInt32(),
            offset: partition.readInt64().toString(),
            leaderEpoch: partition.readInt32(),
            metadata: partition.readUVarIntString(),
          }
          partition.readTaggedFields()
          return found
        }),
      }
      topic.readTaggedFields()
      return entry
    }),
  }

  decoder.readTaggedFields()
  return read
}

/**
 * A response body written without the codecs under test.
 * @param {{ key: string, partitions: { partition: number, errorCode?: number }[] }[]} entries
 * @param {boolean} byId whether a topic is keyed by its id, as v6 does, or by its name
 * @returns {Buffer}
 */
const answer = (entries, byId) =>
  new Encoder()
    .writeInt32(0)
    .writeUVarIntArray(
      entries.map(({ key, partitions }) =>
        (byId ? new Encoder().writeUuid(key) : new Encoder().writeUVarIntString(key))
          .writeUVarIntArray(
            partitions.map(({ partition, errorCode = 0 }) =>
              new Encoder().writeInt32(partition).writeInt16(errorCode).writeTaggedFields()
            )
          )
          .writeTaggedFields()
      )
    )
    .writeTaggedFields().buffer

/** @returns {{ key: string, partitions: { partition: number }[] }[]} every partition of OFFSETS answered */
const everything = (keyOf) =>
  OFFSETS.map(({ topic, partitions }) => ({
    key: keyOf(topic),
    partitions: partitions.map(({ partition }) => ({ partition })),
  }))

/** The successful answer to OFFSETS at the version the request was sent at. */
const succeed = (version) =>
  version >= 6
    ? answer(
        everything((topic) => ALL_IDS.get(topic)),
        true
      )
    : answer(
        everything((t) => t),
        false
      )

/** A protocol error the broker would answer with `code`. */
const protocolError = (code) => new KafkaJSProtocolError(createErrorFromCode(code))

const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

describe('Broker > TxnOffsetCommit v6 and the combined version ceiling', () => {
  const harnesses = []

  /** A real Broker over a fake socket, offering TxnOffsetCommit from `min` to `max`. */
  const offering = ({ min = 0, max, respond = succeed }) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.TxnOffsetCommit]: { minVersion: min, maxVersion: max } },
      respond: ({ apiVersion }) => respond(apiVersion),
    })
    harnesses.push(harness)
    return harness
  }

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  test('known topic IDs select TxnOffsetCommit v6 under TV2', async () => {
    const { broker, requests } = offering({ max: 6 })

    const result = await broker.txnOffsetCommit({
      ...ARGUMENTS,
      transactionVersion: 2,
      topicIds: ALL_IDS,
    })

    expect(requests).toHaveLength(1)
    expect(requests[0]).toMatchObject({ apiKey: apiKeys.TxnOffsetCommit, apiVersion: 6 })
    expect(readRequest(requests[0].body, 6)).toEqual({
      transactionalId: 'transactional-a',
      groupId: 'group-a',
      producerId: '1000',
      producerEpoch: 3,
      generationId: 7,
      memberId: 'member-a',
      groupInstanceId: 'instance-a',
      topics: [
        {
          key: ID_A,
          partitions: [
            { partition: 0, offset: '9007199254740993', leaderEpoch: 3, metadata: null },
            { partition: 1, offset: '10', leaderEpoch: -1, metadata: 'résumé ✓' },
          ],
        },
        { key: ID_B, partitions: [{ partition: 4, offset: '5', leaderEpoch: -1, metadata: '' }] },
      ],
    })
    expect(requests[0].body.includes(Buffer.from('topic-a'))).toBe(false)
    expect(requests[0].body.includes(Buffer.from('topic-b'))).toBe(false)
    expect(result).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      topics: [
        {
          topic: 'topic-a',
          partitions: [
            { partition: 0, errorCode: 0 },
            { partition: 1, errorCode: 0 },
          ],
        },
        { topic: 'topic-b', partitions: [{ partition: 4, errorCode: 0 }] },
      ],
    })
  })

  test.each([
    ['TV1 with every id', 1, ALL_IDS, 6, 4],
    ['TV1 with one id missing', 1, new Map([['topic-a', ID_A]]), 6, 4],
    ['TV2 with every id', 2, ALL_IDS, 6, 6],
    ['TV2 with one id missing', 2, new Map([['topic-a', ID_A]]), 6, 5],
    [
      'TV2 with a null id',
      2,
      new Map([
        ['topic-a', ID_A],
        ['topic-b', null],
      ]),
      6,
      5,
    ],
    [
      'TV2 with the all-zero id',
      2,
      new Map([
        ['topic-a', ID_A],
        ['topic-b', NO_TOPIC_ID],
      ]),
      6,
      5,
    ],
    ['TV2 with every id on a broker that offers v5 at most', 2, ALL_IDS, 5, 5],
  ])(
    'transaction and identity ceilings intersect: %s',
    async (_, transactionVersion, topicIds, offered, expected) => {
      const { broker, requests } = offering({ max: offered })

      await broker.txnOffsetCommit({ ...ARGUMENTS, transactionVersion, topicIds })

      expect(requests[0].apiVersion).toEqual(expected)
      const body = readRequest(requests[0].body, expected)
      expect(body.topics.map(({ key }) => key)).toEqual(
        expected === 6 ? [ID_A, ID_B] : ['topic-a', 'topic-b']
      )
      expect(body.topics.map(({ partitions }) => partitions.length)).toEqual([2, 1])
    }
  )

  test.each([
    ['TV2 with every id', 2, ALL_IDS, 'client implements v0-v6, broker supports v7-v9'],
    [
      'TV2 with one id missing',
      2,
      new Map([['topic-a', ID_A]]),
      'client implements v0-v5, broker supports v7-v9',
    ],
    ['TV1 with every id', 1, ALL_IDS, 'client implements v0-v4, broker supports v7-v9'],
  ])(
    'a broker that offers only v7 to v9 is refused, %s, naming both ranges and sending nothing',
    async (_, transactionVersion, topicIds, ranges) => {
      const { broker, requests } = offering({ min: 7, max: 9 })

      const error = await broker
        .txnOffsetCommit({ ...ARGUMENTS, transactionVersion, topicIds })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
      expect(error.message).toContain(ranges)
      expect(requests).toHaveLength(0)
    }
  )

  test('a request with no topic ids stays at v5 byte for byte on a broker that offers v6', async () => {
    const { hex, fields } = V5_VECTOR
    const { broker, requests } = offering({
      max: 6,
      respond: () => answer([{ key: 'test-topic', partitions: [{ partition: 0 }] }], false),
    })

    await broker.txnOffsetCommit({ ...fields, transactionVersion: 2 })

    expect(requests[0].apiVersion).toEqual(5)
    expect(requests[0].body.toString('hex')).toEqual(hex)
  })

  test("v6 response binds to the original topic snapshot, not to the caller's map as it is later", async () => {
    const { broker, requests } = offering({ max: 6 })
    const topicIds = new Map(ALL_IDS)

    const sending = broker.txnOffsetCommit({ ...ARGUMENTS, transactionVersion: 2, topicIds })
    // What a metadata refresh does while the request is in flight: topic-a has a new id.
    topicIds.set('topic-a', REPLACEMENT_ID_A)
    const result = await sending

    expect(requests[0].apiVersion).toEqual(6)
    expect(readRequest(requests[0].body, 6).topics.map(({ key }) => key)).toEqual([ID_A, ID_B])
    expect(result.topics.map(({ topic }) => topic)).toEqual(['topic-a', 'topic-b'])
  })

  test("an answer for a recreated topic's new id is refused, never taken for the old topic's", async () => {
    const { broker } = offering({
      max: 6,
      respond: () =>
        answer(
          everything((topic) => (topic === 'topic-a' ? REPLACEMENT_ID_A : ID_B)),
          true
        ),
    })

    const error = await broker
      .txnOffsetCommit({ ...ARGUMENTS, transactionVersion: 2, topicIds: ALL_IDS })
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(error.cause).toBeInstanceOf(KafkaJSInvariantViolation)
  })

  test.each([
    [
      'a topic id the request never carried',
      [
        { key: ID_A, partitions: [{ partition: 0 }, { partition: 1 }] },
        { key: FOREIGN_ID, partitions: [{ partition: 4 }] },
      ],
    ],
    [
      'the same topic answered twice',
      [
        { key: ID_A, partitions: [{ partition: 0 }, { partition: 1 }] },
        { key: ID_A, partitions: [{ partition: 0 }, { partition: 1 }] },
        { key: ID_B, partitions: [{ partition: 4 }] },
      ],
    ],
    [
      'a requested topic with no answer',
      [{ key: ID_A, partitions: [{ partition: 0 }, { partition: 1 }] }],
    ],
    [
      'the same partition answered twice',
      [
        { key: ID_A, partitions: [{ partition: 0 }, { partition: 1 }, { partition: 0 }] },
        { key: ID_B, partitions: [{ partition: 4 }] },
      ],
    ],
    [
      'a partition the request did not carry',
      [
        { key: ID_A, partitions: [{ partition: 0 }, { partition: 1 }, { partition: 9 }] },
        { key: ID_B, partitions: [{ partition: 4 }] },
      ],
    ],
    [
      'a requested partition with no answer',
      [
        { key: ID_A, partitions: [{ partition: 0 }] },
        { key: ID_B, partitions: [{ partition: 4 }] },
      ],
    ],
    [
      'a foreign topic that also carries a stale member epoch',
      [
        { key: ID_A, partitions: [{ partition: 0 }, { partition: 1 }] },
        { key: FOREIGN_ID, partitions: [{ partition: 4, errorCode: STALE_MEMBER_EPOCH }] },
      ],
    ],
  ])(
    'foreign duplicate missing topic or partition results reject before staging: %s',
    async (_, entries) => {
      const { broker } = offering({ max: 6, respond: () => answer(entries, true) })

      const error = await broker
        .txnOffsetCommit({ ...ARGUMENTS, transactionVersion: 2, topicIds: ALL_IDS })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(error.cause).toBeInstanceOf(KafkaJSInvariantViolation)
    }
  )

  test('an answer in any order is accepted when it correlates exactly', async () => {
    const { broker } = offering({
      max: 6,
      respond: () =>
        answer(
          [
            { key: ID_B, partitions: [{ partition: 4 }] },
            { key: ID_A, partitions: [{ partition: 1 }, { partition: 0 }] },
          ],
          true
        ),
    })

    const result = await broker.txnOffsetCommit({
      ...ARGUMENTS,
      transactionVersion: 2,
      topicIds: ALL_IDS,
    })

    expect(result.topics.map(({ topic }) => topic)).toEqual(['topic-b', 'topic-a'])
  })

  test('a topic with no partition is left out of the v6 request, and the answer for the others is accepted', async () => {
    const { broker, requests } = offering({
      max: 6,
      respond: () =>
        answer([{ key: ID_A, partitions: [{ partition: 0 }, { partition: 1 }] }], true),
    })
    const topics = [
      OFFSETS[0],
      { topic: 'topic-b', partitions: [] },
      { topic: 'topic-c', partitions: [] },
    ]

    const result = await broker.txnOffsetCommit({
      ...ARGUMENTS,
      topics,
      transactionVersion: 2,
      topicIds: new Map([...ALL_IDS, ['topic-c', FOREIGN_ID]]),
    })

    expect(requests[0].apiVersion).toEqual(6)
    expect(readRequest(requests[0].body, 6).topics.map(({ key }) => key)).toEqual([ID_A])
    expect(result.topics).toEqual([
      {
        topic: 'topic-a',
        partitions: [
          { partition: 0, errorCode: 0 },
          { partition: 1, errorCode: 0 },
        ],
      },
    ])
  })

  test('a call whose topics are all empty keeps them in the v6 request and expects each one back', async () => {
    const { broker, requests } = offering({
      max: 6,
      respond: () => answer([{ key: ID_A, partitions: [] }], true),
    })

    await broker.txnOffsetCommit({
      ...ARGUMENTS,
      topics: [{ topic: 'topic-a', partitions: [] }],
      transactionVersion: 2,
      topicIds: ALL_IDS,
    })

    expect(readRequest(requests[0].body, 6).topics).toEqual([{ key: ID_A, partitions: [] }])
  })
})

describe('Producer > transaction.sendOffsets over TxnOffsetCommit v6', () => {
  const TOPICS = [
    {
      topic: 'topic-a',
      partitions: [
        { partition: '0', offset: '11', leaderEpoch: 3 },
        { partition: '1', offset: '7' },
      ],
    },
  ]
  const GROUP_METADATA = {
    groupId: 'group-a',
    generationId: 7,
    memberId: 'member-a',
    groupInstanceId: 'instance-a',
  }
  const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 2 }
  const open = []

  /**
   * A transactional producer, at transaction version 2 unless told otherwise, with an open
   * transaction, over a fake cluster whose cache holds `ids` and whose one broker coordinates both
   * the transactions and the group - or whose group coordinator is `groupCoordinator`.
   * @param {object} [options]
   * @param {Record<string, string|null>} [options.ids] the topic ids the cluster cache holds
   * @param {object} [options.groupCoordinator] a broker to commit the offsets through
   */
  const openTransaction = async ({ ids = { 'topic-a': ID_A }, groupCoordinator } = {}) => {
    const cluster = createFakeCluster()
    const { broker } = cluster
    cluster.cache = { ...ids }
    cluster.topicId = jest.fn((topic) => cluster.cache[topic])
    Object.assign(broker, {
      finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 } },
      finalizedFeaturesEpoch: 1,
      negotiatedVersion: () => 5,
      addOffsetsToTxn: jest.fn(async () => ({})),
      txnOffsetCommit: jest.fn(async () => ({})),
    })
    broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
      producerId: '1000',
      producerEpoch: producerEpoch + 1,
    }))
    broker.endTxn.mockImplementation(async () => ({ producerId: '-1', producerEpoch: -1 }))
    if (groupCoordinator) {
      cluster.findGroupCoordinator.mockImplementation(async ({ coordinatorType }) =>
        coordinatorType === COORDINATOR_TYPES.GROUP ? groupCoordinator : broker
      )
    }
    const producer = createProducer({
      cluster,
      logger: newLogger(),
      transactionalId: 'transactional-a',
      retry: RETRY,
    })
    await producer.connect()
    const transaction = await producer.transaction()
    open.push(producer)
    return { cluster, broker, producer, transaction }
  }

  const published = (cluster) => cluster.markOffsetAsCommitted.mock.calls.map(([call]) => call)

  afterEach(async () => {
    await Promise.all(open.splice(0).map((producer) => producer.disconnect()))
  })

  test('resolves the topic ids from the cluster cache on every attempt, into a private map', async () => {
    const { cluster, broker, transaction } = await openTransaction()
    const before = JSON.parse(JSON.stringify(TOPICS))
    cluster.topicId.mockImplementation(() =>
      broker.txnOffsetCommit.mock.calls.length === 0 ? ID_A : ID_B
    )
    broker.txnOffsetCommit.mockRejectedValueOnce(protocolError(COORDINATOR_LOAD_IN_PROGRESS))

    await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

    const [first, second] = broker.txnOffsetCommit.mock.calls.map(([call]) => call)
    expect(first.transactionVersion).toEqual(2)
    expect(first.generationId).toEqual(7)
    expect([...first.topicIds]).toEqual([['topic-a', ID_A]])
    expect([...second.topicIds]).toEqual([['topic-a', ID_B]])
    expect(second.topicIds).not.toBe(first.topicIds)
    expect(TOPICS).toEqual(before)
    // the snapshot the wire is built from carries names alone, and is one object across attempts
    expect(second.topics).toBe(first.topics)
    expect(Object.keys(first.topics[0])).toEqual(['topic', 'partitions'])
    expect(Object.isFrozen(first.topics)).toBe(true)
  })

  test.each([
    ['an id the cache does not hold', {}],
    ['a null id', { 'topic-a': null }],
    ['the all-zero id', { 'topic-a': NO_TOPIC_ID }],
  ])('%s keeps the request keyed by name: no private map goes to the broker', async (_, ids) => {
    const { broker, transaction } = await openTransaction({ ids })

    await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

    expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
    expect(broker.txnOffsetCommit.mock.calls[0][0].topicIds).toBeUndefined()
  })

  test('one topic without an id keeps the whole request keyed by name, the topic that has one included', async () => {
    const { broker, transaction } = await openTransaction({ ids: { 'topic-a': ID_A } })
    const two = [...TOPICS, { topic: 'topic-b', partitions: [{ partition: '0', offset: '1' }] }]

    await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: two })

    expect(broker.txnOffsetCommit.mock.calls[0][0].topicIds).toBeUndefined()
  })

  test.each([
    ['GROUP_ID_NOT_FOUND', GROUP_ID_NOT_FOUND],
    ['STALE_MEMBER_EPOCH', STALE_MEMBER_EPOCH],
  ])(
    '%s makes offset commit abortable: abort is accepted, commit is refused, nothing is published',
    async (_, code) => {
      const { cluster, broker, producer, transaction } = await openTransaction()
      const refused = protocolError(code)
      broker.txnOffsetCommit.mockRejectedValue(refused)

      const error = await transaction
        .sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
        .catch((e) => e)
      const commitError = await transaction.commit().catch((e) => e)

      expect(refused.retriable).toBe(false)
      expect(refused.category).toEqual('abortable')
      expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(error.cause).toBe(refused)
      expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
      expect(commitError).toBe(error)
      await expect(transaction.abort()).resolves.toBeUndefined()
      await expect(producer.transaction()).resolves.toBeDefined()
      expect(published(cluster)).toEqual([])
    }
  )

  test('unknown transactional topic ID refreshes metadata before retry, never creating a topic or adding a target', async () => {
    const { cluster, broker, transaction } = await openTransaction()
    const before = JSON.parse(JSON.stringify(TOPICS))
    const order = []
    broker.txnOffsetCommit.mockImplementation(async ({ topicIds }) => {
      order.push(`TxnOffsetCommit with ${topicIds.get('topic-a') === ID_A ? 'ID-A' : 'ID-B'}`)
      if (topicIds.get('topic-a') === ID_A) throw protocolError(UNKNOWN_TOPIC_ID)
    })
    cluster.refreshMetadata.mockImplementation(async () => {
      order.push('refresh metadata')
      cluster.cache['topic-a'] = ID_B
    })

    await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

    expect(order).toEqual([
      'TxnOffsetCommit with ID-A',
      'refresh metadata',
      'TxnOffsetCommit with ID-B',
    ])
    expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
    const [[refreshOptions]] = cluster.refreshMetadata.mock.calls
    expect(refreshOptions).toEqual({
      allowAutoTopicCreation: false,
      fence: expect.any(Object),
      guard: expect.any(Function),
    })
    expect(refreshOptions.guard).toBe(refreshOptions.fence.guard)
    expect(TOPICS).toEqual(before)
    expect(published(cluster)).toEqual([])
    await transaction.commit()
    expect(published(cluster)).toEqual([
      { groupId: 'group-a', topic: 'topic-a', partition: '0', offset: '11' },
      { groupId: 'group-a', topic: 'topic-a', partition: '1', offset: '7' },
    ])
  })

  describe('with a real cluster behind the producer', () => {
    /**
     * A real Cluster over one broker double that answers every Metadata request with the ids
     * `ids` holds at that moment and records what the request named and whether it allowed the
     * broker to create a topic.
     * @param {Record<string, string>} ids the id the broker answers for each topic
     */
    const realCluster = (ids) => {
      const requests = []
      const metadataBroker = {
        nodeId: 0,
        connectionPool: { host: 'localhost', port: 9092 },
        isConnected: () => true,
        disconnect: async () => {},
        retire: async () => {},
        metadata: jest.fn(async (topics, options = {}) => {
          requests.push({ topics, allowAutoTopicCreation: options.allowAutoTopicCreation })
          return {
            brokers: [{ nodeId: 0, host: 'localhost', port: 9092 }],
            controllerId: 0,
            topicMetadata: topics.map((topic) => ({
              topic,
              topicId: ids[topic],
              partitionMetadata: [
                {
                  partitionId: 0,
                  leader: 0,
                  leaderEpoch: 1,
                  replicas: [0],
                  isr: [0],
                  offlineReplicas: [],
                },
              ],
            })),
          }
        }),
      }
      const real = new Cluster({
        ...connectionOpts(),
        logger: newLogger(),
        metadataMaxIdle: 5000,
        retry: { retries: 0 },
      })
      real.brokerPool.seedBroker = metadataBroker
      real.brokerPool.brokers = { 0: metadataBroker }
      real.brokerPool.findConnectedBroker = async () => metadataBroker
      return { real, requests }
    }

    /** Points the producer's fake cluster at `real` for its refreshes and targets. */
    const backedBy = (cluster, real) => {
      cluster.refreshMetadata = jest.fn((options) => real.refreshMetadata(options))
      cluster.addMultipleTargetTopics = jest.fn((...args) => real.addMultipleTargetTopics(...args))
    }

    test('the retry after an unknown topic id uses the id the refresh read, and no request allows topic creation', async () => {
      const ids = { 'topic-a': ID_A }
      const { real, requests } = realCluster(ids)
      await real.addTargetTopic('topic-a')
      const { cluster, broker, transaction } = await openTransaction()
      backedBy(cluster, real)
      cluster.topicId = (topic) => real.topicId(topic)
      const sentIds = []
      broker.txnOffsetCommit.mockImplementation(async ({ topicIds }) => {
        sentIds.push(topicIds.get('topic-a'))
        if (sentIds.length === 1) {
          ids['topic-a'] = ID_B
          throw protocolError(UNKNOWN_TOPIC_ID)
        }
      })
      const registered = requests.length

      await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

      expect(sentIds).toEqual([ID_A, ID_B])
      expect(requests.slice(registered)).toEqual([
        { topics: ['topic-a'], allowAutoTopicCreation: false },
      ])
      expect([...real.targetTopics]).toEqual(['topic-a'])
    })

    test('an unknown topic id makes no target of an offset topic the producer does not send to', async () => {
      const { real, requests } = realCluster({ 'topic-out': ID_B })
      await real.addTargetTopic('topic-out')
      const { cluster, broker, transaction } = await openTransaction({
        ids: { 'topic-a': ID_A },
      })
      backedBy(cluster, real)
      broker.txnOffsetCommit.mockRejectedValueOnce(protocolError(UNKNOWN_TOPIC_ID))
      const registered = requests.length

      await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

      expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(2)
      expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
      expect([...real.targetTopics]).toEqual(['topic-out'])
      expect(requests.slice(registered)).toEqual([
        { topics: ['topic-out'], allowAutoTopicCreation: false },
      ])
    })
  })

  test('a topic id the broker never learns ends in the abortable outcome after the retries, and the producer survives', async () => {
    const { cluster, broker, producer, transaction } = await openTransaction()
    const unknown = protocolError(UNKNOWN_TOPIC_ID)
    broker.txnOffsetCommit.mockRejectedValue(unknown)

    const error = await transaction
      .sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
      .catch((e) => e)

    expect(unknown.retriable).toBe(true)
    expect(unknown.category).toEqual('refreshRetriable')
    expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(error.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    expect(error.cause.cause).toBe(unknown)
    expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(RETRY.retries + 1)
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(RETRY.retries + 1)
    await expect(transaction.commit()).rejects.toBe(error)
    await expect(transaction.abort()).resolves.toBeUndefined()
    await expect(producer.transaction()).resolves.toBeDefined()
    expect(published(cluster)).toEqual([])
  })

  test('a failed refresh leaves the original error to the retry, which still succeeds', async () => {
    const { cluster, broker, transaction } = await openTransaction()
    broker.txnOffsetCommit.mockRejectedValueOnce(protocolError(UNKNOWN_TOPIC_ID))
    cluster.refreshMetadata.mockRejectedValue(new Error('metadata is unreachable'))

    await expect(
      transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
    ).resolves.toBeUndefined()

    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(2)
  })

  test('a producer closed while the coordinator is looked up again refreshes nothing and sends nothing more', async () => {
    const { cluster, broker, producer, transaction } = await openTransaction()
    const held = deferred()
    broker.txnOffsetCommit.mockRejectedValue(protocolError(UNKNOWN_TOPIC_ID))
    cluster.findGroupCoordinator.mockClear()
    cluster.findGroupCoordinator
      .mockImplementationOnce(async () => broker)
      .mockImplementationOnce(async () => held.promise)

    const sending = transaction
      .sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
      .catch((e) => e)
    // The second lookup is the one the refused attempt makes, held here.
    await expect(
      waitForCondition(() => cluster.findGroupCoordinator.mock.calls.length === 2, {
        attempts: 100,
        delay: 5,
      })
    ).resolves.toBe(true)
    const closing = producer.disconnect({ timeout: 0 })
    held.resolve(broker)
    const error = await sending
    await closing

    expect(error).toBeInstanceOf(KafkaJSProducerClosedError)
    expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
    expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(published(cluster)).toEqual([])
  })

  describe('through a real Broker that answers at v6', () => {
    const harnesses = []

    /** A real Broker over a fake socket, offering TxnOffsetCommit up to v6, as group coordinator. */
    const groupCoordinatorAnswering = (respond) => {
      const harness = createGroupApiBroker({
        versions: { [apiKeys.TxnOffsetCommit]: { minVersion: 0, maxVersion: 6 } },
        respond: ({ apiVersion }) => respond(apiVersion),
      })
      harnesses.push(harness)
      return harness
    }

    afterEach(async () => {
      await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
    })

    const everyPartitionOf = (id) => [{ key: id, partitions: [{ partition: 0 }, { partition: 1 }] }]

    test('offsets publish only after confirmed EndTxn', async () => {
      const { requests, broker: coordinator } = groupCoordinatorAnswering(() =>
        answer(everyPartitionOf(ID_A), true)
      )
      const { cluster, broker, transaction } = await openTransaction({
        groupCoordinator: coordinator,
      })
      const held = deferred()
      broker.endTxn.mockImplementation(async () => held.promise)

      await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

      expect(requests.map(({ apiVersion }) => apiVersion)).toEqual([6])
      expect(published(cluster)).toEqual([])

      const committing = transaction.commit()
      await expect(
        waitForCondition(() => broker.endTxn.mock.calls.length === 1, { attempts: 100, delay: 5 })
      ).resolves.toBe(true)
      expect(published(cluster)).toEqual([])

      held.resolve({ producerId: '-1', producerEpoch: -1 })
      await committing

      expect(published(cluster)).toEqual([
        { groupId: 'group-a', topic: 'topic-a', partition: '0', offset: '11' },
        { groupId: 'group-a', topic: 'topic-a', partition: '1', offset: '7' },
      ])
    })

    test('an abort after v6 staged the offsets publishes nothing', async () => {
      const { requests, broker: coordinator } = groupCoordinatorAnswering(() =>
        answer(everyPartitionOf(ID_A), true)
      )
      const { cluster, transaction } = await openTransaction({ groupCoordinator: coordinator })

      await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
      await transaction.abort()

      expect(requests.map(({ apiVersion }) => apiVersion)).toEqual([6])
      expect(published(cluster)).toEqual([])
    })

    test('v6 response binds to the original topic snapshot when the cache changes before the answer', async () => {
      const refreshed = { cache: null }
      const { requests, broker: coordinator } = groupCoordinatorAnswering(() => {
        // The answer arrives after a metadata refresh gave topic-a a new id.
        refreshed.cache['topic-a'] = REPLACEMENT_ID_A
        return answer(everyPartitionOf(ID_A), true)
      })
      const opened = await openTransaction({ groupCoordinator: coordinator })
      refreshed.cache = opened.cluster.cache

      await opened.transaction.sendOffsets({
        consumerGroupMetadata: GROUP_METADATA,
        topics: TOPICS,
      })
      await opened.transaction.commit()

      expect(requests).toHaveLength(1)
      expect(readRequest(requests[0].body, 6).topics.map(({ key }) => key)).toEqual([ID_A])
      expect(published(opened.cluster).map(({ topic }) => topic)).toEqual(['topic-a', 'topic-a'])
    })

    test('an answer that cannot be correlated is abortable: nothing is staged, EndTxn is not sent, abort ends the transaction and the producer carries on', async () => {
      const { broker: coordinator } = groupCoordinatorAnswering(() =>
        answer([{ key: FOREIGN_ID, partitions: [{ partition: 0 }, { partition: 1 }] }], true)
      )
      const { cluster, broker, producer, transaction } = await openTransaction({
        groupCoordinator: coordinator,
      })

      const error = await transaction
        .sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
        .catch((e) => e)
      const commitError = await transaction.commit().catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(error.cause).toBeInstanceOf(KafkaJSInvariantViolation)
      expect(commitError).toBe(error)
      expect(broker.endTxn).not.toHaveBeenCalled()
      expect(published(cluster)).toEqual([])
      await expect(transaction.abort()).resolves.toBeUndefined()
      await expect(producer.transaction()).resolves.toBeDefined()
      expect(published(cluster)).toEqual([])
    })

    test('an invariant violation that is not an answer mismatch still makes the producer unusable', async () => {
      const { broker, producer, transaction } = await openTransaction()
      const violation = new KafkaJSInvariantViolation('unrelated to the answer')
      broker.txnOffsetCommit.mockRejectedValue(violation)

      const error = await transaction
        .sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(error.cause).toBe(violation)
      await expect(producer.transaction()).rejects.toBe(error)
    })

    test.each([
      ['GROUP_ID_NOT_FOUND', GROUP_ID_NOT_FOUND],
      ['STALE_MEMBER_EPOCH', STALE_MEMBER_EPOCH],
    ])('the %s the group coordinator answers aborts the offset commit once', async (_, code) => {
      const { requests, broker: coordinator } = groupCoordinatorAnswering(() =>
        answer(
          [
            {
              key: ID_A,
              partitions: [
                { partition: 0, errorCode: code },
                { partition: 1, errorCode: code },
              ],
            },
          ],
          true
        )
      )
      const { cluster, transaction } = await openTransaction({ groupCoordinator: coordinator })

      const error = await transaction
        .sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(error.cause.code).toEqual(code)
      expect(error.cause.retriable).toBe(false)
      expect(requests).toHaveLength(1)
      await expect(transaction.abort()).resolves.toBeUndefined()
      expect(published(cluster)).toEqual([])
    })
  })
})
