// A pass-through shuffle, so one test can fix the order a full request is sent in.
jest.mock('../../utils/shuffle', () => jest.fn((items) => items))

const shuffle = require('../../utils/shuffle')
const Encoder = require('../../protocol/encoder')
const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger, readRequestHeader } = require('testHelpers')
const {
  apiVersionsBody,
  buildRecordBatch,
  decodeFetchRequest,
  createFakeBroker,
} = require('./fixtures/fetchHarness')

const TOPIC = 'test-topic'
const OTHER_TOPIC = 'other-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const NODE_ID = 0

const brokers = []
let RECORDS
beforeAll(async () => {
  RECORDS = await buildRecordBatch()
})
afterEach(async () => {
  shuffle.mockClear()
  await Promise.all(brokers.splice(0).map((broker) => broker.disconnect()))
})

/**
 * One partition row of a flexible (v12+) Fetch response.
 * @param {{ partition: number, errorCode?: number, records?: Buffer|null,
 *   preferredReadReplica?: number }} row
 * @returns {Encoder}
 */
const partitionRow = ({ partition, errorCode = 0, records = null, preferredReadReplica = -1 }) => {
  const encoder = new Encoder()
    .writeInt32(partition)
    .writeInt16(errorCode)
    .writeInt64(100) // highWatermark
    .writeInt64(100) // lastStableOffset
    .writeInt64(0) // logStartOffset
    .writeUVarIntArray([]) // abortedTransactions
    .writeInt32(preferredReadReplica)
  if (records === null) encoder.writeUVarInt(0)
  else encoder.writeUVarInt(records.length + 1).writeBuffer(records)
  return encoder.writeTaggedFields()
}

/**
 * A flexible Fetch response body with any topic entries, each keyed by `topicId` (v13+) or by
 * `topicName` (v12; `null` writes a null name).
 * @param {{ topicName?: string|null, topicId?: string, partitions: object[] }[]} topics
 * @param {{ topLevel?: number, throttleTime?: number }} [options]
 * @returns {Buffer}
 */
const answer = (topics, { topLevel = 0, throttleTime = 0 } = {}) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeInt16(topLevel)
    .writeInt32(0) // sessionId
    .writeUVarIntArray(
      topics.map(({ topicName, topicId, partitions }) =>
        (topicId ? new Encoder().writeUuid(topicId) : new Encoder().writeUVarIntString(topicName))
          .writeUVarIntArray(partitions.map(partitionRow))
          .writeTaggedFields()
      )
    )
    .writeTaggedFields().buffer

/**
 * A `ConsumerGroup` whose join is skipped (assignment and positions primed at offset 0) over a
 * real `Broker` on the fake socket, so the Fetch request, its answer and the consumer's handling
 * of it are all real. Sessionless, as by default.
 * @param {object} options
 * @param {number} options.fetchMaxVersion the Fetch maximum the fake broker advertises
 * @param {string[]} [options.topics] topics the group is assigned
 * @param {number[]} [options.partitions] partitions assigned per topic
 * @param {(header: {apiVersion: number}) => Buffer} options.respond the Fetch answer
 */
const createGroup = async ({ fetchMaxVersion, topics = [TOPIC], partitions = [0], respond }) => {
  const { broker, state } = createFakeBroker((header) =>
    header.apiKey === apiKeys.ApiVersions
      ? apiVersionsBody(fetchMaxVersion, false)
      : respond(header)
  )
  await broker.connect()
  brokers.push(broker)

  const topicIds = { [TOPIC]: TOPIC_ID, [OTHER_TOPIC]: OTHER_TOPIC_ID }
  const cluster = {
    refreshMetadataIfNecessary: async () => {},
    refreshMetadata: jest.fn(async () => {}),
    findBroker: async () => broker,
    findTopicPartitionMetadata: () =>
      partitions.map((partitionId) => ({ partitionId, leader: NODE_ID })),
    committedOffsets: () => ({}),
    topicId: (topic) => topicIds[topic],
    leaderEpoch: () => -1,
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
  for (const topic of topics) {
    for (const partition of partitions) {
      group.offsetManager.committedOffsets()[topic][partition] = '0'
    }
  }

  return {
    group,
    cluster,
    logger: group.logger,
    fetchVersions: () =>
      state.writes
        .map(readRequestHeader)
        .filter(({ apiKey }) => apiKey === apiKeys.Fetch)
        .map(({ apiVersion }) => apiVersion),
    lastFetchRequest: () => decodeFetchRequest(state.writes),
  }
}

/**
 * @param {number} version the negotiated Fetch version
 * @param {string} topic
 * @returns {{ topicId: string }|{ topicName: string }} the key an answer at that version uses
 */
const keyOf = (version, topic) =>
  version >= 13 ? { topicId: topic === TOPIC ? TOPIC_ID : OTHER_TOPIC_ID } : { topicName: topic }

/** @param {object} group @returns {string[]} the topics holding preferred-replica entries */
const replicaTopics = (group) => Object.keys(group.preferredReadReplicasPerTopicPartition)

describe('Consumer > Fetch answers that do not match the request', () => {
  test.each([12, 16])(
    'an answer with an unrequested partition of a requested topic rejects the round retriably and leaves the preferred-replica map unchanged (Fetch v%i)',
    async (version) => {
      const { group, fetchVersions } = await createGroup({
        fetchMaxVersion: version,
        respond: () =>
          answer([
            {
              ...keyOf(version, TOPIC),
              partitions: [
                { partition: 0, preferredReadReplica: 1 },
                { partition: 1, records: RECORDS },
              ],
            },
          ]),
      })

      await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: `Fetch response names partition 1 of topic ${TOPIC} that the request did not carry`,
      })

      expect(fetchVersions()).toEqual([version])
      expect(replicaTopics(group)).toEqual([])
    }
  )

  test.each([
    ['with partitions', [{ partition: 0 }], 'partition 0 of topic other-topic'],
    ['without partitions', [], 'topic other-topic'],
  ])(
    'an answer with an unrequested named topic rejects the round; no preferred-replica entry is created for it (%s)',
    async (_label, partitions, named) => {
      const { group } = await createGroup({
        fetchMaxVersion: 12,
        respond: () =>
          answer([
            { topicName: TOPIC, partitions: [{ partition: 0 }] },
            { topicName: OTHER_TOPIC, partitions },
          ]),
      })

      await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: `Fetch response names ${named} that the request did not carry`,
      })

      expect(replicaTopics(group)).toEqual([])
    }
  )

  test('two rows for one partition, both with records, reject the round: no batch', async () => {
    const { group } = await createGroup({
      fetchMaxVersion: 16,
      respond: () =>
        answer([
          {
            topicId: TOPIC_ID,
            partitions: [
              { partition: 0, records: RECORDS },
              { partition: 0, records: RECORDS },
            ],
          },
        ]),
    })

    await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: `Fetch response names partition 0 of topic ${TOPIC} twice`,
    })
  })

  test('a row with records and a row with an error for one partition reject the round: no batch, no error handling', async () => {
    const { group, cluster } = await createGroup({
      fetchMaxVersion: 16,
      respond: () =>
        answer([
          {
            topicId: TOPIC_ID,
            partitions: [
              { partition: 0, records: RECORDS },
              { partition: 0, errorCode: 6 },
            ],
          },
        ]),
    })

    await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: `Fetch response names partition 0 of topic ${TOPIC} twice`,
    })

    expect([...group.consecutiveFetchErrors.keys()]).toEqual([])
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  test('a v12 row with a null topic name is refused, not treated as an unknown id', async () => {
    const { group, cluster, logger } = await createGroup({
      fetchMaxVersion: 12,
      respond: () => answer([{ topicName: null, partitions: [{ partition: 0 }] }]),
    })
    const warn = jest.spyOn(logger, 'warn')

    await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: 'Fetch response names partition 0 of topic null that the request did not carry',
    })

    expect(warn).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  test('two topics whose partitions the request interleaves (A0, B0, A1, B1), answered in that order, are delivered', async () => {
    shuffle.mockImplementationOnce((items) => {
      const [a, b] = [TOPIC, OTHER_TOPIC].map((name) => items.filter(({ topic }) => topic === name))
      return [a[0], b[0], a[1], b[1]]
    })
    const harness = await createGroup({
      fetchMaxVersion: 16,
      topics: [TOPIC, OTHER_TOPIC],
      partitions: [0, 1],
      // Echoes the request: every entry in the order it was sent, every partition with records.
      respond: () =>
        answer(
          harness.lastFetchRequest().topics.map(({ topicId, partitions }) => ({
            topicId,
            partitions: partitions.map(({ partition }) => ({ partition, records: RECORDS })),
          }))
        ),
    })

    const batches = await harness.group.fetch(NODE_ID)

    expect(harness.lastFetchRequest().topics.map(({ topicId }) => topicId)).toEqual([
      TOPIC_ID,
      OTHER_TOPIC_ID,
      TOPIC_ID,
      OTHER_TOPIC_ID,
    ])
    expect(batches.map(({ topic, partition }) => `${topic}|${partition}`).sort()).toEqual([
      `${OTHER_TOPIC}|0`,
      `${OTHER_TOPIC}|1`,
      `${TOPIC}|0`,
      `${TOPIC}|1`,
    ])
  })

  test('a partition refused for authorization, appended after the others, is reported as that error', async () => {
    const { group } = await createGroup({
      fetchMaxVersion: 16,
      partitions: [0, 1],
      respond: () =>
        answer([
          { topicId: TOPIC_ID, partitions: [{ partition: 0, records: RECORDS }] },
          { topicId: TOPIC_ID, partitions: [{ partition: 1, errorCode: 29 }] },
        ]),
    })

    await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
      type: 'TOPIC_AUTHORIZATION_FAILED',
    })
  })

  test('a top-level error answer with no rows keeps its existing handling', async () => {
    const { group, cluster } = await createGroup({
      fetchMaxVersion: 16,
      respond: () => answer([], { topLevel: 70 }),
    })

    await expect(group.fetch(NODE_ID)).resolves.toEqual([])

    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('a throttled empty answer is delivered as an empty round', async () => {
    const { group, cluster } = await createGroup({
      fetchMaxVersion: 16,
      respond: () => answer([], { throttleTime: 1 }),
    })

    await expect(group.fetch(NODE_ID)).resolves.toEqual([])

    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  test('an answer missing a partition delivers the partitions it names', async () => {
    const { group } = await createGroup({
      fetchMaxVersion: 16,
      partitions: [0, 1],
      respond: () =>
        answer([{ topicId: TOPIC_ID, partitions: [{ partition: 1, records: RECORDS }] }]),
    })

    const batches = await group.fetch(NODE_ID)

    expect(batches.map(({ topic, partition }) => `${topic}|${partition}`)).toEqual([`${TOPIC}|1`])
  })
})
