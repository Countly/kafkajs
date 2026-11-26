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
 * @param {{ topLevel?: number, throttleTime?: number, sessionId?: number }} [options]
 * @returns {Buffer}
 */
const answer = (topics, { topLevel = 0, throttleTime = 0, sessionId = 0 } = {}) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeInt16(topLevel)
    .writeInt32(sessionId)
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
 * of it are all real. Sessionless unless `fetchSessions` is set.
 * @param {object} options
 * @param {number} options.fetchMaxVersion the Fetch maximum the fake broker advertises
 * @param {string[]} [options.topics] topics the group is assigned
 * @param {number[]} [options.partitions] partitions assigned per topic
 * @param {boolean} [options.fetchSessions=false] the group's `fetchSessions` option
 * @param {(header: {apiVersion: number}) => Buffer} options.respond the Fetch answer
 */
const createGroup = async ({
  fetchMaxVersion,
  topics = [TOPIC],
  partitions = [0],
  fetchSessions = false,
  respond,
}) => {
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

describe('Broker > a full Fetch request answered for one partition', () => {
  /**
   * A real `Broker` on the fake socket whose every Fetch is answered with `body`.
   * @param {number} version the Fetch maximum the fake broker advertises
   * @param {Buffer} body
   * @returns {Promise<object>} the connected broker
   */
  const connect = async (version, body) => {
    const { broker } = createFakeBroker((header) =>
      header.apiKey === apiKeys.ApiVersions ? apiVersionsBody(version, false) : body
    )
    await broker.connect()
    brokers.push(broker)
    return broker
  }

  /**
   * @param {number} version
   * @param {number} named how many times the request names partition 0
   * @returns {object} a full request naming partition 0 `named` times
   */
  const request = (version, named) => ({
    topics: [
      {
        topic: TOPIC,
        partitions: Array.from({ length: named }, () => ({
          partition: 0,
          fetchOffset: '0',
          maxBytes: 1048576,
        })),
      },
    ],
    topicIds: version >= 13 ? new Map([[TOPIC, TOPIC_ID]]) : undefined,
  })

  /** @param {number} version @param {number} rows @returns {Promise<object>} a broker answering `rows` rows for partition 0 */
  const answering = (version, rows) =>
    connect(
      version,
      answer([
        {
          ...keyOf(version, TOPIC),
          partitions: Array.from({ length: rows }, () => ({ partition: 0 })),
        },
      ])
    )

  /** @param {object} response @returns {Array} topic name and partition ids of each entry */
  const shape = ({ responses }) =>
    responses.map(({ topicName, partitions }) => [
      topicName,
      partitions.map(({ partition }) => partition),
    ])

  test.each([12, 16])(
    'a request that names it once is refused retriably when two rows answer it (Fetch v%i)',
    async (version) => {
      const broker = await answering(version, 2)

      await expect(broker.fetch(request(version, 1))).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: `Fetch response names partition 0 of topic ${TOPIC} twice`,
      })
    }
  )

  test.each([12, 16])(
    'a request that names it twice is answered by one row (Fetch v%i)',
    async (version) => {
      const broker = await answering(version, 1)

      expect(shape(await broker.fetch(request(version, 2)))).toEqual([[TOPIC, [0]]])
    }
  )

  test.each([12, 16])(
    'a request that names it twice is answered by two rows (Fetch v%i)',
    async (version) => {
      const broker = await answering(version, 2)

      expect(shape(await broker.fetch(request(version, 2)))).toEqual([[TOPIC, [0, 0]]])
    }
  )

  test.each([12, 16])(
    'a request that names it twice is refused retriably when three rows answer it (Fetch v%i)',
    async (version) => {
      const broker = await answering(version, 3)

      await expect(broker.fetch(request(version, 2))).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: `Fetch response names partition 0 of topic ${TOPIC} twice`,
      })
    }
  )
})

describe('Consumer > Fetch session ids that cannot answer the request', () => {
  /**
   * A group with fetch sessions on whose Fetch answers are played back in order.
   * @param {Buffer[]} answers
   */
  const sessionGroup = (answers) =>
    createGroup({ fetchMaxVersion: 16, fetchSessions: true, respond: () => answers.shift() })

  /** @param {object} [extra] @returns {object[]} one entry answering partition 0 */
  const row = (extra = {}) => [{ topicId: TOPIC_ID, partitions: [{ partition: 0, ...extra }] }]

  /** @param {object[]} batches @returns {string[]} */
  const delivered = (batches) => batches.map(({ topic, partition }) => `${topic}|${partition}`)

  test("an incremental answer carrying another session's id rejects the round retriably; the next request reopens the session in full and its answer is delivered", async () => {
    const { group, lastFetchRequest } = await sessionGroup([
      answer(row(), { sessionId: 42 }),
      answer([], { sessionId: 43 }),
      answer(row({ records: RECORDS }), { sessionId: 44 }),
    ])
    await group.fetch(NODE_ID)

    await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: 'Fetch response carries session id 43, which cannot answer session 42 at epoch 1',
    })
    expect(lastFetchRequest()).toMatchObject({ sessionId: 42, sessionEpoch: 1 })

    const batches = await group.fetch(NODE_ID)

    expect(lastFetchRequest()).toMatchObject({ sessionId: 42, sessionEpoch: 0 })
    expect(delivered(batches)).toEqual([`${TOPIC}|0`])
  })

  test('a full answer with a negative session id rejects the round retriably; the next full request is answered and delivered', async () => {
    const { group, lastFetchRequest } = await sessionGroup([
      answer(row(), { sessionId: -1 }),
      answer(row({ records: RECORDS }), { sessionId: 7 }),
    ])

    await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: 'Fetch response carries session id -1, which cannot answer session 0 at epoch 0',
    })

    const batches = await group.fetch(NODE_ID)

    expect(lastFetchRequest()).toMatchObject({ sessionId: 0, sessionEpoch: 0 })
    expect(delivered(batches)).toEqual([`${TOPIC}|0`])
  })

  test('an incremental answer with id 0 ends the session without failing the round; the next request is full', async () => {
    const { group, lastFetchRequest } = await sessionGroup([
      answer(row(), { sessionId: 42 }),
      answer([], { sessionId: 0 }),
      answer(row(), { sessionId: 0 }),
    ])
    await group.fetch(NODE_ID)

    await expect(group.fetch(NODE_ID)).resolves.toEqual([])
    await group.fetch(NODE_ID)

    expect(lastFetchRequest()).toMatchObject({ sessionId: 0, sessionEpoch: 0 })
  })

  test.each([70, 71])(
    "a top-level code %i carrying another session's id resets the session without failing the round",
    async (code) => {
      const { group, lastFetchRequest } = await sessionGroup([
        answer(row(), { sessionId: 42 }),
        answer([], { topLevel: code, sessionId: 43 }),
        answer(row(), { sessionId: 42 }),
      ])
      await group.fetch(NODE_ID)

      await expect(group.fetch(NODE_ID)).resolves.toEqual([])
      await group.fetch(NODE_ID)

      expect(lastFetchRequest()).toMatchObject({
        sessionId: code === 70 ? 0 : 42,
        sessionEpoch: 0,
      })
    }
  )

  test('a sessionless answer is read whatever session id it carries', async () => {
    const { group } = await createGroup({
      fetchMaxVersion: 16,
      respond: () => answer(row({ records: RECORDS }), { sessionId: -1 }),
    })

    expect(delivered(await group.fetch(NODE_ID))).toEqual([`${TOPIC}|0`])
  })
})
