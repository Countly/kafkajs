const Cluster = require('../../cluster')
const createAdmin = require('../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSConnectionError, KafkaJSDeleteTopicRecordsError } = require('../../errors')
const CONFIG_RESOURCE_TYPES = require('../../protocol/configResourceTypes')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { connectionOpts, newLogger } = require('testHelpers')

const LEADER_NOT_AVAILABLE = 5
const OFFSET_OUT_OF_RANGE = 1
const NOT_LEADER_OR_FOLLOWER = 6
const HEALTHY = 'healthy'
const LEADERLESS = 'leaderless'
const SPLIT = 'split'
const NODES = [1, 2].map((nodeId) => ({ nodeId, host: `kafka${nodeId}`, port: 9092, rack: null }))
// Spent within a few milliseconds, and counted: several outcomes below are the budget running out.
const RETRY = { retries: 3, initialRetryTime: 1, maxRetryTime: 2 }

/**
 * A partition as a KRaft broker reports it: a leader of -1 comes with LEADER_NOT_AVAILABLE.
 * @param {number} leader
 * @param {number} partitionId
 */
const partitionOf = (leader, partitionId) => ({
  partitionErrorCode: leader < 0 ? LEADER_NOT_AVAILABLE : 0,
  partitionId,
  leader,
  leaderEpoch: 3,
  replicas: [3],
  isr: leader < 0 ? [] : [leader],
  offlineReplicas: leader < 0 ? [3] : [],
})

/**
 * A real `Cluster` over fake brokers. Each Metadata answer goes through the parse the request
 * itself selects (`requests.Metadata` at v13), so a request for every topic is parsed the way one
 * is on the wire. `leaders` holds, per topic, the leader each answer reports for each partition;
 * every call to a broker is recorded in `calls`.
 * @param {{ keepsMetadata?: boolean }} [options] `false` builds the cluster as `kafka.admin()`
 *   does, with no `metadataMaxAge`, so every refresh-if-necessary asks again
 */
const createLeaderlessCluster = ({ keepsMetadata = true } = {}) => {
  const leaders = { [HEALTHY]: [1], [LEADERLESS]: [-1], [SPLIT]: [1, 2] }
  const calls = []
  // By default a cluster that keeps its metadata, as a consumer's does: an admin's own refreshes
  // it at once, which only narrows the window in which a cached answer for every topic is read.
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    retry: RETRY,
    ...(keepsMetadata ? { metadataMaxAge: 300000 } : {}),
  })
  const pool = cluster.brokerPool

  pool.buildConnectionPool = async ({ host = 'kafka1', port = 9092, rack = null } = {}) => ({
    host,
    port,
    rack,
    destroy: async () => {},
  })

  pool.createBroker = ({ connectionPool, nodeId }) => {
    let connected = false
    const record = (api, request = {}) => calls.push({ api, nodeId, ...request })

    return {
      nodeId,
      connectionPool,
      isConnected: () => connected,
      connect: async () => {
        connected = true
      },
      disconnect: async () => {
        connected = false
      },
      retire: async () => {
        connected = false
      },
      metadata: async (topics = []) => {
        record('Metadata', { topics })
        const named = topics.length > 0 ? topics : Object.keys(leaders)
        const { response } = requests.Metadata.protocol({ version: 13 })({ topics })
        return response.parse({
          throttleTime: 0,
          clientSideThrottleTime: 0,
          errorCode: 0,
          brokers: NODES,
          clusterId: 'cluster-a',
          controllerId: 1,
          topicMetadata: named.map((topic) => ({
            topicErrorCode: 0,
            topic,
            topicId: null,
            isInternal: false,
            partitionMetadata: (leaders[topic] ?? [1]).map(partitionOf),
          })),
        })
      },
      createTopics: async ({ topics }) => record('CreateTopics', { topics }),
      describeConfigs: async ({ resources }) => {
        record('DescribeConfigs')
        return {
          resources: resources.map(({ type, name }) => ({
            errorCode: 0,
            errorMessage: null,
            resourceType: type,
            resourceName: name,
            configEntries: [],
          })),
        }
      },
      describeTopicPartitions: async ({ topics }) => {
        record('DescribeTopicPartitions', { topics })
        return {
          nextCursor: null,
          topics: topics.map((name) => ({
            errorCode: 0,
            name,
            topicId: null,
            isInternal: false,
            // As a broker answers: no error code on a partition with no leader, only leaderId -1.
            // https://github.com/apache/kafka/blob/4.3/metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java
            partitions: [
              {
                errorCode: 0,
                partitionIndex: 0,
                leaderId: -1,
                leaderEpoch: 3,
                replicaNodes: [3],
                isrNodes: [],
                eligibleLeaderReplicas: null,
                lastKnownElr: null,
                offlineReplicas: [3],
              },
            ],
            topicAuthorizedOperations: -2147483648,
          })),
        }
      },
      listOffsets: async ({ topics }) => {
        record('ListOffsets')
        return {
          responses: topics.map(({ topic, partitions }) => ({
            topic,
            partitions: partitions.map(({ partition }) => ({ partition, offset: '5' })),
          })),
        }
      },
      deleteRecords: jest.fn(async ({ topics }) => record('DeleteRecords', { topics })),
    }
  }

  const admin = createAdmin({ cluster, logger: newLogger(), retry: RETRY })
  const asked = (api) => calls.filter((call) => call.api === api)
  return { cluster, admin, leaders, calls, asked }
}

describe('Admin > a partition with no leader', () => {
  let harness

  beforeEach(() => {
    harness = createLeaderlessCluster()
  })

  afterEach(async () => {
    await harness?.cluster.disconnect()
    harness = null
  })

  test('describeCluster, listTopics and fetchTopicMetadata() answer from a Metadata response for every topic', async () => {
    const { admin } = harness
    await admin.connect()

    await expect(admin.describeCluster()).resolves.toEqual({
      brokers: NODES.map(({ nodeId, host, port }) => ({ nodeId, host, port })),
      controller: 1,
      clusterId: 'cluster-a',
    })
    expect((await admin.listTopics()).sort()).toEqual([HEALTHY, LEADERLESS, SPLIT])
    const { topics } = await admin.fetchTopicMetadata()
    expect(topics.find(({ name }) => name === LEADERLESS).partitions).toEqual([
      expect.objectContaining({ partitionErrorCode: LEADER_NOT_AVAILABLE, leader: -1 }),
    ])
  })

  test('createTopics and describeConfigs, which first refresh the metadata of every topic, succeed', async () => {
    const { admin, asked } = harness
    await admin.connect()

    await expect(admin.createTopics({ topics: [{ topic: 'created' }] })).resolves.toBe(true)
    await expect(
      admin.describeConfigs({ resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: HEALTHY }] })
    ).resolves.toMatchObject({ resources: [{ resourceName: HEALTHY, errorCode: 0 }] })
    expect(asked('Metadata').map(({ topics }) => topics)).toEqual([[], ['created'], []])
  })

  test('fetchTopicMetadata through DescribeTopicPartitions, as the first call of an admin, reports the partition with no leader', async () => {
    const { admin, asked } = harness
    await admin.connect()

    const { topics } = await admin.fetchTopicMetadata({
      topics: [LEADERLESS],
      useDescribeTopicPartitions: true,
    })

    expect(asked('Metadata').map(({ topics: asked }) => asked)).toEqual([[]])
    expect(topics[0].partitions).toEqual([
      expect.objectContaining({ partitionErrorCode: 0, leader: -1 }),
    ])
  })

  test('fetchTopicOffsets of a topic whose cached leader is -1 refreshes the metadata and asks the leader it then names', async () => {
    const { admin, cluster, leaders, asked } = harness
    await admin.connect()
    leaders[LEADERLESS] = [1]
    await admin.fetchTopicOffsets(LEADERLESS)
    // The partition loses its leader, and a later answer for every topic is cached as it is.
    leaders[LEADERLESS] = [-1]
    cluster.brokerPool.metadataExpireAt = 0
    await admin.listTopics()
    leaders[LEADERLESS] = [2]
    const refreshes = asked('Metadata').length

    await expect(admin.fetchTopicOffsets(LEADERLESS)).resolves.toEqual([
      { partition: 0, offset: '5', high: '5', low: '5' },
    ])
    expect(asked('Metadata').length).toBe(refreshes + 1)
    expect(
      asked('ListOffsets')
        .slice(-2)
        .map(({ nodeId }) => nodeId)
    ).toEqual([2, 2])
  })

  describe('deleteTopicRecords', () => {
    const records = [{ partition: 0, offset: '3' }]

    /**
     * The admin with an answer for every topic cached - the partition's leader -1 - and its offsets
     * read without a refresh or a leader lookup, so the lookup under test is `deleteTopicRecords`'
     * own.
     */
    const cachedLeaderless = async () => {
      const { admin, cluster, leaders } = harness
      await admin.connect()
      await admin.listTopics()
      jest.spyOn(cluster, 'addTargetTopic').mockResolvedValue()
      jest
        .spyOn(cluster, 'fetchTopicsOffset')
        .mockImplementation(async ([{ topic }]) => [
          { topic, partitions: [{ partition: 0, offset: '5' }] },
        ])
      jest.spyOn(cluster, 'refreshMetadata')
      jest.spyOn(cluster, 'findBroker')
      return { admin, cluster, leaders }
    }

    test('looks the leader up inside its retrier: a leader of -1 refreshes the metadata before any request, and the next attempt deletes', async () => {
      const { admin, cluster, leaders } = await cachedLeaderless()
      cluster.refreshMetadata.mockImplementation(async () => {
        leaders[LEADERLESS] = [2]
        await cluster.brokerPool.refreshMetadata([])
      })

      await admin.deleteTopicRecords({ topic: LEADERLESS, partitions: records })

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(cluster.findBroker.mock.calls.map(([{ nodeId }]) => nodeId)).toEqual(['2'])
      const [refreshed] = cluster.refreshMetadata.mock.invocationCallOrder
      const [deleted] = cluster.findBroker.mock.invocationCallOrder
      expect(refreshed).toBeLessThan(deleted)
    })

    test('a leader that stays -1 rejects once the retries are spent, LEADER_NOT_AVAILABLE as the cause', async () => {
      const { admin, cluster } = await cachedLeaderless()
      cluster.refreshMetadata.mockImplementation(async () => {})

      const error = await admin
        .deleteTopicRecords({ topic: LEADERLESS, partitions: records })
        .catch((e) => e)

      expect(error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        retryCount: RETRY.retries,
      })
      expect(error.cause).toMatchObject({
        name: 'KafkaJSProtocolError',
        type: 'LEADER_NOT_AVAILABLE',
      })
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(RETRY.retries + 1)
      expect(cluster.findBroker).not.toHaveBeenCalled()
    })

    test("as an admin's first call, reads the topic's metadata before looking its leaders up", async () => {
      const { admin, cluster } = harness
      await admin.connect()

      await admin.deleteTopicRecords({ topic: HEALTHY, partitions: records })

      const broker = await cluster.findBroker({ nodeId: 1 })
      expect(broker.deleteRecords).toHaveBeenCalledWith({
        topics: [{ topic: HEALTHY, partitions: records }],
      })
    })

    describe('on the metadata settings of `kafka.admin()`, whose offsets read refreshes by name', () => {
      beforeEach(() => {
        harness = createLeaderlessCluster({ keepsMetadata: false })
      })

      test('a partition whose leader comes back within the retries is deleted at its new leader', async () => {
        const { admin, leaders, asked } = harness
        let answers = 0
        // The first two answers naming the topic report no leader; the next ones name node 2.
        Object.defineProperty(leaders, LEADERLESS, {
          enumerable: true,
          get: () => (answers++ < 2 ? [-1] : [2]),
        })
        await admin.connect()

        await admin.deleteTopicRecords({ topic: LEADERLESS, partitions: records })

        expect(asked('DeleteRecords').map(({ nodeId }) => nodeId)).toEqual([2])
      })

      test('a partition that stays without a leader rejects as the offsets read does, before any request', async () => {
        const { admin, asked } = harness
        await admin.connect()

        const error = await admin
          .deleteTopicRecords({ topic: LEADERLESS, partitions: records })
          .catch((e) => e)

        expect(error).toMatchObject({
          name: 'KafkaJSNonRetriableError',
          type: 'LEADER_NOT_AVAILABLE',
          retriable: false,
        })
        expect(error.cause).toMatchObject({
          name: 'KafkaJSProtocolError',
          type: 'LEADER_NOT_AVAILABLE',
        })
        expect(asked('DeleteRecords')).toEqual([])
      })

      test('a partition the topic lacks is reported only once the offsets read succeeds: on a topic that stays leaderless, the offsets read fails first', async () => {
        const { admin, asked } = harness
        await admin.connect()

        const error = await admin
          .deleteTopicRecords({ topic: LEADERLESS, partitions: [{ partition: 7, offset: '3' }] })
          .catch((e) => e)

        expect(error).toMatchObject({
          name: 'KafkaJSNonRetriableError',
          type: 'LEADER_NOT_AVAILABLE',
          retriable: false,
        })
        expect(asked('DeleteRecords')).toEqual([])
      })
    })

    test('a partition the metadata does not know rejects at once, unwrapped, before any request', async () => {
      const { admin, cluster } = await cachedLeaderless()

      const error = await admin
        .deleteTopicRecords({ topic: LEADERLESS, partitions: [{ partition: 7, offset: '3' }] })
        .catch((e) => e)

      expect(error).toMatchObject({ name: 'KafkaJSDeleteTopicRecordsError', retriable: false })
      expect(error.partitions).toEqual([
        expect.objectContaining({
          partition: 7,
          error: expect.objectContaining({ name: 'KafkaJSBrokerNotFound' }),
        }),
      ])
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(cluster.findBroker).not.toHaveBeenCalled()
    })

    test('a broker that already deleted its partitions is not asked again when another is retried', async () => {
      const { admin, cluster } = await cachedLeaderless()
      const first = await cluster.findBroker({ nodeId: 1 })
      const second = await cluster.findBroker({ nodeId: 2 })
      second.deleteRecords.mockRejectedValueOnce(
        new KafkaJSConnectionError('Connection error: socket hang up')
      )

      await admin.deleteTopicRecords({
        topic: SPLIT,
        partitions: [
          { partition: 0, offset: '3' },
          { partition: 1, offset: '3' },
        ],
      })

      expect(first.deleteRecords).toHaveBeenCalledTimes(1)
      expect(second.deleteRecords).toHaveBeenCalledTimes(2)
    })

    describe('a retriable failure that names no partition', () => {
      /** The healthy topic: the broker that leads it fails the first `failures` requests. */
      const failing = async (failures) => {
        const { admin, cluster } = await cachedLeaderless()
        const broker = await cluster.findBroker({ nodeId: 1 })
        cluster.findBroker.mockClear()
        let calls = 0
        broker.deleteRecords.mockImplementation(async () => {
          calls += 1
          if (calls <= failures) {
            throw new KafkaJSConnectionError('Connection error: socket hang up')
          }
        })
        const call = admin.deleteTopicRecords({ topic: HEALTHY, partitions: records })
        return { call, broker }
      }

      test('is retried, and the next attempt deletes', async () => {
        const { call, broker } = await failing(1)

        await expect(call).resolves.toBeUndefined()
        expect(broker.deleteRecords).toHaveBeenCalledTimes(2)
      })

      test('that persists rejects once the retries are spent, the connection error as the cause', async () => {
        const { call, broker } = await failing(Infinity)

        const error = await call.catch((e) => e)
        expect(error).toMatchObject({ name: 'KafkaJSNumberOfRetriesExceeded' })
        expect(error.cause).toMatchObject({ name: 'KafkaJSConnectionError' })
        expect(broker.deleteRecords).toHaveBeenCalledTimes(RETRY.retries + 1)
      })
    })

    test('a partition error that says the metadata is stale still refreshes it', async () => {
      const { admin, cluster } = await cachedLeaderless()
      const broker = await cluster.findBroker({ nodeId: 1 })
      broker.deleteRecords.mockRejectedValueOnce(
        new KafkaJSDeleteTopicRecordsError({
          partitions: [
            { partition: 0, offset: '3', error: createErrorFromCode(NOT_LEADER_OR_FOLLOWER) },
          ],
        })
      )

      await admin.deleteTopicRecords({ topic: HEALTHY, partitions: records })

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(broker.deleteRecords).toHaveBeenCalledTimes(2)
    })

    test('a partition refused for good fails the call at once, with no refresh, even beside a stale one', async () => {
      const { admin, cluster } = await cachedLeaderless()
      const first = await cluster.findBroker({ nodeId: 1 })
      first.deleteRecords.mockRejectedValueOnce(
        new KafkaJSDeleteTopicRecordsError({
          partitions: [
            { partition: 0, offset: '3', error: createErrorFromCode(NOT_LEADER_OR_FOLLOWER) },
            { partition: 0, offset: '3', error: createErrorFromCode(OFFSET_OUT_OF_RANGE) },
          ],
        })
      )

      const error = await admin
        .deleteTopicRecords({ topic: HEALTHY, partitions: records })
        .catch((e) => e)

      expect(error).toMatchObject({ name: 'KafkaJSNonRetriableError' })
      expect(error.cause).toMatchObject({
        name: 'KafkaJSDeleteTopicRecordsError',
        retriable: false,
      })
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(first.deleteRecords).toHaveBeenCalledTimes(1)
    })
  })
})

describe('Admin > deleteTopicRecords at DeleteRecords v2', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  /** @returns {{ hex: string, fields: object }} a vector Apache Kafka's own codec produced */
  const vector = (kind, caseId) =>
    require(
      `../../protocol/requests/deleteRecords/fixtures/java/deleteRecords${kind}-v2-${caseId}.json`
    )

  /** An admin whose partition leaders are one real broker on a fake socket offering v2. */
  const adminOver = (responseCase) => {
    harness = createGroupApiBroker({
      versions: { [apiKeys.DeleteRecords]: { minVersion: 0, maxVersion: 2 } },
      respond: () => Buffer.from(vector('Response', responseCase).hex, 'hex'),
    })
    return createAdmin({
      logger: newLogger(),
      retry: { retries: 0 },
      cluster: {
        addTargetTopic: async () => {},
        refreshMetadataIfNecessary: async () => {},
        refreshMetadata: async () => {},
        findTopicPartitionMetadata: () => [{ partitionId: 0 }, { partitionId: 2 }],
        fetchTopicsOffset: async ([{ topic, partitions }]) => [
          { topic, partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })) },
        ],
        findLeaderForPartitions: (topic, partitions) => ({ 1: partitions.map(Number) }),
        findBroker: async () => harness.broker,
      },
    })
  }

  const { fields } = vector('Request', 'basic')
  const [{ topic, partitions }] = fields.topics

  test("sends the request Apache Kafka's codec writes and resolves", async () => {
    const admin = adminOver('ok')

    await expect(admin.deleteTopicRecords({ topic, partitions })).resolves.toBeUndefined()
    expect(harness.requests[0].apiVersion).toEqual(2)
    expect(harness.requests[0].body.toString('hex')).toEqual(vector('Request', 'basic').hex)
  })

  test('a partition the broker refuses rejects, naming it', async () => {
    const admin = adminOver('error')

    const error = await admin.deleteTopicRecords({ topic, partitions }).catch((e) => e)

    expect(error).toMatchObject({ name: 'KafkaJSNonRetriableError' })
    expect(error.cause).toMatchObject({ name: 'KafkaJSDeleteTopicRecordsError' })
    expect(error.cause.partitions).toEqual([
      expect.objectContaining({
        partition: 2,
        error: expect.objectContaining({ type: 'OFFSET_OUT_OF_RANGE' }),
      }),
    ])
  })
})
