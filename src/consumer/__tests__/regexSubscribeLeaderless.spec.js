const Cluster = require('../../cluster')
const createConsumer = require('../index')
const { requests } = require('../../protocol/requests')
const { NODE_ID, TOPIC, createFetchGroup, fetchResponse } = require('./fixtures/fetchHarness')
const { connectionOpts, newLogger } = require('testHelpers')

const LEADER_NOT_AVAILABLE = 5
const NODES = [1, 2].map((nodeId) => ({ nodeId, host: `kafka${nodeId}`, port: 9092, rack: null }))
// Spent within a few milliseconds: a Metadata answer that fails keeps failing here.
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 2 }

/**
 * A real `Cluster` over fake brokers whose Metadata answers go through the parse the request
 * selects (`requests.Metadata` at v13). `orders-1` and `orders-2` have a leader; `audit`'s only
 * partition has none, as a KRaft broker reports it.
 * @returns {{ cluster: Cluster, asked: string[][] }} `asked` holds the topics of every Metadata
 *   request, `[]` for every topic
 */
const createCluster = () => {
  const leaders = { 'orders-1': 1, 'orders-2': 2, audit: -1 }
  const asked = []
  const cluster = new Cluster({ ...connectionOpts(), logger: newLogger(), retry: RETRY })
  const pool = cluster.brokerPool

  pool.buildConnectionPool = async ({ host = 'kafka1', port = 9092, rack = null } = {}) => ({
    host,
    port,
    rack,
    destroy: async () => {},
  })

  pool.createBroker = ({ connectionPool, nodeId }) => {
    let connected = false
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
        asked.push(topics)
        const { response } = requests.Metadata.protocol({ version: 13 })({ topics })
        return response.parse({
          throttleTime: 0,
          clientSideThrottleTime: 0,
          errorCode: 0,
          brokers: NODES,
          clusterId: 'cluster-a',
          controllerId: 1,
          topicMetadata: (topics.length > 0 ? topics : Object.keys(leaders)).map((topic) => ({
            topicErrorCode: 0,
            topic,
            topicId: null,
            isInternal: false,
            partitionMetadata: [
              {
                partitionErrorCode: leaders[topic] < 0 ? LEADER_NOT_AVAILABLE : 0,
                partitionId: 0,
                leader: leaders[topic],
                leaderEpoch: 0,
                replicas: [3],
                isr: [],
                offlineReplicas: [],
              },
            ],
          })),
        })
      },
    }
  }

  return { cluster, asked }
}

describe('Consumer > a partition with no leader elsewhere in the cluster', () => {
  let cluster

  afterEach(async () => {
    await cluster?.disconnect()
    cluster = null
  })

  test('a regex subscription reads the metadata of every topic and subscribes to the topics it matches', async () => {
    const created = createCluster()
    cluster = created.cluster
    const consumer = createConsumer({ cluster, groupId: 'group-a', logger: newLogger() })

    await expect(consumer.subscribe({ topics: [/^orders-/] })).resolves.toBeUndefined()

    expect([...cluster.targetTopics].sort()).toEqual(['orders-1', 'orders-2'])
    expect(created.asked.at(-1).sort()).toEqual(['orders-1', 'orders-2'])
  })
})

describe('Consumer > the fetch grouping, for a partition whose cached leader is negative', () => {
  // As for any stale-metadata error raised before the request: the whole round is skipped and
  // the next one is built on the refreshed metadata.
  test('a fetch round sends no request, refreshes the metadata instead and resolves with no batch', async () => {
    const { group, cluster, findBrokerCalls, setResponse } = await createFetchGroup({
      partitions: [0, 1],
      partitionLeaders: { 0: NODE_ID, 1: -1 },
    })
    setResponse(fetchResponse([{ partition: 0, errorCode: 0 }]))

    await expect(group.fetch(NODE_ID)).resolves.toEqual([])
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(findBrokerCalls).toEqual([])
  })

  test.each([-1, -2])(
    'is read from nowhere at %d, its preferred read replica included',
    async (leader) => {
      const { group } = await createFetchGroup({
        partitions: [0],
        partitionLeaders: { 0: leader },
        preferredReadReplica: 2,
      })

      expect(() => group.findReadReplicaForPartitions(TOPIC, [0])).toThrow(
        expect.objectContaining({ type: 'LEADER_NOT_AVAILABLE', code: 5, retriable: true })
      )
    }
  )

  test.each([null, undefined])(
    'a leader of %s is invalid partition metadata, not a node to read from',
    async (leader) => {
      const { group } = await createFetchGroup({
        partitions: [0],
        partitionLeaders: { 0: leader },
        preferredReadReplica: 2,
      })

      expect(() => group.findReadReplicaForPartitions(TOPIC, [0])).toThrow(
        'Invalid partition metadata'
      )
    }
  )
})
