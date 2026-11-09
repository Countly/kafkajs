const Cluster = require('../../cluster')
const createAdmin = require('..')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { newLogger } = require('testHelpers')

const NODES = [1, 2, 3].map((id) => ({ id, host: `node${id}`, port: 9092 }))
const RETRY = { retries: 3, initialRetryTime: 1, maxRetryTime: 2, factor: 0 }

/**
 * A broker snapshot before or after node 3 stops. Its only partition loses its leader.
 * @param {boolean} stopped whether node 3 has stopped
 * @returns {object} Metadata v13 response fields
 */
const metadata = (stopped) => ({
  brokers: NODES.filter(({ id }) => !stopped || id !== 3).map(({ id, ...address }) => ({
    nodeId: id,
    ...address,
    rack: null,
  })),
  clusterId: 'leaderless-cluster',
  controllerId: 1,
  topics: [
    {
      name: 'leaderless',
      topicId: '11111111-1111-4111-8111-111111111111',
      errorCode: 0,
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitions: [
        {
          errorCode: stopped ? 5 : 0,
          partitionIndex: 0,
          leaderId: stopped ? -1 : 3,
          leaderEpoch: 1,
          replicas: [3],
          isr: stopped ? [] : [3],
          offlineReplicas: stopped ? [3] : [],
        },
      ],
    },
  ],
})

describe('Admin > all-topic metadata after a broker stops', () => {
  afterEach(() => jest.restoreAllMocks())

  test.each(['describeCluster', 'listTopics', 'fetchTopicMetadata'])(
    '%s reaches a live broker when the stopped broker is selected first on every retry',
    async (method) => {
      // Pin the shuffle to [3, 1, 2] so a retry cannot hide the stopped broker.
      jest.spyOn(Math, 'random').mockReturnValue(0.4)
      const wire = createScriptedCluster({ nodes: NODES })
      let stopped = false
      wire.answer('Metadata', () => metadata(stopped))
      const cluster = new Cluster({
        ...wire.kafkaOptions({ brokers: ['node1:9092'], retry: RETRY }),
        logger: newLogger(),
      })
      const admin = createAdmin({ cluster, logger: newLogger(), retry: RETRY })
      try {
        await admin.connect()
        await cluster.refreshMetadata()
        await cluster.findBroker({ nodeId: 3 })
        const oldSocket = wire.sockets().find(({ endpoint }) => endpoint.host === 'node3')
        stopped = true
        wire.down(3)
        oldSocket.destroy()
        await new Promise((resolve) => setImmediate(resolve))
        const before = wire.requests().length

        const result = await admin[method]()

        if (method === 'describeCluster') {
          expect(result).toEqual({
            brokers: [
              { nodeId: 1, host: 'node1', port: 9092 },
              { nodeId: 2, host: 'node2', port: 9092 },
            ],
            controller: 1,
            clusterId: 'leaderless-cluster',
          })
        } else if (method === 'listTopics') {
          expect(result).toEqual(['leaderless'])
        } else {
          expect(result.topics).toEqual([
            expect.objectContaining({
              name: 'leaderless',
              partitions: [expect.objectContaining({ partitionErrorCode: 5, leader: -1 })],
            }),
          ])
        }
        expect(
          wire
            .requests()
            .slice(before)
            .map(({ node, api }) => ({ node, api }))
        ).toEqual([{ node: 1, api: 'Metadata' }])
        expect(wire.sockets().filter(({ endpoint }) => endpoint.host === 'node3')).toHaveLength(2)
        expect(oldSocket.destroyed).toBe(true)
      } finally {
        await admin.disconnect()
        wire.assertClean()
        expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
      }
    }
  )
})
