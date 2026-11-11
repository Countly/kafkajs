const {
  createConnectionPool,
  connectionOpts,
  secureRandom,
  newLogger,
  waitFor,
  waitForTopicPropagation,
} = require('testHelpers')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')

const Broker = require('../index')

describe('Broker > alterPartitionReassignments', () => {
  let seedBroker, broker

  beforeEach(async () => {
    seedBroker = new Broker({
      connectionPool: createConnectionPool(connectionOpts()),
      logger: newLogger(),
    })
    await seedBroker.connect()

    const metadata = await seedBroker.metadata()
    const newBrokerData = metadata.brokers.find((b) => b.nodeId === metadata.controllerId)

    broker = new Broker({
      connectionPool: createConnectionPool(newBrokerData),
      logger: newLogger(),
      allowAutoTopicCreation: false,
    })
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  // An empty partition is reassigned in milliseconds, so the reassignment is asserted complete: no
  // longer listed, and the partition on its new replicas. The in-flight shape is asserted on the
  // scripted cluster below.
  test('request', async () => {
    await broker.connect()
    const topicName = `test-topic-${secureRandom()}`

    // The KRaft test cluster numbers its nodes 1-3 (`docker-compose.yml`); there is no broker 0.
    await broker.createTopics({
      topics: [
        {
          topic: topicName,
          replicaAssignment: [{ partition: 0, replicas: [1, 3] }],
        },
      ],
    })
    // CreateTopics answers once the controller committed the topic; this broker's own Metadata
    // answers UNKNOWN_TOPIC_OR_PARTITION until it has replayed the record.
    await waitForTopicPropagation(topicName, 1)

    await broker.alterPartitionReassignments({
      topics: [
        {
          topic: topicName,
          partitionAssignment: [{ partition: 0, replicas: [3, 2] }],
        },
      ],
    })

    const observed = []
    await waitFor(
      async () => {
        const listed = await broker.listPartitionReassignments({
          topics: [{ topic: topicName, partitions: [0] }],
        })
        const { topicMetadata } = await broker.metadata([topicName])
        const replicas = [...topicMetadata[0].partitionMetadata[0].replicas].sort()
        observed.push({ listed, replicas })
        return listed.topics.length === 0 && replicas.join() === '2,3'
      },
      { delay: 50, maxWait: 10000 }
    )

    expect(observed.at(-1)).toEqual({
      listed: { throttleTime: 0, clientSideThrottleTime: 0, errorCode: 0, topics: [] },
      replicas: [2, 3],
    })

    await broker.deleteTopics({ topics: [topicName] })
  })
})

describe('Broker > listPartitionReassignments > on the scripted cluster', () => {
  test('decodes a reassignment in flight: the replicas, those being added and those being removed', async () => {
    const cluster = createScriptedCluster({
      nodes: [{ id: 0, host: 'node', port: 9092 }],
      advertise: { ListPartitionReassignments: [0, 0] },
    })
    const broker = new Broker({
      connectionPool: createConnectionPool({
        host: 'node',
        port: 9092,
        socketFactory: cluster.socketFactory,
      }),
      logger: newLogger(),
    })
    cluster.schedule.once(
      { api: 'ListPartitionReassignments' },
      {
        throttleTime: 0,
        errorCode: 0,
        errorMessage: null,
        topics: [
          {
            name: 'moving-topic',
            partitions: [
              {
                partitionIndex: 0,
                replicas: [1, 2, 3],
                addingReplicas: [2],
                removingReplicas: [1],
              },
            ],
          },
        ],
      }
    )

    try {
      await broker.connect()
      const response = await broker.listPartitionReassignments({
        topics: [{ topic: 'moving-topic', partitions: [0] }],
      })

      expect(response).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 0,
        topics: [
          {
            name: 'moving-topic',
            partitions: [
              { partition: 0, replicas: [1, 2, 3], addingReplicas: [2], removingReplicas: [1] },
            ],
          },
        ],
      })
      const [request] = cluster.requests().filter(({ api }) => api === 'ListPartitionReassignments')
      expect(request.fields.topics).toEqual([{ name: 'moving-topic', partitionIndexes: [0] }])
    } finally {
      await broker.disconnect()
    }
    cluster.assertClean()
  })
})
