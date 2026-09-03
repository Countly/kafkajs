const { createConnectionPool, connectionOpts, secureRandom, newLogger } = require('testHelpers')

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
    const newBrokerData = metadata.brokers.find(b => b.nodeId === metadata.controllerId)

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

  // Quarantined, see QUARANTINE.md: the assertion below requires the reassignment to still be in
  // flight, but an empty partition is reassigned in milliseconds, so under load the list request
  // arrives after it completed and `topics` comes back empty.
  test.skip('request', async () => {
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

    await broker.alterPartitionReassignments({
      topics: [
        {
          topic: topicName,
          partitionAssignment: [{ partition: 0, replicas: [3, 2] }],
        },
      ],
    })

    const response = await broker.listPartitionReassignments({
      topics: [
        {
          topic: topicName,
          partitions: [0],
        },
      ],
    })

    response.topics[0].partitions[0].replicas.sort()
    expect(response).toEqual({
      throttleTime: 0,
      errorCode: 0,
      topics: [
        {
          name: topicName,
          partitions: [
            { partition: 0, replicas: [1, 2, 3], addingReplicas: [2], removingReplicas: [1] },
          ],
        },
      ],
    })

    await broker.deleteTopics({ topics: [topicName] })
  })
})
