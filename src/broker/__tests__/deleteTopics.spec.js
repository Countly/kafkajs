const { createConnectionPool, connectionOpts, secureRandom, newLogger } = require('testHelpers')

const Broker = require('../index')
const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

describe('Broker > deleteTopics', () => {
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
    })
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    await broker.connect()
    const topicName1 = `test-topic-${secureRandom()}`
    const topicName2 = `test-topic-${secureRandom()}`

    await broker.createTopics({
      topics: [{ topic: topicName1 }, { topic: topicName2 }],
    })

    const response = await broker.deleteTopics({
      topics: [topicName1, topicName2],
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      // From v5 each topic result also carries an error message and, from v6, the topic id.
      topicErrors: [
        { topic: topicName1, errorCode: 0 },
        { topic: topicName2, errorCode: 0 },
      ]
        .sort(topicNameComparator)
        .map((topicError) => expect.objectContaining(topicError)),
    })
  })
})
