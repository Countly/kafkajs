const {
  createCluster,
  secureRandom,
  createTopic,
  createConnectionPool,
  newLogger,
} = require('testHelpers')

const Broker = require('../../broker')
const {
  KafkaJSLockTimeout,
  KafkaJSConnectionError,
  KafkaJSBrokerNotFound,
} = require('../../errors')

describe('Cluster > findBroker', () => {
  let cluster, topic

  beforeEach(async () => {
    topic = `test-topic-${secureRandom()}`
    cluster = createCluster()

    await createTopic({ topic })
    await cluster.connect()
    await cluster.addTargetTopic(topic)
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('returns the broker given by the broker pool', async () => {
    cluster.brokerPool.findBroker = jest.fn()
    const nodeId = 1
    await cluster.findBroker({ nodeId })
    expect(cluster.brokerPool.findBroker).toHaveBeenCalledWith({ nodeId })
  })

  test('refresh metadata on lock timeout', async () => {
    // The KRaft test cluster numbers its nodes 1-3 (`docker-compose.yml`); there is no broker 0, so a
    // refreshMetadata() would drop a mock installed under it.
    const nodeId = cluster.brokerPool.getNodeIds()[0]
    const mockBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })

    jest.spyOn(mockBroker, 'connect').mockImplementationOnce(() => {
      throw new KafkaJSLockTimeout('Timeout while acquiring lock')
    })

    jest.spyOn(cluster, 'refreshMetadata')
    cluster.brokerPool.brokers[nodeId] = mockBroker

    await expect(cluster.findBroker({ nodeId })).rejects.toHaveProperty(
      'name',
      'KafkaJSLockTimeout'
    )

    await expect(cluster.findBroker({ nodeId })).resolves.toBeInstanceOf(Broker)
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('refresh metadata on KafkaJSConnectionError ECONNREFUSED', async () => {
    // The KRaft test cluster numbers its nodes 1-3 (`docker-compose.yml`); there is no broker 0, so a
    // refreshMetadata() would drop a mock installed under it.
    const nodeId = cluster.brokerPool.getNodeIds()[0]
    const mockBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })

    jest.spyOn(mockBroker, 'connect').mockImplementationOnce(() => {
      throw new KafkaJSConnectionError('Connection error: ECONNREFUSED', { code: 'ECONNREFUSED' })
    })

    jest.spyOn(cluster, 'refreshMetadata')
    cluster.brokerPool.brokers[nodeId] = mockBroker

    await expect(cluster.findBroker({ nodeId })).rejects.toHaveProperty(
      'name',
      'KafkaJSConnectionError'
    )

    await expect(cluster.findBroker({ nodeId })).resolves.toBeInstanceOf(Broker)
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('refresh metadata on KafkaJSBrokerNotFound', async () => {
    // The KRaft test cluster numbers its nodes 1-3 (`docker-compose.yml`); there is no broker 0, so a
    // refreshMetadata() would drop a mock installed under it.
    const nodeId = cluster.brokerPool.getNodeIds()[0]
    cluster.brokerPool.findBroker = jest.fn(() => {
      throw new KafkaJSBrokerNotFound('Broker not found')
    })

    jest.spyOn(cluster, 'refreshMetadata')

    await expect(cluster.findBroker({ nodeId })).rejects.toHaveProperty(
      'name',
      'KafkaJSBrokerNotFound'
    )

    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('refresh metadata on KafkaJSConnectionError Connection Timeout', async () => {
    // The KRaft test cluster numbers its nodes 1-3 (`docker-compose.yml`); there is no broker 0, so a
    // refreshMetadata() would drop a mock installed under it.
    const nodeId = cluster.brokerPool.getNodeIds()[0]
    const mockBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })

    jest.spyOn(mockBroker, 'connect').mockImplementationOnce(() => {
      throw new KafkaJSConnectionError('Connection timeout', { broker: mockBroker })
    })

    jest.spyOn(cluster, 'refreshMetadata')
    cluster.brokerPool.brokers[nodeId] = mockBroker

    await expect(cluster.findBroker({ nodeId })).rejects.toHaveProperty(
      'name',
      'KafkaJSConnectionError'
    )

    await expect(cluster.findBroker({ nodeId })).resolves.toBeInstanceOf(Broker)
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })
})
