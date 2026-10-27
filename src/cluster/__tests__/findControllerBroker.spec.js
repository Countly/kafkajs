const { createCluster } = require('testHelpers')
const { KafkaJSMetadataNotLoaded, KafkaJSBrokerNotFound } = require('../../errors')

describe('Cluster > findControllerBroker', () => {
  let cluster

  beforeEach(async () => {
    cluster = createCluster()
    cluster.brokerPool.metadata = { controllerId: '0' }
    cluster.findBroker = jest.fn()
  })

  test('finds the broker of the controller', async () => {
    cluster.findBroker.mockImplementationOnce(() => true)
    await expect(cluster.findControllerBroker()).resolves.toEqual(true)
    expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '0' })
  })

  test('throws KafkaJSTopicMetadataNotLoaded if metadata is not loaded', async () => {
    cluster.brokerPool.metadata = null
    await expect(cluster.findControllerBroker()).rejects.toThrow(KafkaJSMetadataNotLoaded)
  })

  test('throws KafkaJSTopicMetadataNotLoaded if the controllerId is invalid', async () => {
    cluster.brokerPool.metadata = { controllerId: undefined }
    await expect(cluster.findControllerBroker()).rejects.toThrow(KafkaJSMetadataNotLoaded)
  })

  test('throws KafkaJSBrokerNotFound naming the controller when the broker table has no such broker', async () => {
    cluster.findBroker.mockResolvedValueOnce(undefined)

    const error = await cluster.findControllerBroker().catch((thrown) => thrown)

    expect(error).toBeInstanceOf(KafkaJSBrokerNotFound)
    expect(error.message).toBe('Controller broker with id 0 not found in the cached metadata')
  })

  test('throws KafkaJSBrokerNotFound if the node is not in the cache', async () => {
    cluster.findBroker.mockImplementationOnce(() => {
      throw new KafkaJSBrokerNotFound('not found')
    })

    await expect(cluster.findControllerBroker()).rejects.toThrow(KafkaJSBrokerNotFound)
  })
})
