const { createCluster } = require('testHelpers')
const Broker = require('../../broker')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const {
  KafkaJSBrokerNotFound,
  KafkaJSConnectionError,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../../errors')

const API = apiKeys.UnregisterController
const CAPABLE = { [API]: { minVersion: 0, maxVersion: 0 } }

/** @param {number} nodeId broker id @param {object} versions broker ranges @returns {Broker} a real Broker with its negotiated table, never connected */
const broker = (nodeId, versions) =>
  Object.assign(Object.create(Broker.prototype), { nodeId, versions })

/** @param {object[]} brokers pool contents @returns {object} cluster over a stubbed pool */
const fixture = (brokers) => {
  const cluster = createCluster()
  cluster.brokerPool.brokers = Object.fromEntries(brokers.map((b) => [b.nodeId, b]))
  cluster.refreshMetadataIfNecessary = jest.fn(async () => {})
  cluster.findBroker = jest.fn(async ({ nodeId }) => cluster.brokerPool.brokers[nodeId])
  return cluster
}

describe('Cluster > findBrokerSupporting', () => {
  test('visits known nodes in ascending numeric order and stops at the first capable one', async () => {
    const cluster = fixture([broker(10, CAPABLE), broker(2, {}), broker(1, {}), broker(3, CAPABLE)])

    const found = await cluster.findBrokerSupporting(API, requests.UnregisterController)

    expect(found.nodeId).toBe(3)
    expect(cluster.findBroker.mock.calls.map(([{ nodeId }]) => nodeId)).toEqual(['1', '2', '3'])
    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalledTimes(1)
  })

  test('throws unsupported API only after every known broker lacks the key', async () => {
    const cluster = fixture([broker(1, {}), broker(2, {})])

    const error = await cluster
      .findBrokerSupporting(API, requests.UnregisterController)
      .catch((reason) => reason)

    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error).toMatchObject({ apiKey: API, apiName: 'UnregisterController' })
    expect(cluster.findBroker).toHaveBeenCalledTimes(2)
  })

  test('propagates a connection failure unchanged instead of calling the key unsupported', async () => {
    const cluster = fixture([broker(1, {}), broker(2, CAPABLE)])
    const failure = new KafkaJSConnectionError('refused')
    cluster.findBroker.mockRejectedValueOnce(failure)

    await expect(cluster.findBrokerSupporting(API, requests.UnregisterController)).rejects.toBe(
      failure
    )
    expect(cluster.findBroker).toHaveBeenCalledTimes(1)
  })

  test('throws broker not found when the cluster lists no broker', async () => {
    const cluster = fixture([])

    await expect(
      cluster.findBrokerSupporting(API, requests.UnregisterController)
    ).rejects.toBeInstanceOf(KafkaJSBrokerNotFound)
  })
})
