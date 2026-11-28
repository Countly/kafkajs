jest.mock('../../utils/shuffle')
const shuffle = require('../../utils/shuffle')
shuffle.mockImplementation((brokers) => brokers.sort((a, b) => a > b))

const Broker = require('../../broker')
const { ON_WRITE } = require('../../broker/privateOptions')
const { createCluster, secureRandom } = require('testHelpers')
const { KafkaJSBrokerNotFound, KafkaJSConnectionError } = require('../../errors')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')

describe('Cluster > findGroupCoordinator', () => {
  let cluster, groupId

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
    await cluster.refreshMetadata()
    jest.spyOn(Broker.prototype, 'findGroupCoordinator')
    groupId = `test-group-${secureRandom()}`
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('find the group coordinator', async () => {
    const broker = await cluster.findGroupCoordinator({ groupId })

    expect(Broker.prototype.findGroupCoordinator).toHaveBeenCalledWith({
      groupId,
      coordinatorType: COORDINATOR_TYPES.GROUP,
      [ON_WRITE]: expect.any(Function),
    })
    expect(broker).not.toBeFalsy()
  })

  test('find the coordinator if transactional', async () => {
    const broker = await cluster.findGroupCoordinator({
      groupId,
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    })

    expect(Broker.prototype.findGroupCoordinator).toHaveBeenCalledWith({
      groupId,
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      [ON_WRITE]: expect.any(Function),
    })
    expect(broker).not.toBeFalsy()
  })

  test('refresh the metadata and try again in case of broker not found', async () => {
    jest.spyOn(cluster.brokerPool, 'findBroker').mockImplementationOnce(() => {
      throw new KafkaJSBrokerNotFound('Not found')
    })

    const coordinator = await cluster.findGroupCoordinator({ groupId })
    expect(coordinator).toBe(cluster.brokerPool.brokers[coordinator.nodeId])
    expect(cluster.brokerPool.findBroker).toHaveBeenCalledTimes(4)
  })

  test('attempt to find coordinator across all brokers until one is found', async () => {
    // Node ids come from the broker pool: the KRaft test cluster numbers its nodes 1-3, not 0-2.
    const nodeIds = cluster.brokerPool.getNodeIds()
    const lastNodeId = nodeIds[nodeIds.length - 1]
    const lastBroker = cluster.brokerPool.brokers[lastNodeId]

    for (const nodeId of nodeIds.slice(0, -1)) {
      jest
        .spyOn(cluster.brokerPool.brokers[nodeId], 'findGroupCoordinator')
        .mockImplementation(() => {
          throw new KafkaJSConnectionError('Something went wrong')
        })
    }
    const { host, port } = lastBroker.connectionPool
    jest.spyOn(lastBroker, 'findGroupCoordinator').mockResolvedValue({
      coordinator: { nodeId: lastBroker.nodeId, host, port },
    })

    await expect(cluster.findGroupCoordinator({ groupId })).resolves.toBe(lastBroker)
  })

  test('retry on ECONNREFUSED', async () => {
    const broker = await cluster.findGroupCoordinator({ groupId })
    await broker.disconnect()

    jest.spyOn(broker, 'connect').mockImplementationOnce(() => {
      throw new KafkaJSConnectionError(`Connection error: connect ECONNREFUSED <ip>:<port>`, {
        code: 'ECONNREFUSED',
      })
    })

    await expect(cluster.findGroupCoordinator({ groupId })).resolves.toBeTruthy()
  })
})
