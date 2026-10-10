const { partitionResults } = require('../../../protocol/requests/produce/partitionResults')

/**
 * The cluster surface a producer reads - its send path, its eosManager and its transactions -
 * over one broker that leads every partition, answers InitProducerId with producer id 1000 at
 * epoch 0, and answers every Produce with success unless told otherwise - per partition, as
 * `Broker#produce` resolves it, when the request asks for `perPartitionResults`. Every broker
 * method is a `jest.fn`, so a spec can read what was sent. For unit specs that drive a real
 * producer without a cluster.
 * @param {object} [options]
 * @param {number} [options.partitions=1] partitions per topic
 * @param {(request: object) => Promise<object>} [options.produce] replaces the Produce answer
 * @param {object} [options.options={}] what a `Cluster` constructor was given, kept as `options`
 * @returns {object} the cluster; `broker` is its one broker
 */
const createFakeCluster = ({ partitions = 1, produce, options = {} } = {}) => {
  const succeed = async ({ topicData }) => ({
    topics: topicData.map(({ topic, partitions: written }) => ({
      topicName: topic,
      partitions: written.map(({ partition }) => ({
        partition,
        errorCode: 0,
        baseOffset: '0',
        logAppendTime: '-1',
        logStartOffset: '0',
      })),
    })),
  })
  const answer = produce || succeed
  const broker = {
    nodeId: 0,
    initProducerId: jest.fn(async () => ({ producerId: '1000', producerEpoch: 0 })),
    produce: jest.fn(async (request) => {
      const response = await answer(request)
      return request.perPartitionResults ? partitionResults(response) : response
    }),
    addPartitionsToTxn: jest.fn(async () => ({})),
    endTxn: jest.fn(async () => ({})),
  }
  const targetTopics = new Set()

  return {
    options,
    broker,
    retry: options.retry,
    targetTopics,
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    isConnected: () => true,
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    addMultipleTargetTopics: jest.fn(async (topics) => topics.forEach((t) => targetTopics.add(t))),
    findTopicPartitionMetadata: () =>
      Array.from({ length: partitions }, (_, partitionId) => ({ partitionId, leader: 0 })),
    findLeaderForPartitions: (topic, ids) => ({ 0: ids.map(Number) }),
    findBroker: jest.fn(async () => broker),
    findConnectedBroker: jest.fn(async () => broker),
    findGroupCoordinator: jest.fn(async () => broker),
    topicId: () => undefined,
    removeBroker: jest.fn(),
    markOffsetAsCommitted: jest.fn(),
  }
}

module.exports = { createFakeCluster }
