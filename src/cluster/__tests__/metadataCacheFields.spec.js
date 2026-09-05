const { createCluster, createTopic, newLogger, secureRandom } = require('testHelpers')
const { Metadata: METADATA_API_KEY } = require('../../protocol/requests/apiKeys')

describe('Cluster > metadata cache (Metadata v12)', () => {
  let cluster
  let topicName

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    await createTopic({ topic: topicName })
    cluster = createCluster({ logger: newLogger() })
    await cluster.connect()
    await cluster.addTargetTopic(topicName)
    await cluster.refreshMetadata()
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('broker advertises at least v12 and the client caches the new fields', async () => {
    const broker = await cluster.findControllerBroker()
    expect(broker.versions[METADATA_API_KEY].maxVersion).toBeGreaterThanOrEqual(12)

    const metadata = cluster.brokerPool.metadata
    const topic = metadata.topicMetadata.find((entry) => entry.topic === topicName)

    expect(topic.topicId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
    expect(topic.topicAuthorizedOperations).toEqual(-2147483648)
    for (const partition of topic.partitionMetadata) {
      expect(partition.leaderEpoch).toBeGreaterThanOrEqual(0)
    }
  })

  test('still resolves leaders exactly as before', () => {
    const partitions = cluster.findTopicPartitionMetadata(topicName)
    expect(partitions.length).toBeGreaterThan(0)
    expect(partitions[0].partitionId).toEqual(0)

    const leaders = cluster.findLeaderForPartitions(topicName, [0])

    expect(Object.keys(leaders)).toEqual([String(partitions[0].leader)])
    expect(leaders[partitions[0].leader]).toEqual([0])
  })
})
