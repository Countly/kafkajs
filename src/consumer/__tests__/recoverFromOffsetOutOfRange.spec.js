const ConsumerGroup = require('../consumerGroup')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')

describe('Consumer > ConsumerGroup > recoverFromOffsetOutOfRange', () => {
  let consumerGroup, setDefaultOffset

  beforeEach(() => {
    setDefaultOffset = jest.fn(async () => {})
    consumerGroup = new ConsumerGroup({
      logger: newLogger(),
      topics: ['topic-a'],
      topicConfigurations: {},
      cluster: { getNodeIds: () => [], isConnected: () => true },
      groupId: 'g',
      assigners: [],
      instrumentationEmitter: new InstrumentationEventEmitter(),
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      maxBytesPerPartition: 1024,
      minBytes: 1,
      maxBytes: 1024,
      maxWaitTimeInMs: 100,
      isolationLevel: 1,
      metadataMaxAge: 1000,
    })
    consumerGroup.offsetManager = { setDefaultOffset }
  })

  it('retries from the leader when a preferred read replica is active (KIP-392)', async () => {
    consumerGroup.preferredReadReplicasPerTopicPartition = {
      'topic-a': { 0: { nodeId: 3, expireAt: Date.now() + 60000 } },
    }

    await consumerGroup.recoverFromOffsetOutOfRange({ topic: 'topic-a', partition: 0 })

    expect(setDefaultOffset).not.toHaveBeenCalled()
    expect(consumerGroup.preferredReadReplicasPerTopicPartition['topic-a'][0]).toBeUndefined()
  })

  it('resets the offset when there is no preferred read replica', async () => {
    consumerGroup.preferredReadReplicasPerTopicPartition = { 'topic-a': {} }

    await consumerGroup.recoverFromOffsetOutOfRange({ topic: 'topic-a', partition: 0 })

    expect(setDefaultOffset).toHaveBeenCalledWith({ topic: 'topic-a', partition: 0 })
  })
})
