const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')

const TOPIC = 'test-topic'
const NODE_ID = 0

/**
 * A `ConsumerGroup` whose join/sync dance is skipped - `subscriptionState` and `offsetManager`
 * are primed directly, mirroring `checkCrcsCrash.spec.js` - and whose broker is a plain stub
 * resolving with an already-decoded Fetch v12 response. Decoding itself is covered by
 * `fetch/v12/response.spec.js`; this goes straight to the shape `ConsumerGroup#fetch` reads, to
 * isolate the one thing this spec is about: the partition-error block right after `broker.fetch`
 * returns.
 * @param {{ fetch: Function }} broker
 * @returns {{ consumerGroup: ConsumerGroup, cluster: object }}
 */
const createConsumerGroup = (broker) => {
  const cluster = {
    refreshMetadataIfNecessary: async () => {},
    refreshMetadata: jest.fn(async () => {}),
    findBroker: async () => broker,
    findTopicPartitionMetadata: () => [{ partitionId: 0, leader: NODE_ID }],
    committedOffsets: () => ({}),
    topicId: () => undefined,
    leaderEpoch: () => -1,
  }

  const consumerGroup = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'test-group',
    topics: [TOPIC],
    topicConfigurations: { [TOPIC]: {} },
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    metadataMaxAge: 300000,
  })

  consumerGroup.subscriptionState.assign([{ topic: TOPIC, partitions: [0] }])
  consumerGroup.offsetManager = new OffsetManager({
    cluster,
    coordinator: broker,
    memberAssignment: { [TOPIC]: [0] },
    autoCommit: false,
    topicConfigurations: { [TOPIC]: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 0,
    memberId: 'member-1',
  })
  // A real join would have resolved this from OffsetFetch; priming it directly skips the join
  // dance while keeping `ConsumerGroup#fetch` itself completely real.
  consumerGroup.offsetManager.committedOffsets()[TOPIC][0] = '0'

  return { consumerGroup, cluster }
}

/** A Fetch v12-shaped decoded response with one partition failing with `errorCode`. */
const v12Response = (errorCode) => ({
  responses: [
    {
      topicName: TOPIC,
      partitions: [
        {
          partition: 0,
          errorCode,
          highWatermark: '10',
          lastStableOffset: '10',
          lastStartOffset: '0',
          abortedTransactions: [],
          preferredReadReplica: -1,
          divergingEpoch: null,
          currentLeader: null,
          snapshotId: null,
          messages: [],
        },
      ],
    },
  ],
})

describe('Consumer > ConsumerGroup > fetch > v12 partition errors are surfaced', () => {
  test('an OFFSET_OUT_OF_RANGE partition reaches recoverFromOffsetOutOfRange', async () => {
    const broker = { fetch: jest.fn().mockResolvedValue(v12Response(1)) }
    const { consumerGroup } = createConsumerGroup(broker)
    const recover = jest.spyOn(consumerGroup, 'recoverFromOffsetOutOfRange').mockResolvedValue()

    await consumerGroup.fetch(NODE_ID)

    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'KafkaJSOffsetOutOfRange', topic: TOPIC, partition: 0 })
    )
  })

  test('another code (NOT_LEADER_FOR_PARTITION) reaches recoverFromFetch, not offset recovery', async () => {
    const broker = { fetch: jest.fn().mockResolvedValue(v12Response(6)) }
    const { consumerGroup, cluster } = createConsumerGroup(broker)
    const recover = jest.spyOn(consumerGroup, 'recoverFromOffsetOutOfRange').mockResolvedValue()
    const joinAndSync = jest.spyOn(consumerGroup, 'joinAndSync').mockResolvedValue()

    await consumerGroup.fetch(NODE_ID)

    expect(recover).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).toHaveBeenCalled()
    expect(joinAndSync).toHaveBeenCalled()
  })
})
