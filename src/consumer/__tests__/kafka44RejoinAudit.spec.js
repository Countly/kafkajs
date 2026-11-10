const ConsumerGroup = require('../consumerGroup')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { KafkaJSStaleTopicMetadataAssignment } = require('../../errors')
const { newLogger } = require('testHelpers')

const TOPIC = 'rack-topic'
const OFFLINE = [4, 5]

/**
 * A cluster view the test edits between metadata updates: brokers with racks, and partitions with
 * replicas. `findTopicPartitionMetadata` is the shape the consumer reads after a metadata refresh.
 * @returns {{ brokers: Map<number, string|null>, partitions: object[], cluster: object }}
 */
const createTopology = () => {
  const brokers = new Map([
    [1, 'rack-a'],
    [2, 'rack-a'],
    [3, 'rack-b'],
    [6, 'rack-c'],
  ])
  const partitions = [0, 1].map((partitionId) => ({
    partitionId,
    leader: 1,
    replicas: [1, 2, 3],
    isr: [1, 2, 3],
  }))
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    committedOffsets: () => ({}),
    topicId: () => undefined,
    findGroupCoordinator: jest.fn(async () => ({ isConnected: () => true })),
    findTopicPartitionMetadata: (topic) =>
      topic === TOPIC
        ? partitions.map((partition) => ({
            ...partition,
            offlineReplicas: partition.replicas.filter((id) => !brokers.has(id)),
          }))
        : [],
  }
  return { brokers, partitions, cluster }
}

/**
 * A real `ConsumerGroup` for a member configured with `rackId: 'rack-a'`, joined to generation 1
 * with the topic's current partitions, and a coordinator that records rejoin requests.
 */
const createRackGroup = (cluster) => {
  const group = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'rack-group',
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
    rackId: 'rack-a',
    checkCrcs: true,
    metadataMaxAge: 300000,
  })
  const requestRejoin = jest.fn()
  group.memberId = 'member-1'
  group.generationId = 1
  group.coordinator = {
    isConnected: () => true,
    nodeId: 0,
    offsetFetch: jest.fn(async () => ({ responses: [] })),
    offsetCommit: jest.fn(async () => {}),
  }
  group.rebalanceCoordinator = {
    classify: jest.fn(() => ({ kind: 'revoked', reason: 'stale-assignment' })),
    requestRejoin,
  }
  group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()
  return { group, requestRejoin }
}

/*
 * Fork-constructed: the fork's rejoin trigger is `ConsumerGroup#checkForStaleAssignment`, which
 * compares a topic's partition ids only. Java (apache/kafka PR #22768, KAFKA-20778) also compares
 * the racks of replicas, so it rejoins on the last three metadata changes below and the fork does
 * not. Rack-triggered rebalancing is not implemented here.
 */
describe('Kafka 4.4 client fix audit', () => {
  test('KAFKA-20778 a replica bounce requests no rejoin from a rack-configured classic member', async () => {
    const { brokers, partitions, cluster } = createTopology()
    const { group, requestRejoin } = createRackGroup(cluster)
    const assignment = group.partitionsPerSubscribedTopic
    const steps = [
      {
        name: 'a nonleader replica broker leaves and returns',
        updates: [() => brokers.delete(3), () => brokers.set(3, 'rack-b')],
      },
      {
        name: 'a redundant same-rack replica loses its rack and gets it back',
        updates: [() => brokers.set(2, null), () => brokers.set(2, 'rack-a')],
      },
      {
        name: 'a replica moves to an offline broker and the broker comes online',
        updates: [
          () => (partitions[0].replicas = [1, 2, OFFLINE[0]]),
          () => brokers.set(OFFLINE[0], 'rack-b'),
        ],
      },
      {
        name: 'a replica moves to a new rack while its broker is offline',
        updates: [
          () => (partitions[1].replicas = [1, 2, OFFLINE[1]]),
          () => brokers.set(OFFLINE[1], 'rack-c'),
        ],
      },
      {
        name: 'an online replica loses its rack',
        updates: [() => brokers.set(6, null), () => brokers.set(6, 'rack-c')],
      },
    ]

    for (const { name, updates } of steps) {
      for (const update of updates) {
        update()
        expect(() => group.checkForStaleAssignment()).not.toThrow()
        expect([name, group.staleAssignmentPending]).toEqual([name, false])
      }
    }

    expect(requestRejoin).not.toHaveBeenCalled()
    expect(group.partitionsPerSubscribedTopic).toBe(assignment)
    expect(assignment.get(TOPIC)).toEqual([0, 1])

    // Control: a partition gain is what the fork's trigger does see.
    partitions.push({ partitionId: 2, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] })
    let thrown
    try {
      group.checkForStaleAssignment()
    } catch (error) {
      thrown = error
    }
    expect(thrown).toBeInstanceOf(KafkaJSStaleTopicMetadataAssignment)
    expect(thrown.topic).toBe(TOPIC)
    expect(thrown.unknownPartitions).toEqual([2])
    expect(group.staleAssignmentPending).toBe(true)
    expect(() => group.checkForStaleAssignment()).not.toThrow()

    await group.recoverFromFetch(thrown)
    expect(requestRejoin).toHaveBeenCalledTimes(1)

    await group.applyRebalanceResult(
      { assignment: { [TOPIC]: [0, 1, 2] } },
      { rebuildOffsets: false }
    )
    expect(group.partitionsPerSubscribedTopic.get(TOPIC)).toEqual([0, 1, 2])
    expect(group.staleAssignmentPending).toBe(false)
    expect(() => group.checkForStaleAssignment()).not.toThrow()
  })
})
