const ConsumerGroup = require('../consumerGroup')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const ConsumerGroupProtocol = require('../groupProtocol/consumer')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { events } = require('../instrumentationEvents')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { joinDeadlineError, stampGeneration } = require('../rebalanceErrorContract')
const { newLogger } = require('testHelpers')

const TOPIC_A = 'events-a'
const TOPIC_B = 'events-b'

const partitionMetadata = (count) =>
  Array.from({ length: count }, (_, partitionId) => ({ partitionId }))

/**
 * A real `ConsumerGroup` over a stubbed cluster and coordinator, with `retry.retries` high enough
 * that `joinAndSync`'s retrier can actually run a second attempt - a `retries: 0` harness would
 * make every retry-arm case pass for the wrong reason.
 */
const createGroup = ({ topics = [TOPIC_A], ...options } = {}) => {
  const committedOffsets = {}
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    committedOffsets: () => committedOffsets,
    // Every `events-*` topic exists with two partitions, whether it was subscribed at construction
    // or grown into the subscription later - `growSubscription`'s cases depend on the latter.
    findTopicPartitionMetadata: (topic) => (/^events-/.test(topic) ? partitionMetadata(2) : []),
    findGroupCoordinator: jest.fn(async () => ({ isConnected: () => true })),
    addMultipleTargetTopics: jest.fn(async () => {}),
    leaderEpoch: () => -1,
  }

  const instrumentationEmitter = new InstrumentationEventEmitter()

  const group = new ConsumerGroup({
    retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 2 },
    cluster,
    groupId: 'group-a',
    topics,
    topicConfigurations: topics.reduce((all, topic) => ({ ...all, [topic]: {} }), {}),
    logger: newLogger(),
    instrumentationEmitter,
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    heartbeatInterval: 3000,
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
    ...options,
  })

  group.memberId = 'member-1'
  group.generationId = 1
  group.coordinator = {
    isConnected: () => true,
    nodeId: 0,
    offsetFetch: jest.fn(async () => ({ responses: [] })),
    offsetCommit: jest.fn(async () => {}),
  }
  group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()

  return group
}

/** Records every RECEIVED_UNSUBSCRIBED_TOPICS payload the group emits. */
const collectUnsubscribedTopics = (group) => {
  const emitted = []
  group.instrumentationEmitter.addListener(events.RECEIVED_UNSUBSCRIBED_TOPICS, ({ payload }) =>
    emitted.push(payload)
  )
  return emitted
}

describe('ConsumerGroup', () => {
  let consumerGroup

  beforeEach(() => {
    consumerGroup = new ConsumerGroup({
      logger: newLogger(),
      topics: ['topic1'],
      cluster: {},
    })
  })

  describe('uncommittedOffsets', () => {
    it("calls the offset manager's uncommittedOffsets", async () => {
      const mockOffsets = { topics: [] }
      consumerGroup.offsetManager = { uncommittedOffsets: jest.fn(() => mockOffsets) }

      expect(consumerGroup.uncommittedOffsets()).toStrictEqual(mockOffsets)
      expect(consumerGroup.offsetManager.uncommittedOffsets).toHaveBeenCalled()
    })
  })

  describe('resetGenerationLocally', () => {
    it('drops the generation and moves the assignment epoch on', () => {
      consumerGroup.groupMembership = { resetGeneration: jest.fn() }
      const before = consumerGroup.assignmentEpoch

      consumerGroup.resetGenerationLocally()

      expect(consumerGroup.groupMembership.resetGeneration).toHaveBeenCalledWith({
        clearMemberId: true,
      })
      expect(consumerGroup.assignmentEpoch).toBe(before + 1)
    })
  })

  describe('resetOffset', () => {
    it("calls the offset manager's resetOffset with the partition", () => {
      consumerGroup.offsetManager = { resetOffset: jest.fn() }

      consumerGroup.resetOffset({ topic: 'topic1', partition: 3 })

      expect(consumerGroup.offsetManager.resetOffset).toHaveBeenCalledTimes(1)
      expect(consumerGroup.offsetManager.resetOffset).toHaveBeenCalledWith({
        topic: 'topic1',
        partition: 3,
      })
    })
  })

  describe('commitOffsets', () => {
    it("calls the offset manager's commitOffsets", async () => {
      consumerGroup.offsetManager = { commitOffsets: jest.fn(() => Promise.resolve()) }

      const offsets = { topics: [{ partitions: [{ offset: '0', partition: 0 }] }] }
      await consumerGroup.commitOffsets(offsets)
      expect(consumerGroup.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
      expect(consumerGroup.offsetManager.commitOffsets).toHaveBeenCalledWith(offsets, undefined)
    })
  })
})

describe('Consumer > ConsumerGroup > the group protocol seam', () => {
  it('builds the classic protocol by default and reports its kind', () => {
    const group = createGroup()
    expect(group.kind).toEqual('classic')
    expect(group.memberEpoch).toBeNull()
    expect(group.groupMembership).toBeInstanceOf(ClassicGroupProtocol)
  })

  it('builds the consumer protocol when asked, with the assignor, the topics and the regex', () => {
    const group = createGroup({
      topics: ['events-a', 'events-b'],
      groupProtocol: 'consumer',
      groupRemoteAssignor: 'uniform',
      subscribedTopicRegex: 'events-.*',
    })

    expect(group.kind).toEqual('consumer')
    expect(group.groupMembership).toBeInstanceOf(ConsumerGroupProtocol)
    // `groupRemoteAssignor` is this layer's name for the protocol's `serverAssignor` option; a
    // constructor that dropped it would leave the wire field null and `groupProtocol` at ''.
    expect(group.groupMembership.serverAssignor).toEqual('uniform')
    expect(group.groupMembership.groupProtocol).toEqual('uniform')
    // `topics` seeds the subscription; nothing else ever writes that field, so an omission here
    // makes a non-regex member send an empty subscription and be assigned nothing.
    expect(group.groupMembership.subscribedTopicNames).toEqual(['events-a', 'events-b'])
    expect(group.groupMembership.subscribedTopicRegex).toEqual('events-.*')
  })

  it('gives the consumer protocol the client retry configuration for its backoff', () => {
    const group = createGroup({
      groupProtocol: 'consumer',
      retry: { retries: 7, initialRetryTime: 11, maxRetryTime: 13 },
    })

    expect(group.groupMembership.backoff.maxRetryTime).toEqual(13)
  })

  it('passes the configured heartbeat interval to the classic protocol', () => {
    const group = createGroup({ heartbeatInterval: 5000 })

    expect(group.groupMembership.heartbeatInterval()).toEqual(5000)
    expect(group.heartbeatInterval()).toEqual(5000)
  })

  it('attaches commitBeforeRelease, so the membership can commit before it leaves', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })
    const attach = jest.fn()
    group.groupMembership.attach = attach

    group.attach({ rebalanceCoordinator: { runner: {} }, heartbeatManager: {} })

    const [{ commitBeforeRelease }] = attach.mock.calls[0]
    expect(typeof commitBeforeRelease).toEqual('function')

    // It is the group's own method, bound to the group: the membership calls it with the owned set.
    group.autoCommit = true
    group.commitBeforeRelease = jest.fn(async () => {})
    await commitBeforeRelease([{ topic: TOPIC_A, partitions: [0] }])
    expect(group.commitBeforeRelease).toHaveBeenCalledWith([{ topic: TOPIC_A, partitions: [0] }])
  })

  it('forwards every seam member to the membership', () => {
    const group = createGroup()
    const membership = group.groupMembership
    membership.memberEpoch = 4
    membership.heartbeatInterval = jest.fn(() => 500)
    membership.needsReconciliation = jest.fn(() => true)
    membership.onMembershipError = jest.fn()
    membership.onStopRequested = jest.fn()
    membership.leaveForPollExpiry = jest.fn(async () => {})

    const error = new Error('fenced')
    expect(group.memberEpoch).toEqual(4)
    expect(group.heartbeatInterval()).toEqual(500)
    expect(group.needsReconciliation()).toBe(true)
    group.onMembershipError(error)
    group.onStopRequested()

    expect(membership.onMembershipError).toHaveBeenCalledWith(error)
    expect(membership.onStopRequested).toHaveBeenCalledTimes(1)
    return expect(group.leaveForPollExpiry()).resolves.toBeUndefined()
  })

  it('reads resolvedOffsetAt through the offset manager, and answers before one exists', () => {
    const group = createGroup()
    expect(group.resolvedOffsetAt('topic-a', 0)).toBeUndefined()

    group.offsetManager = { resolvedOffsetAt: jest.fn(() => '42') }
    expect(group.resolvedOffsetAt('topic-a', 0)).toEqual('42')
  })
})

describe('Consumer > ConsumerGroup > growSubscription', () => {
  it('widens the subscription without mutating this.topics', async () => {
    const group = createGroup({
      topics: [],
      subscribedTopicRegex: 'events-.*',
      regexSubscription: { fromBeginning: true },
    })
    const topicsBefore = group.topics

    group.groupMembership.rebalance = jest.fn(async (context) => {
      await context.growSubscription(['events-a', 'events-b'])
      context.beforeSync()
      return { assignment: {}, userData: null }
    })

    await group.joinAndSync()

    expect(group.topicsSubscribed).toEqual(['events-a', 'events-b'])
    // Same object as `topics` at construction: a push would have edited both.
    expect(group.topics).toBe(topicsBefore)
    expect(group.topics).toEqual([])
    expect(group.topicConfigurations['events-a']).toEqual({ fromBeginning: true })
    expect(group.topicConfigurations['events-b']).toEqual({ fromBeginning: true })
    expect(group.cluster.addMultipleTargetTopics).toHaveBeenCalledWith(['events-a', 'events-b'])
  })

  it('is a no-op for an empty list and for a name it already holds', async () => {
    const group = createGroup({ topics: ['events-a'], regexSubscription: { fromBeginning: true } })
    group.topicConfigurations['events-a'] = { fromBeginning: false }

    group.groupMembership.rebalance = jest.fn(async (context) => {
      const grow = context.growSubscription
      await grow([])
      await grow(['events-a'])
      // Only this third call has anything new in it, so it is the only one that may reach the
      // cluster - and it must not re-clone the configuration of the name it already held.
      await grow(['events-a', 'events-b'])
      return { assignment: {}, userData: null }
    })

    await group.joinAndSync()

    expect(group.cluster.addMultipleTargetTopics).toHaveBeenCalledTimes(1)
    expect(group.cluster.addMultipleTargetTopics).toHaveBeenCalledWith(['events-b'])
    expect(group.topicConfigurations['events-a']).toEqual({ fromBeginning: false })
    expect(group.topicConfigurations['events-b']).toEqual({ fromBeginning: true })
  })

  it('is seen by applyRebalanceResult, so no assignment is stripped as unsubscribed', async () => {
    const group = createGroup({ topics: [], regexSubscription: { fromBeginning: false } })
    const unsubscribed = collectUnsubscribedTopics(group)
    group.groupMembership.rebalance = jest.fn(async (context) => {
      await context.growSubscription(['events-a'])
      context.beforeSync()
      return { assignment: { 'events-a': [0] }, userData: null }
    })

    const result = await group.joinAndSync()
    const { assignment } = await group.applyRebalanceResult(result, { rebuildOffsets: true })

    expect(assignment).toEqual([{ topic: 'events-a', partitions: [0] }])
    expect(unsubscribed).toEqual([])
  })
})

describe('Consumer > ConsumerGroup > an assignment that names partitions the member has not heard of', () => {
  it('refreshes the metadata once, relearns the partitions of every subscribed topic and keeps the assignment', async () => {
    const group = createGroup({ topics: [TOPIC_A, TOPIC_B] })
    let known = 2
    group.cluster.findTopicPartitionMetadata = () => partitionMetadata(known)
    group.cluster.refreshMetadata = jest.fn(async () => {
      known = 4
    })
    group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()
    const warn = jest.spyOn(group.logger, 'warn')

    const { assignment } = await group.applyRebalanceResult({
      assignment: { [TOPIC_A]: [0, 1, 2], [TOPIC_B]: [2, 3] },
    })

    expect(group.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(
      'Consumer is not aware of all assigned partitions, refreshing metadata',
      expect.objectContaining({
        topic: TOPIC_A,
        knownPartitions: [0, 1],
        assignedPartitions: [0, 1, 2],
      })
    )
    expect(group.partitionsPerSubscribedTopic.get(TOPIC_A)).toEqual([0, 1, 2, 3])
    expect(group.partitionsPerSubscribedTopic.get(TOPIC_B)).toEqual([0, 1, 2, 3])
    expect(assignment).toEqual([
      { topic: TOPIC_A, partitions: [0, 1, 2] },
      { topic: TOPIC_B, partitions: [2, 3] },
    ])
  })

  it('leaves the metadata alone when every assigned partition is known', async () => {
    const group = createGroup()
    const warn = jest.spyOn(group.logger, 'warn')

    await group.applyRebalanceResult({ assignment: { [TOPIC_A]: [0, 1] } })

    expect(group.cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(warn).not.toHaveBeenCalled()
  })
})

describe('Consumer > ConsumerGroup > findReadReplicaForPartitions', () => {
  it('leaves out a partition the cached metadata does not know and keeps the others with their leaders', () => {
    const group = createGroup()
    group.cluster.findTopicPartitionMetadata = () => [
      { partitionId: 0, leader: 1 },
      { partitionId: 1, leader: 2 },
    ]

    expect(group.findReadReplicaForPartitions(TOPIC_A, ['0', 7, 1])).toEqual({ 1: [0], 2: [1] })
    expect(group.findReadReplicaForPartitions(TOPIC_A, [7])).toEqual({})
  })
})

describe('Consumer > ConsumerGroup > joinAndSync retry arms', () => {
  it('retries a FENCED_MEMBER_EPOCH instead of bailing', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })
    const fenced = new KafkaJSProtocolError(createErrorFromCode(110))
    group.groupMembership.rebalance = jest
      .fn()
      .mockRejectedValueOnce(fenced)
      .mockResolvedValue({ assignment: {}, userData: null })

    await expect(group.joinAndSync()).resolves.toEqual({ assignment: {}, userData: null })
    expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(2)
  })

  it('bails a fatal membership error and a fenced instance', async () => {
    for (const code of [82, 112]) {
      const group = createGroup({ groupProtocol: 'consumer' })
      const error = new KafkaJSProtocolError(createErrorFromCode(code))
      group.groupMembership.rebalance = jest.fn().mockRejectedValue(error)

      await expect(group.joinAndSync()).rejects.toMatchObject({ code })
      expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(1)
    }
  })

  it('does not clear the member id under the consumer protocol', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })
    const unknownMember = new KafkaJSProtocolError(createErrorFromCode(25))
    unknownMember.type = 'UNKNOWN_MEMBER_ID'
    const before = group.memberId
    group.groupMembership.rebalance = jest
      .fn()
      .mockRejectedValueOnce(unknownMember)
      .mockResolvedValue({ assignment: {}, userData: null })

    await group.joinAndSync()
    expect(group.memberId).toEqual(before)
  })

  it('still clears the member id under the classic protocol', async () => {
    const group = createGroup()
    const unknownMember = new KafkaJSProtocolError(createErrorFromCode(25))
    unknownMember.type = 'UNKNOWN_MEMBER_ID'
    group.groupMembership.rebalance = jest
      .fn()
      .mockRejectedValueOnce(unknownMember)
      .mockResolvedValue({ assignment: {}, userData: null })

    await group.joinAndSync()
    expect(group.memberId).toBeNull()
  })

  it('retries a join that exceeded its deadline instead of bailing', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })
    group.groupMembership.rebalance = jest
      .fn()
      .mockRejectedValueOnce(joinDeadlineError({ rebalanceTimeoutMs: 60000 }))
      .mockResolvedValue({ assignment: {}, userData: null })

    await expect(group.joinAndSync()).resolves.toEqual({ assignment: {}, userData: null })
    expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(2)
  })
})

describe('Consumer > ConsumerGroup > the pre-release commit retry', () => {
  const staleEpoch = () => {
    const error = new KafkaJSProtocolError(createErrorFromCode(113))
    error.type = 'STALE_MEMBER_EPOCH'
    return error
  }

  const withStopFlag = (group, stopRequested) => {
    group.rebalanceCoordinator = { runner: { stopRequested } }
  }

  it('re-sends once the coordinator has installed a newer epoch', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })
    withStopFlag(group, false)
    group.autoCommit = true
    group.rebalanceTimeout = 5000

    let attempts = 0
    group.offsetManager = {
      useMembership: jest.fn(),
      commitOffsets: jest.fn(async () => {
        attempts += 1
        if (attempts === 1) {
          // The coordinator answers the first attempt with a stale epoch, and the heartbeats
          // install a new one before the retry goes out.
          group.groupMembership.memberEpoch = 8
          throw stampGeneration(staleEpoch(), 7)
        }
      }),
    }
    group.groupMembership.memberEpoch = 7

    await group.commitBeforeRelease([{ topic: TOPIC_A, partitions: [0] }])

    expect(attempts).toEqual(2)
    expect(group.offsetManager.useMembership).toHaveBeenLastCalledWith({
      generationId: 8,
      memberId: group.memberId,
      coordinator: group.coordinator,
    })
  })

  it('gives up at the rebalance timeout when no new epoch arrives', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })
    withStopFlag(group, false)
    group.autoCommit = true
    // Short enough that the deadline is reached inside the case's own budget.
    group.rebalanceTimeout = 300
    group.groupMembership.memberEpoch = 7

    group.offsetManager = {
      useMembership: jest.fn(),
      commitOffsets: jest.fn(async () => {
        throw stampGeneration(staleEpoch(), 7)
      }),
    }

    await expect(
      group.commitBeforeRelease([{ topic: TOPIC_A, partitions: [0] }])
    ).resolves.toBeUndefined()
    expect(group.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
  })

  it('abandons the retry the moment a stop is requested', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })
    withStopFlag(group, true)
    group.autoCommit = true
    group.rebalanceTimeout = 60000
    group.groupMembership.memberEpoch = 7

    group.offsetManager = {
      useMembership: jest.fn(),
      commitOffsets: jest.fn(async () => {
        throw stampGeneration(staleEpoch(), 7)
      }),
    }

    const startedAt = Date.now()
    await group.commitBeforeRelease([{ topic: TOPIC_A, partitions: [0] }])

    // It returned on the stop flag, not on the 60 s deadline.
    expect(Date.now() - startedAt).toBeLessThan(1000)
    expect(group.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
  })

  it('does not retry anything but a stale member epoch', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })
    withStopFlag(group, false)
    group.autoCommit = true
    group.offsetManager = {
      useMembership: jest.fn(),
      commitOffsets: jest.fn(async () => {
        throw new Error('coordinator is unavailable')
      }),
    }

    await group.commitBeforeRelease([{ topic: TOPIC_A, partitions: [0] }])
    expect(group.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
  })

  it('does not retry a stale member epoch under the classic protocol', async () => {
    const group = createGroup()
    withStopFlag(group, false)
    group.autoCommit = true
    group.generationId = 7
    group.offsetManager = {
      useMembership: jest.fn(),
      commitOffsets: jest.fn(async () => {
        throw stampGeneration(staleEpoch(), 7)
      }),
    }

    await group.commitBeforeRelease([{ topic: TOPIC_A, partitions: [0] }])
    expect(group.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
  })
})

describe('Consumer > ConsumerGroup > the offset manager it builds', () => {
  it('tells the manager which membership protocol it is committing under', async () => {
    const group = createGroup({ groupProtocol: 'consumer' })

    await group.applyRebalanceResult({ assignment: { [TOPIC_A]: [0] } }, { rebuildOffsets: true })

    expect(group.offsetManager.membershipProtocol).toEqual('consumer')
    expect(group.offsetManager.staleEpochTimeout).toEqual(group.rebalanceTimeout)
  })

  it('defaults to classic for a classic group', async () => {
    const group = createGroup()

    await group.applyRebalanceResult({ assignment: { [TOPIC_A]: [0] } }, { rebuildOffsets: true })

    expect(group.offsetManager.membershipProtocol).toEqual('classic')
  })
})

describe('Consumer > ConsumerGroup > buildFetchRequests', () => {
  /** A group with an offset manager and a committed offset on both partitions of TOPIC_A. */
  const createFetchingGroup = async () => {
    const group = createGroup()
    await group.applyRebalanceResult(
      { assignment: { [TOPIC_A]: [0, 1] } },
      { rebuildOffsets: true }
    )
    Object.assign(group.offsetManager.committedOffsets(), { [TOPIC_A]: { 0: '10', 1: '20' } })
    return group
  }

  it('fetches every assigned partition from the offset manager, and takes no options', async () => {
    const group = await createFetchingGroup()
    const [{ topic, partitions }] = group.buildFetchRequests([
      { topic: TOPIC_A, partitions: [0, 1] },
    ])

    expect(group.buildFetchRequests).toHaveLength(1)
    expect(topic).toEqual(TOPIC_A)
    expect(partitions.map(({ partition, fetchOffset }) => [partition, fetchOffset])).toEqual([
      [0, '10'],
      [1, '20'],
    ])
  })
})

describe('Consumer > ConsumerGroup > unassigned prototype topic seeks', () => {
  it.each(['__proto__', 'constructor', 'toString', 'hasOwnProperty', 'valueOf'])(
    'ignores a seek for an unassigned topic named %s',
    async (topic) => {
      const before = Object.getOwnPropertyDescriptors(Object.prototype)
      const group = createGroup()
      await group.applyRebalanceResult({ assignment: { [TOPIC_A]: [0] } })

      await expect(group.offsetManager.seek({ topic, partition: 0, offset: '100' })).resolves.toBe(
        false
      )
      expect(Object.getOwnPropertyDescriptors(Object.prototype)).toEqual(before)
    }
  )
})
