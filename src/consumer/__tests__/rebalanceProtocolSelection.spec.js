const { selectRebalanceProtocol, EAGER, COOPERATIVE } = require('../groupProtocol/interface')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const ConsumerGroup = require('../consumerGroup')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { MemberMetadata } = require('../assignerProtocol')
const { KafkaJSNonRetriableError } = require('../../errors')
const { newLogger } = require('testHelpers')

const assigner = (name, supportedProtocols, extra = {}) => ({
  name,
  version: 0,
  assign: async () => [],
  protocol: ({ topics }) => ({ name, metadata: MemberMetadata.encode({ version: 0, topics }) }),
  ...(supportedProtocols ? { supportedProtocols: () => supportedProtocols } : {}),
  ...extra,
})

describe('Consumer > rebalance protocol selection', () => {
  const COMBINATIONS = [
    [[['eager']], EAGER],
    [[undefined], EAGER],
    [[['cooperative', 'eager']], COOPERATIVE],
    [[['eager'], ['cooperative', 'eager']], EAGER],
    [
      [
        ['cooperative', 'eager'],
        ['cooperative', 'eager'],
      ],
      COOPERATIVE,
    ],
    [[undefined, ['cooperative', 'eager']], EAGER],
    [[['cooperative']], COOPERATIVE],
    [[['cooperative'], ['cooperative', 'eager']], COOPERATIVE],
  ]

  test.each(COMBINATIONS)('%p selects %s', (protocols, expected) => {
    const assigners = protocols.map((p, i) => assigner(`assigner-${i}`, p))

    expect(selectRebalanceProtocol(assigners)).toEqual(expected)
  })

  it('selects eager when no assigner is configured at all', () => {
    // An empty list has always been accepted at construction and rejected by the broker at
    // JoinGroup; the intersection of nothing must not read as "everything".
    expect(selectRebalanceProtocol([])).toEqual(EAGER)
  })

  it('throws naming every assigner and its protocols when the intersection is empty', () => {
    const assigners = [assigner('a', ['eager']), assigner('b', ['cooperative'])]

    expect(() => selectRebalanceProtocol(assigners)).toThrow(KafkaJSNonRetriableError)
    expect(() => selectRebalanceProtocol(assigners)).toThrow(
      /a.*eager.*b.*cooperative|b.*cooperative.*a.*eager/s
    )
  })

  it('fails consumer construction when the configured assigners share no protocol', () => {
    const { Kafka } = require('../../../index')
    const kafka = new Kafka({ clientId: 'mode', brokers: ['localhost:9092'] })

    expect(() =>
      kafka.consumer({
        groupId: 'mode-group',
        partitionAssigners: [
          () => assigner('eager-only', ['eager']),
          () => assigner('cooperative-only', ['cooperative']),
        ],
      })
    ).toThrow(KafkaJSNonRetriableError)
  })

  it('logs the selection exactly once when the consumer is built', () => {
    const { Kafka, logLevel } = require('../../../index')
    // Three things this has to get right, all of them load-bearing:
    // (1) `logCreator` must return a FUNCTION - the logger factory calls `logCreator(level)`
    //     and then invokes the result;
    // (2) the consumer logs through `rootLogger.namespace('Consumer')`, and `namespace()`
    //     returns a NEW object, so a spy on any logger object sees nothing;
    // (3) `newLogger()` is at logLevel.NOTHING, which would filter an INFO away entirely.
    const lines = []
    const kafka = new Kafka({
      clientId: 'mode',
      brokers: ['localhost:9092'],
      logLevel: logLevel.INFO,
      logCreator: () => (entry) => lines.push(entry),
    })

    kafka.consumer({ groupId: 'mode-group' })

    const selections = lines.filter(({ log }) =>
      String(log.message).includes('Rebalance protocol selected')
    )
    expect(selections).toHaveLength(1)
    expect(selections[0].log).toMatchObject({ rebalanceProtocol: EAGER })
  })
})

describe('Consumer > JoinGroup protocol entry list', () => {
  const build = (assigners, mode) =>
    new ClassicGroupProtocol({
      cluster: { findGroupCoordinator: async () => coordinator },
      groupId: 'entries-group',
      groupInstanceId: null,
      sessionTimeout: 30000,
      rebalanceTimeout: 300000,
      assigners,
      mode,
      rackId: '',
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })

  let coordinator

  beforeEach(() => {
    coordinator = {
      joinGroup: jest.fn(async () => ({
        generationId: 1,
        leaderId: 'm1',
        memberId: 'm1',
        members: [],
        groupProtocol: 'RoundRobinAssigner',
      })),
      negotiatedVersion: () => 5,
    }
  })

  it('advertises one entry per configured assigner, in configuration order', async () => {
    const roundRobin = assigner('RoundRobinAssigner', ['eager'])
    const coop = assigner('cooperative-sticky', ['cooperative', 'eager'])
    const protocol = build([roundRobin, coop], EAGER)

    await protocol.join({ topicsSubscribed: ['topic-a'] })

    const { groupProtocols } = coordinator.joinGroup.mock.calls[0][0]
    expect(groupProtocols.map(({ name }) => name)).toEqual([
      'RoundRobinAssigner',
      'cooperative-sticky',
    ])
    expect(groupProtocols[0].metadata).toEqual(
      MemberMetadata.encode({ version: 0, topics: ['topic-a'] })
    )
  })

  it('passes all four keys to protocol()', async () => {
    const seen = []
    const spy = {
      name: 'spy',
      version: 3,
      assign: async () => [],
      protocol: (argument) => {
        seen.push(argument)
        return { name: 'spy', metadata: Buffer.alloc(0) }
      },
    }
    const protocol = build([spy], EAGER)
    protocol.generationId = 12
    protocol.setOwned([{ topic: 'topic-a', partitions: [0, 1] }])

    await protocol.join({ topicsSubscribed: ['topic-a'] })

    expect(seen).toEqual([
      {
        topics: ['topic-a'],
        ownedPartitions: [{ topic: 'topic-a', partitions: [0, 1] }],
        generationId: 12,
        rackId: null,
      },
    ])
  })

  it('leaves a topics-only assigner byte-unchanged and never consults subscriptionUserData', async () => {
    const subscriptionUserData = jest.fn(() => Buffer.from([9]))
    const legacy = assigner('legacy', undefined, { subscriptionUserData })
    const protocol = build([legacy], EAGER)

    await protocol.join({ topicsSubscribed: ['topic-a'] })

    const { groupProtocols } = coordinator.joinGroup.mock.calls[0][0]
    expect(groupProtocols[0].metadata).toEqual(
      MemberMetadata.encode({ version: 0, topics: ['topic-a'] })
    )
    expect(subscriptionUserData).not.toHaveBeenCalled()
  })

  it('hands an assigner a copy of the owned partitions, not the live set', async () => {
    const mutating = {
      name: 'mutating',
      version: 1,
      assign: async () => [],
      protocol: ({ ownedPartitions }) => {
        ownedPartitions[0].partitions.push(99)
        ownedPartitions.length = 0
        return { name: 'mutating', metadata: Buffer.alloc(0) }
      },
    }
    const protocol = build([mutating], EAGER)
    protocol.setOwned([{ topic: 'topic-a', partitions: [0, 1] }])

    await protocol.join({ topicsSubscribed: ['topic-a'] })

    expect(protocol.owned()).toEqual([{ topic: 'topic-a', partitions: [0, 1] }])
  })

  it('reports -1 for the generation before the first join', async () => {
    const seen = []
    const spy = {
      name: 'spy',
      version: 3,
      assign: async () => [],
      protocol: (argument) => {
        seen.push(argument.generationId)
        return { name: 'spy', metadata: Buffer.alloc(0) }
      },
    }

    await build([spy], EAGER).join({ topicsSubscribed: ['topic-a'] })

    expect(seen).toEqual([-1])
  })
})

describe('Consumer > onAssignment', () => {
  it('is invoked for every assigner that defines it, and cleared on leave and reset', async () => {
    const onAssignment = jest.fn()
    const withCallback = assigner('with', ['eager'], { onAssignment })
    const without = assigner('without', ['eager'])
    const protocol = new ClassicGroupProtocol({
      cluster: {},
      groupId: 'g',
      groupInstanceId: null,
      sessionTimeout: 30000,
      rebalanceTimeout: 300000,
      assigners: [withCallback, without],
      mode: EAGER,
      rackId: '',
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
    protocol.generationId = 4
    protocol.groupProtocol = 'with'
    protocol.memberId = 'm1'
    protocol.coordinator = { leaveGroup: async () => {} }

    protocol.notifyAssignment({
      partitions: [{ topic: 't', partitions: [0] }],
      userData: Buffer.alloc(0),
    })

    expect(onAssignment).toHaveBeenCalledWith(
      { partitions: [{ topic: 't', partitions: [0] }], userData: Buffer.alloc(0) },
      { generationId: 4, groupProtocol: 'with' }
    )

    onAssignment.mockClear()
    protocol.resetGeneration()
    expect(onAssignment).toHaveBeenCalledWith(null, { generationId: -1, groupProtocol: null })

    onAssignment.mockClear()
    protocol.memberId = 'm1'
    await protocol.leave()
    expect(onAssignment).toHaveBeenCalledWith(null, { generationId: -1, groupProtocol: null })
  })
})

describe('Consumer > applying a sync result', () => {
  /**
   * A real `ConsumerGroup` over a stub cluster, so the apply runs its own body rather than a mock.
   * @param {Object[]} assigners
   * @returns {ConsumerGroup}
   */
  const buildGroup = (assigners) =>
    new ConsumerGroup({
      retry: { retries: 0 },
      cluster: {
        connect: async () => {},
        refreshMetadata: async () => {},
        refreshMetadataIfNecessary: async () => {},
        committedOffsets: () => ({}),
        findTopicPartitionMetadata: () => [{ partitionId: 0 }, { partitionId: 1 }],
      },
      groupId: 'apply-group',
      topics: ['topic-a'],
      topicConfigurations: { 'topic-a': {} },
      logger: newLogger(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      assigners,
      sessionTimeout: 30000,
      rebalanceTimeout: 300000,
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
      groupInstanceId: null,
      metadataMaxAge: 300000,
    })

  /**
   * Puts a freshly built group in the state one sync result arrives into.
   * @param {ConsumerGroup} group
   * @returns {ConsumerGroup}
   */
  const ready = (group) => {
    group.generationId = 7
    group.groupProtocol = 'with'
    group.memberId = 'm1'
    group.coordinator = { leaveGroup: async () => {} }
    group.groupMembership.setOwned([{ topic: 'topic-a', partitions: [1] }])
    group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()
    return group
  }

  it('notifies every assigner that wants it, with the applied assignment, before owned is widened', async () => {
    const ownedWhileNotified = []
    const onAssignment = jest.fn()
    const group = ready(
      buildGroup([assigner('with', ['eager'], { onAssignment }), assigner('without', ['eager'])])
    )
    onAssignment.mockImplementation(() => ownedWhileNotified.push(group.groupMembership.owned()))
    const userData = Buffer.from([7])

    const { assignment } = await group.applyRebalanceResult({
      assignment: { 'topic-a': [0, 1] },
      userData,
    })

    expect(onAssignment).toHaveBeenCalledTimes(1)
    expect(onAssignment).toHaveBeenCalledWith(
      { partitions: [{ topic: 'topic-a', partitions: [0, 1] }], userData },
      { generationId: 7, groupProtocol: 'with' }
    )
    // The callback runs before `setOwned`, so an assigner reading its own ownership sees the
    // generation it is leaving, not the one it is entering.
    expect(ownedWhileNotified).toEqual([[{ topic: 'topic-a', partitions: [1] }]])
    expect(group.groupMembership.owned()).toEqual([{ topic: 'topic-a', partitions: [0, 1] }])
    expect(assignment).toEqual([{ topic: 'topic-a', partitions: [0, 1] }])
  })

  it('reports null user data when the sync result carried none', async () => {
    const onAssignment = jest.fn()
    const group = ready(buildGroup([assigner('with', ['eager'], { onAssignment })]))

    await group.applyRebalanceResult({ assignment: { 'topic-a': [0] } })

    expect(onAssignment).toHaveBeenCalledWith(
      { partitions: [{ topic: 'topic-a', partitions: [0] }], userData: null },
      { generationId: 7, groupProtocol: 'with' }
    )
  })

  it('clears retained assigner state on demand, and on leaving the group', async () => {
    const onAssignment = jest.fn()
    const group = ready(buildGroup([assigner('with', ['eager'], { onAssignment })]))

    group.clearAssignerState()
    expect(onAssignment).toHaveBeenCalledWith(null, { generationId: -1, groupProtocol: null })

    onAssignment.mockClear()
    await group.leave()
    expect(onAssignment).toHaveBeenCalledWith(null, { generationId: -1, groupProtocol: null })
  })
})
