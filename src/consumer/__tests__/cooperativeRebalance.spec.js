const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const Batch = require('../batch')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const PollTimer = require('../pollTimer')
const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')
const { COOPERATIVE, EAGER } = require('../groupProtocol/interface')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { events } = require('../instrumentationEvents')
const { newLogger } = require('testHelpers')

const TOPIC_A = 'topic-a'
const TOPIC_B = 'topic-b'

const partitionMetadata = (count) =>
  Array.from({ length: count }, (_, partitionId) => ({ partitionId }))

/**
 * A real `ConsumerGroup` with a stubbed cluster and a real `OffsetManager`, so every assertion is
 * about the shipped apply rather than a double of it.
 */
const createGroup = ({ mode = COOPERATIVE, topics = [TOPIC_A, TOPIC_B] } = {}) => {
  const committedOffsets = {}
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    committedOffsets: () => committedOffsets,
    findTopicPartitionMetadata: (topic) => (topics.includes(topic) ? partitionMetadata(4) : []),
    findGroupCoordinator: jest.fn(async () => ({ isConnected: () => true })),
  }

  const instrumentationEmitter = new InstrumentationEventEmitter()
  const emitted = []
  instrumentationEmitter.addListener(events.RECEIVED_UNSUBSCRIBED_TOPICS, (event) =>
    emitted.push(event)
  )

  const group = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'cooperative-group',
    topics,
    topicConfigurations: topics.reduce((all, topic) => ({ ...all, [topic]: {} }), {}),
    logger: newLogger(),
    instrumentationEmitter,
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
    rebalanceProtocol: mode,
    metadataMaxAge: 300000,
    pendingTopicResets: new Map(),
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

  return { group, cluster, committedOffsets, emitted, instrumentationEmitter }
}

/** Applies one assignment the way a cooperative round does. */
const applyCooperative = (group, assignment) =>
  group.applyRebalanceResult({ assignment }, { rebuildOffsets: false })

/** Applies one assignment the way an eager round does. */
const applyEager = (group, assignment) => group.applyRebalanceResult({ assignment })

/**
 * A real `RebalanceCoordinator` over a recording group and a recording fetch manager, for the
 * two cases whose subject is what a round asks of its collaborators.
 */
const createCoordinator = () => {
  const applies = []
  const consumerGroup = {
    rebalanceProtocol: () => COOPERATIVE,
    assigned: () => [],
    memberId: 'member-1',
    leaderId: 'member-1',
    groupProtocol: 'cooperative-sticky',
    isLeader: () => true,
    joinAndSync: jest.fn(async () => ({ assignment: {} })),
    applyRebalanceResult: jest.fn(async (result, options) => {
      applies.push(options)
      return { assignment: [], added: [], revoked: [] }
    }),
    clearStaleAssignmentPending: jest.fn(),
  }

  const fetchManager = {
    start: jest.fn(async () => {}),
    stop: jest.fn(async () => {}),
    getFetchers: jest.fn(() => []),
  }

  const coordinator = new RebalanceCoordinator({
    runner: { stopRequested: false, onCrash: jest.fn() },
    consumerGroup,
    fetchManager,
    heartbeatManager: { ensureStarted: jest.fn(), stop: jest.fn() },
    pollTimer: new PollTimer({ pollDeadline: 8000 }),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
    groupId: 'cooperative-group',
  })

  return { coordinator, consumerGroup, fetchManager, applies }
}

describe('Consumer > cooperative rebalance apply', () => {
  describe('OffsetManager', () => {
    it('keeps a retained partition offset, committed offset and epoch, and drops a revoked one', async () => {
      const { group, committedOffsets } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })

      group.resolveOffset({ topic: TOPIC_A, partition: 0, offset: '10', leaderEpoch: 5 })
      group.resolveOffset({ topic: TOPIC_A, partition: 1, offset: '20', leaderEpoch: 7 })
      committedOffsets[TOPIC_A][0] = '11'
      committedOffsets[TOPIC_A][1] = '21'

      await applyCooperative(group, { [TOPIC_A]: [0] })

      const { offsetManager } = group
      expect(offsetManager.resolvedOffsets[TOPIC_A][0]).toEqual('11')
      expect(offsetManager.resolvedEpochs[TOPIC_A][0]).toEqual(5)
      expect(offsetManager.committedOffsets()[TOPIC_A][0]).toEqual('11')

      expect(offsetManager.resolvedOffsets[TOPIC_A][1]).toBeUndefined()
      expect(offsetManager.resolvedEpochs[TOPIC_A][1]).toBeUndefined()
      expect(offsetManager.committedOffsets()[TOPIC_A][1]).toBeUndefined()
    })

    it('keeps an empty object for a fully revoked topic, so the still-assigned ones stay indexable', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1], [TOPIC_B]: [0] })

      group.resolveOffset({ topic: TOPIC_A, partition: 0, offset: '10' })
      group.resolveOffset({ topic: TOPIC_B, partition: 0, offset: '30' })

      await applyCooperative(group, { [TOPIC_A]: [0] })

      const { offsetManager } = group
      // The partially revoked topic keeps its key, so the fetch path can still index it. Read
      // through `nextOffset`, which is what the fetch round itself calls and what a topic-level
      // prune would turn into a TypeError.
      expect(offsetManager.nextOffset(TOPIC_A, 0).toString()).toEqual('11')
      expect(() =>
        group.resolveOffset({ topic: TOPIC_A, partition: 0, offset: '12' })
      ).not.toThrow()

      expect(offsetManager.committedOffsets()[TOPIC_B]).toEqual({})
      expect(offsetManager.resolvedOffsets[TOPIC_B]).toEqual({})
      expect(offsetManager.resolvedEpochs[TOPIC_B]).toEqual({})
    })

    it('creates the topic keys a newly added topic needs', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0] })

      await applyCooperative(group, { [TOPIC_A]: [0], [TOPIC_B]: [2] })

      const { offsetManager } = group
      expect(offsetManager.committedOffsets()[TOPIC_B]).toEqual({})
      expect(offsetManager.nextOffset(TOPIC_B, 2).toString()).toEqual('0')
      expect(offsetManager.memberAssignment).toEqual({ [TOPIC_A]: [0], [TOPIC_B]: [2] })
      expect(offsetManager.topics).toEqual([TOPIC_A, TOPIC_B])
    })

    it('prunes the cluster-level committed offsets for the revoked partition only', async () => {
      const { group, committedOffsets } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      committedOffsets[TOPIC_A][0] = '11'
      committedOffsets[TOPIC_A][1] = '21'

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(committedOffsets[TOPIC_A]).toEqual({ 0: '11' })
    })

    it('is a no-op for resolveOffset on a partition this member no longer holds', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      group.resolveOffset({ topic: TOPIC_A, partition: 1, offset: '20' })

      await applyCooperative(group, { [TOPIC_A]: [0] })
      group.resolveOffset({ topic: TOPIC_A, partition: 1, offset: '99' })

      expect(group.offsetManager.resolvedOffsets[TOPIC_A][1]).toBeUndefined()
      expect(group.offsetManager.owns(TOPIC_A, 1)).toBe(false)
      expect(group.offsetManager.owns(TOPIC_A, 0)).toBe(true)
    })

    it('does not throw when a handler resolves an offset for a fully revoked topic', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0], [TOPIC_B]: [0] })

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(() => group.resolveOffset({ topic: TOPIC_B, partition: 0, offset: '5' })).not.toThrow()
      expect(group.offsetManager.resolvedOffsets[TOPIC_B]).toEqual({})
    })

    it('re-resolves the coordinator, so one that moved between rounds is used next', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0] })
      const first = group.offsetManager.coordinator

      const moved = { isConnected: () => true, nodeId: 7 }
      group.coordinator = moved
      group.generationId = 2
      group.memberId = 'member-1-b'
      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(first).not.toBe(moved)
      expect(group.offsetManager.coordinator).toBe(moved)
      expect(group.offsetManager.generationId).toEqual(2)
      expect(group.offsetManager.memberId).toEqual('member-1-b')
    })
  })

  describe('per-assignment caches', () => {
    it('clears consecutiveFetchErrors for revoked partitions only', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      group.recordFetchError(TOPIC_A, 0, 1)
      group.recordFetchError(TOPIC_A, 1, 1)

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(group.consecutiveFetchErrors.get(`${TOPIC_A}|0`)).toEqual(1)
      expect(group.consecutiveFetchErrors.has(`${TOPIC_A}|1`)).toBe(false)
    })

    it('clears validationUnusableLogged wholesale, because it holds broker node ids', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      group.validationUnusableLogged.add(0)
      group.validationUnusableLogged.add(3)

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(group.validationUnusableLogged.size).toEqual(0)
    })

    it('drops a pending seek for a revoked partition and keeps one for a retained partition', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      group.seek({ topic: TOPIC_A, partition: 0, offset: '100' })
      group.seek({ topic: TOPIC_A, partition: 1, offset: '200' })

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(group.hasSeekOffset({ topic: TOPIC_A, partition: 0 })).toBe(true)
      expect(group.hasSeekOffset({ topic: TOPIC_A, partition: 1 })).toBe(false)

      await group.seekOffsets([{ topic: TOPIC_A, partitions: [0] }])
      expect(group.offsetManager.resolvedOffsets[TOPIC_A][0]).toEqual('100')
    })
  })

  describe('PositionValidator', () => {
    it('forgets the revoked partitions without bumping the assignment generation', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      const { positionValidator } = group
      positionValidator.markValidated(TOPIC_A, 0, 4)
      positionValidator.markValidated(TOPIC_A, 1, 4)
      const generation = positionValidator.assignmentGeneration

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(positionValidator.assignmentGeneration).toEqual(generation)
      expect(positionValidator.validatedAgainst(TOPIC_A, 0)).toEqual(4)
      expect(positionValidator.validatedAgainst(TOPIC_A, 1)).toBeUndefined()
    })

    it('leaves a retained partition with a validation in flight fetchable', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      const { positionValidator } = group
      const { action, generation } = positionValidator.request(TOPIC_A, 0, {
        offset: '10',
        trackedEpoch: 3,
        currentLeaderEpoch: 4,
      })
      expect(action).toEqual('awaiting')

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(positionValidator.isAwaiting(TOPIC_A, 0)).toBe(true)

      const outcome = positionValidator.complete(
        TOPIC_A,
        0,
        { offset: '10', trackedEpoch: 3 },
        { errorCode: 0, endOffset: '50', leaderEpoch: 4, seekQueued: false, generation }
      )

      expect(outcome.action).toEqual('validated')
      expect(positionValidator.isAwaiting(TOPIC_A, 0)).toBe(false)
      expect(positionValidator.validatedAgainst(TOPIC_A, 0)).toEqual(4)
    })
  })

  describe('SubscriptionState', () => {
    it('keeps a retained partition paused across the round, leaving the pause state untouched', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      group.pause([{ topic: TOPIC_A, partitions: [0] }])

      const { subscriptionState } = group
      const before = subscriptionState.subscriptionStatesByTopic
      const snapshot = JSON.stringify(
        Object.entries(before).map(([topic, state]) => [
          topic,
          [...state.paused].sort(),
          [...state.resumed].sort(),
          state.pauseAll,
        ])
      )

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(subscriptionState.subscriptionStatesByTopic).toBe(before)
      expect(
        JSON.stringify(
          Object.entries(subscriptionState.subscriptionStatesByTopic).map(([topic, state]) => [
            topic,
            [...state.paused].sort(),
            [...state.resumed].sort(),
            state.pauseAll,
          ])
        )
      ).toEqual(snapshot)
      expect(group.isPaused(TOPIC_A, 0)).toBe(true)
      expect(group.assigned()).toEqual([{ topic: TOPIC_A, partitions: [0] }])
    })

    it('replaces the assignment through update() without touching the pause state', () => {
      const { group } = createGroup()
      const { subscriptionState } = group
      subscriptionState.assign([{ topic: TOPIC_A, partitions: [0, 1] }])
      subscriptionState.pause([{ topic: TOPIC_A, partitions: [1] }])

      subscriptionState.update([{ topic: TOPIC_A, partitions: [1] }])

      expect(subscriptionState.assigned()).toEqual([{ topic: TOPIC_A, partitions: [1] }])
      expect(subscriptionState.isAssigned(TOPIC_A, 0)).toBe(false)
      expect(subscriptionState.isAssigned(TOPIC_A, 1)).toBe(true)
      expect(subscriptionState.isPaused(TOPIC_A, 1)).toBe(true)
    })
  })

  describe('the two modes', () => {
    it('builds a fresh OffsetManager on every eager sync and keeps the instance on a cooperative one', async () => {
      const eager = createGroup({ mode: EAGER }).group
      await applyEager(eager, { [TOPIC_A]: [0, 1] })
      const eagerManager = eager.offsetManager
      await applyEager(eager, { [TOPIC_A]: [0] })
      expect(eager.offsetManager).not.toBe(eagerManager)

      const cooperative = createGroup().group
      await applyCooperative(cooperative, { [TOPIC_A]: [0, 1] })
      const cooperativeManager = cooperative.offsetManager
      await applyCooperative(cooperative, { [TOPIC_A]: [0] })
      expect(cooperative.offsetManager).toBe(cooperativeManager)
    })

    it('does not move the assignment epoch on a cooperative round', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      const epoch = group.assignmentEpoch

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(group.assignmentEpoch).toEqual(epoch)
    })

    it('builds the first OffsetManager even when the round is cooperative', async () => {
      const { group } = createGroup()

      await applyCooperative(group, { [TOPIC_A]: [0] })

      expect(group.offsetManager).not.toBeNull()
      expect(group.offsetManager.memberAssignment).toEqual({ [TOPIC_A]: [0] })
    })

    it('tells the apply which mode the round ran under', async () => {
      const { coordinator, consumerGroup, applies } = createCoordinator()

      await coordinator.runRound('initial', 'initial')
      expect(applies).toEqual([{ rebuildOffsets: false }])

      consumerGroup.rebalanceProtocol = () => EAGER
      await coordinator.runRound('rebalance-in-progress', 'revoked')
      expect(applies[1]).toEqual({ rebuildOffsets: true })
    })

    it('touches the fetch layer on neither side of a cooperative round', async () => {
      const { coordinator, fetchManager } = createCoordinator()

      await coordinator.runRound('rebalance-in-progress', 'revoked')

      // Fetchers are per broker and each round re-derives its node's partitions from the current
      // assignment, so the apply IS the reconciliation - nothing starts, stops or restarts.
      expect(fetchManager.start).not.toHaveBeenCalled()
      expect(fetchManager.stop).not.toHaveBeenCalled()
      expect(fetchManager.getFetchers).not.toHaveBeenCalled()
    })
  })

  describe('the delta the apply reports', () => {
    it('returns assignment, added and revoked, carries no lost, and emits nothing', async () => {
      const { group, emitted } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })

      const result = await applyCooperative(group, { [TOPIC_A]: [1, 2] })

      expect(Object.keys(result).sort()).toEqual(['added', 'assignment', 'revoked'])
      expect(result.assignment).toEqual([{ topic: TOPIC_A, partitions: [1, 2] }])
      expect(result.added).toEqual([{ topic: TOPIC_A, partitions: [2] }])
      expect(result.revoked).toEqual([{ topic: TOPIC_A, partitions: [0] }])
      expect(emitted).toEqual([])
    })

    it.each([
      ['cooperative', COOPERATIVE],
      ['eager', EAGER],
    ])('filters an unsubscribed topic and emits the event once under %s', async (_name, mode) => {
      const { group, emitted } = createGroup({ mode, topics: [TOPIC_A] })

      const apply = mode === COOPERATIVE ? applyCooperative : applyEager
      const result = await apply(group, { [TOPIC_A]: [0], 'not-subscribed': [0, 1] })

      expect(result.assignment).toEqual([{ topic: TOPIC_A, partitions: [0] }])
      expect(group.assigned()).toEqual([{ topic: TOPIC_A, partitions: [0] }])
      expect(emitted).toHaveLength(1)
      expect(emitted[0].payload.topicsNotSubscribed).toEqual(['not-subscribed'])
    })

    it('applies a zero-length assignment and keeps the member in the group', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })

      const result = await applyCooperative(group, {})

      expect(result.assignment).toEqual([])
      expect(result.revoked).toEqual([{ topic: TOPIC_A, partitions: [0, 1] }])
      expect(group.assigned()).toEqual([])
      expect(group.memberId).toEqual('member-1')
      expect(group.generationId).toEqual(1)
    })

    it('narrows owned before the apply and widens it with setOwned afterwards', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })

      const calls = []
      const { groupMembership } = group
      jest.spyOn(groupMembership, 'narrowOwned')
      jest.spyOn(groupMembership, 'setOwned')
      groupMembership.narrowOwned.mockImplementation((topicPartitions) => {
        calls.push(['narrowOwned', groupMembership.owned()])
        return ClassicGroupProtocol.prototype.narrowOwned.call(groupMembership, topicPartitions)
      })
      groupMembership.setOwned.mockImplementation((topicPartitions) => {
        calls.push(['setOwned', groupMembership.owned()])
        return ClassicGroupProtocol.prototype.setOwned.call(groupMembership, topicPartitions)
      })
      jest.spyOn(group.offsetManager, 'updateAssignment').mockImplementation(function (...args) {
        calls.push(['updateAssignment', groupMembership.owned()])
        return Object.getPrototypeOf(this).updateAssignment.apply(this, args)
      })

      await applyCooperative(group, { [TOPIC_A]: [1, 2] })

      expect(calls.map(([name]) => name)).toEqual(['narrowOwned', 'updateAssignment', 'setOwned'])
      // Narrowed to the intersection before the manager is told anything.
      expect(calls[1][1]).toEqual([{ topic: TOPIC_A, partitions: [1] }])
      expect(groupMembership.owned()).toEqual([{ topic: TOPIC_A, partitions: [1, 2] }])
    })
  })

  describe('in-flight delivery', () => {
    const buildRunner = ({ group, eachBatch, concurrency = 4 }) => {
      const runner = new Runner({
        logger: newLogger(),
        consumerGroup: group,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        concurrency,
        heartbeatInterval: 3000,
        maxPollInterval: 300000,
        eachBatch,
        onCrash: jest.fn(),
      })

      runner.running = true
      runner.heartbeatManager.stop()
      return runner
    }

    const messageBatch = (topic, partition) =>
      new Batch(topic, 0, {
        partition,
        highWatermark: '100',
        messages: [{ offset: '0', key: null, value: null, headers: {} }],
      })

    it('drops a queued batch for a partition this member no longer holds', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      const eachBatch = jest.fn(async () => {})
      const runner = buildRunner({ group, eachBatch })
      const autoCommitOffsets = jest.spyOn(runner, 'autoCommitOffsets')
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})

      await applyCooperative(group, { [TOPIC_A]: [0] })
      await runner.handleBatch(messageBatch(TOPIC_A, 1))

      expect(eachBatch).not.toHaveBeenCalled()
      expect(autoCommitOffsets).not.toHaveBeenCalled()
    })

    it('makes resolveOffset and commitOffsetsIfNecessary no-ops for a revoked partition mid-handler', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})
      const commitOffsetsIfNecessary = jest
        .spyOn(group, 'commitOffsetsIfNecessary')
        .mockImplementation(async () => {})
      const commitOffsets = jest.spyOn(group, 'commitOffsets').mockImplementation(async () => {})

      const eachBatch = jest.fn(async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
        // The round lands while this handler is running.
        await applyCooperative(group, { [TOPIC_A]: [0] })
        resolveOffset(batch.messages[0].offset)
        await commitOffsetsIfNecessary()
        await commitOffsetsIfNecessary({ topics: [] })
      })

      const runner = buildRunner({ group, eachBatch })
      await runner.handleBatch(messageBatch(TOPIC_A, 1))

      expect(eachBatch).toHaveBeenCalled()
      expect(group.offsetManager.resolvedOffsets[TOPIC_A][1]).toBeUndefined()
      expect(commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(commitOffsets).not.toHaveBeenCalledWith({ topics: [] })
    })

    it('still commits for a partition this member retained', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})
      const commitOffsetsIfNecessary = jest
        .spyOn(group, 'commitOffsetsIfNecessary')
        .mockImplementation(async () => {})

      const eachBatch = jest.fn(async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
        await applyCooperative(group, { [TOPIC_A]: [0] })
        resolveOffset(batch.messages[0].offset)
        await commitOffsetsIfNecessary()
      })

      const runner = buildRunner({ group, eachBatch })
      await runner.handleBatch(messageBatch(TOPIC_A, 0))

      expect(group.offsetManager.resolvedOffsets[TOPIC_A][0]).toEqual('1')
      expect(commitOffsetsIfNecessary).toHaveBeenCalledTimes(1)
    })

    it('commits the kept partitions of a caller-supplied payload and drops the revoked ones', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1], [TOPIC_B]: [0] })
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})
      const commitOffsets = jest.spyOn(group, 'commitOffsets').mockImplementation(async () => {})

      const payload = {
        topics: [
          {
            topic: TOPIC_A,
            partitions: [
              { partition: 0, offset: '5' },
              { partition: 1, offset: '9' },
            ],
          },
          { topic: TOPIC_B, partitions: [{ partition: 0, offset: '3' }] },
        ],
      }

      const eachBatch = jest.fn(async ({ commitOffsetsIfNecessary }) => {
        // The round lands while this handler is running: it keeps topic-a/0 and loses the rest.
        await applyCooperative(group, { [TOPIC_A]: [0] })
        await commitOffsetsIfNecessary(payload)
      })

      const runner = buildRunner({ group, eachBatch })
      await runner.handleBatch(messageBatch(TOPIC_A, 0))

      const committed = commitOffsets.mock.calls.find(([offsets]) => offsets && offsets.topics)
      expect(committed[0].topics).toEqual([
        { topic: TOPIC_A, partitions: [{ partition: 0, offset: '5' }] },
      ])
    })

    it('commits nothing when a caller-supplied payload names only revoked partitions', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})
      const commitOffsets = jest.spyOn(group, 'commitOffsets').mockImplementation(async () => {})

      const eachBatch = jest.fn(async ({ commitOffsetsIfNecessary }) => {
        await applyCooperative(group, { [TOPIC_A]: [0] })
        await commitOffsetsIfNecessary({
          topics: [{ topic: TOPIC_A, partitions: [{ partition: 1, offset: '9' }] }],
        })
      })

      const runner = buildRunner({ group, eachBatch })
      await runner.handleBatch(messageBatch(TOPIC_A, 0))

      expect(commitOffsets.mock.calls.filter(([offsets]) => offsets && offsets.topics)).toEqual([])
    })

    it('commits a caller-supplied payload for partitions this member kept, from a revoked batch', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})
      const commitOffsets = jest.spyOn(group, 'commitOffsets').mockImplementation(async () => {})

      const eachBatch = jest.fn(async ({ commitOffsetsIfNecessary }) => {
        // This batch's own partition is the one that goes away; the payload names another.
        await applyCooperative(group, { [TOPIC_A]: [0] })
        await commitOffsetsIfNecessary({
          topics: [{ topic: TOPIC_A, partitions: [{ partition: 0, offset: '5' }] }],
        })
      })

      const runner = buildRunner({ group, eachBatch })
      await runner.handleBatch(messageBatch(TOPIC_A, 1))

      const committed = commitOffsets.mock.calls.find(([offsets]) => offsets && offsets.topics)
      expect(committed[0].topics).toEqual([
        { topic: TOPIC_A, partitions: [{ partition: 0, offset: '5' }] },
      ])
    })

    it('does not resolve or auto-commit from the control-batch fast-forward of a revoked partition', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      const runner = buildRunner({ group, eachBatch: async () => {} })
      const autoCommitOffsetsIfNecessary = jest.spyOn(runner, 'autoCommitOffsetsIfNecessary')
      const resolveOffset = jest.spyOn(group, 'resolveOffset')
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})

      const controlBatch = new Batch(TOPIC_A, 0, {
        partition: 1,
        highWatermark: '100',
        messages: [],
        // A batch whose only records were filtered out is empty due to filtering, not empty.
        lastOffset: '5',
      })
      jest.spyOn(controlBatch, 'isEmptyDueToFiltering').mockReturnValue(true)

      await applyCooperative(group, { [TOPIC_A]: [0] })
      await runner.handleBatch(controlBatch)

      expect(resolveOffset).not.toHaveBeenCalled()
      expect(autoCommitOffsetsIfNecessary).not.toHaveBeenCalled()
    })
  })

  describe('the stale-assignment latch', () => {
    const withStaleMetadata = (group) => {
      group.partitionsPerSubscribedTopic = new Map([
        [TOPIC_A, [0, 1]],
        [TOPIC_B, [0, 1, 2, 3]],
      ])
    }

    it('reports once per pending period however many fetchers ask', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      withStaleMetadata(group)

      const warn = jest.spyOn(group.logger, 'warn')
      const requestRejoin = jest.fn()
      group.rebalanceCoordinator = {
        classify: jest.fn(() => ({ kind: 'revoked', reason: 'stale-assignment' })),
        requestRejoin,
      }

      let thrown = 0
      // Four fetchers, several rounds each.
      for (let round = 0; round < 12; round++) {
        try {
          group.checkForStaleAssignment()
        } catch (e) {
          thrown++
          await group.recoverFromFetch(e)
        }
      }

      expect(thrown).toEqual(1)
      expect(requestRejoin).toHaveBeenCalledTimes(1)
      expect(group.staleAssignmentPending).toBe(true)
      expect(warn.mock.calls.filter(([message]) => /resync group/.test(message))).toHaveLength(1)
    })

    it('re-arms detection when the apply lands, so the next genuine change is reported', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      withStaleMetadata(group)

      expect(() => group.checkForStaleAssignment()).toThrow('Topic has been updated')
      expect(() => group.checkForStaleAssignment()).not.toThrow()

      await applyCooperative(group, { [TOPIC_A]: [0, 1] })

      expect(group.staleAssignmentPending).toBe(false)
      withStaleMetadata(group)
      expect(() => group.checkForStaleAssignment()).toThrow('Topic has been updated')
    })
  })

  describe('the leader-side cooperative check', () => {
    const encodeOwned = (ownedPartitions) =>
      MemberMetadata.encode({ version: 1, topics: [TOPIC_A], ownedPartitions })

    const encodeAssignment = (assignment) => MemberAssignment.encode({ version: 1, assignment })

    const buildProtocol = ({ mode, groupProtocol, assignment }) => {
      const assigner = { name: groupProtocol, assign: jest.fn(async () => assignment) }
      const protocol = new ClassicGroupProtocol({
        cluster: { refreshMetadata: jest.fn(async () => {}) },
        groupId: 'cooperative-group',
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        assigners: [assigner],
        mode,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: newLogger(),
      })

      protocol.memberId = 'member-1'
      protocol.leaderId = 'member-1'
      protocol.generationId = 1
      protocol.groupProtocol = groupProtocol
      protocol.members = [
        {
          memberId: 'member-1',
          memberMetadata: encodeOwned([{ topic: TOPIC_A, partitions: [0] }]),
        },
        {
          memberId: 'member-2',
          memberMetadata: encodeOwned([{ topic: TOPIC_A, partitions: [1] }]),
        },
      ]
      protocol.coordinator = {
        syncGroup: jest.fn(async () => ({ memberAssignment: encodeAssignment({}) })),
      }

      return protocol
    }

    const movesAnOwnedPartition = [
      { memberId: 'member-1', memberAssignment: encodeAssignment({ [TOPIC_A]: [0, 1] }) },
      { memberId: 'member-2', memberAssignment: encodeAssignment({}) },
    ]

    it('throws when a non-cooperative-sticky assignor moves a partition another member owns', async () => {
      const protocol = buildProtocol({
        mode: COOPERATIVE,
        groupProtocol: 'range',
        assignment: movesAnOwnedPartition,
      })

      await expect(
        protocol.sync({ topics: [TOPIC_A], topicsSubscribed: [TOPIC_A], beforeSync: () => {} })
      ).rejects.toThrow(/topic-a-1.*member-1.*member-2|topic-a-1/)

      expect(protocol.coordinator.syncGroup).not.toHaveBeenCalled()
    })

    it('names the topic, the partition and both member ids', async () => {
      const protocol = buildProtocol({
        mode: COOPERATIVE,
        groupProtocol: 'range',
        assignment: movesAnOwnedPartition,
      })

      const error = await protocol
        .sync({ topics: [TOPIC_A], topicsSubscribed: [TOPIC_A], beforeSync: () => {} })
        .catch((e) => e)

      expect(error.name).toEqual('KafkaJSNonRetriableError')
      expect(error.message).toContain(TOPIC_A)
      expect(error.message).toContain('1')
      expect(error.message).toContain('member-1')
      expect(error.message).toContain('member-2')
    })

    it('exempts cooperative-sticky, whose own withhold rule guarantees it', async () => {
      const protocol = buildProtocol({
        mode: COOPERATIVE,
        groupProtocol: 'cooperative-sticky',
        assignment: movesAnOwnedPartition,
      })

      await expect(
        protocol.sync({ topics: [TOPIC_A], topicsSubscribed: [TOPIC_A], beforeSync: () => {} })
      ).resolves.toMatchObject({ assignment: {} })
    })

    it('does not check under eager, where every member revoked everything first', async () => {
      const protocol = buildProtocol({
        mode: EAGER,
        groupProtocol: 'range',
        assignment: movesAnOwnedPartition,
      })

      await expect(
        protocol.sync({ topics: [TOPIC_A], topicsSubscribed: [TOPIC_A], beforeSync: () => {} })
      ).resolves.toMatchObject({ assignment: {} })
    })

    it('accepts an assignment that only hands a member back what it already owns', async () => {
      const protocol = buildProtocol({
        mode: COOPERATIVE,
        groupProtocol: 'range',
        assignment: [
          { memberId: 'member-1', memberAssignment: encodeAssignment({ [TOPIC_A]: [0, 2] }) },
          { memberId: 'member-2', memberAssignment: encodeAssignment({ [TOPIC_A]: [1] }) },
        ],
      })

      await expect(
        protocol.sync({ topics: [TOPIC_A], topicsSubscribed: [TOPIC_A], beforeSync: () => {} })
      ).resolves.toMatchObject({ assignment: {} })
    })

    it('tolerates a member whose subscription carries no owned partitions', async () => {
      const protocol = buildProtocol({
        mode: COOPERATIVE,
        groupProtocol: 'range',
        assignment: [
          { memberId: 'member-1', memberAssignment: encodeAssignment({ [TOPIC_A]: [0, 1] }) },
        ],
      })
      protocol.members = [
        {
          memberId: 'member-1',
          memberMetadata: MemberMetadata.encode({ version: 0, topics: [TOPIC_A] }),
        },
        { memberId: 'member-2', memberMetadata: Buffer.alloc(0) },
      ]

      await expect(
        protocol.sync({ topics: [TOPIC_A], topicsSubscribed: [TOPIC_A], beforeSync: () => {} })
      ).resolves.toMatchObject({ assignment: {} })
    })
  })
})
