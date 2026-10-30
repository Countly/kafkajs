const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const Cluster = require('../../cluster')
const Runner = require('../runner')
const Batch = require('../batch')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const IdleWait = require('../idleWait')
const PollTimer = require('../pollTimer')
const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')
const { COOPERATIVE, EAGER } = require('../groupProtocol/interface')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { events } = require('../instrumentationEvents')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const {
  REBALANCE_PATHS,
  stampGeneration,
  stampOwnership,
  sentGeneration,
} = require('../rebalanceErrorContract')
const { newLogger } = require('testHelpers')

/** Rounds the cases below begin, retired after each case. */
const rounds = []

/** @returns {object} a real round over a private idle wait, for a protocol call made directly */
const realRound = () => {
  const group = Object.create(ConsumerGroup.prototype)
  Object.assign(group, {
    idleWait: new IdleWait(),
    rebalanceTimeout: 60000,
    cluster: { allowAutoTopicCreation: true },
    isStopping: () => false,
  })
  const round = group.beginRebalanceRound()
  rounds.push(round)
  return round
}

afterEach(() => rounds.splice(0).forEach((round) => round.retire()))

const ILLEGAL_GENERATION = 22
const FENCED_INSTANCE_ID = 82

const TOPIC_A = 'topic-a'
const TOPIC_B = 'topic-b'

const partitionMetadata = (count) =>
  Array.from({ length: count }, (_, partitionId) => ({ partitionId }))

/**
 * A real `ConsumerGroup` with a stubbed cluster and a real `OffsetManager`, so every assertion is
 * about the shipped apply rather than a double of it.
 */
const createGroup = ({
  mode = COOPERATIVE,
  topics = [TOPIC_A, TOPIC_B],
  committedOffsets = {},
} = {}) => {
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    committedOffsets: () => committedOffsets,
    topicId: () => undefined,
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
    generationId: 1,
    groupId: 'cooperative-group',
    discardUncommittedOffsets: jest.fn(),
    attach: jest.fn(),
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
    forgetOwnedPartitions: jest.fn(),
    beginRebalanceRound: jest.fn(() => ({
      guard: () => null,
      wait: (work) => work(),
      retire: jest.fn(),
    })),
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
      expect(applies).toEqual([{ rebuildOffsets: false, round: expect.any(Object) }])

      consumerGroup.rebalanceProtocol = () => EAGER
      await coordinator.runRound('rebalance-in-progress', 'revoked')
      expect(applies[1]).toEqual({ rebuildOffsets: true, round: expect.any(Object) })
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

  describe('a generation this member has already left', () => {
    const answeredUnder = (code, generationId) =>
      stampGeneration(createErrorFromCode(code), generationId)

    it('records the generation a heartbeat was sent under on what it throws', async () => {
      const protocol = new ClassicGroupProtocol({
        cluster: {},
        groupId: 'cooperative-group',
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        assigners: [],
        mode: COOPERATIVE,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: newLogger(),
      })
      protocol.memberId = 'member-1'
      protocol.generationId = 7
      protocol.lastRequest = 0
      protocol.coordinator = {
        heartbeat: jest.fn(async () => {
          // The round the answer is late for: the member is already in generation 8.
          protocol.generationId = 8
          throw createErrorFromCode(ILLEGAL_GENERATION)
        }),
      }

      const error = await protocol.heartbeat({ interval: 3000 }).catch((e) => e)

      expect(sentGeneration(error)).toEqual(7)
    })

    it('records the generation an offset commit was sent under on what it throws', async () => {
      const manager = new OffsetManager({
        cluster: { committedOffsets: () => ({}) },
        coordinator: {
          isConnected: () => true,
          offsetCommit: jest.fn(async () => {
            throw createErrorFromCode(ILLEGAL_GENERATION)
          }),
        },
        memberAssignment: { [TOPIC_A]: [0] },
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        topicConfigurations: {},
        instrumentationEmitter: new InstrumentationEventEmitter(),
        groupId: 'cooperative-group',
        generationId: 7,
        memberId: 'member-1',
      })

      const error = await manager
        .commitOffsets({
          topics: [{ topic: TOPIC_A, partitions: [{ partition: 0, offset: '5' }] }],
        })
        .catch((e) => e)

      expect(sentGeneration(error)).toEqual(7)
    })

    it('drops a lost verdict answered about a generation the member has left', () => {
      const { coordinator, consumerGroup, fetchManager } = createCoordinator()
      consumerGroup.generationId = 2

      coordinator.requestRejoin({
        kind: 'lost',
        reason: 'lost',
        error: answeredUnder(ILLEGAL_GENERATION, 1),
      })

      expect(fetchManager.stop).not.toHaveBeenCalled()
      expect(coordinator.takeVerdict()).toBeNull()
    })

    it('keeps a lost verdict answered about the generation the member still holds', () => {
      const { coordinator, consumerGroup, fetchManager } = createCoordinator()
      consumerGroup.generationId = 2

      coordinator.requestRejoin({
        kind: 'lost',
        reason: 'lost',
        error: answeredUnder(ILLEGAL_GENERATION, 2),
      })

      expect(fetchManager.stop).toHaveBeenCalled()
      expect(coordinator.takeVerdict()).not.toBeNull()
    })

    it('never drops a fenced static member, whatever the generation says', () => {
      const { coordinator, consumerGroup, fetchManager } = createCoordinator()
      consumerGroup.generationId = 2

      coordinator.requestRejoin({
        kind: 'lost',
        reason: 'lost',
        error: answeredUnder(FENCED_INSTANCE_ID, 1),
      })

      expect(fetchManager.stop).toHaveBeenCalled()
    })

    it('never drops a verdict that came from no group request, such as a poll overrun', () => {
      const { coordinator, consumerGroup, fetchManager } = createCoordinator()
      consumerGroup.generationId = null

      coordinator.requestRejoin({
        kind: 'lost',
        reason: 'poll-interval-exceeded',
        error: new Error('the handler overran'),
      })

      expect(fetchManager.stop).toHaveBeenCalled()
    })

    it('does not discard offsets or rejoin for it on the fetch path', async () => {
      const { coordinator, consumerGroup } = createCoordinator()
      const runner = new Runner({
        logger: newLogger(),
        consumerGroup,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        concurrency: 1,
        heartbeatInterval: 3000,
        maxPollInterval: 300000,
        eachBatch: async () => {},
        onCrash: jest.fn(),
      })
      runner.heartbeatManager.stop()
      runner.rebalanceCoordinator = coordinator
      consumerGroup.generationId = 2

      await runner.handleRebalanceError({
        kind: 'lost',
        reason: 'lost',
        error: answeredUnder(ILLEGAL_GENERATION, 1),
      })

      expect(consumerGroup.discardUncommittedOffsets).not.toHaveBeenCalled()
      expect(runner.quiescing).toBe(false)
      expect(consumerGroup.joinAndSync).not.toHaveBeenCalled()
    })

    describe('and what it does to the membership', () => {
      /** A member holding partitions 0 and 1 of generation 1, with a real coordinator attached. */
      const createMember = async () => {
        const { group } = createGroup({ topics: [TOPIC_A] })
        group.autoCommit = true
        const coordinator = new RebalanceCoordinator({
          runner: { stopRequested: false, onCrash: jest.fn() },
          consumerGroup: group,
          fetchManager: {
            start: jest.fn(async () => {}),
            stop: jest.fn(async () => {}),
            getFetchers: () => [],
          },
          heartbeatManager: { ensureStarted: jest.fn(), stop: jest.fn() },
          pollTimer: new PollTimer({ pollDeadline: 8000 }),
          instrumentationEmitter: new InstrumentationEventEmitter(),
          logger: newLogger(),
          groupId: 'cooperative-group',
        })
        group.attach({
          rebalanceCoordinator: coordinator,
          heartbeatManager: { ensureStarted: jest.fn(), stop: jest.fn() },
        })
        await group.applyRebalanceResult({ assignment: { [TOPIC_A]: [0, 1] } })
        group.resolveOffset({ topic: TOPIC_A, partition: 1, offset: '4' })
        group.coordinator.offsetCommit.mockClear()
        return { group, coordinator }
      }

      /** An answer to a request sent under `generationId` and the membership held now. */
      const answeredOnMembership = (group, code, generationId) =>
        stampOwnership(answeredUnder(code, generationId), group.ownership.token())

      /** The cooperative sync revokes partition 1; returns the OffsetCommits that apply sent. */
      const revokePartitionOne = async (group) => {
        await applyCooperative(group, { [TOPIC_A]: [0] })
        return group.coordinator.offsetCommit.mock.calls.map(([request]) => request)
      }

      it('keeps the pre-release commit alive after an auto-commit sent under the old generation is refused', async () => {
        const { group } = await createMember()
        group.generationId = 2
        group.coordinator.offsetCommit.mockRejectedValueOnce(
          createErrorFromCode(ILLEGAL_GENERATION)
        )

        await group.offsetManager.commitOffsets({}, { auto: true }).catch(() => {})

        expect(group.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
        expect(group.coordinator.offsetCommit.mock.calls[0][0].groupGenerationId).toBe(1)
        expect(group.ownership.refusal(group.ownership.token())).toBeNull()
        group.coordinator.offsetCommit.mockClear()
        const sent = await revokePartitionOne(group)
        expect(sent).toHaveLength(1)
        expect(sent[0]).toMatchObject({ groupGenerationId: 2, topics: [{ topic: TOPIC_A }] })
        expect(sent[0].topics[0].partitions.map(({ partition }) => Number(partition))).toEqual([1])
      })

      it('keeps the pre-release commit alive after a heartbeat sent under the old generation is answered', async () => {
        const { group, coordinator } = await createMember()
        group.generationId = 2

        const verdict = coordinator.classify(
          answeredOnMembership(group, ILLEGAL_GENERATION, 1),
          REBALANCE_PATHS.HEARTBEAT
        )
        coordinator.requestRejoin(verdict)

        expect(group.ownership.refusal(group.ownership.token())).toBeNull()
        const sent = await revokePartitionOne(group)
        expect(sent).toHaveLength(1)
        expect(sent[0]).toMatchObject({ groupGenerationId: 2 })
      })

      it('keeps the pre-release commit alive when the stale verdict reaches requestRejoin alone', async () => {
        const { group, coordinator } = await createMember()
        group.generationId = 2

        coordinator.requestRejoin({
          kind: 'lost',
          reason: 'lost',
          error: answeredOnMembership(group, ILLEGAL_GENERATION, 1),
        })

        expect(group.ownership.refusal(group.ownership.token())).toBeNull()
        expect(await revokePartitionOne(group)).toHaveLength(1)
      })

      it('refuses the pre-release commit after a heartbeat answered about the generation held now', async () => {
        const { group, coordinator } = await createMember()
        group.generationId = 2

        const verdict = coordinator.classify(
          answeredOnMembership(group, ILLEGAL_GENERATION, 2),
          REBALANCE_PATHS.HEARTBEAT
        )
        coordinator.requestRejoin(verdict)

        expect(group.ownership.refusal(group.ownership.token())).not.toBeNull()
        expect(await revokePartitionOne(group)).toHaveLength(0)
      })

      it('refuses the pre-release commit after an auto-commit answered about the generation held now', async () => {
        const { group } = await createMember()
        group.coordinator.offsetCommit.mockRejectedValueOnce(
          createErrorFromCode(ILLEGAL_GENERATION)
        )

        await group.offsetManager.commitOffsets({}, { auto: true }).catch(() => {})

        expect(group.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
        expect(group.ownership.refusal(group.ownership.token())).not.toBeNull()
        group.coordinator.offsetCommit.mockClear()
        expect(await revokePartitionOne(group)).toHaveLength(0)
      })

      it('refuses the pre-release commit after a fenced static member, whatever generation the answer names', async () => {
        const { group, coordinator } = await createMember()
        group.generationId = 2

        coordinator.classify(
          answeredOnMembership(group, FENCED_INSTANCE_ID, 1),
          REBALANCE_PATHS.HEARTBEAT
        )

        expect(group.ownership.refusal(group.ownership.token())).not.toBeNull()
        expect(await revokePartitionOne(group)).toHaveLength(0)
      })

      it('learns a loss under the consumer protocol, where the epoch an answer names carries no staleness', async () => {
        const { group } = await createMember()
        Object.defineProperty(group, 'kind', { value: 'consumer' })
        group.generationId = 5

        group.ownership.lose(
          answeredOnMembership(group, ILLEGAL_GENERATION, 3),
          group.ownership.token()
        )

        expect(group.ownership.refusal(group.ownership.token())).not.toBeNull()
      })
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

    it('skips automatic progress and refuses explicit commits for a revoked partition mid-handler', async () => {
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
        await expect(commitOffsetsIfNecessary({ topics: [] })).rejects.toMatchObject({
          name: 'KafkaJSCommitFailedError',
          reason: 'stale-assignment',
        })
      })

      const runner = buildRunner({ group, eachBatch })
      await runner.handleBatch(messageBatch(TOPIC_A, 1))

      expect(eachBatch).toHaveBeenCalled()
      expect(group.offsetManager.resolvedOffsets[TOPIC_A][1]).toBeUndefined()
      expect(commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(commitOffsets).not.toHaveBeenCalledWith({ topics: [] })
    })

    it('resolves nothing for a partition revoked mid-batch, and still resolves a retained one', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})

      const eachBatch = jest.fn(async ({ batch, resolveOffset }) => {
        if (batch.partition === 1) {
          // The round that takes this partition away lands while the handler holds it.
          await applyCooperative(group, { [TOPIC_A]: [0] })
        }

        resolveOffset(batch.messages[0].offset)
      })

      const runner = buildRunner({ group, eachBatch })
      await runner.handleBatch(messageBatch(TOPIC_A, 1))
      await runner.handleBatch(messageBatch(TOPIC_A, 0))

      expect(group.offsetManager.resolvedOffsets[TOPIC_A][1]).toBeUndefined()
      expect(group.offsetManager.resolvedOffsets[TOPIC_A][0]).toEqual('1')
      // And nothing was committed for it either: only the retained partition reaches the wire.
      expect(
        group.coordinator.offsetCommit.mock.calls.flatMap(([{ topics }]) =>
          topics.flatMap(({ topic, partitions }) =>
            partitions.map(({ partition }) => `${topic}-${partition}`)
          )
        )
      ).toEqual([`${TOPIC_A}-0`])
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

    it('refuses an explicit payload from a revoked batch even when it names a retained partition', async () => {
      const { group } = createGroup()
      await applyCooperative(group, { [TOPIC_A]: [0, 1] })
      jest.spyOn(group, 'heartbeat').mockImplementation(async () => {})
      const commitOffsets = jest.spyOn(group, 'commitOffsets').mockImplementation(async () => {})

      const eachBatch = jest.fn(async ({ commitOffsetsIfNecessary }) => {
        // This batch's own partition is the one that goes away; the payload names another.
        await applyCooperative(group, { [TOPIC_A]: [0] })
        await expect(
          commitOffsetsIfNecessary({
            topics: [{ topic: TOPIC_A, partitions: [{ partition: 0, offset: '5' }] }],
          })
        ).rejects.toMatchObject({ name: 'KafkaJSCommitFailedError', reason: 'stale-assignment' })
      })

      const runner = buildRunner({ group, eachBatch })
      await runner.handleBatch(messageBatch(TOPIC_A, 1))

      const committed = commitOffsets.mock.calls.find(([offsets]) => offsets && offsets.topics)
      expect(committed).toBeUndefined()
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

  describe('the committed-offsets cache shared by one client', () => {
    const buildManager = (cluster, memberAssignment) =>
      new OffsetManager({
        cluster,
        coordinator: { isConnected: () => true },
        memberAssignment,
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        topicConfigurations: {},
        instrumentationEmitter: new InstrumentationEventEmitter(),
        groupId: 'shared-group',
        generationId: 1,
        memberId: 'member-1',
      })

    it('is one object per client for a group, which is what makes the sharing real', () => {
      const offsets = new Map()
      const options = { brokers: ['localhost:9092'], logger: newLogger(), offsets }
      const one = new Cluster(options)
      const two = new Cluster(options)

      expect(one.committedOffsets({ groupId: 'shared-group' })).toBe(
        two.committedOffsets({ groupId: 'shared-group' })
      )
    })

    it("leaves another member's keys alone when a manager is built", () => {
      const committedOffsets = { [TOPIC_A]: { 0: '10', 1: '20' }, [TOPIC_B]: { 0: '30' } }
      const cluster = { committedOffsets: () => committedOffsets }

      buildManager(cluster, { [TOPIC_A]: [0] })

      expect(committedOffsets[TOPIC_A][1]).toEqual('20')
      expect(committedOffsets[TOPIC_B]).toEqual({ 0: '30' })
      // Its own partition is reset, which is what a fresh manager means.
      expect(committedOffsets[TOPIC_A][0]).toBeUndefined()
    })

    it("leaves another member's keys alone when uncommitted offsets are discarded", () => {
      const committedOffsets = { [TOPIC_A]: { 0: '10', 1: '20' } }
      const cluster = { committedOffsets: () => committedOffsets }
      const manager = buildManager(cluster, { [TOPIC_A]: [0] })
      committedOffsets[TOPIC_A][0] = '11'

      manager.clearAllOffsets()

      expect(committedOffsets[TOPIC_A]).toEqual({ 1: '20' })
    })

    it("prunes a revoked partition without touching another member's", () => {
      const committedOffsets = { [TOPIC_A]: { 0: '10', 1: '20', 2: '30' } }
      const cluster = { committedOffsets: () => committedOffsets }
      const manager = buildManager(cluster, { [TOPIC_A]: [0, 1] })
      committedOffsets[TOPIC_A][0] = '11'
      committedOffsets[TOPIC_A][1] = '21'

      manager.updateAssignment({
        revoked: [{ topic: TOPIC_A, partitions: [1] }],
        memberAssignment: { [TOPIC_A]: [0] },
        generationId: 2,
        memberId: 'member-1',
        coordinator: { isConnected: () => true },
      })

      expect(committedOffsets[TOPIC_A]).toEqual({ 0: '11', 2: '30' })
    })

    it('survives a sibling manager whose interim assignment is empty', () => {
      const committedOffsets = { [TOPIC_A]: { 0: '10' } }
      const cluster = { committedOffsets: () => committedOffsets }
      const incumbent = buildManager(cluster, { [TOPIC_A]: [0, 1] })
      committedOffsets[TOPIC_A][0] = '11'

      // The joining member's withheld first round: a manager over no topics at all.
      buildManager(cluster, {})

      expect(() =>
        incumbent.updateAssignment({
          revoked: [{ topic: TOPIC_A, partitions: [1] }],
          memberAssignment: { [TOPIC_A]: [0] },
          generationId: 2,
          memberId: 'member-1',
          coordinator: { isConnected: () => true },
        })
      ).not.toThrow()

      expect(committedOffsets[TOPIC_A][0]).toEqual('11')
    })

    it('keeps two consumer groups of one client alive through a cooperative round', async () => {
      const committedOffsets = {}
      const incumbent = createGroup({ committedOffsets, topics: [TOPIC_A] }).group
      const joining = createGroup({ committedOffsets, topics: [TOPIC_A] }).group
      joining.memberId = 'member-2'

      await applyCooperative(incumbent, { [TOPIC_A]: [0, 1] })
      incumbent.resolveOffset({ topic: TOPIC_A, partition: 0, offset: '10', leaderEpoch: 4 })

      // Round 1: the joining member is withheld everything, the incumbent gives up partition 1.
      await applyCooperative(joining, {})
      await expect(applyCooperative(incumbent, { [TOPIC_A]: [0] })).resolves.toBeDefined()

      // Round 2: the withheld partition reaches its new owner.
      await applyCooperative(joining, { [TOPIC_A]: [1] })

      expect(incumbent.offsetManager.resolvedOffsets[TOPIC_A][0]).toEqual('11')
      expect(incumbent.offsetManager.resolvedEpochs[TOPIC_A][0]).toEqual(4)
      expect(joining.offsetManager.memberAssignment).toEqual({ [TOPIC_A]: [1] })
    })

    it('survives the pre-release commit writing a key the apply then deletes', async () => {
      const committedOffsets = {}
      const cluster = { committedOffsets: () => committedOffsets }
      const offsetCommit = jest.fn(async () => {})
      // Both managers are built BEFORE the cache is seeded: a fresh manager resets its own
      // partitions, so seeding first would be undone here rather than in production.
      const releasing = buildManager(cluster, { [TOPIC_A]: [0, 1] })
      const sibling = buildManager(cluster, { [TOPIC_B]: [0] })
      releasing.coordinator = { isConnected: () => true, offsetCommit }
      committedOffsets[TOPIC_A] = { 0: '10', 1: '20' }
      committedOffsets[TOPIC_B] = { 0: '30' }

      releasing.resolveOffset({ topic: TOPIC_A, partition: 1, offset: '41' })
      await releasing.commitOffsets({}, { partitions: [{ topic: TOPIC_A, partitions: [1] }] })

      // The commit wrote into the object every manager on this client shares.
      expect(committedOffsets[TOPIC_A][1]).toEqual('42')

      releasing.updateAssignment({
        revoked: [{ topic: TOPIC_A, partitions: [1] }],
        memberAssignment: { [TOPIC_A]: [0] },
        generationId: 2,
        memberId: 'member-1',
        coordinator: { isConnected: () => true },
      })

      // The window itself: the key the commit just wrote is gone, and nothing else is - not the
      // partition this member kept, and not the sibling's topic.
      expect(committedOffsets[TOPIC_A]).toEqual({ 0: '10' })
      expect(committedOffsets[TOPIC_B]).toEqual({ 0: '30' })
      expect(sibling.committedOffsets()[TOPIC_B]).toEqual({ 0: '30' })
    })
  })

  describe('the leader-side cooperative check', () => {
    const encodeOwned = (ownedPartitions) =>
      MemberMetadata.encode({ version: 1, topics: [TOPIC_A], ownedPartitions })

    const encodeAssignment = (assignment) => MemberAssignment.encode({ version: 1, assignment })

    const buildProtocol = ({ mode, groupProtocol, assignment }) => {
      const assigner = { name: groupProtocol, assign: jest.fn(async () => assignment) }
      const protocol = new ClassicGroupProtocol({
        cluster: {
          refreshMetadata: jest.fn(async () => {}),
          findTopicPartitionMetadata: () => [],
        },
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
        protocol.sync({
          topics: [TOPIC_A],
          topicsSubscribed: [TOPIC_A],
          beforeSync: () => {},
          round: realRound(),
        })
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
        .sync({
          topics: [TOPIC_A],
          topicsSubscribed: [TOPIC_A],
          beforeSync: () => {},
          round: realRound(),
        })
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
        protocol.sync({
          topics: [TOPIC_A],
          topicsSubscribed: [TOPIC_A],
          beforeSync: () => {},
          round: realRound(),
        })
      ).resolves.toMatchObject({ assignment: {} })
    })

    it('does not check under eager, where every member revoked everything first', async () => {
      const protocol = buildProtocol({
        mode: EAGER,
        groupProtocol: 'range',
        assignment: movesAnOwnedPartition,
      })

      await expect(
        protocol.sync({
          topics: [TOPIC_A],
          topicsSubscribed: [TOPIC_A],
          beforeSync: () => {},
          round: realRound(),
        })
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
        protocol.sync({
          topics: [TOPIC_A],
          topicsSubscribed: [TOPIC_A],
          beforeSync: () => {},
          round: realRound(),
        })
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
        protocol.sync({
          topics: [TOPIC_A],
          topicsSubscribed: [TOPIC_A],
          beforeSync: () => {},
          round: realRound(),
        })
      ).resolves.toMatchObject({ assignment: {} })
    })
  })
})

describe('Consumer > cooperative rebalance > commit before release', () => {
  /**
   * Applies a first assignment, resolves an offset on every partition of it, then applies the
   * second one incrementally. The resolve is what gives the pre-release commit something to send.
   */
  const applyTwice = async (group, { first, second, autoCommit = true, resolve = true }) => {
    group.autoCommit = autoCommit
    await group.applyRebalanceResult({ assignment: first }, { rebuildOffsets: true })

    if (resolve) {
      for (const [topic, partitions] of Object.entries(first)) {
        for (const partition of partitions) {
          group.resolveOffset({ topic, partition, offset: `${10 + partition}` })
        }
      }
    }

    return group.applyRebalanceResult({ assignment: second }, { rebuildOffsets: false })
  }

  it('commits the resolved offsets of the revoked partitions before pruning them', async () => {
    const { group } = createGroup()
    const offsetCommit = jest.fn(async () => {})
    group.coordinator.offsetCommit = offsetCommit

    await applyTwice(group, {
      first: { [TOPIC_A]: [0, 1] },
      second: { [TOPIC_A]: [0] },
    })

    // The commit went out, and it named the partition being released - and only that one.
    expect(offsetCommit).toHaveBeenCalledTimes(1)
    const [{ topics }] = offsetCommit.mock.calls[0]
    const released = topics
      .find(({ topic }) => topic === TOPIC_A)
      .partitions.map(({ partition }) => Number(partition))
    expect(released).toEqual([1])
  })

  it('sends it under the refreshed membership, not the previous snapshot', async () => {
    const { group } = createGroup()
    const offsetCommit = jest.fn(async () => {})
    group.autoCommit = true
    group.coordinator.offsetCommit = offsetCommit

    await group.applyRebalanceResult(
      { assignment: { [TOPIC_A]: [0, 1] } },
      { rebuildOffsets: true }
    )
    group.resolveOffset({ topic: TOPIC_A, partition: 1, offset: '10' })
    group.generationId = 7
    group.memberId = 'member-7'

    await group.applyRebalanceResult({ assignment: { [TOPIC_A]: [0] } }, { rebuildOffsets: false })

    expect(offsetCommit).toHaveBeenCalledWith(
      expect.objectContaining({ groupGenerationId: 7, memberId: 'member-7' }),
      { guard: expect.any(Function) }
    )
  })

  it('proceeds with the round when the commit fails', async () => {
    const { group } = createGroup()
    const offsetCommit = jest.fn(async () => {
      throw new Error('coordinator is unavailable')
    })
    group.coordinator.offsetCommit = offsetCommit

    const result = await applyTwice(group, {
      first: { [TOPIC_A]: [0, 1] },
      second: { [TOPIC_A]: [0] },
    })

    // The throwing mock was actually reached, and the round still completed.
    expect(offsetCommit).toHaveBeenCalledTimes(1)
    expect(result.revoked).toEqual([{ topic: TOPIC_A, partitions: [1] }])
    expect(group.assigned()).toEqual([{ topic: TOPIC_A, partitions: [0] }])
  })

  it('swallows a LOST-class commit failure instead of throwing out of the apply', async () => {
    const { group } = createGroup()
    const offsetCommit = jest.fn(async () => {
      throw new KafkaJSProtocolError(createErrorFromCode(ILLEGAL_GENERATION))
    })
    group.coordinator.offsetCommit = offsetCommit

    await expect(
      applyTwice(group, { first: { [TOPIC_A]: [0, 1] }, second: { [TOPIC_A]: [0] } })
    ).resolves.toMatchObject({ revoked: [{ topic: TOPIC_A, partitions: [1] }] })
    expect(offsetCommit).toHaveBeenCalledTimes(1)
  })

  it('does not commit when autoCommit is off', async () => {
    const { group } = createGroup()
    const offsetCommit = jest.fn(async () => {})
    group.coordinator.offsetCommit = offsetCommit

    await applyTwice(group, {
      first: { [TOPIC_A]: [0, 1] },
      second: { [TOPIC_A]: [0] },
      autoCommit: false,
    })

    expect(offsetCommit).not.toHaveBeenCalled()
  })

  it('does not commit when a cooperative round revokes nothing', async () => {
    const { group } = createGroup()
    const offsetCommit = jest.fn(async () => {})
    group.coordinator.offsetCommit = offsetCommit

    await applyTwice(group, {
      first: { [TOPIC_A]: [0] },
      second: { [TOPIC_A]: [0, 1] },
    })

    expect(offsetCommit).not.toHaveBeenCalled()
  })

  it('does not commit on the rebuild path, where the verdict already committed', async () => {
    const { group } = createGroup()
    const offsetCommit = jest.fn(async () => {})
    group.autoCommit = true
    group.coordinator.offsetCommit = offsetCommit

    await group.applyRebalanceResult(
      { assignment: { [TOPIC_A]: [0, 1] } },
      { rebuildOffsets: true }
    )
    group.resolveOffset({ topic: TOPIC_A, partition: 1, offset: '10' })
    await group.applyRebalanceResult({ assignment: { [TOPIC_A]: [0] } }, { rebuildOffsets: true })

    expect(offsetCommit).not.toHaveBeenCalled()
  })
})
