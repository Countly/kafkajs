const createConsumer = require('../index')
const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const OffsetOperations = require('../offsetManager/offsetOperations')
const Batch = require('../batch')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { events } = require('../instrumentationEvents')
const waitFor = require('../../utils/waitFor')
const { newLogger } = require('testHelpers')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const Subscription = require('../subscription')
const ConsumerGroupProtocol = require('../groupProtocol/consumer')

/** Every runner this file builds, torn down after the case that built it. */
const built = []

const GROUP_ID = 'reconciliation-group'
const TOPIC = 'topic-a'
const TOPIC_ID = 'kOJl1vDcS4Ct5xLg5rGqaQ'
const PARTITIONS = [0, 1, 2]
const FENCED_MEMBER_EPOCH = 110

/** One ConsumerGroupHeartbeat response carrying an assignment for TOPIC_ID. */
const assignment = (memberEpoch, partitions) => ({
  throttleTime: 0,
  errorCode: 0,
  errorMessage: null,
  memberId: 'member-1',
  memberEpoch,
  heartbeatIntervalMs: 5000,
  assignment: { topicPartitions: [{ topicId: TOPIC_ID, partitions }] },
})

/** A response that changes nothing: the coordinator has nothing new for this member. */
const idle = (memberEpoch) => ({ ...assignment(memberEpoch, []), assignment: null })

/**
 * A coordinator that answers ConsumerGroupHeartbeat from a script and records every request and
 * every OffsetCommit, in one ordered log so "before" and "after" are assertable.
 * One script entry is consumed per request; the last entry repeats.
 *
 * @param {object[]} script ConsumerGroupHeartbeat responses, in order
 * @param {object[]} log the shared ordered record every call appends to
 * @returns {object} a broker double
 */
const scriptedCoordinator = (script, log) => {
  let index = 0

  return {
    isConnected: () => true,
    nodeId: 0,
    consumerGroupHeartbeat: jest.fn(async (request) => {
      log.push({ kind: 'heartbeat', request })
      return script[Math.min(index++, script.length - 1)]
    }),
    offsetCommit: jest.fn(async (payload) => {
      log.push({ kind: 'commit', payload })
    }),
    offsetFetch: jest.fn(async () => ({ responses: [] })),
  }
}

/**
 * A real ConsumerGroup on the consumer protocol, a real RebalanceCoordinator and a real Runner over
 * the scripted coordinator. `log` is the single ordered record of everything that reached the
 * coordinator; `emitted` the same for instrumentation events.
 *
 * @param {object} options
 * @param {object[]} options.script ConsumerGroupHeartbeat responses, in order
 * @param {(payload: object) => Promise<void>} [options.eachBatch]
 * @param {number} [options.concurrency=1] how many workers the runner runs
 * @param {number} [options.rebalanceTimeout=60000] the join deadline
 * @returns {{ runner: Runner, group: ConsumerGroup, coordinator: object, log: object[],
 *   emitted: object[], trip: () => Promise<void>, joinArmed: () => Promise<void> }}
 */
const build = ({
  script,
  eachBatch = async () => {},
  concurrency = 1,
  rebalanceTimeout = 60000,
}) => {
  const log = []
  const emitted = []
  const coordinator = scriptedCoordinator(script, log)

  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadataForTopicIds: jest.fn(async () => {}),
    committedOffsets: () => ({}),
    topicName: (topicId) => (topicId === TOPIC_ID ? TOPIC : undefined),
    topicId: () => undefined,
    // `leader` is what routes a partition to a node; without it the fetch path rejects the
    // metadata rather than fetching.
    findTopicPartitionMetadata: (topic) =>
      topic === TOPIC ? PARTITIONS.map((partitionId) => ({ partitionId, leader: 0 })) : [],
    findBroker: jest.fn(async () => ({ fetch: async () => ({ responses: [] }) })),
    findGroupCoordinator: jest.fn(async () => coordinator),
    addMultipleTargetTopics: jest.fn(async () => {}),
    registerAssignedTopics: jest.fn(async (names) => names),
  }

  const instrumentationEmitter = new InstrumentationEventEmitter()
  for (const name of [
    events.PARTITIONS_ASSIGNED,
    events.PARTITIONS_REVOKED,
    events.PARTITIONS_LOST,
    events.GROUP_JOIN,
    events.REBALANCING,
  ]) {
    instrumentationEmitter.addListener(name, ({ type, payload }) => {
      emitted.push({ type, payload })
      log.push({ kind: 'event', type, payload })
    })
  }

  const group = new ConsumerGroup({
    retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
    cluster,
    groupId: GROUP_ID,
    groupProtocol: 'consumer',
    topics: [TOPIC],
    topicConfigurations: { [TOPIC]: {} },
    logger: newLogger(),
    instrumentationEmitter,
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: true,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    metadataMaxAge: 300000,
  })

  group.coordinator = coordinator
  group.getNodeIds = () => [0]
  group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()

  const runner = new Runner({
    consumerGroup: group,
    instrumentationEmitter,
    logger: newLogger(),
    eachBatch,
    concurrency,
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    autoCommit: true,
    onCrash: jest.fn(),
  })
  runner.running = true
  // Scheduling is `heartbeatRequestTick.spec.js`'s subject: with the timer disabled, every request
  // in `log` is one this file asked for, so a trip is exactly one round trip.
  runner.heartbeatManager.disable()
  built.push(runner)

  /** Resolves once a join is waiting for the heartbeat only this file can send. */
  const joinArmed = () =>
    waitFor(() => group.groupMembership.immediatePending, {
      delay: 5,
      maxWait: 5000,
      timeoutMessage: 'the join never armed a heartbeat',
    })

  /**
   * One coordinator trip: send the heartbeat the membership is waiting on, then let whatever chain
   * it started settle.
   *
   * The first trip is also the join. `Runner#start()` would need a connected cluster, so the round
   * is started here instead, and the heartbeat goes out once the join has armed one - the join
   * waits for a response, and only a heartbeat can carry it.
   */
  const trip = async () => {
    if (group.memberEpoch <= 0) {
      runner.rebalanceCoordinator.runRound(null, 'initial')
      await joinArmed()
    }

    await group.heartbeat({ interval: 0 })
    await runner.rebalanceCoordinator.idle()
  }

  return { runner, group, coordinator, log, emitted, trip, joinArmed }
}

const CONTROL = 'rx-control'
const DELETED = 'rx-0'
const LATE = 'rx-2'
const LITERAL = 'literal-a'
const TOPIC_IDS = {
  [LITERAL]: 'bElTbElTbElTbElTbElTbA',
  [CONTROL]: 'cTRLcTRLcTRLcTRLcTRLcQ',
  [DELETED]: 'zERozERozERozERozERozQ',
  [LATE]: 'tWOotWOotWOotWOotWOotQ',
}

/** A ConsumerGroupHeartbeat response for a client-pattern member, one entry per topic. */
const target = (memberEpoch, byTopic) => ({
  throttleTime: 0,
  errorCode: 0,
  errorMessage: null,
  memberId: 'member-1',
  memberEpoch,
  heartbeatIntervalMs: 1,
  assignment:
    byTopic === null
      ? null
      : {
          topicPartitions: Object.entries(byTopic).map(([topic, partitions]) => ({
            topicId: TOPIC_IDS[topic],
            partitions,
          })),
        },
})

/**
 * The same real group, runner and coordinator over a client RegExp subscription and a cluster whose
 * topic metadata the case edits: `known` is what metadata currently names and serves.
 * @param {object[]} script ConsumerGroupHeartbeat responses, the last repeating
 * @param {object} [options]
 * @param {boolean} [options.literal=false] also subscribe to the literal topic `LITERAL`
 * @returns {object} the group, runner, records and helpers to discover and to trip
 */
const buildPattern = (script, { literal = false } = {}) => {
  const log = []
  const emitted = []
  const fetched = []
  const known = new Map([
    [CONTROL, [0, 1]],
    [DELETED, [0]],
    ...(literal ? [[LITERAL, [0, 1]]] : []),
  ])
  const coordinator = scriptedCoordinator(script, log)
  coordinator.offsetFetch = jest.fn(async ({ topics }) => ({
    responses: topics.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition }) => ({
        partition,
        offset: '5',
        committedLeaderEpoch: -1,
        metadata: null,
        errorCode: 0,
      })),
    })),
  }))
  const metadata = () => ({
    topicMetadata: [...known.keys()].map((topic) => ({
      topic,
      topicId: TOPIC_IDS[topic],
      topicErrorCode: 0,
      isInternal: false,
    })),
  })
  const subscription = new Subscription()
  subscription.add({ topics: [/^rx-/], policy: { kind: 'earliest' } })
  if (literal) subscription.add({ topics: [LITERAL], policy: { kind: 'earliest' } })
  const desired = subscription.evaluate(metadata())
  subscription.observe(metadata())

  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadataForTopicIds: jest.fn(async () => {}),
    committedOffsets: () => ({}),
    topicName: (topicId) =>
      [...known.keys()].find((topic) => TOPIC_IDS[topic] === topicId) || undefined,
    topicId: (topic) => (known.has(topic) ? TOPIC_IDS[topic] : undefined),
    leaderEpoch: () => -1,
    findTopicPartitionMetadata: (topic) =>
      (known.get(topic) || []).map((partitionId) => ({ partitionId, leader: 0 })),
    findBroker: jest.fn(async () => ({
      fetch: async ({ topics }) => {
        for (const { topic, partitions } of topics) {
          for (const { partition } of partitions) fetched.push(`${topic}|${partition}`)
        }
        return { responses: [] }
      },
      listOffsets: async ({ topics }) =>
        topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: '0', errorCode: 0 })),
        })),
    })),
    fetchTopicsOffset: jest.fn(async (requests) =>
      requests.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
      }))
    ),
    findGroupCoordinator: jest.fn(async () => coordinator),
    addMultipleTargetTopics: jest.fn(async () => {}),
    registerAssignedTopics: jest.fn(async (names) => names),
  }

  const instrumentationEmitter = new InstrumentationEventEmitter()
  for (const name of [
    events.PARTITIONS_ASSIGNED,
    events.PARTITIONS_REVOKED,
    events.PARTITIONS_LOST,
    events.GROUP_JOIN,
    events.REBALANCING,
  ]) {
    instrumentationEmitter.addListener(name, ({ type, payload }) => emitted.push({ type, payload }))
  }

  const group = new ConsumerGroup({
    retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
    cluster,
    groupId: GROUP_ID,
    groupProtocol: 'consumer',
    topics: desired.names,
    topicConfigurations: Subscription.configurationsOf(desired),
    subscription,
    logger: newLogger(),
    instrumentationEmitter,
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 1,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    metadataMaxAge: 300000,
  })
  group.coordinator = coordinator
  group.getNodeIds = () => [0]

  const onCrash = jest.fn()
  const runner = new Runner({
    consumerGroup: group,
    instrumentationEmitter,
    logger: newLogger(),
    eachBatch: async () => {},
    concurrency: 1,
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    autoCommit: false,
    onCrash,
  })
  runner.running = true
  runner.heartbeatManager.disable()
  built.push(runner)

  /**
   * Accepts one full metadata response naming exactly `topics`, as a refresh would.
   * @param {Record<string, number[]>} topics name to partitions
   */
  const discover = async (topics) => {
    known.clear()
    for (const [topic, partitions] of Object.entries(topics)) known.set(topic, partitions)
    group.observeDiscovery(metadata())
    const snapshot = group.discovered
    group.discovered = null
    await group.updateSubscription(snapshot)
  }

  /** One heartbeat opportunity, then a timer turn so a pending inter-round wait can elapse. */
  const opportunity = async () => {
    await group.heartbeat({ interval: 0 })
    await new Promise((resolve) => setTimeout(resolve, 3))
  }

  const count = (type) => emitted.filter((event) => event.type === type).length

  /** Every later heartbeat gets `response`. */
  const answerWith = (response) => script.splice(0, script.length, response)

  return { group, runner, log, emitted, fetched, onCrash, discover, opportunity, count, answerWith }
}

const acknowledgements = (log) =>
  log
    .filter(({ kind, request }) => kind === 'heartbeat' && request.topicPartitions != null)
    .map(({ request }) => request.topicPartitions)

const owned = (group) =>
  Object.fromEntries(group.assigned().map(({ topic, partitions }) => [topic, partitions]))

const partitionsOfEvery = (emitted, type) =>
  emitted.filter((event) => event.type === type).map(({ payload }) => payload.partitions)

const partitionsOf = (emitted, type) =>
  emitted
    .filter((event) => event.type === type)
    .map(({ payload }) => payload.partitions.find(({ topic }) => topic === TOPIC).partitions)

describe('Consumer > the consumer protocol reconciliation', () => {
  afterEach(async () => {
    for (const runner of built.splice(0)) {
      runner.running = false
      runner.heartbeatManager.stop()
      runner.pollTimer.stop()
      await runner.fetchManager.stop()
    }
  })

  it('ends reconciliation once it owns the subscribed part of a target that still names a deleted topic', async () => {
    const { group, runner, log, emitted, fetched, onCrash, discover, opportunity, count } =
      buildPattern([target(1, { [CONTROL]: [0], [DELETED]: [0] }), target(1, null)])

    runner.rebalanceCoordinator.runRound(null, 'initial')
    await waitFor(() => group.groupMembership.immediatePending, { delay: 1, maxWait: 5000 })
    await opportunity()
    await runner.rebalanceCoordinator.idle()
    await opportunity()
    expect(owned(group)).toEqual({ [CONTROL]: [0], [DELETED]: [0] })

    const fetching = runner.fetchManager.start()
    await discover({ [CONTROL]: [0, 1] })
    group.checkSubscriptionChange()
    const joinsBefore = count(events.GROUP_JOIN)
    const acksBefore = acknowledgements(log).length

    // Every heartbeat answers with the unchanged epoch and no assignment, so the coordinator's
    // target keeps naming the deleted topic for far longer than one chain's round budget.
    for (let i = 0; i < 30; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()

    expect(onCrash).not.toHaveBeenCalled()
    expect(count(events.PARTITIONS_REVOKED)).toEqual(1)
    expect(
      emitted.find(({ type }) => type === events.PARTITIONS_REVOKED).payload.partitions
    ).toEqual([{ topic: DELETED, partitions: [0] }])
    expect(count(events.GROUP_JOIN) - joinsBefore).toEqual(1)
    expect(acknowledgements(log).slice(acksBefore)).toEqual([
      [{ topicId: TOPIC_IDS[CONTROL], partitions: [0] }],
    ])
    expect(owned(group)).toEqual({ [CONTROL]: [0] })
    expect(runner.rebalanceCoordinator.isRebalancing()).toBe(false)
    const controlFetches = fetched.filter((key) => key === `${CONTROL}|0`).length
    await opportunity()
    expect(fetched.filter((key) => key === `${CONTROL}|0`).length).toBeGreaterThan(controlFetches)
    expect(fetched).not.toContain(`${DELETED}|0`)

    await runner.fetchManager.stop()
    await fetching
  })

  it('still adopts a new subscribed assignment and a newly resolvable topic id while the target names a deleted topic', async () => {
    const { group, runner, log, onCrash, discover, opportunity, count, answerWith } = buildPattern([
      target(1, { [CONTROL]: [0], [DELETED]: [0] }),
      target(1, null),
    ])

    runner.rebalanceCoordinator.runRound(null, 'initial')
    await waitFor(() => group.groupMembership.immediatePending, { delay: 1, maxWait: 5000 })
    await opportunity()
    await runner.rebalanceCoordinator.idle()
    await discover({ [CONTROL]: [0, 1] })
    group.checkSubscriptionChange()
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()
    expect(owned(group)).toEqual({ [CONTROL]: [0] })
    const joinsBefore = count(events.GROUP_JOIN)

    answerWith(target(2, { [CONTROL]: [0, 1], [DELETED]: [0] }))
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()
    expect(owned(group)).toEqual({ [CONTROL]: [0, 1] })
    expect(acknowledgements(log).at(-1)).toEqual([
      { topicId: TOPIC_IDS[CONTROL], partitions: [0, 1] },
    ])

    // The coordinator names a topic this member cannot resolve yet; nothing is owed until it can.
    answerWith(target(3, { [CONTROL]: [0, 1], [DELETED]: [0], [LATE]: [0] }))
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()
    expect(owned(group)).toEqual({ [CONTROL]: [0, 1] })

    await discover({ [CONTROL]: [0, 1], [LATE]: [0] })
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()

    expect(onCrash).not.toHaveBeenCalled()
    expect(owned(group)).toEqual({ [CONTROL]: [0, 1], [LATE]: [0] })
    expect(acknowledgements(log).at(-1)).toEqual([
      { topicId: TOPIC_IDS[CONTROL], partitions: [0, 1] },
      { topicId: TOPIC_IDS[LATE], partitions: [0] },
    ])
    expect(count(events.GROUP_JOIN) - joinsBefore).toEqual(2)
  })

  it('keeps reconciling a subscription that changes during every round for sixty rounds, warning every twenty, and settles once the churn stops', async () => {
    jest.useFakeTimers()
    let fetching
    try {
      const churnName = (k) => `rx-churn-${k}`
      for (let k = 1; k <= 61; k++) {
        TOPIC_IDS[churnName(k)] = Buffer.alloc(16, k).toString('base64url')
      }
      const { group, runner, log, fetched, onCrash, discover, count, answerWith } = buildPattern([
        target(1, { [CONTROL]: [0], [DELETED]: [0] }),
        target(1, null),
      ])
      const coordinator = runner.rebalanceCoordinator
      const warn = jest.spyOn(coordinator.logger, 'warn')
      /** Advances fake time in 5 ms steps until `done()` holds, for at most `budget` ms. */
      const until = async (done, budget = 1000) => {
        for (let spent = 0; !done() && spent < budget; spent += 5) {
          await jest.advanceTimersByTimeAsync(5)
        }
      }
      const INTERVAL = 250
      const churnTarget = (memberEpoch, byTopic) => ({
        ...target(memberEpoch, byTopic),
        heartbeatIntervalMs: INTERVAL,
      })
      const settle = () => until(() => !coordinator.isRebalancing())

      coordinator.runRound(null, 'initial')
      await until(() => group.groupMembership.immediatePending)
      await group.heartbeat({ interval: 0 })
      await settle()
      await group.heartbeat({ interval: 0 })
      fetching = runner.fetchManager.start()

      // Each round applies the pin it took, and a new matching topic replaces the old one before
      // the round checks whether it converged: it now owns a partition the subscription dropped,
      // so every round owes the next one.
      const joinedAt = []
      const joinedLogAt = []
      const recordJoin = coordinator.onJoinComplete
      coordinator.onJoinComplete = () => {
        recordJoin()
        joinedAt.push(Date.now())
        joinedLogAt.push(log.length)
        if (joinedAt.length <= 60)
          discover({ [CONTROL]: [0], [churnName(joinedAt.length + 1)]: [0] })
      }
      // The coordinator's interval is in force before the churn starts.
      answerWith({ ...target(1, null), heartbeatIntervalMs: INTERVAL })
      await jest.advanceTimersByTimeAsync(INTERVAL)
      await group.heartbeat({ interval: 0 })
      await jest.advanceTimersByTimeAsync(INTERVAL)
      expect(group.heartbeatInterval()).toEqual(INTERVAL)
      const startedAt = Date.now()
      const controlFetches = []

      await discover({ [CONTROL]: [0], [churnName(1)]: [0] })
      answerWith(churnTarget(2, { [CONTROL]: [0], [churnName(1)]: [0] }))
      await group.heartbeat({ interval: 0 })
      group.checkSubscriptionChange()

      for (let round = 1; round <= 61; round++) {
        await until(() => joinedAt.length >= round || onCrash.mock.calls.length > 0)
        if (round % 20 === 0)
          controlFetches.push(fetched.filter((k) => k === `${CONTROL}|0`).length)
        if (round <= 60) {
          answerWith(churnTarget(round + 2, { [CONTROL]: [0], [churnName(round + 1)]: [0] }))
          await group.heartbeat({ interval: 0 })
        }
      }
      await settle()

      expect(onCrash).not.toHaveBeenCalled()
      expect(joinedAt).toHaveLength(61)
      expect(count(events.GROUP_JOIN)).toEqual(62)
      expect(owned(group)).toEqual({ [CONTROL]: [0], [churnName(61)]: [0] })
      expect(group.subscriptionPending()).toBe(false)
      expect(group.groupMembership.needsReconciliation()).toBe(false)
      expect(
        warn.mock.calls.map(([message, extra]) => [
          message,
          extra.groupId,
          extra.rounds,
          extra.reason,
        ])
      ).toEqual(
        [20, 40, 60].map((rounds) => [
          'Reconciliation is still running',
          GROUP_ID,
          rounds,
          'assignment-changed',
        ])
      )

      // Paced: every follow-up round started one wait - the membership's 250 ms interval - after
      // the one before it. Each wait put exactly one heartbeat carrying ownership on the wire (the
      // acknowledgement, sent with this case's heartbeat) and at most one other: the fetch loop's
      // own, which the interval gate admits once per interval. The background timer is disabled.
      // The one exception is the first target of the chain: its eligible part equals what the member
      // owns, so it is acknowledged when it arrives, in the first wait instead of the second.
      expect(joinedAt.slice(1).map((at, i) => at - joinedAt[i])).toEqual(
        Array(joinedAt.length - 1).fill(INTERVAL)
      )
      expect(Date.now() - startedAt).toBeGreaterThanOrEqual(60 * INTERVAL)
      const perWait = joinedLogAt.slice(1).map((end, i) => {
        const sent = log.slice(joinedLogAt[i], end).filter(({ kind }) => kind === 'heartbeat')
        const acks = sent.filter(({ request }) => request.topicPartitions).length
        return { acks, others: sent.length - acks }
      })
      expect(perWait).toHaveLength(60)
      expect(perWait.slice(0, 2)).toEqual([
        { acks: 2, others: 0 },
        { acks: 0, others: 1 },
      ])
      expect(perWait.slice(2).filter(({ acks, others }) => acks !== 1 || others > 1)).toEqual([])

      // The retained partition kept being fetched while the chain ran.
      expect(controlFetches[0]).toBeGreaterThan(0)
      expect(controlFetches[1]).toBeGreaterThan(controlFetches[0])
      expect(controlFetches[2]).toBeGreaterThan(controlFetches[1])
    } finally {
      // The fetch loop's timers are fake: it has to stop before real time comes back.
      const stopping = built.at(-1)?.fetchManager.stop()
      await jest.advanceTimersByTimeAsync(100)
      await stopping
      await fetching
      jest.useRealTimers()
    }
  })

  /**
   * Runs `change` inside the next round's topic resolution: after the round pinned its
   * subscription, before it checks what it already owns and before anything is acknowledged.
   * @param {object} group the real consumer group
   * @param {() => Promise<void>} change
   * @returns {{ pinned: () => string[]|null }} the names the round had pinned when the change ran
   */
  const changeMidRound = (group, change) => {
    const membership = group.groupMembership
    const resolve = membership.resolveAssignmentTopicNames
    let pinned = null
    jest
      .spyOn(membership, 'resolveAssignmentTopicNames')
      .mockImplementationOnce(async (...args) => {
        pinned = [...group.subscription.round.names]
        await change()
        return resolve.apply(membership, args)
      })
    return { pinned: () => pinned }
  }

  /** @returns {object[]} each acknowledgement as `{ topic: partitions }` */
  const acknowledgedOwnership = (log, from) =>
    acknowledgements(log)
      .slice(from)
      .map((sent) =>
        Object.fromEntries(
          sent.map(({ topicId, partitions }) => [
            Object.keys(TOPIC_IDS).find((topic) => TOPIC_IDS[topic] === topicId),
            partitions,
          ])
        )
      )

  it('acknowledges only what it owns when a client pattern drops a target topic in the middle of a round, then applies the newer revision in the next round', async () => {
    const { group, runner, log, emitted, onCrash, discover, opportunity, count, answerWith } =
      buildPattern([target(1, { [CONTROL]: [0] }), target(1, null)])
    runner.rebalanceCoordinator.runRound(null, 'initial')
    await waitFor(() => group.groupMembership.immediatePending, { delay: 1, maxWait: 5000 })
    await opportunity()
    await runner.rebalanceCoordinator.idle()
    await opportunity()
    expect(owned(group)).toEqual({ [CONTROL]: [0] })
    const joinsBefore = count(events.GROUP_JOIN)
    const eventsBefore = emitted.length
    const acksBefore = acknowledgements(log).length

    const round = changeMidRound(group, () => discover({ [CONTROL]: [0, 1] }))
    answerWith(target(2, { [CONTROL]: [0], [DELETED]: [0] }))
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()
    for (let i = 0; i < 5; i++) await opportunity()

    expect(round.pinned()).toEqual([DELETED, CONTROL])
    expect(onCrash).not.toHaveBeenCalled()
    expect(count(events.GROUP_JOIN) - joinsBefore).toEqual(2)
    expect(
      emitted
        .slice(eventsBefore)
        .filter(
          ({ type }) => type === events.PARTITIONS_ASSIGNED || type === events.PARTITIONS_REVOKED
        )
    ).toEqual([])
    expect(acknowledgedOwnership(log, acksBefore)).toEqual([{ [CONTROL]: [0] }])
    expect(owned(group)).toEqual({ [CONTROL]: [0] })
    expect(group.subscription.applied.names).toEqual([CONTROL])
    expect(group.subscriptionPending()).toBe(false)
    expect(group.needsReconciliation()).toBe(false)
  })

  it('applies the pinned revision and acknowledges what it applied when a client pattern adds a target topic in the middle of a round, then adopts it in the next round', async () => {
    const { group, runner, log, emitted, onCrash, discover, opportunity, count, answerWith } =
      buildPattern([target(1, { [CONTROL]: [0] }), target(1, null)])
    runner.rebalanceCoordinator.runRound(null, 'initial')
    await waitFor(() => group.groupMembership.immediatePending, { delay: 1, maxWait: 5000 })
    await opportunity()
    await runner.rebalanceCoordinator.idle()
    await opportunity()
    const joinsBefore = count(events.GROUP_JOIN)
    const acksBefore = acknowledgements(log).length

    // LATE is not in metadata yet: the round pins a subscription without it, and the change makes
    // it both resolvable and desired before the round resolves the target.
    const round = changeMidRound(group, () =>
      discover({ [CONTROL]: [0, 1], [DELETED]: [0], [LATE]: [0] })
    )
    answerWith(target(2, { [CONTROL]: [0, 1], [LATE]: [0] }))
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()
    await opportunity()

    expect(round.pinned()).toEqual([DELETED, CONTROL])
    expect(onCrash).not.toHaveBeenCalled()
    expect(count(events.GROUP_JOIN) - joinsBefore).toEqual(2)
    expect(partitionsOfEvery(emitted, events.PARTITIONS_ASSIGNED).slice(-2)).toEqual([
      [{ topic: CONTROL, partitions: [1] }],
      [{ topic: LATE, partitions: [0] }],
    ])
    expect(acknowledgedOwnership(log, acksBefore)).toEqual([
      { [CONTROL]: [0, 1] },
      { [CONTROL]: [0, 1], [LATE]: [0] },
    ])
    expect(owned(group)).toEqual({ [CONTROL]: [0, 1], [LATE]: [0] })
    expect(group.subscriptionPending()).toBe(false)
    expect(group.needsReconciliation()).toBe(false)
  })

  it('applies the pinned revision, literal topic included, when a client pattern drops a target topic in the middle of a round, and releases that topic in the next round', async () => {
    const { group, runner, log, emitted, onCrash, discover, opportunity, count, answerWith } =
      buildPattern([target(1, { [LITERAL]: [0], [CONTROL]: [0] }), target(1, null)], {
        literal: true,
      })
    runner.rebalanceCoordinator.runRound(null, 'initial')
    await waitFor(() => group.groupMembership.immediatePending, { delay: 1, maxWait: 5000 })
    await opportunity()
    await runner.rebalanceCoordinator.idle()
    await opportunity()
    expect(owned(group)).toEqual({ [LITERAL]: [0], [CONTROL]: [0] })
    const joinsBefore = count(events.GROUP_JOIN)
    const acksBefore = acknowledgements(log).length

    const round = changeMidRound(group, () =>
      discover({ [LITERAL]: [0, 1], [CONTROL]: [0, 1], [DELETED]: [0] }).then(() =>
        discover({ [LITERAL]: [0, 1], [CONTROL]: [0, 1] })
      )
    )
    answerWith(target(2, { [LITERAL]: [0, 1], [CONTROL]: [0], [DELETED]: [0] }))
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()
    for (let i = 0; i < 5; i++) await opportunity()
    await runner.rebalanceCoordinator.idle()
    await opportunity()

    expect(round.pinned()).toEqual([LITERAL, DELETED, CONTROL])
    expect(onCrash).not.toHaveBeenCalled()
    expect(count(events.GROUP_JOIN) - joinsBefore).toEqual(2)
    expect(acknowledgedOwnership(log, acksBefore)).toEqual([
      { [LITERAL]: [0, 1], [CONTROL]: [0], [DELETED]: [0] },
      { [LITERAL]: [0, 1], [CONTROL]: [0] },
    ])
    expect(partitionsOfEvery(emitted, events.PARTITIONS_REVOKED).at(-1)).toEqual([
      { topic: DELETED, partitions: [0] },
    ])
    expect(owned(group)).toEqual({ [LITERAL]: [0, 1], [CONTROL]: [0] })
    expect(group.subscriptionPending()).toBe(false)
    expect(group.needsReconciliation()).toBe(false)
  })

  /**
   * Runs a two-round chain over the real group: the first round's apply discovers a new matching
   * topic, so a second round is owed after the wait. In that wait a heartbeat is fenced
   * (FENCED_MEMBER_EPOCH) and routed to the runner as a background heartbeat error, so a LOST
   * verdict reaches the slot. From then on `answer` serves every heartbeat, which the runner's own
   * heartbeat manager sends, routing its errors to the runner as in production.
   * @param {(request: object) => object} answer the coordinator's answer after the fence
   * @returns {Promise<object>} the harness, the chain's outcome and every heartbeat sent from the fence on
   */
  const fenceDuringWait = async (answer) => {
    const built = buildPattern([target(1, { [CONTROL]: [0] }), target(1, null)])
    const { group, runner, opportunity } = built
    const coordinator = runner.rebalanceCoordinator
    coordinator.runRound(null, 'initial')
    await waitFor(() => group.groupMembership.immediatePending, { delay: 1, maxWait: 5000 })
    await opportunity()
    await coordinator.idle()
    await opportunity()
    expect(owned(group)).toEqual({ [CONTROL]: [0] })

    const apply = group.applyRebalanceResult.bind(group)
    jest.spyOn(group, 'applyRebalanceResult').mockImplementationOnce(async (...args) => {
      await built.discover({ [CONTROL]: [0], [LATE]: [0] })
      return apply(...args)
    })
    const sent = []
    const fenced = new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
    const wait = coordinator.yieldBetweenRounds.bind(coordinator)
    jest.spyOn(coordinator, 'yieldBetweenRounds').mockImplementationOnce(() => {
      const waiting = wait()
      group.coordinator.consumerGroupHeartbeat.mockImplementation(async (request) => {
        sent.push(request)
        if (sent.length === 1) throw fenced
        return answer(request)
      })
      group.heartbeat({ interval: 0 }).catch((error) => runner.onHeartbeatError(error))
      return waiting
    })

    let outcome = null
    coordinator.runRound('subscription-changed', 'revoked').then(
      () => (outcome = { resolved: true }),
      (error) => (outcome = { error })
    )
    runner.heartbeatManager.enable()
    runner.heartbeatManager.ensureStarted()
    await waitFor(() => outcome !== null, { delay: 5, maxWait: 2000 })
    runner.heartbeatManager.stop()
    return { ...built, coordinator, outcome, sent }
  }

  it('ends the chain without rejoining when a fence reaches the slot during the wait, leaving the LOST verdict for the runner', async () => {
    const { group, coordinator, onCrash, outcome, sent } = await fenceDuringWait((request) =>
      request.memberEpoch === 0 ? target(100, { [CONTROL]: [0], [LATE]: [0] }) : target(100, null)
    )

    expect(outcome).toEqual({ resolved: true })
    expect(coordinator.yieldBetweenRounds).toHaveBeenCalledTimes(1)
    expect(sent.map(({ memberEpoch }) => memberEpoch)).toEqual([1])
    expect(group.groupMembership.state).toEqual('FENCED')
    expect(coordinator.hasVerdict()).toBe(true)
    expect(coordinator.isRebalancing()).toBe(false)
    expect(onCrash).not.toHaveBeenCalled()
  })

  it('refuses an ordinary apply while the membership carries a loss that moved no state, keeping the lost ownership and assignment, and lets the LOST round apply renew it', async () => {
    const { group, trip } = build({ script: [assignment(1, [0]), idle(1)] })
    await trip()
    const token = group.ownership.token()
    const fence = new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
    group.ownership.lose(fence, token)
    expect(['FENCED', 'FATAL']).not.toContain(group.groupMembership.state)

    await expect(
      group.applyRebalanceResult({ assignment: { [TOPIC]: [0, 1] } }, { rebuildOffsets: false })
    ).rejects.toBe(fence)
    expect(group.ownership.token()).toEqual(token)
    expect(group.ownership.refusal(token).cause).toBe(fence)
    expect(group.assigned()).toEqual([{ topic: TOPIC, partitions: [0] }])

    await group.applyRebalanceResult({ assignment: { [TOPIC]: [0, 1] } }, { lost: true })
    expect(group.ownership.token()).toEqual(token + 1)
    expect(group.ownership.refusal(token + 1)).toBeNull()
    expect(group.assigned()).toEqual([{ topic: TOPIC, partitions: [0, 1] }])
  })

  it('assigns, then revokes, then assigns again across three heartbeat trips', async () => {
    const { emitted, group, trip } = build({
      script: [assignment(1, [0, 1]), assignment(2, [0]), assignment(3, [0, 2]), idle(3)],
    })

    await trip()
    await trip()
    await trip()

    expect(partitionsOf(emitted, events.PARTITIONS_ASSIGNED)).toEqual([[0, 1], [2]])
    expect(partitionsOf(emitted, events.PARTITIONS_REVOKED)).toEqual([[1]])
    expect(emitted.filter(({ type }) => type === events.PARTITIONS_LOST)).toHaveLength(0)
    expect(emitted.filter(({ type }) => type === events.GROUP_JOIN)).toHaveLength(3)
    expect(group.assigned()).toEqual([{ topic: TOPIC, partitions: [0, 2] }])
    // Every REBALANCING says which membership produced it, and none of them was a loss.
    for (const { payload } of emitted.filter(({ type }) => type === events.REBALANCING)) {
      expect(payload.membershipProtocol).toEqual('consumer')
      expect(payload.reason).toEqual('assignment-changed')
    }
  })

  it('acknowledges each target with the set it now owns', async () => {
    const { log, trip } = build({
      script: [assignment(1, [0, 1]), assignment(2, [0]), idle(2)],
    })

    // Three: an acknowledgement rides the trip AFTER the one whose target it applied.
    await trip()
    await trip()
    await trip()

    const acks = log
      .filter(({ kind, request }) => kind === 'heartbeat' && request.topicPartitions != null)
      .map(({ request }) => request.topicPartitions)

    // The joining heartbeat sends every field, so its own empty owned set comes first; after it,
    // one ack per applied target, each carrying the whole owned set rather than the delta.
    expect(acks).toEqual([
      [],
      [{ topicId: TOPIC_ID, partitions: [0, 1] }],
      [{ topicId: TOPIC_ID, partitions: [0] }],
    ])
  })

  it('commits the released partition before it announces the revocation', async () => {
    const { group, log, trip } = build({
      script: [assignment(1, [0, 1]), assignment(2, [0]), idle(2)],
    })

    await trip()
    group.resolveOffset({ topic: TOPIC, partition: 1, offset: '41' })

    await trip()

    const commitAt = log.findIndex(({ kind }) => kind === 'commit')
    const revokedAt = log.findIndex(
      ({ kind, type }) => kind === 'event' && type === events.PARTITIONS_REVOKED
    )

    expect(commitAt).toBeGreaterThanOrEqual(0)
    expect(revokedAt).toBeGreaterThanOrEqual(0)
    expect(commitAt).toBeLessThan(revokedAt)

    // Sent under the epoch the round has just established, not the one it started with.
    const { payload } = log[commitAt]
    expect(payload.groupGenerationId).toEqual(2)
    const committed = payload.topics
      .find(({ topic }) => topic === TOPIC)
      .partitions.map(({ partition }) => Number(partition))
    // Deliberately `toContain` rather than `toEqual`: the pre-release commit may legitimately
    // overlap the runner's own auto-commit, which is not narrowed to the released set.
    expect(committed).toContain(1)
  })

  it('leaves every fetcher object in place across a reconciliation', async () => {
    const { runner, trip } = build({
      script: [assignment(1, [0, 1]), assignment(2, [0]), idle(2)],
    })

    // After the join, not before: a fetch needs the offset manager the first apply builds.
    await trip()
    const started = runner.fetchManager.start()
    const before = runner.fetchManager.getFetchers()
    await trip()
    const after = runner.fetchManager.getFetchers()

    // Identity, not shape: the apply IS the reconciliation, so nothing stops or rebuilds a fetcher.
    expect(after).toHaveLength(before.length)
    after.forEach((fetcher, index) => expect(fetcher).toBe(before[index]))

    await runner.fetchManager.stop()
    await started
  })

  it('drops an in-flight batch for a partition the reconciliation took away', async () => {
    const handled = []
    const { runner, group, trip } = build({
      script: [assignment(1, [0, 1]), assignment(2, [0]), idle(2)],
      eachBatch: async ({ batch }) => handled.push(`${batch.topic}|${batch.partition}`),
    })

    await trip()
    const inFlight = new Batch(TOPIC, group.assignmentEpoch, {
      partition: 1,
      highWatermark: '100',
      messages: [{ offset: '10', key: null, value: Buffer.alloc(0), headers: {} }],
    })

    await trip()
    await runner.handleBatch(inFlight)

    expect(handled).toEqual([])
    expect(group.resolvedOffsetAt(TOPIC, 1)).toBeUndefined()

    // The control: a batch for a partition the member kept is still handled.
    const kept = new Batch(TOPIC, group.assignmentEpoch, {
      partition: 0,
      highWatermark: '100',
      messages: [{ offset: '10', key: null, value: Buffer.alloc(0), headers: {} }],
    })
    await runner.handleBatch(kept)
    expect(handled).toEqual([`${TOPIC}|0`])
  })

  it('never enters the eager quiesce', async () => {
    const { runner, trip } = build({
      script: [assignment(1, [0, 1]), assignment(2, [0]), assignment(3, [0, 2]), idle(3)],
    })
    const fetchManagerStop = jest.spyOn(runner.fetchManager, 'stop')

    await trip()
    await trip()
    await trip()

    expect(runner.quiescing).toBe(false)
    expect(fetchManagerStop).not.toHaveBeenCalled()
  })

  it('sends one heartbeat when several workers heartbeat at once', async () => {
    const { runner, log, trip } = build({
      script: [assignment(1, [0, 1]), idle(1)],
      concurrency: 3,
    })

    await trip()
    const before = log.filter(({ kind }) => kind === 'heartbeat').length

    await Promise.all([runner.heartbeat(), runner.heartbeat(), runner.heartbeat()])

    // `partitionsConsumedConcurrently > 1` puts several workers on the same membership; its
    // single flight is what keeps them to one request rather than one each.
    expect(log.filter(({ kind }) => kind === 'heartbeat').length).toEqual(before + 1)
  })

  it('rejoins on a fencing learned somewhere other than the heartbeat, keeping its member id', async () => {
    const { runner, group, joinArmed, trip } = build({
      script: [assignment(1, [0, 1]), assignment(5, [0, 1]), idle(5)],
    })

    await trip()
    const memberId = group.memberId

    const handled = runner.handleRebalanceError({
      kind: 'lost',
      reason: 'fenced',
      error: new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH)),
    })
    await joinArmed()
    await group.heartbeat({ interval: 0 })
    await handled

    // The recovery round joined - which it can only do if the state machine was told about a
    // fencing no heartbeat response carried - and a KIP-848 member keeps its id for life.
    expect(group.memberEpoch).toEqual(5)
    expect(group.memberId).toEqual(memberId)
  })

  it('retries a join the coordinator never answered rather than dying on it', async () => {
    const { runner } = build({ script: [idle(1)], rebalanceTimeout: 50 })

    // The deadline exists so a round always settles, and it is the one budget of the round: its
    // expiry ends the join chain with the retriable deadline error instead of buying a fresh one.
    await expect(runner.rebalanceCoordinator.runRound(null, 'initial')).rejects.toMatchObject({
      type: 'JOIN_DEADLINE_EXCEEDED',
      retriable: true,
    })
  })
})

const FULL_IDS = {
  'topic-a': 'aAaAaAaAaAaAaAaAaAaAaA',
  'topic-b': 'bBbBbBbBbBbBbBbBbBbBbB',
  'topic-c': 'cCcCcCcCcCcCcCcCcCcCcA',
  'topic-d': 'dDdDdDdDdDdDdDdDdDdDdA',
}
const FULL_NAMES = Object.fromEntries(Object.entries(FULL_IDS).map(([name, id]) => [id, name]))

/**
 * A ConsumerGroupHeartbeat response naming topics by id, with a 500 ms interval.
 * @param {number} memberEpoch
 * @param {Record<string, number[]>|null} byTopic null for "no change"
 * @returns {object}
 */
const fullTarget = (memberEpoch, byTopic) => ({
  throttleTime: 0,
  errorCode: 0,
  errorMessage: null,
  memberId: 'member-1',
  memberEpoch,
  heartbeatIntervalMs: 500,
  assignment:
    byTopic === null
      ? null
      : {
          topicPartitions: Object.entries(byTopic).map(([topic, partitions]) => ({
            topicId: FULL_IDS[topic],
            partitions,
          })),
        },
})

/**
 * A whole consumer-protocol consumer - `createConsumer`, its runner, group, heartbeat manager,
 * fetch loop and auto-commit - over a coordinator and a broker double. `state.answer(request)`
 * answers every heartbeat; `state.queue['topic|partition']` holds the offsets the broker serves,
 * one record per Fetch; `state.unknown` holds topic names metadata does not know yet (one is
 * learned per topic-id refresh), `state.partial` topics whose metadata names partition 0 only (until
 * the next metadata refresh), `state.metadataDelay` delays the next metadata refresh once and
 * `state.idDelays` the next topic-id refreshes, one entry each. `log` is
 * one ordered record of heartbeats, commits and partition events.
 * @param {object} [options] consumer options that replace the defaults below
 * @returns {object} the consumer and the double's records and switches
 */
const buildConsumer = (options = {}) => {
  const committed = {}
  const state = {
    answer: null,
    queue: {},
    unknown: new Set(),
    partial: new Set(),
    metadataDelay: 0,
    idDelays: [],
  }
  const log = []
  const crashes = []
  const delayOnce = async (key) => {
    const delay = state[key]
    state[key] = 0
    if (delay) await new Promise((resolve) => setTimeout(resolve, delay))
  }
  const coordinator = {
    isConnected: () => true,
    nodeId: 0,
    negotiatedVersion: jest.fn(() => null),
    consumerGroupHeartbeat: jest.fn(async (request) => {
      const entry = { kind: 'heartbeat', at: Date.now(), epoch: request.memberEpoch }
      log.push(entry)
      try {
        return state.answer(request)
      } catch (error) {
        entry.error = error.type
        throw error
      }
    }),
    offsetCommit: jest.fn(async ({ topics, groupGenerationId }) => {
      for (const { topic, partitions } of topics) {
        for (const { partition, offset } of partitions) {
          committed[`${topic}|${partition}`] = String(offset)
          log.push({
            kind: 'commit',
            at: Date.now(),
            tp: `${topic}|${partition}`,
            offset: String(offset),
            generationId: groupGenerationId,
          })
        }
      }
    }),
    offsetFetch: jest.fn(async ({ topics }) => ({
      responses: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          offset: committed[`${topic}|${partition}`] ?? '40',
          committedLeaderEpoch: -1,
          metadata: null,
          errorCode: 0,
        })),
      })),
    })),
  }
  const record = (offset) => ({
    offset: String(offset),
    key: Buffer.from('k'),
    value: Buffer.from(`v${offset}`),
    headers: {},
    timestamp: '0',
    attributes: 0,
    isControlRecord: false,
    batchContext: {
      partitionLeaderEpoch: 0,
      firstOffset: String(offset),
      firstTimestamp: '0',
      maxTimestamp: '0',
      magicByte: 2,
      producerId: '-1',
      producerEpoch: -1,
      firstSequence: -1,
      isTransactional: false,
      isControlBatch: false,
    },
  })
  const fetches = []
  const broker = {
    fetch: jest.fn(async ({ topics }) => {
      let served = false
      const responses = topics.map(({ topic, partitions }) => ({
        topicName: topic,
        partitions: partitions.map(({ partition, fetchOffset }) => {
          const tp = `${topic}|${partition}`
          fetches.push({ tp, fetchOffset: String(fetchOffset), at: Date.now() })
          const messages = (state.queue[tp] || [])
            .filter((offset) => offset >= Number(fetchOffset))
            .slice(0, 1)
            .map(record)
          served = served || messages.length > 0
          return {
            partition,
            errorCode: 0,
            highWatermark: '100',
            lastStableOffset: '100',
            logStartOffset: '0',
            abortedTransactions: [],
            preferredReadReplica: -1,
            messages,
          }
        }),
      }))
      await new Promise((resolve) => setTimeout(resolve, served ? 5 : 50))
      return { throttleTime: 0, errorCode: 0, sessionId: 0, responses }
    }),
  }
  const known = (name) => name !== undefined && !state.unknown.has(name)
  const cluster = {
    instrumentationEmitter: new InstrumentationEventEmitter(),
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    isConnected: () => true,
    removeBroker: jest.fn(),
    onTopicRecreated: jest.fn(() => () => {}),
    addMultipleTargetTopics: jest.fn(async () => {}),
    registerAssignedTopics: jest.fn(async (names) => names),
    refreshMetadata: jest.fn(async () => {
      await delayOnce('metadataDelay')
      state.partial.clear()
    }),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadataForTopicIds: jest.fn(async () => {
      const delay = state.idDelays.shift()
      if (delay) await new Promise((resolve) => setTimeout(resolve, delay))
      const [next] = state.unknown
      state.unknown.delete(next)
    }),
    useConsumerMetadataScope: jest.fn(),
    releaseConsumerMetadataScope: jest.fn(),
    useBrokerRegex: jest.fn(),
    resetTargetTopics: jest.fn(async (_names, { publish } = {}) => publish?.()),
    committedOffsets: () => ({}),
    offsetOperations: () => new OffsetOperations(),
    defaultOffset: () => -2,
    topicId: (name) => (known(name) ? FULL_IDS[name] : undefined),
    topicName: (id) => (known(FULL_NAMES[id]) ? FULL_NAMES[id] : undefined),
    leaderEpoch: () => -1,
    getNodeIds: () => [0],
    findTopicPartitionMetadata: (topic) =>
      (!known(topic) ? [] : state.partial.has(topic) ? [0] : [0, 1]).map((partitionId) => ({
        partitionId,
        leader: 0,
      })),
    findBroker: jest.fn(async () => broker),
    findGroupCoordinator: jest.fn(async () => coordinator),
    fetchTopicsOffset: jest.fn(async (requests) =>
      requests.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
      }))
    ),
    retry: {},
  }
  const consumer = createConsumer({
    cluster,
    groupId: GROUP_ID,
    logger: newLogger(),
    instrumentationEmitter: cluster.instrumentationEmitter,
    groupProtocol: 'consumer',
    fetchSessions: false,
    maxWaitTimeInMs: 50,
    retry: { initialRetryTime: 300 },
    ...options,
  })
  for (const key of ['PARTITIONS_LOST', 'PARTITIONS_REVOKED', 'PARTITIONS_ASSIGNED']) {
    consumer.on(consumer.events[key], ({ payload }) =>
      log.push({ kind: key, at: Date.now(), partitions: payload.partitions })
    )
  }
  consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
  return { consumer, coordinator, broker, cluster, state, log, crashes, fetches }
}

/**
 * The partition events and epoch 0 rejoins of a log, in order.
 * @param {object[]} log entries of `buildConsumer`'s log
 * @returns {Array<string|number>} event names, and 0 for each rejoin heartbeat
 */
const partitionEventsAndRejoins = (log) =>
  log
    .filter(({ kind, epoch }) => kind.startsWith('PARTITIONS_') || epoch === 0)
    .map(({ kind, epoch }) => (kind === 'heartbeat' ? epoch : kind))

/**
 * Advances fake time in 10 ms steps until `done()` holds, for at most `budget` ms.
 * @param {() => boolean} done
 * @param {number} [budget]
 * @returns {Promise<void>}
 */
const advanceUntil = async (done, budget = 5000) => {
  for (let spent = 0; !done() && spent < budget; spent += 10) {
    await jest.advanceTimersByTimeAsync(10)
  }
}

describe('Consumer > a whole consumer-protocol consumer during reconciliation', () => {
  let consumer

  beforeEach(() => jest.useFakeTimers())
  afterEach(async () => {
    const stopping = consumer.disconnect().catch(() => {})
    await jest.advanceTimersByTimeAsync(3000)
    await stopping
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  /**
   * Joins with topic-a:0, handles a:0@40 and a:0@41 (41 stays uncommitted: the auto-commit interval
   * is 30 s), then lets a:0@42 start a handler that runs for two seconds. Then the coordinator moves
   * the member twice: epoch 2 adds topic-b, whose metadata takes 700 ms, and epoch 3, answered while
   * that round applies, adds topic-a:1, so a second round is owed after the 500 ms wait. The
   * heartbeat sent as the wait starts is fenced; from then on an epoch 0 heartbeat gets epoch 100
   * with `rejoined` and an older epoch is fenced again.
   * @param {object} [options]
   * @param {object} [options.rebalanceListener]
   * @param {boolean} [options.busy=true] false: the handler of a:0@42 returns at once, so the fetch
   *   loop is free during the wait
   * @param {'fence'|'poll'|'commit'} [options.trigger='fence'] 'poll': instead of the fenced
   *   heartbeat, the coordinator is handed a poll expiry (`onPollExpired`, as the heartbeat manager
   *   calls it) as the wait starts, so the member leaves the group;
   *   'commit': a `commitOffsets()` call sent as the wait starts is fenced, and heartbeats at the
   *   old epoch keep succeeding
   * @returns {Promise<object>} the built consumer and what the case observed
   */
  const fenceWhileHandlerRuns = async ({
    rebalanceListener,
    busy = true,
    trigger = 'fence',
  } = {}) => {
    const built = buildConsumer()
    consumer = built.consumer
    const handled = []
    const s = { phase: 'steady', fenced: false, waits: 0, slow: busy }
    const full = { 'topic-a': [0, 1], 'topic-b': [0, 1] }
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (s.fenced) {
        if (request.memberEpoch === 0) return fullTarget(100, full)
        if (request.memberEpoch >= 100) return fullTarget(100, null)
        if (trigger === 'commit') return fullTarget(request.memberEpoch, null)
        throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      }
      if (s.phase === 'grow') {
        s.phase = 'grown'
        return fullTarget(2, { 'topic-a': [0], 'topic-b': [0, 1] })
      }
      if (s.phase === 'grown') {
        s.phase = 'regrown'
        return fullTarget(3, full)
      }
      if (s.phase === 'steady') {
        return request.memberEpoch === 0 ? fullTarget(1, { 'topic-a': [0] }) : fullTarget(1, null)
      }
      return fullTarget(request.memberEpoch, null)
    }
    const commit = built.coordinator.offsetCommit.getMockImplementation()
    built.coordinator.offsetCommit.mockImplementation(async (request) => {
      if (s.fenced && request.groupGenerationId < 100) {
        throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      }
      return commit(request)
    })
    const starts = jest.spyOn(Runner.prototype, 'start')
    const wait = RebalanceCoordinator.prototype.yieldBetweenRounds
    jest
      .spyOn(RebalanceCoordinator.prototype, 'yieldBetweenRounds')
      .mockImplementation(function () {
        if (++s.waits === 1) {
          s.fenced = true
          s.fencedAt = Date.now()
          s.fencedIndex = built.log.length
          // The heartbeat manager's own call when a tick finds the poll timer expired.
          if (trigger === 'poll') this.onPollExpired({ elapsedMs: 1, isStatic: false })
          else if (trigger === 'commit') {
            consumer
              .commitOffsets([{ topic: 'topic-a', partition: 0, offset: '41' }])
              .catch(() => {})
          } else this.heartbeatManager.requestTick()
        }
        return wait.call(this)
      })

    await consumer.subscribe({ topics: ['topic-a', 'topic-b'] })
    const logged =
      rebalanceListener &&
      Object.fromEntries(
        Object.entries(rebalanceListener).map(([name, call]) => [
          name,
          async (payload) => {
            built.log.push({ kind: name, at: Date.now() })
            return call(payload)
          },
        ])
      )
    consumer.run({
      autoCommitInterval: 30000,
      ...(logged && { rebalanceListener: logged }),
      eachMessage: async ({ topic, partition, message }) => {
        handled.push({ at: Date.now(), tp: `${topic}|${partition}`, offset: message.offset })
        if (s.slow && topic === 'topic-a' && message.offset === '42') {
          s.slow = false
          await new Promise((resolve) => setTimeout(resolve, 2000))
        }
      },
    })
    built.state.queue['topic-a|0'] = [40, 41]
    await advanceUntil(() => handled.length >= 2)
    built.state.queue['topic-a|0'] = [40, 41, 42]
    await advanceUntil(() => handled.length >= 3)

    built.state.partial.add('topic-b')
    built.state.metadataDelay = 700
    s.phase = 'grow'
    await advanceUntil(() => s.fenced, 10000)
    // Only records the rejoined member is handed again from here on.
    built.state.queue['topic-a|0'] = [40, 41]
    await advanceUntil(() => built.log.slice(s.fencedIndex).some(({ epoch }) => epoch === 0), 10000)
    await jest.advanceTimersByTimeAsync(3000)
    // A record on topic-b after the auto-commit interval makes the next commit.
    await jest.advanceTimersByTimeAsync(30000)
    built.state.queue['topic-b|0'] = [40]
    await advanceUntil(() => handled.some(({ tp }) => tp === 'topic-b|0'))
    await jest.advanceTimersByTimeAsync(1000)
    return { ...built, handled, starts, fencedAt: s.fencedAt, fencedIndex: s.fencedIndex }
  }

  it('runs the LOST round before any rejoin when a fence reaches the slot during the wait while a handler keeps the fetch loop busy: one PARTITIONS_LOST, uncommitted progress discarded and redelivered, nothing resolved under the fenced epoch committed', async () => {
    const run = await fenceWhileHandlerRuns()
    const after = run.log.slice(run.fencedIndex)
    const lostAt = after.findIndex(({ kind }) => kind === 'PARTITIONS_LOST')

    expect(partitionEventsAndRejoins(after)).toEqual(['PARTITIONS_LOST', 0, 'PARTITIONS_ASSIGNED'])
    expect(after.filter(({ kind }) => kind === 'PARTITIONS_LOST')).toEqual([
      expect.objectContaining({
        partitions: [
          { topic: 'topic-a', partitions: [0] },
          { topic: 'topic-b', partitions: [0, 1] },
        ],
      }),
    ])
    // The handler of a:0@42 returned after the wait had ended.
    expect(after[lostAt].at).toBeGreaterThan(run.fencedAt + 500)
    // Redelivered from the committed offset 40, not resumed at 43.
    expect(
      run.handled
        .filter(({ at, tp }) => at > run.fencedAt && tp === 'topic-a|0')
        .map(({ offset }) => offset)
    ).toEqual(['40', '41'])
    expect(run.log.filter(({ kind, tp }) => kind === 'commit' && tp === 'topic-a|0')).toEqual([
      expect.objectContaining({ offset: '42', generationId: 100 }),
    ])
    expect(run.crashes).toEqual([])
    expect(run.starts).toHaveBeenCalledTimes(1)
  })

  it('runs the LOST round before the rejoin when a fence reaches the slot during the wait while the fetch loop is free, and redelivers from the committed offset', async () => {
    const run = await fenceWhileHandlerRuns({ busy: false })

    expect(partitionEventsAndRejoins(run.log.slice(run.fencedIndex))).toEqual([
      'PARTITIONS_LOST',
      0,
      'PARTITIONS_ASSIGNED',
    ])
    expect(
      run.handled
        .filter(({ at, tp }) => at > run.fencedAt && tp === 'topic-a|0')
        .map(({ offset }) => offset)
    ).toEqual(['40', '41'])
    expect(run.log.filter(({ kind, tp }) => kind === 'commit' && tp === 'topic-a|0')).toEqual([
      expect.objectContaining({ offset: '42', generationId: 100 }),
    ])
    expect(run.crashes).toEqual([])
  })

  it.each([
    ['keeps the fetch loop busy', true],
    ['leaves the fetch loop free', false],
  ])(
    'leaves, runs one LOST round and rejoins when a poll expiry is handed to the coordinator as the wait starts while a handler %s, and redelivers from the committed offset',
    async (_, busy) => {
      const run = await fenceWhileHandlerRuns({ busy, trigger: 'poll' })
      const after = run.log.slice(run.fencedIndex)

      expect(after.find(({ kind }) => kind === 'heartbeat').epoch).toEqual(-1)
      expect(partitionEventsAndRejoins(after)).toEqual([
        'PARTITIONS_LOST',
        0,
        'PARTITIONS_ASSIGNED',
      ])
      expect(after.find(({ kind }) => kind === 'PARTITIONS_LOST').partitions).toEqual([
        { topic: 'topic-a', partitions: [0] },
        { topic: 'topic-b', partitions: [0, 1] },
      ])
      expect(
        run.handled
          .filter(({ at, tp }) => at > run.fencedAt && tp === 'topic-a|0')
          .map(({ offset }) => offset)
      ).toEqual(['40', '41'])
      expect(run.log.filter(({ kind, tp }) => kind === 'commit' && tp === 'topic-a|0')).toEqual([
        expect.objectContaining({ offset: '42', generationId: 100 }),
      ])
      const [runner] = run.starts.mock.contexts
      expect(runner.consumerGroup.groupMembership.state).toEqual('STABLE')
      expect(runner.consumerGroup.groupMembership.memberEpoch).toEqual(100)
      expect(run.crashes).toEqual([])
      expect(run.starts).toHaveBeenCalledTimes(1)
    }
  )

  it('runs the LOST round before any other round when a commit is fenced during the wait and the fetch loop takes the verdict before the wait ends', async () => {
    const run = await fenceWhileHandlerRuns({ busy: false, trigger: 'commit' })
    const after = run.log.slice(run.fencedIndex)

    expect(partitionEventsAndRejoins(after)).toEqual(['PARTITIONS_LOST', 0, 'PARTITIONS_ASSIGNED'])
    expect(after[after.findIndex(({ kind }) => kind === 'PARTITIONS_LOST')].partitions).toEqual([
      { topic: 'topic-a', partitions: [0] },
      { topic: 'topic-b', partitions: [0, 1] },
    ])
    expect(
      run.handled
        .filter(({ at, tp }) => at > run.fencedAt && tp === 'topic-a|0')
        .map(({ offset }) => offset)
    ).toEqual(['40', '41'])
    expect(run.log.filter(({ kind, tp }) => kind === 'commit' && tp === 'topic-a|0')).toEqual([
      expect.objectContaining({ offset: '42', generationId: 100 }),
    ])
    expect(run.crashes).toEqual([])
    expect(run.starts).toHaveBeenCalledTimes(1)
  })

  it('runs the LOST round, not a reconciliation, when a commit is fenced while a handler runs and a heartbeat sent before the fence is answered after it with a new target, and rejoins with the latest target', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    const handled = []
    const latest = { 'topic-a': [0, 1], 'topic-b': [0, 1] }
    const s = { phase: 'steady', fenced: false }
    let releaseHandler
    let releaseHeartbeat
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (request.memberEpoch === 0) {
        return s.fenced ? fullTarget(100, latest) : fullTarget(1, { 'topic-a': [0] })
      }
      if (s.phase === 'hold') {
        s.phase = 'held'
        return new Promise((resolve) => {
          releaseHeartbeat = () => resolve(fullTarget(2, { 'topic-a': [0], 'topic-b': [0, 1] }))
        })
      }
      if (s.phase === 'next') {
        s.phase = 'done'
        return fullTarget(3, latest)
      }
      return fullTarget(request.memberEpoch, null)
    }
    const commit = built.coordinator.offsetCommit.getMockImplementation()
    built.coordinator.offsetCommit.mockImplementation(async (request) => {
      if (s.fenced && request.groupGenerationId < 100) {
        throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      }
      return commit(request)
    })
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: ['topic-a', 'topic-b'] })
    consumer.run({
      autoCommitInterval: 30000,
      eachMessage: async ({ topic, partition, message }) => {
        handled.push({ at: Date.now(), tp: `${topic}|${partition}`, offset: message.offset })
        if (!s.fenced) await new Promise((resolve) => (releaseHandler = resolve))
      },
    })
    await advanceUntil(() => built.fetches.length > 0)
    built.state.queue['topic-a|0'] = [40]
    await advanceUntil(() => !!releaseHandler)
    s.phase = 'hold'
    await advanceUntil(() => !!releaseHeartbeat)
    const [runner] = starts.mock.contexts

    s.fenced = true
    const fencedIndex = built.log.length
    const fencedAt = Date.now()
    await expect(
      consumer.commitOffsets([{ topic: 'topic-a', partition: 0, offset: '41' }])
    ).rejects.toThrow()
    built.state.partial.add('topic-b')
    built.state.metadataDelay = 700
    s.phase = 'next'
    releaseHeartbeat()
    await jest.advanceTimersByTimeAsync(1500)
    releaseHandler()
    await advanceUntil(() => built.log.slice(fencedIndex).some(({ epoch }) => epoch === 0))
    await jest.advanceTimersByTimeAsync(3000)
    const after = built.log.slice(fencedIndex)

    expect(partitionEventsAndRejoins(after)).toEqual(['PARTITIONS_LOST', 0, 'PARTITIONS_ASSIGNED'])
    expect(after.find(({ kind }) => kind === 'PARTITIONS_LOST').partitions).toEqual([
      { topic: 'topic-a', partitions: [0] },
    ])
    expect(after.find(({ kind }) => kind === 'PARTITIONS_ASSIGNED').partitions).toEqual([
      { topic: 'topic-a', partitions: [0, 1] },
      { topic: 'topic-b', partitions: [0, 1] },
    ])
    // Redelivered from the committed offset 40: the progress past it was discarded.
    expect(
      handled
        .filter(({ at, tp }) => at > fencedAt && tp === 'topic-a|0')
        .map(({ offset }) => offset)
    ).toEqual(['40'])
    expect(after.filter(({ kind }) => kind === 'commit')).toEqual([])
    expect(runner.consumerGroup.groupMembership.memberEpoch).toEqual(100)
    expect(built.crashes).toEqual([])
    expect(starts).toHaveBeenCalledTimes(1)
  })

  it('backs off between rejoins the coordinator keeps fencing instead of rejoining once per millisecond, and joins once it accepts one', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    let fenced = false
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (fenced) throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      return request.memberEpoch === 0 ? fullTarget(1, { 'topic-a': [0] }) : fullTarget(1, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: ['topic-a'] })
    consumer.run({ eachMessage: async () => {} })
    await advanceUntil(() => built.fetches.length > 0)
    const [runner] = starts.mock.contexts

    fenced = true
    const fencedAt = Date.now()
    runner.heartbeatManager.requestTick()
    await jest.advanceTimersByTimeAsync(10000)
    const rejoins = built.log
      .filter(({ kind, epoch, at }) => kind === 'heartbeat' && epoch === 0 && at > fencedAt)
      .map(({ at }) => at)

    // Exponential from the 300 ms retry time, capped at 1 s: at most 1 + 10 s / 240 ms.
    expect(rejoins.length).toBeGreaterThanOrEqual(5)
    expect(rejoins.length).toBeLessThanOrEqual(15)
    expect(Math.min(...rejoins.slice(1).map((at, i) => at - rejoins[i]))).toBeGreaterThanOrEqual(
      240
    )
    expect(built.crashes).toEqual([])

    fenced = false
    await advanceUntil(() => runner.consumerGroup.groupMembership.memberEpoch === 1, 3000)
    expect(runner.consumerGroup.groupMembership.memberEpoch).toEqual(1)
    expect(starts).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['a rebalance timeout shorter than the backoff', { rebalanceTimeout: 100 }, 300],
    ['a zero retry time', {}, 0],
  ])(
    'waits before sending again a rejoin the coordinator fenced, with %s, and rejoins without a crash',
    async (_, timeouts, initialRetryTime) => {
      const built = buildConsumer({
        ...timeouts,
        retry: { initialRetryTime, factor: 0, retries: 0 },
      })
      consumer = built.consumer
      const s = { fenced: false, rejoins: 0 }
      built.state.answer = (request) => {
        if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
        if (s.fenced && request.memberEpoch === 0 && ++s.rejoins === 1) {
          throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
        }
        if (s.fenced && request.memberEpoch === 1) {
          throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
        }
        if (request.memberEpoch === 0) return fullTarget(s.fenced ? 2 : 1, { 'topic-a': [0] })
        return fullTarget(request.memberEpoch, null)
      }
      const starts = jest.spyOn(Runner.prototype, 'start')
      await consumer.subscribe({ topics: ['topic-a'] })
      consumer.run({ eachMessage: async () => {} })
      await advanceUntil(() => built.fetches.length > 0)
      const [runner] = starts.mock.contexts

      s.fenced = true
      const fencedAt = Date.now()
      runner.heartbeatManager.requestTick()
      await advanceUntil(() => runner.consumerGroup.groupMembership.memberEpoch === 2, 3000)
      const rejoins = built.log
        .filter(({ kind, epoch, at }) => kind === 'heartbeat' && epoch === 0 && at >= fencedAt)
        .map(({ at }) => at)

      expect(built.crashes).toEqual([])
      expect(starts).toHaveBeenCalledTimes(1)
      expect(runner.consumerGroup.groupMembership.memberEpoch).toEqual(2)
      expect(rejoins).toHaveLength(2)
      // 100 ms is the floor of the wait before a fenced join is sent again.
      expect(rejoins[1] - rejoins[0]).toBeGreaterThanOrEqual(Math.max(initialRetryTime, 100))
    }
  )

  it('keeps waiting at least 100 ms between more than 1,024 successive fenced rejoins with a zero retry time and factor, without a join deadline crash, and joins once one is accepted', async () => {
    const built = buildConsumer({
      rebalanceTimeout: 100,
      retry: { initialRetryTime: 0, factor: 0, retries: 0 },
    })
    consumer = built.consumer
    let fenced = false
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (fenced) throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      return request.memberEpoch === 0 ? fullTarget(1, { 'topic-a': [0] }) : fullTarget(1, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: ['topic-a'] })
    consumer.run({ eachMessage: async () => {} })
    await advanceUntil(() => built.fetches.length > 0)
    const [runner] = starts.mock.contexts

    fenced = true
    const fencedAt = Date.now()
    runner.heartbeatManager.requestTick()
    await jest.advanceTimersByTimeAsync(110000)
    const rejoins = built.log
      .filter(({ kind, epoch, at }) => kind === 'heartbeat' && epoch === 0 && at > fencedAt)
      .map(({ at }) => at)

    expect(rejoins.length).toBeGreaterThan(1025)
    expect(Math.min(...rejoins.slice(1).map((at, i) => at - rejoins[i]))).toBeGreaterThanOrEqual(
      100
    )
    expect(built.crashes).toEqual([])

    fenced = false
    await advanceUntil(() => runner.consumerGroup.groupMembership.memberEpoch === 1, 3000)
    expect(runner.consumerGroup.groupMembership.memberEpoch).toEqual(1)
    expect(starts).toHaveBeenCalledTimes(1)
  })

  it('schedules no timer longer than 2147483647 ms, or NaN, when a fenced rejoin waits out its backoff under the largest maxPollInterval, and rejoins without a crash', async () => {
    const delays = []
    const schedule = global.setTimeout
    jest.spyOn(global, 'setTimeout').mockImplementation((fn, delay, ...args) => {
      delays.push(delay)
      return schedule(fn, delay, ...args)
    })
    const built = buildConsumer({
      maxPollInterval: 2147483647,
      retry: { initialRetryTime: 300, factor: 0, retries: 0 },
    })
    consumer = built.consumer
    const s = { fenced: false, rejoins: 0 }
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (
        s.fenced &&
        (request.memberEpoch === 1 || (request.memberEpoch === 0 && ++s.rejoins === 1))
      ) {
        throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      }
      if (request.memberEpoch === 0) return fullTarget(s.fenced ? 2 : 1, { 'topic-a': [0] })
      return fullTarget(request.memberEpoch, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: ['topic-a'] })
    consumer.run({ eachMessage: async () => {} })
    await advanceUntil(() => built.fetches.length > 0)
    const [runner] = starts.mock.contexts

    s.fenced = true
    runner.heartbeatManager.requestTick()
    await advanceUntil(() => runner.consumerGroup.groupMembership.memberEpoch === 2, 3000)

    expect(runner.consumerGroup.groupMembership.memberEpoch).toEqual(2)
    expect(s.rejoins).toEqual(2)
    expect(delays.filter((delay) => !(delay <= 2147483647))).toEqual([])
    expect(built.crashes).toEqual([])
    expect(starts).toHaveBeenCalledTimes(1)
  })

  describe.each([
    ['a literal subscription', { topics: ['topic-a', 'topic-c', 'topic-d'] }],
    ['a broker-side regex', { regex: 'topic-.*' }],
  ])('with %s', (_, subscription) => {
    it.each([
      ['adds a partition', { 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }, []],
      [
        'revokes an owned partition',
        { 'topic-a': [1], 'topic-c': [0], 'topic-d': [0] },
        [[{ topic: 'topic-a', partitions: [0] }]],
      ],
    ])(
      'resolves again, inside the round, a target replaced while the round resolves its topic ids by one that %s, and applies the newer target once, without a crash',
      async (_, newer, revocations) => {
        const built = buildConsumer()
        consumer = built.consumer
        const s = { phase: 'steady' }
        const named = { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] }
        built.state.answer = (request) => {
          if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
          if (s.phase === 'grow') {
            s.phase = 'grown'
            return fullTarget(2, named)
          }
          if (s.phase === 'grown') {
            s.phase = 'regrown'
            return fullTarget(3, newer)
          }
          if (s.phase === 'steady') {
            return request.memberEpoch === 0
              ? fullTarget(1, { 'topic-a': [0] })
              : fullTarget(1, null)
          }
          return fullTarget(request.memberEpoch, null)
        }
        const starts = jest.spyOn(Runner.prototype, 'start')
        built.state.unknown.add('topic-c').add('topic-d')
        await consumer.subscribe(subscription)
        consumer.run({ eachMessage: async () => {} })
        await advanceUntil(() => built.fetches.length > 0)
        const [runner] = starts.mock.contexts
        const steps = jest.spyOn(runner.consumerGroup.groupMembership, 'rebalance')

        // The membership's own refresh names topic-c and schedules the round; the round's step
        // waits 700 ms for topic-d, and the heartbeat sent meanwhile brings epoch 3.
        built.state.idDelays.push(0, 700)
        s.phase = 'grow'
        await advanceUntil(() => s.phase === 'regrown')
        await jest.advanceTimersByTimeAsync(3000)

        expect(built.crashes).toEqual([])
        expect(starts).toHaveBeenCalledTimes(1)
        expect(runner.running).toBe(true)
        expect(steps).toHaveBeenCalledTimes(1)
        await expect(steps.mock.results[0].value).resolves.toEqual({
          assignment: newer,
          userData: null,
        })
        expect(
          Object.fromEntries(
            runner.consumerGroup.assigned().map(({ topic, partitions }) => [topic, partitions])
          )
        ).toEqual(newer)
        expect(runner.consumerGroup.groupMembership.memberEpoch).toEqual(3)
        expect(runner.consumerGroup.groupMembership.state).toEqual('STABLE')
        expect(
          built.log
            .filter(({ kind }) => kind === 'PARTITIONS_REVOKED')
            .map(({ partitions }) => partitions)
        ).toEqual(revocations)
        const fetchedSince = Date.now() - 1000
        expect(
          new Set(built.fetches.filter(({ at }) => at > fetchedSince).map(({ tp }) => tp))
        ).toEqual(
          new Set(
            Object.entries(newer).flatMap(([topic, partitions]) =>
              partitions.map((partition) => `${topic}|${partition}`)
            )
          )
        )
      }
    )
  })

  /**
   * Joins with topic-a:0, then epoch 2 names topic-c and topic-d by id; the round's lookup of
   * topic-d takes 1,200 ms. Epoch 3 replaces that target 500 ms in, so the step resolves again and
   * returns the epoch 3 target, and the heartbeat sent at epoch 3 is held until that step has
   * ended, then refused with `code`. Rejoins at epoch 0 get epoch 100 with the latest target.
   * @param {number} code the error code the held heartbeat is refused with
   * @returns {Promise<object>} the built consumer, its runner and the log index of the refusal
   */
  const refuseHeldHeartbeatAcrossSupersededStep = async (code) => {
    const built = buildConsumer()
    consumer = built.consumer
    const latest = { 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }
    const s = { phase: 'steady' }
    let refuse
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) =>
      built.log.push({ kind: 'GROUP_JOIN', at: Date.now(), assignment: payload.memberAssignment })
    )
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (request.memberEpoch === 0) {
        return s.phase === 'refused' ? fullTarget(100, latest) : fullTarget(1, { 'topic-a': [0] })
      }
      if (s.phase === 'A') {
        s.phase = 'B'
        return fullTarget(2, { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] })
      }
      if (s.phase === 'B') {
        s.phase = 'hold'
        return fullTarget(3, latest)
      }
      if (s.phase === 'hold') {
        s.phase = 'held'
        return new Promise((_, reject) => {
          refuse = () => reject(new KafkaJSProtocolError(createErrorFromCode(code)))
        })
      }
      return fullTarget(request.memberEpoch, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: ['topic-a', 'topic-c', 'topic-d'] })
    consumer.run({ eachMessage: async () => {} })
    await advanceUntil(() => built.fetches.length > 0)
    const [runner] = starts.mock.contexts
    const group = runner.consumerGroup
    const steps = jest.spyOn(group.groupMembership, 'rebalance')
    built.state.unknown.add('topic-c').add('topic-d')
    built.state.idDelays.push(0, 1200)
    s.phase = 'A'
    await advanceUntil(() => !!refuse)
    const heldIndex = built.log.length
    const token = group.ownership.token()
    // The step ends, and its round applies, while the epoch 3 heartbeat is held.
    let settled = false
    steps.mock.results[0].value.then(
      () => (settled = true),
      () => (settled = true)
    )
    await advanceUntil(() => settled, 1000)
    await jest.advanceTimersByTimeAsync(10)
    const held = built.log.slice(heldIndex)
    const tokenBeforeRefusal = group.ownership.token()
    s.phase = 'refused'
    const refusedIndex = built.log.length
    refuse()
    await jest.advanceTimersByTimeAsync(3000)
    return { ...built, runner, group, held, token, tokenBeforeRefusal, refusedIndex }
  }

  it('acts on a fence answering a heartbeat sent while a superseded step resolved its names: the step applies the live target, then one PARTITIONS_LOST of it and a rejoin with the latest target', async () => {
    const run = await refuseHeldHeartbeatAcrossSupersededStep(FENCED_MEMBER_EPOCH)
    const after = run.log.slice(run.refusedIndex)

    expect(
      run.held.filter(({ kind }) => kind === 'GROUP_JOIN').map((join) => join.assignment)
    ).toEqual([{ 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }])
    expect(run.tokenBeforeRefusal).toEqual(run.token + 1)
    expect(
      after
        .filter(
          ({ kind, epoch }) =>
            kind.startsWith('PARTITIONS_') || kind === 'GROUP_JOIN' || epoch === 0
        )
        .map(({ kind, epoch }) => (kind === 'heartbeat' ? epoch : kind))
    ).toEqual(['PARTITIONS_LOST', 0, 'PARTITIONS_ASSIGNED', 'GROUP_JOIN'])
    expect(after.find(({ kind }) => kind === 'PARTITIONS_LOST').partitions).toEqual([
      { topic: 'topic-a', partitions: [0, 1] },
      { topic: 'topic-c', partitions: [0] },
      { topic: 'topic-d', partitions: [0] },
    ])
    expect(after.find(({ kind }) => kind === 'GROUP_JOIN').assignment).toEqual({
      'topic-a': [0, 1],
      'topic-c': [0],
      'topic-d': [0],
    })
    // Nothing went out under the fenced epoch after the fence.
    expect(
      after.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch > 0 && epoch < 100)
    ).toEqual([])
    expect(run.group.groupMembership.memberEpoch).toEqual(100)
    expect(run.crashes).toEqual([])
  })

  it('reports a fatal error answering a heartbeat sent while a superseded step resolved its names, even when a later heartbeat is authorized', async () => {
    const run = await refuseHeldHeartbeatAcrossSupersededStep(30)
    const after = run.log.slice(run.refusedIndex)

    expect(
      run.held.filter(({ kind }) => kind === 'GROUP_JOIN').map((join) => join.assignment)
    ).toEqual([{ 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }])
    expect(run.tokenBeforeRefusal).toEqual(run.token + 1)
    expect(run.crashes.map(({ error }) => error.type)).toEqual(['GROUP_AUTHORIZATION_FAILED'])
    expect(after.filter(({ kind }) => kind === 'GROUP_JOIN')).toEqual([])
    expect(after.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch >= 0)).toEqual([])
    expect(run.group.groupMembership.state).toEqual('FATAL')
  })

  /**
   * Joins with topic-a:0, then epoch 2 names topic-c and topic-d by id; the round's lookup of
   * topic-d takes 1,200 ms. The heartbeat sent at epoch 2 during that lookup is held until the
   * round has applied the epoch 2 target (an ordinary apply: nothing replaced it), then refused
   * with `code`. Later heartbeats are authorized; rejoins at epoch 0 get epoch 100 with `latest`.
   * @param {number} code the error code the held heartbeat is refused with
   * @returns {Promise<object>} the built consumer, its runner and what the case observed
   */
  const refuseHeldHeartbeatAcrossApply = async (code) => {
    const built = buildConsumer()
    consumer = built.consumer
    const named = { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] }
    const latest = { 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }
    const s = { phase: 'steady' }
    let refuse
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) =>
      built.log.push({ kind: 'GROUP_JOIN', at: Date.now(), assignment: payload.memberAssignment })
    )
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (request.memberEpoch === 0) {
        return s.phase === 'refused' ? fullTarget(100, latest) : fullTarget(1, { 'topic-a': [0] })
      }
      if (s.phase === 'grow') {
        s.phase = 'hold'
        return fullTarget(2, named)
      }
      if (s.phase === 'hold') {
        s.phase = 'held'
        return new Promise((_, reject) => {
          refuse = () => reject(new KafkaJSProtocolError(createErrorFromCode(code)))
        })
      }
      return fullTarget(request.memberEpoch, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: ['topic-a', 'topic-c', 'topic-d'] })
    consumer.run({ eachMessage: async () => {} })
    await advanceUntil(() => built.fetches.length > 0)
    const [runner] = starts.mock.contexts
    const group = runner.consumerGroup
    built.state.unknown.add('topic-c').add('topic-d')
    built.state.idDelays.push(0, 1200)
    const grownIndex = built.log.length
    s.phase = 'grow'
    await advanceUntil(() => !!refuse)
    const token = group.ownership.token()
    const joined = () => built.log.slice(grownIndex).filter(({ kind }) => kind === 'GROUP_JOIN')
    await advanceUntil(() => joined().length > 0, 2000)
    await jest.advanceTimersByTimeAsync(10)
    const applied = joined()
    const tokenBeforeRefusal = group.ownership.token()
    s.phase = 'refused'
    const refusedIndex = built.log.length
    refuse()
    await jest.advanceTimersByTimeAsync(3000)
    return { ...built, runner, group, applied, token, tokenBeforeRefusal, refusedIndex }
  }

  it('acts on a fence answering a heartbeat sent before an ordinary apply completed: one PARTITIONS_LOST of the applied assignment, then a rejoin with the latest target', async () => {
    const run = await refuseHeldHeartbeatAcrossApply(FENCED_MEMBER_EPOCH)
    const after = run.log.slice(run.refusedIndex)

    expect(run.applied.map(({ assignment }) => assignment)).toEqual([
      { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] },
    ])
    expect(run.tokenBeforeRefusal).toEqual(run.token + 1)
    expect(
      after
        .filter(
          ({ kind, epoch }) =>
            kind.startsWith('PARTITIONS_') || kind === 'GROUP_JOIN' || epoch === 0
        )
        .map(({ kind, epoch }) => (kind === 'heartbeat' ? epoch : kind))
    ).toEqual(['PARTITIONS_LOST', 0, 'PARTITIONS_ASSIGNED', 'GROUP_JOIN'])
    expect(after.find(({ kind }) => kind === 'PARTITIONS_LOST').partitions).toEqual([
      { topic: 'topic-a', partitions: [0] },
      { topic: 'topic-c', partitions: [0] },
      { topic: 'topic-d', partitions: [0] },
    ])
    expect(
      after.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch > 0 && epoch < 100)
    ).toEqual([])
    expect(run.group.groupMembership.memberEpoch).toEqual(100)
    expect(run.crashes).toEqual([])
  })

  it('reports a fatal error answering a heartbeat sent before an ordinary apply completed, even when a later heartbeat is authorized', async () => {
    const run = await refuseHeldHeartbeatAcrossApply(30)
    const after = run.log.slice(run.refusedIndex)

    expect(run.applied).toHaveLength(1)
    expect(run.tokenBeforeRefusal).toEqual(run.token + 1)
    expect(run.crashes.map(({ error }) => error.type)).toEqual(['GROUP_AUTHORIZATION_FAILED'])
    expect(after.filter(({ kind }) => kind === 'GROUP_JOIN')).toEqual([])
    expect(after.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch >= 0)).toEqual([])
    expect(run.group.groupMembership.state).toEqual('FATAL')
  })

  it('applies an empty assignment that replaces the first target while the first step resolves its names: run() resolves after one GROUP_JOIN for the empty assignment, no crash, then delivery once a later heartbeat grants a partition', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    const joins = []
    const handled = []
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload.memberAssignment))
    let answered = 0
    let grant = false
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      answered += 1
      if (request.memberEpoch === 0) {
        return fullTarget(1, { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] })
      }
      if (answered === 2) return fullTarget(2, {})
      if (grant) {
        grant = false
        return fullTarget(3, { 'topic-a': [0] })
      }
      return fullTarget(request.memberEpoch, null)
    }
    built.state.unknown.add('topic-c').add('topic-d')
    built.state.idDelays.push(700)
    built.state.queue['topic-a|0'] = [40]
    await consumer.subscribe({ topics: ['topic-a', 'topic-c', 'topic-d'] })
    let ran = false
    consumer
      .run({
        eachMessage: async ({ topic, partition, message }) =>
          handled.push({ tp: `${topic}|${partition}`, offset: message.offset }),
      })
      .then(() => (ran = true))
    await advanceUntil(() => ran)
    await jest.advanceTimersByTimeAsync(2000)

    expect(ran).toBe(true)
    expect(joins).toEqual([{}])
    expect(built.crashes).toEqual([])
    expect(built.fetches).toEqual([])

    grant = true
    await advanceUntil(() => handled.length > 0)

    expect(handled).toEqual([{ tp: 'topic-a|0', offset: '40' }])
    expect(joins).toEqual([{}, { 'topic-a': [0] }])
    expect(built.crashes).toEqual([])
  })

  /**
   * Joins with topic-a:0 at epoch 1; then the epoch 1 heartbeat is fenced. The LOST round rejoins
   * and gets epoch 100 with A (topic-c and topic-d by id, whose lookup takes 1,200 ms); the
   * heartbeat at epoch 100 brings epoch 101 with B, and the next one, sent at epoch 101, is held
   * until the LOST round's step has settled, then refused with `code`. Rejoins at epoch 0 from
   * then on get epoch 200 with B.
   * @param {number} code the error code the held heartbeat is refused with
   * @returns {Promise<object>} the built consumer, its runner and the log index of the refusal
   */
  const refuseHeldHeartbeatDuringRecovery = async (code) => {
    const built = buildConsumer()
    consumer = built.consumer
    const latest = { 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }
    const old = { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] }
    const s = { phase: 'steady' }
    let refuse
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) =>
      built.log.push({ kind: 'GROUP_JOIN', at: Date.now(), assignment: payload.memberAssignment })
    )
    built.state.answer = (request) => {
      const epoch = request.memberEpoch
      if (epoch < 0) return fullTarget(epoch, null)
      if (epoch === 0) {
        if (s.phase === 'B') return fullTarget(100, old)
        if (s.phase === 'refused') return fullTarget(200, latest)
        return fullTarget(1, { 'topic-a': [0] })
      }
      if (epoch === 1 && s.phase === 'fence') {
        s.phase = 'B'
        throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      }
      if (epoch === 100 && s.phase === 'B') {
        s.phase = 'hold'
        return fullTarget(101, latest)
      }
      if (epoch === 101 && s.phase === 'hold') {
        s.phase = 'held'
        return new Promise((_, reject) => {
          refuse = () => reject(new KafkaJSProtocolError(createErrorFromCode(code)))
        })
      }
      return fullTarget(epoch, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: ['topic-a', 'topic-c', 'topic-d'] })
    consumer.run({ eachMessage: async () => {} })
    await advanceUntil(() => built.fetches.length > 0)
    const [runner] = starts.mock.contexts
    const group = runner.consumerGroup
    const steps = jest.spyOn(group.groupMembership, 'rebalance')
    built.state.unknown.add('topic-c').add('topic-d')
    built.state.idDelays.push(1200)
    s.phase = 'fence'
    await advanceUntil(() => !!refuse)
    let settled = false
    steps.mock.results[0].value.then(
      () => (settled = true),
      () => (settled = true)
    )
    await advanceUntil(() => settled, 3000)
    await jest.advanceTimersByTimeAsync(10)
    s.phase = 'refused'
    const refusedIndex = built.log.length
    refuse()
    await jest.advanceTimersByTimeAsync(3000)
    return { ...built, runner, group, refusedIndex }
  }

  it('acts on a fence answering a heartbeat sent under the rejoined membership while the LOST round resolved a superseded target: one PARTITIONS_LOST, then a rejoin with the latest target', async () => {
    const run = await refuseHeldHeartbeatDuringRecovery(FENCED_MEMBER_EPOCH)
    const after = run.log.slice(run.refusedIndex)

    expect(
      run.log
        .slice(0, run.refusedIndex)
        .filter(({ kind }) => kind === 'PARTITIONS_LOST')
        .map(({ partitions }) => partitions)
    ).toEqual([[{ topic: 'topic-a', partitions: [0] }]])
    expect(partitionEventsAndRejoins(after)).toEqual(['PARTITIONS_LOST', 0, 'PARTITIONS_ASSIGNED'])
    expect(after.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch === 101)).toEqual([])
    expect(
      after.filter(({ kind }) => kind === 'GROUP_JOIN').map((join) => join.assignment)
    ).toEqual([{ 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }])
    expect(run.group.groupMembership.memberEpoch).toEqual(200)
    expect(run.crashes).toEqual([])
  })

  it('reports a fatal error answering a heartbeat sent under the rejoined membership while the LOST round resolved a superseded target, even when a later heartbeat is authorized', async () => {
    const run = await refuseHeldHeartbeatDuringRecovery(30)
    const after = run.log.slice(run.refusedIndex)

    expect(run.crashes.map(({ error }) => error.type)).toEqual(['GROUP_AUTHORIZATION_FAILED'])
    expect(after.filter(({ kind }) => kind === 'GROUP_JOIN')).toEqual([])
    expect(after.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch >= 0)).toEqual([])
    expect(run.group.groupMembership.state).toEqual('FATAL')
  })

  it('starts the run when a newer target replaces the first target while its step resolves names, applying the newer target in that step', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    const newer = { 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }
    let answered = 0
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      answered += 1
      if (request.memberEpoch === 0) {
        return fullTarget(1, { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] })
      }
      return answered === 2 ? fullTarget(2, newer) : fullTarget(request.memberEpoch, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    const steps = jest.spyOn(ConsumerGroupProtocol.prototype, 'rebalance')
    built.state.unknown.add('topic-c').add('topic-d')
    built.state.idDelays.push(700)
    await consumer.subscribe({ topics: ['topic-a', 'topic-c', 'topic-d'] })
    let ran = false
    consumer.run({ eachMessage: async () => {} }).then(() => (ran = true))
    await advanceUntil(() => ran)
    await jest.advanceTimersByTimeAsync(2000)
    const [runner] = starts.mock.contexts

    expect(ran).toBe(true)
    expect(steps).toHaveBeenCalledTimes(1)
    await expect(steps.mock.results[0].value).resolves.toEqual({
      assignment: newer,
      userData: null,
    })
    expect(built.crashes).toEqual([])
    expect(starts).toHaveBeenCalledTimes(1)
    expect(
      Object.fromEntries(
        runner.consumerGroup.assigned().map(({ topic, partitions }) => [topic, partitions])
      )
    ).toEqual(newer)
    const fetchedSince = Date.now() - 500
    expect(
      new Set(built.fetches.filter(({ at }) => at > fetchedSince).map(({ tp }) => tp))
    ).toEqual(new Set(['topic-a|0', 'topic-a|1', 'topic-c|0', 'topic-d|0']))
  })

  it('resolves again under the current scope a step whose metadata scope a failed earlier subscribe() retired, and applies the unchanged target once', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    const joins = []
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload.memberAssignment))
    const named = { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] }
    let grow = false
    let rejectSubscribe
    built.cluster.refreshMetadata.mockImplementationOnce(async ({ onMetadata }) =>
      onMetadata({
        topicMetadata: ['topic-a', 'topic-c', 'topic-d'].map((topic) => ({
          topic,
          topicErrorCode: 0,
        })),
      })
    )
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (request.memberEpoch === 0) return fullTarget(1, { 'topic-a': [0] })
      if (grow) {
        grow = false
        return fullTarget(2, named)
      }
      return fullTarget(request.memberEpoch, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: [/^topic-/] })
    built.cluster.refreshMetadata.mockImplementationOnce(
      () => new Promise((_, reject) => (rejectSubscribe = reject))
    )
    const subscribing = consumer.subscribe({ topics: [/^other-/] }).catch((error) => error)
    consumer.run({ eachMessage: async () => {} })
    await advanceUntil(() => built.fetches.length > 0)
    const [runner] = starts.mock.contexts
    const group = runner.consumerGroup
    const membership = group.groupMembership
    const steps = jest.spyOn(membership, 'rebalance')
    built.state.unknown.add('topic-c').add('topic-d')
    built.state.idDelays.push(0, 700)
    grow = true
    await advanceUntil(() => built.cluster.refreshMetadataForTopicIds.mock.calls.length >= 2)
    const target = membership.targetRevision
    const token = group.ownership.token()

    rejectSubscribe(new Error('pattern metadata failure'))
    expect((await subscribing).message).toEqual('pattern metadata failure')
    let settled = false
    steps.mock.results[0].value.then(
      () => (settled = true),
      () => (settled = true)
    )
    await advanceUntil(() => settled, 1000)

    await expect(steps.mock.results[0].value).resolves.toEqual({
      assignment: named,
      userData: null,
    })
    await jest.advanceTimersByTimeAsync(2000)
    expect(steps).toHaveBeenCalledTimes(1)
    expect(joins).toEqual([{ 'topic-a': [0] }, named])
    expect(membership.targetRevision).toEqual(target)
    expect(group.ownership.token()).toEqual(token + 1)
    expect(built.crashes).toEqual([])
    expect(starts).toHaveBeenCalledTimes(1)
  })

  it('fails a step left behind by a fenced round once the member has rejoined, without a crash, and reports one PARTITIONS_LOST', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    const s = { phase: 'steady' }
    built.state.answer = (request) => {
      if (request.memberEpoch < 0) return fullTarget(request.memberEpoch, null)
      if (request.memberEpoch === 0) {
        return fullTarget(s.phase === 'fenced' ? 100 : 1, { 'topic-a': [0] })
      }
      if (s.phase === 'grow') {
        s.phase = 'fence'
        return fullTarget(2, { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] })
      }
      if (s.phase === 'fence') {
        s.phase = 'fenced'
        throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      }
      return fullTarget(request.memberEpoch, null)
    }
    const starts = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topics: ['topic-a', 'topic-c', 'topic-d'] })
    consumer.run({ eachMessage: async () => {} })
    await advanceUntil(() => built.fetches.length > 0)
    const [runner] = starts.mock.contexts
    const membership = runner.consumerGroup.groupMembership
    const steps = jest.spyOn(membership, 'rebalance')
    built.state.unknown.add('topic-c').add('topic-d')
    built.state.idDelays.push(0, 1200)
    s.phase = 'grow'
    await advanceUntil(() => membership.memberEpoch === 100)
    expect(membership.target).not.toBeNull()
    const leftBehind = steps.mock.results[0].value
    leftBehind.catch(() => {})
    await jest.advanceTimersByTimeAsync(1000)

    await expect(leftBehind).rejects.toMatchObject({ type: 'ASSIGNMENT_CHANGED' })
    expect(built.log.filter(({ kind }) => kind === 'PARTITIONS_LOST')).toHaveLength(1)
    expect(built.crashes).toEqual([])
    expect(starts).toHaveBeenCalledTimes(1)
  })

  it("calls the listener's onPartitionsLost once, before any rejoin, when a fence reaches the slot during the wait while a handler keeps the fetch loop busy", async () => {
    const calls = []
    const record = (name) => async (payload) => calls.push({ name, at: Date.now(), payload })
    const run = await fenceWhileHandlerRuns({
      rebalanceListener: {
        onPartitionsAssigned: record('assigned'),
        onPartitionsRevoked: record('revoked'),
        onPartitionsLost: record('lost'),
      },
    })
    const later = calls.filter(({ at }) => at > run.fencedAt)

    expect(later.map(({ name }) => name)).toEqual(['lost', 'assigned'])
    expect(later[0].payload.partitions).toEqual([
      { topic: 'topic-a', partitions: [0] },
      { topic: 'topic-b', partitions: [0, 1] },
    ])
    expect(
      run.log
        .slice(run.fencedIndex)
        .filter(({ kind, epoch }) => kind.startsWith('onPartitions') || epoch === 0)
        .map(({ kind, epoch }) => (kind === 'heartbeat' ? epoch : kind))
    ).toEqual(['onPartitionsLost', 0, 'onPartitionsAssigned'])
    expect(partitionEventsAndRejoins(run.log.slice(run.fencedIndex))).toEqual([
      'PARTITIONS_LOST',
      0,
      'PARTITIONS_ASSIGNED',
    ])
    expect(
      run.handled
        .filter(({ at, tp }) => at > run.fencedAt && tp === 'topic-a|0')
        .map(({ offset }) => offset)
    ).toEqual(['40', '41'])
    expect(run.log.filter(({ kind, tp }) => kind === 'commit' && tp === 'topic-a|0')).toEqual([
      expect.objectContaining({ offset: '42', generationId: 100 }),
    ])
    expect(run.crashes).toEqual([])
  })
})

describe('Consumer > a whole consumer-protocol consumer > a refusal held across reconciliation', () => {
  let consumer

  beforeEach(() => jest.useFakeTimers())
  afterEach(async () => {
    const stopping = consumer.disconnect().catch(() => {})
    await jest.advanceTimersByTimeAsync(5000)
    await stopping
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  const NAMED = { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] }
  const HOLD = Symbol('hold')
  const FENCE = Symbol('fence')
  const REJOIN_EPOCH = 200

  /**
   * What each phase answers. `joins` answer epoch 0 heartbeats in order; once the case is armed,
   * `steps` answer the other heartbeats in order: FENCE refuses one, HOLD keeps one in flight until
   * the case refuses it. Every phase ends with a target naming topic-c and topic-d by id replaced,
   * while the round's step looks those ids up, by `newer`, and the heartbeat sent under that
   * membership held. After the refusal, a rejoin gets epoch 200 with topic-a:0.
   * @type {Record<string, (newer: object) => { joins: object[], steps: Array<object|symbol>,
   *   idDelays: number[], armed: boolean }>}
   */
  const PHASES = {
    startup: (newer) => ({
      joins: [fullTarget(1, NAMED)],
      steps: [fullTarget(2, newer), HOLD],
      idDelays: [1200],
      armed: true,
    }),
    ordinary: (newer) => ({
      joins: [fullTarget(1, { 'topic-a': [0] })],
      steps: [fullTarget(2, NAMED), fullTarget(3, newer), HOLD],
      idDelays: [0, 1200],
      armed: false,
    }),
    lost: (newer) => ({
      joins: [fullTarget(1, { 'topic-a': [0] }), fullTarget(100, NAMED)],
      steps: [FENCE, fullTarget(101, newer), HOLD],
      idDelays: [1200],
      armed: false,
    }),
  }

  /**
   * Runs one row: a whole consumer whose step is superseded by `newer` while a heartbeat sent
   * under the live membership is held, the held answer released once that step has settled and its
   * round applied (and, for a busy handler, once a handler of topic-a:0 runs), as a refusal `code`.
   * A busy handler takes 3,000 ms for every record; with nothing owned no handler can run.
   * @param {{ phase: string, newer: object, busy: boolean, code: number }} row
   * @returns {Promise<object>} the log after the refusal and what the case observed
   */
  const play = async ({ phase, newer, busy, code }) => {
    const built = buildConsumer()
    consumer = built.consumer
    const script = PHASES[phase](newer)
    const s = { armed: script.armed, refused: false, running: 0 }
    let refuse
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) =>
      built.log.push({ kind: 'GROUP_JOIN', at: Date.now(), assignment: payload.memberAssignment })
    )
    built.state.answer = (request) => {
      const epoch = request.memberEpoch
      if (epoch < 0) return fullTarget(epoch, null)
      if (epoch === 0) {
        return s.refused || !script.joins.length
          ? fullTarget(REJOIN_EPOCH, { 'topic-a': [0] })
          : script.joins.shift()
      }
      const step = s.armed && !s.refused ? script.steps.shift() : undefined
      if (step === FENCE) throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      if (step === HOLD) {
        return new Promise((_, reject) => {
          refuse = () => reject(new KafkaJSProtocolError(createErrorFromCode(code)))
        })
      }
      return step || fullTarget(epoch, null)
    }
    built.state.queue['topic-a|0'] = [40]
    const steps = jest.spyOn(ConsumerGroupProtocol.prototype, 'rebalance')
    const starts = jest.spyOn(Runner.prototype, 'start')
    const arm = () => {
      built.state.unknown.add('topic-c').add('topic-d')
      built.state.idDelays.push(...script.idDelays)
      s.armed = true
    }
    if (s.armed) arm()
    await consumer.subscribe({ topics: ['topic-a', 'topic-c', 'topic-d'] })
    let atRun = null
    consumer
      .run({
        eachMessage: async () => {
          s.running += 1
          if (busy) await new Promise((resolve) => setTimeout(resolve, 3000))
          s.running -= 1
        },
      })
      .then(() => {
        const group = starts.mock.contexts[0].consumerGroup
        atRun = {
          joins: built.log.filter(({ kind }) => kind === 'GROUP_JOIN').length,
          offsetManager: group.offsetManager != null,
          lossOwed: group.ownership.refusal(group.ownership.token()) != null,
          stopRequested: starts.mock.contexts[0].stopRequested,
        }
      })
    if (!s.armed) {
      await advanceUntil(() => (busy ? s.running > 0 : built.fetches.length > 0))
      arm()
    }
    await advanceUntil(() => !!refuse, 10000)
    let settled = false
    steps.mock.results.at(-1).value.then(
      () => (settled = true),
      () => (settled = true)
    )
    await advanceUntil(() => settled, 3000)
    await jest.advanceTimersByTimeAsync(10)
    if (busy) await advanceUntil(() => s.running > 0, 1000)
    const [runner] = starts.mock.contexts
    const group = runner.consumerGroup
    const owned = group.assigned()
    const busyAtRefusal = s.running > 0
    const refusedIndex = built.log.length
    const joined = built.log.filter(({ kind }) => kind === 'GROUP_JOIN')
    s.refused = true
    refuse()
    await jest.advanceTimersByTimeAsync(10000)
    return {
      ...built,
      appliedBeforeRefusal: joined[joined.length - 1].assignment,
      after: built.log.slice(refusedIndex),
      group,
      owned,
      busyAtRefusal,
      atRun,
    }
  }

  const ROWS = []
  for (const phase of ['startup', 'ordinary', 'lost']) {
    for (const [target, newer] of [
      ['empty', {}],
      ['non-empty', { 'topic-a': [0, 1] }],
    ]) {
      for (const busy of [true, false])
        ROWS.push([phase, target, busy ? 'busy' : 'free', newer, busy])
    }
  }

  describe.each(ROWS)(
    '%s round, %s newer target, handler %s',
    (phase, _target, _h, newer, busy) => {
      /**
       * The row's own preconditions: the superseded step applied the newer target before the
       * refusal, and a busy handler was running then wherever the member owned, or had owned, a
       * partition with a record (not with an empty assignment at startup or after a LOST round).
       * @param {object} run what `play` observed
       * @returns {void}
       */
      const expectRow = (run) => {
        expect(run.appliedBeforeRefusal).toEqual(newer)
        expect(run.busyAtRefusal).toBe(
          busy && (phase === 'ordinary' || Object.keys(newer).length > 0)
        )
      }

      it('acts on a fence answering the held heartbeat: no apply or commit of lost progress before one PARTITIONS_LOST of what it owned and the epoch 0 rejoin; run() resolved after one GROUP_JOIN with offsets', async () => {
        const run = await play({ phase, newer, busy, code: FENCED_MEMBER_EPOCH })
        expectRow(run)
        const rejoinAt = run.after.findIndex(({ epoch }) => epoch === 0)
        const beforeRejoin = run.after.slice(0, rejoinAt)

        expect(rejoinAt).toBeGreaterThanOrEqual(0)
        expect(
          beforeRejoin
            .filter(({ kind }) => kind === 'PARTITIONS_LOST')
            .map(({ partitions }) => partitions)
        ).toEqual(run.owned.length ? [run.owned] : [])
        expect(run.after.filter(({ kind }) => kind === 'PARTITIONS_LOST')).toHaveLength(
          run.owned.length ? 1 : 0
        )
        expect(
          beforeRejoin.filter(({ kind }) => kind === 'GROUP_JOIN' || kind === 'PARTITIONS_ASSIGNED')
        ).toEqual([])
        expect(
          run.after.filter(
            ({ kind, generationId }) => kind === 'commit' && generationId < REJOIN_EPOCH
          )
        ).toEqual([])
        expect(run.group.groupMembership.memberEpoch).toEqual(REJOIN_EPOCH)
        expect(run.atRun).toEqual({
          joins: 1,
          offsetManager: true,
          lossOwed: false,
          stopRequested: false,
        })
        expect(run.crashes).toEqual([])
      })

      it('reports a fatal error answering the held heartbeat, even when a later heartbeat is authorized, and commits nothing after it', async () => {
        const run = await play({ phase, newer, busy, code: 30 })
        expectRow(run)

        expect(run.crashes.map(({ error }) => error.type)).toEqual(['GROUP_AUTHORIZATION_FAILED'])
        expect(run.after.filter(({ kind }) => kind === 'GROUP_JOIN' || kind === 'commit')).toEqual(
          []
        )
        expect(run.after.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch >= 0)).toEqual(
          []
        )
        expect(run.group.groupMembership.state).toEqual('FATAL')
        expect(run.atRun).toEqual({
          joins: 1,
          offsetManager: true,
          lossOwed: false,
          stopRequested: false,
        })
      })
    }
  )
})

describe('Consumer > a whole consumer-protocol consumer > the heartbeat outage clock', () => {
  const COORDINATOR_LOAD_IN_PROGRESS = 14
  let consumer

  beforeEach(() => jest.useFakeTimers())
  afterEach(async () => {
    const stopping = consumer.disconnect().catch(() => {})
    await jest.advanceTimersByTimeAsync(3000)
    await stopping
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it('a live timer heartbeat success ends the outage a failed handler heartbeat started', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    const starts = jest.spyOn(Runner.prototype, 'start')
    const heartbeats = []
    let releaseHandler
    let entered = false
    let failNext = false
    built.state.answer = ({ memberEpoch }) => {
      if (memberEpoch === 0) return fullTarget(1, { 'topic-a': [0] })
      if (failNext) {
        failNext = false
        throw new KafkaJSProtocolError(createErrorFromCode(COORDINATOR_LOAD_IN_PROGRESS))
      }
      return fullTarget(memberEpoch, null)
    }
    consumer.on(consumer.events.HEARTBEAT, ({ payload }) =>
      heartbeats.push(payload.groupGenerationId)
    )
    await consumer.subscribe({ topics: ['topic-a'] })
    try {
      consumer.run({
        eachMessage: async () => {
          if (entered) return
          entered = true
          await new Promise((resolve) => (releaseHandler = resolve))
        },
      })
      await advanceUntil(() => built.fetches.length > 0)
      const [runner] = starts.mock.contexts
      runner.heartbeatManager.stop()
      failNext = true
      await jest.advanceTimersByTimeAsync(500)
      const failure = await runner.heartbeat().then(
        () => null,
        (error) => error
      )
      expect(failure).toMatchObject({ code: COORDINATOR_LOAD_IN_PROGRESS })
      expect(runner.rideOutage(failure)).toBeDefined()
      expect(runner.outages.heartbeat.since).not.toBeNull()
      const count = heartbeats.length
      built.state.queue['topic-a|0'] = [40]
      await advanceUntil(() => entered)
      expect(entered).toBe(true)

      runner.heartbeatManager.ensureStarted()
      await advanceUntil(() => heartbeats.length > count)

      expect(heartbeats).toHaveLength(count + 1)
      expect(runner.outages.heartbeat.since).toBeNull()
    } finally {
      releaseHandler?.()
    }
  })
})

describe('Consumer > a whole consumer-protocol consumer > a saved eachBatch resolver', () => {
  let consumer

  beforeEach(() => jest.useFakeTimers())
  afterEach(async () => {
    const stopping = consumer.disconnect().catch(() => {})
    await jest.advanceTimersByTimeAsync(3000)
    await stopping
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  /**
   * Runs `eachBatch` without auto-resolve over a coordinator that hands out `targets` in order,
   * one per accepted heartbeat once `s.next()` is called, and fences every epoch below 100 once
   * `s.fenced` is set (an epoch 0 join then gets epoch 100 with `rejoinTarget`). The first batch of
   * topic-a:0 at offset 40 keeps its `resolveOffset` and, while `s.hold` is set, waits for it.
   * @param {object} options
   * @param {Object<string, number[]>} options.first the epoch 1 assignment
   * @param {Object<string, number[]>} [options.rejoinTarget]
   * @returns {Promise<object>} the built consumer, its switches and what the handler saw
   */
  const start = async ({ first, rejoinTarget = first }) => {
    const built = buildConsumer()
    consumer = built.consumer
    const s = { fenced: false, epoch: 1, pending: [], hold: null, saved: null, handled: [] }
    s.next = (target) => s.pending.push(target)
    built.state.answer = ({ memberEpoch }) => {
      if (memberEpoch < 0) return fullTarget(memberEpoch, null)
      if (s.fenced) {
        if (memberEpoch === 0) return fullTarget(100, rejoinTarget)
        if (memberEpoch >= 100) return fullTarget(100, null)
        throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      }
      if (memberEpoch === 0) return fullTarget(1, first)
      if (s.pending.length) return fullTarget(++s.epoch, s.pending.shift())
      return fullTarget(s.epoch, null)
    }
    await consumer.subscribe({ topics: ['topic-a', 'topic-b'] })
    consumer.run({
      autoCommitInterval: 1000,
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset }) => {
        const tp = `${batch.topic}|${batch.partition}`
        for (const message of batch.messages) s.handled.push(`${tp}@${message.offset}`)
        if (!s.saved && tp === 'topic-a|0') {
          s.saved = resolveOffset
          if (s.hold) await s.hold
          return
        }
        for (const message of batch.messages) resolveOffset(message.offset)
      },
    })
    return { ...built, s }
  }

  /** @returns {string[]} the committed offsets of topic-a:0, in order */
  const commitsOf = (log, tp) =>
    log.filter(({ kind, tp: at }) => kind === 'commit' && at === tp).map(({ offset }) => offset)

  it('writes nothing once its partition was lost and the replacement published: the new manager commits only fresh progress', async () => {
    const run = await start({ first: { 'topic-a': [0] }, rejoinTarget: { 'topic-a': [0, 1] } })
    run.state.queue['topic-a|0'] = [40]
    await advanceUntil(() => run.s.saved !== null)
    run.state.queue['topic-a|0'] = []
    run.s.fenced = true
    await advanceUntil(() => run.log.some(({ epoch }) => epoch === 100), 10000)
    expect(run.log.some(({ kind }) => kind === 'PARTITIONS_LOST')).toBe(true)
    run.state.queue['topic-a|0'] = [40]
    await advanceUntil(() => run.s.handled.filter((h) => h === 'topic-a|0@40').length === 2)

    run.s.saved('77')
    await jest.advanceTimersByTimeAsync(1500)
    run.state.queue['topic-a|1'] = [40]
    await advanceUntil(() => run.s.handled.includes('topic-a|1@40'))
    await advanceUntil(() => commitsOf(run.log, 'topic-a|1').length > 0)

    expect(commitsOf(run.log, 'topic-a|1')).toContain('41')
    expect(commitsOf(run.log, 'topic-a|0')).not.toContain('78')
    expect(commitsOf(run.log, 'topic-a|0')).toContain('41')
  })

  it('writes nothing once its partition was revoked and acquired again under the same manager', async () => {
    const run = await start({ first: { 'topic-a': [0, 1] } })
    let release
    run.s.hold = new Promise((resolve) => (release = resolve))
    run.state.queue['topic-a|0'] = [40]
    await advanceUntil(() => run.s.saved !== null)
    run.s.next({ 'topic-a': [1] })
    await advanceUntil(() => run.log.some(({ kind }) => kind === 'PARTITIONS_REVOKED'), 10000)
    run.s.next({ 'topic-a': [0, 1] })
    await advanceUntil(
      () => run.log.filter(({ kind }) => kind === 'PARTITIONS_ASSIGNED').length >= 2,
      10000
    )
    release()

    run.s.saved('77')
    await jest.advanceTimersByTimeAsync(1500)
    run.state.queue['topic-a|1'] = [40]
    await advanceUntil(() => run.s.handled.includes('topic-a|1@40'))
    await advanceUntil(() => commitsOf(run.log, 'topic-a|1').length > 0)

    expect(commitsOf(run.log, 'topic-a|1')).toContain('41')
    expect(commitsOf(run.log, 'topic-a|0')).not.toContain('78')
  })

  it('keeps its progress across an ordinary apply that renews the ownership while the handler runs: the partition resumes after it', async () => {
    const run = await start({ first: { 'topic-a': [0] } })
    let release
    run.s.hold = new Promise((resolve) => (release = resolve))
    run.state.queue['topic-a|0'] = [40]
    await advanceUntil(() => run.s.saved !== null)
    run.s.next({ 'topic-a': [0], 'topic-b': [0] })
    await advanceUntil(
      () =>
        run.log.some(
          ({ kind }) =>
            kind === 'PARTITIONS_ASSIGNED' &&
            run.log.filter((e) => e.kind === 'PARTITIONS_ASSIGNED').length >= 2
        ),
      10000
    )

    run.s.saved('40')
    release()
    run.state.queue['topic-a|0'] = [40, 41]
    await advanceUntil(() => run.s.handled.includes('topic-a|0@41'))
    await jest.advanceTimersByTimeAsync(1500)
    run.state.queue['topic-b|0'] = [40]
    await advanceUntil(() => run.s.handled.includes('topic-b|0@40'))
    await advanceUntil(() => commitsOf(run.log, 'topic-b|0').length > 0)

    expect(run.s.handled.filter((h) => h === 'topic-a|0@40')).toHaveLength(1)
    expect(commitsOf(run.log, 'topic-a|0')[0]).toBe('42')
  })

  it('still resolves the progress a natural stop commits while the handler drains', async () => {
    const run = await start({ first: { 'topic-a': [0] } })
    let release
    run.s.hold = new Promise((resolve) => (release = resolve))
    run.state.queue['topic-a|0'] = [40]
    await advanceUntil(() => run.s.saved !== null)

    const stopping = consumer.stop()
    await jest.advanceTimersByTimeAsync(100)
    run.s.saved('40')
    release()
    await jest.advanceTimersByTimeAsync(3000)
    await stopping

    expect(commitsOf(run.log, 'topic-a|0')).toEqual(['41'])
  })
})

describe('Consumer > a whole consumer-protocol consumer > observers and fetch admission', () => {
  let consumer

  beforeEach(() => jest.useFakeTimers())
  afterEach(async () => {
    const stopping = consumer.disconnect().catch(() => {})
    await jest.advanceTimersByTimeAsync(3000)
    await stopping
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it.each([
    ['null', null],
    ['undefined', undefined],
    ['an Error', new Error('observer')],
  ])(
    'a HEARTBEAT listener that throws %s does not stop startup: the member joins and the first record is delivered',
    async (_, thrown) => {
      const built = buildConsumer({
        maxPollInterval: 1500,
        retry: { retries: 0, initialRetryTime: 100, restartOnFailure: async () => false },
      })
      consumer = built.consumer
      const received = []
      const joins = []
      built.state.queue['topic-a|0'] = [40]
      built.state.answer = ({ memberEpoch }) =>
        memberEpoch === 0 ? fullTarget(1, { 'topic-a': [0] }) : fullTarget(memberEpoch, null)
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload.memberAssignment))
      consumer.on(consumer.events.HEARTBEAT, () => {
        throw thrown
      })
      await consumer.subscribe({ topics: ['topic-a'] })
      const run = consumer.run({
        eachMessage: async ({ message }) => received.push(message.offset),
      })
      await advanceUntil(() => received.length || built.crashes.length)
      await run

      expect(built.crashes).toEqual([])
      expect(joins).toEqual([{ 'topic-a': [0] }])
      expect(received).toEqual(['40'])
    },
    5000
  )

  it('an ordinary Fetch waiting for its broker when a heartbeat learns the member was fenced is not sent', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    const starts = jest.spyOn(Runner.prototype, 'start')
    let releaseBroker
    let fenced = false
    built.state.queue['topic-a|0'] = [40]
    built.state.answer = ({ memberEpoch }) => {
      if (memberEpoch === 0) return fullTarget(fenced ? 2 : 1, { 'topic-a': [0] })
      if (memberEpoch === 1 && fenced) {
        throw new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
      }
      return fullTarget(memberEpoch, null)
    }
    await consumer.subscribe({ topics: ['topic-a'] })
    const startup = consumer.run({
      eachMessage: async () => {
        if (releaseBroker) return
        built.cluster.findBroker.mockImplementationOnce(
          () => new Promise((resolve) => (releaseBroker = () => resolve(built.broker)))
        )
      },
    })
    await advanceUntil(() => releaseBroker || built.crashes.length)
    await startup
    expect(releaseBroker).toEqual(expect.any(Function))
    const [runner] = starts.mock.contexts
    const group = runner.consumerGroup
    const membership = group.groupMembership
    const fencedToken = group.ownership.token()
    const sent = []
    const fetch = built.broker.fetch.getMockImplementation()
    built.broker.fetch.mockImplementation(async (request) => {
      const refusal = request.guard?.()
      if (refusal) throw refusal
      sent.push(group.ownership.token())
      return fetch(request)
    })
    try {
      fenced = true
      membership.lastRequestAt = Date.now() - membership.heartbeatIntervalMs - 1
      await expect(runner.heartbeat()).rejects.toMatchObject({ type: 'FENCED_MEMBER_EPOCH' })
      expect(group.ownership.refusal(fencedToken)?.cause.type).toBe('FENCED_MEMBER_EPOCH')
    } finally {
      releaseBroker()
    }
    await jest.advanceTimersByTimeAsync(10)

    expect(sent.filter((token) => token === fencedToken)).toEqual([])
    expect(built.crashes).toEqual([])
  })

  it('an empty first assignment whose assigned hook is held delivers the only grant that arrives during the hook', async () => {
    const built = buildConsumer()
    consumer = built.consumer
    const starts = jest.spyOn(Runner.prototype, 'start')
    let grant = false
    let granted = false
    let releaseFirst
    let firstManager
    const deliveries = []
    built.state.queue['topic-a|0'] = [40]
    built.state.answer = ({ memberEpoch }) => {
      if (memberEpoch === 0) return fullTarget(1, {})
      if (grant && !granted) {
        granted = true
        return fullTarget(2, { 'topic-a': [0] })
      }
      return fullTarget(memberEpoch, null)
    }
    await consumer.subscribe({ topics: ['topic-a'] })
    const run = consumer.run({
      eachMessage: async ({ message }) => deliveries.push(message.offset),
      rebalanceListener: {
        onPartitionsAssigned: ({ partitions }) => {
          if (partitions.length) return
          firstManager = starts.mock.contexts[0].consumerGroup.offsetManager
          return new Promise((resolve) => (releaseFirst = resolve))
        },
      },
    })
    try {
      await advanceUntil(() => releaseFirst)
      expect(firstManager).toBeTruthy()
      grant = true
      await advanceUntil(() => granted)
      expect(deliveries).toEqual([])
    } finally {
      releaseFirst?.()
    }
    await advanceUntil(() => deliveries.length || built.crashes.length)
    await run

    expect(deliveries).toEqual(['40'])
    expect(built.crashes).toEqual([])
  })
})
