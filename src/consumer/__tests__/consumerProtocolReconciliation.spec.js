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
      expect(perWait.filter(({ acks, others }) => acks !== 1 || others > 1)).toEqual([])

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
   *   heartbeat, the poll timer expires as the wait starts, so the member leaves the group;
   *   'commit': a `commitOffsets()` call sent as the wait starts is fenced, and heartbeats at the
   *   old epoch keep succeeding
   * @returns {Promise<object>} the built consumer and what the case observed
   */
  const fenceWhileHandlerRuns = async ({ rebalanceListener, busy = true, trigger = 'fence' } = {}) => {
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
    consumer.run({
      autoCommitInterval: 30000,
      ...(rebalanceListener && { rebalanceListener }),
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
    await advanceUntil(() => built.log.some(({ epoch }) => epoch === 0 && s.fenced), 10000)
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
    const after = run.log.filter(({ at }) => at >= run.fencedAt)
    const lostAt = after.findIndex(({ kind }) => kind === 'PARTITIONS_LOST')
    const rejoinAt = after.findIndex(({ kind, epoch }) => kind === 'heartbeat' && epoch === 0)

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
    expect(rejoinAt).toBeGreaterThan(lostAt)
    expect(after.filter(({ kind }) => kind === 'PARTITIONS_REVOKED')).toEqual([])
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
    const after = run.log.filter(({ at }) => at >= run.fencedAt)
    const lostAt = after.findIndex(({ kind }) => kind === 'PARTITIONS_LOST')

    expect(after.filter(({ kind }) => kind === 'PARTITIONS_LOST')).toHaveLength(1)
    expect(
      after.findIndex(({ kind, epoch }) => kind === 'heartbeat' && epoch === 0)
    ).toBeGreaterThan(lostAt)
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
    'leaves, runs one LOST round and rejoins when the poll timer expires during the wait while a handler %s, and redelivers from the committed offset',
    async (_, busy) => {
      const run = await fenceWhileHandlerRuns({ busy, trigger: 'poll' })
      const after = run.log.filter(({ at }) => at >= run.fencedAt)
      const order = after
        .filter(({ kind, epoch }) => kind === 'PARTITIONS_LOST' || epoch === -1 || epoch === 0)
        .map(({ kind, epoch }) => (kind === 'heartbeat' ? epoch : kind))

      expect(order).toEqual([-1, 'PARTITIONS_LOST', 0])
      expect(after.find(({ kind }) => kind === 'PARTITIONS_LOST').partitions).toEqual([
        { topic: 'topic-a', partitions: [0] },
        { topic: 'topic-b', partitions: [0, 1] },
      ])
      expect(after.filter(({ kind }) => kind === 'PARTITIONS_REVOKED')).toEqual([])
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
    const order = after
      .filter(({ kind, epoch }) => kind.startsWith('PARTITIONS_') || epoch === 0)
      .map(({ kind, epoch }) => (kind === 'heartbeat' ? epoch : kind))

    expect(order).toEqual(['PARTITIONS_LOST', 0, 'PARTITIONS_ASSIGNED'])
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

  describe.each([
    ['a literal subscription', { topics: ['topic-a', 'topic-c', 'topic-d'] }],
    ['a broker-side regex', { regex: 'topic-.*' }],
  ])('with %s', (_, subscription) => {
    it('abandons a round whose target is replaced while it resolves topic ids and reconciles the newer target in the next round, without a crash', async () => {
      const built = buildConsumer()
      consumer = built.consumer
      const s = { phase: 'steady' }
      const named = { 'topic-a': [0], 'topic-c': [0], 'topic-d': [0] }
      const newer = { 'topic-a': [0, 1], 'topic-c': [0], 'topic-d': [0] }
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
          return request.memberEpoch === 0 ? fullTarget(1, { 'topic-a': [0] }) : fullTarget(1, null)
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
      expect(steps.mock.calls.length).toBeGreaterThanOrEqual(2)
      expect(
        Object.fromEntries(
          runner.consumerGroup.assigned().map(({ topic, partitions }) => [topic, partitions])
        )
      ).toEqual(newer)
      expect(runner.consumerGroup.groupMembership.memberEpoch).toEqual(3)
      expect(runner.consumerGroup.groupMembership.state).toEqual('STABLE')
      const fetchedSince = Date.now() - 1000
      expect(
        new Set(built.fetches.filter(({ at }) => at > fetchedSince).map(({ tp }) => tp))
      ).toEqual(new Set(['topic-a|0', 'topic-a|1', 'topic-c|0', 'topic-d|0']))
    })
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
    const rejoin = run.log.find(
      ({ kind, epoch, at }) => kind === 'heartbeat' && epoch === 0 && at >= run.fencedAt
    )
    const later = calls.filter(({ at }) => at > run.fencedAt)

    expect(later.map(({ name }) => name)).toEqual(['lost', 'assigned'])
    expect(later[0].payload.partitions).toEqual([
      { topic: 'topic-a', partitions: [0] },
      { topic: 'topic-b', partitions: [0, 1] },
    ])
    expect(later[0].at).toBeLessThanOrEqual(rejoin.at)
    expect(run.log.indexOf(rejoin)).toBeGreaterThan(
      run.log.findIndex(({ kind, at }) => kind === 'PARTITIONS_LOST' && at >= run.fencedAt)
    )
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
