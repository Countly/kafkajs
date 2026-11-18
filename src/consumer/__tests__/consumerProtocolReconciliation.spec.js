const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
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
const TOPIC_IDS = {
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
 * @returns {object} the group, runner, records and helpers to discover and to trip
 */
const buildPattern = (script) => {
  const log = []
  const emitted = []
  const fetched = []
  const known = new Map([
    [CONTROL, [0, 1]],
    [DELETED, [0]],
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
