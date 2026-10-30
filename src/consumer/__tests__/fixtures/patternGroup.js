const ConsumerGroup = require('../../consumerGroup')
const Subscription = require('../../subscription')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { newLogger } = require('testHelpers')

const NO_ID = '00000000-0000-0000-0000-000000000000'
const PARTITIONS = [0, 1]

/**
 * A real `ConsumerGroup` that holds a client-side pattern, over a stub cluster whose topic catalog
 * the test edits. A refresh answers with the catalog through the same `onMetadata` and
 * `isCurrent` contract the broker pool applies, so the group's own observer, pin and apply run.
 * The membership's `rebalance` is a stub that assigns every partition of the pinned names, and can
 * be held by a gate to stand in for the old owner's final work.
 *
 * @param {object} [options]
 * @param {Array<string|RegExp>} [options.declarations] names and patterns, one declaration call
 * @param {{kind: string, durationMs?: number}} [options.policy] the call's reset policy
 * @param {Record<string, string>} [options.catalog] topic name to topic id, two partitions each
 * @param {string} [options.committed] what the coordinator holds for every partition before a commit
 * @param {object} [options.group] overrides for the `ConsumerGroup` options
 * @returns {object} `group`, `subscription`, `cluster`, `catalog` (a Map), `set(name, id)`,
 *   `drop(name)`, `metadata()`, `refresh()` (one discovery refresh), `round()` (a full
 *   begin/join/apply cycle), `gate` (hold the next membership rebalance), `rebalances` (the
 *   `topicsSubscribed` of every join) and `applied()` (the group's applied names)
 */
const createPatternGroup = ({
  declarations = [/^orders-/],
  policy = { kind: 'earliest' },
  catalog: initial = {},
  committed: initialCommit = '99',
  group: groupOptions = {},
} = {}) => {
  const catalog = new Map(Object.entries(initial))
  const subscription = new Subscription()
  subscription.add({ topics: declarations, policy })

  const metadata = () => ({
    brokers: [],
    topicMetadata: [...catalog].map(([topic, topicId]) => ({
      topic,
      topicId,
      topicErrorCode: 0,
      isInternal: false,
      partitionMetadata: PARTITIONS.map((partitionId) => ({ partitionId, leader: 0 })),
    })),
  })
  const refreshes = []
  const refresh = async (options = {}) => {
    refreshes.push(options)
    if (options.isCurrent?.() !== false) options.onMetadata?.(metadata())
  }
  const committed = {}
  const cluster = {
    connect: jest.fn(async () => {}),
    refreshMetadata: jest.fn(refresh),
    refreshMetadataIfNecessary: jest.fn(refresh),
    committedOffsets: () => committed,
    findTopicPartitionMetadata: (topic) =>
      catalog.has(topic) ? PARTITIONS.map((partitionId) => ({ partitionId, leader: 0 })) : [],
    topicId: (topic) => (catalog.get(topic) === NO_ID ? undefined : catalog.get(topic)),
    findGroupCoordinator: jest.fn(async () => ({ isConnected: () => true })),
    addMultipleTargetTopics: jest.fn(async () => {}),
    leaderEpoch: () => -1,
    defaultOffset: ({ fromBeginning }) => (fromBeginning ? '-2' : '-1'),
    fetchTopicsOffset: jest.fn(async (topics) =>
      topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
      }))
    ),
  }

  const names = Subscription.configurationsOf(subscription.desired)
  const group = new ConsumerGroup({
    retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 2 },
    cluster,
    groupId: 'group-a',
    topics: [...subscription.desired.names],
    topicConfigurations: names,
    subscription,
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
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
    ...groupOptions,
  })
  group.memberId = 'member-1'
  group.generationId = 1
  // A coordinator that behaves like a broker: it holds the last commit of each partition.
  const stored = new Map()
  const heldAt = (topic, partition) => stored.get(`${topic}|${partition}`) ?? initialCommit
  group.coordinator = {
    isConnected: () => true,
    nodeId: 0,
    offsetFetch: jest.fn(async ({ topics }) => ({
      responses: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          offset: heldAt(topic, partition),
          committedLeaderEpoch: -1,
          metadata: null,
          errorCode: 0,
        })),
      })),
    })),
    offsetCommit: jest.fn(async ({ topics }) => {
      for (const { topic, partitions } of topics) {
        for (const { partition, offset } of partitions) stored.set(`${topic}|${partition}`, offset)
      }
      return {}
    }),
  }

  const rebalances = []
  let held = null
  group.groupMembership.rebalance = jest.fn(async ({ topicsSubscribed, beforeSync }) => {
    rebalances.push([...topicsSubscribed])
    if (held) await held.promise
    beforeSync()
    return {
      assignment: Object.fromEntries(
        topicsSubscribed.filter((topic) => catalog.has(topic)).map((topic) => [topic, PARTITIONS])
      ),
      userData: null,
    }
  })

  return {
    group,
    subscription,
    cluster,
    catalog,
    rebalances,
    refreshes,
    /** @returns {string} what the coordinator holds for a partition */
    brokerHolds: heldAt,
    metadata,
    /** One discovery refresh, as the group's own connect makes it. */
    refresh: () => group.connect(),
    set: (name, id) => catalog.set(name, id),
    drop: (name) => catalog.delete(name),
    /** Holds the next membership rebalance until `release()`. @returns {{release: Function}} */
    gate: () => {
      let release
      held = { promise: new Promise((resolve) => (release = resolve)) }
      return {
        release: () => {
          held = null
          release()
        },
      }
    },
    /**
     * One whole round: begin, join, apply.
     * @param {{rebuildOffsets?: boolean}} [options]
     * @returns {Promise<object>} the apply's result
     */
    round: async ({ rebuildOffsets = true } = {}) => {
      const round = group.beginRebalanceRound()
      try {
        const result = await group.joinAndSync(round)
        return await group.applyRebalanceResult(result, { rebuildOffsets, round })
      } finally {
        round.retire()
      }
    },
    applied: () => [...group.topicsSubscribed].sort(),
  }
}

module.exports = { createPatternGroup, NO_ID }
