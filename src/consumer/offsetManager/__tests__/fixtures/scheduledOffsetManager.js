const Cluster = require('../../../../cluster')
const OffsetManager = require('../../index')
const ConsumerGroup = require('../../../consumerGroup')
const OffsetOperations = require('../../offsetOperations')
const ResetObligations = require('../../resetObligations')
const InstrumentationEventEmitter = require('../../../../instrumentation/emitter')
const { Schedule } = require('../../../../../testHelpers/schedule')
const { newLogger } = require('testHelpers')

const TOPIC = 'test-topic'
const OLD_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const NEW_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const GROUP_ID = 'test-group'

/**
 * The decoded OffsetFetch answer of a coordinator that holds `offsets`.
 * @param {Record<number, string>} offsets committed offset by partition; '-1' is "none"
 * @param {number} [committedLeaderEpoch] the epoch each commit carried, -1 for none
 * @returns {object}
 */
const offsetFetchAnswer = (offsets, committedLeaderEpoch = -1) => ({
  responses: [
    {
      topic: TOPIC,
      partitions: Object.entries(offsets).map(([partition, offset]) => ({
        partition: Number(partition),
        offset,
        committedLeaderEpoch,
        metadata: null,
        errorCode: 0,
      })),
    },
  ],
})

/**
 * The decoded ListOffsets answer for `offsets`.
 * @param {Record<number, string>} offsets log offset by partition
 * @returns {object[]}
 */
const listOffsetsAnswer = (offsets) => [
  {
    topic: TOPIC,
    partitions: Object.entries(offsets).map(([partition, offset]) => ({
      partition: Number(partition),
      offset,
    })),
  },
]

/**
 * A real `OffsetManager` over a stub cluster and coordinator whose OffsetFetch, ListOffsets and
 * OffsetCommit are holdable through a `Schedule`. The coordinator behaves like a broker: a commit
 * is stored when it arrives, and OffsetFetch answers what is stored.
 *
 * @param {object} [options]
 * @param {number[]} [options.partitions] the assigned partitions of the one topic
 * @param {string} [options.topicId] the topic id the metadata cache holds, none by default
 * @param {string} [options.committed] what the coordinator holds for every partition, '-1' for none
 * @param {string} [options.listOffset] what ListOffsets answers for every partition
 * @param {boolean} [options.autoCommit] whether a seek commits
 * @param {OffsetOperations} [options.operations] the group's ledger
 * @param {ResetObligations} [options.obligations] the consumer's reset obligations
 * @returns {object} the manager, `rebuild()` for a manager built as a rebalance would, the
 *   schedule, the stubs, the shared cache, the ledger and obligations, and helpers that read the
 *   state a case asserts: `next` (the offset the next fetch starts from), `committedAt` (the
 *   committed cache), `owed` (the reset obligation)
 */
const createScheduledOffsetManager = ({
  partitions = [0],
  topicId,
  committed = '99',
  listOffset = '0',
  autoCommit = false,
  operations = new OffsetOperations(),
  obligations = new ResetObligations(),
} = {}) => {
  obligations.attachTo(operations)
  const schedule = new Schedule()
  const state = { topicId }
  const stored = Object.fromEntries(partitions.map((partition) => [partition, committed]))
  const cache = {}

  const cluster = {
    committedOffsets: () => cache,
    topicId: () => state.topicId,
    fetchTopicsOffset: jest.fn((topics) =>
      schedule.run({ api: 'ListOffsets', topics }, () =>
        listOffsetsAnswer(
          Object.fromEntries(
            topics.flatMap(({ partitions: asked }) =>
              asked.map(({ partition }) => [partition, listOffset])
            )
          )
        )
      )
    ),
  }

  const coordinator = {
    isConnected: () => true,
    offsetFetch: jest.fn(({ topics }) =>
      schedule.run({ api: 'OffsetFetch', topics }, () =>
        offsetFetchAnswer(
          Object.fromEntries(
            topics.flatMap(({ partitions: asked }) =>
              asked.map(({ partition }) => [partition, stored[partition]])
            )
          )
        )
      )
    ),
    offsetCommit: jest.fn(({ topics }) => {
      topics.forEach(({ partitions: sent }) =>
        sent.forEach(({ partition, offset }) => {
          stored[partition] = offset
        })
      )
      return schedule.run({ api: 'OffsetCommit', topics }, () => ({}))
    }),
  }

  const options = {
    cluster,
    coordinator,
    memberAssignment: { [TOPIC]: partitions },
    autoCommit,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { [TOPIC]: { fromBeginning: true } },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: GROUP_ID,
    generationId: 1,
    memberId: 'member-a',
    operations,
    obligations,
    logger: newLogger(),
  }
  const manager = new OffsetManager(options)

  return {
    manager,
    /** A manager built as a rebalance builds one, optionally for another consumer of the group. */
    build: (overrides) => new OffsetManager({ ...options, ...overrides }),
    rebuild: () => new OffsetManager(options),
    schedule,
    cluster,
    coordinator,
    cache,
    stored,
    /**
     * Publishes through the real cluster cache writer, including revision invalidation.
     * @param {number|string} partition partition id
     * @param {string} offset confirmed committed offset
     * @returns {void}
     */
    publish: (partition, offset) =>
      Cluster.prototype.markOffsetAsCommitted.call(cluster, {
        groupId: GROUP_ID,
        topic: TOPIC,
        partition,
        offset,
      }),
    operations,
    obligations,
    setTopicId: (id) => {
      state.topicId = id
    },
    /** What the Consumer's listener and `ConsumerGroup#invalidateTopic` do for a recreation. */
    recreate: (id) => {
      state.topicId = id
      obligations.owe(TOPIC, id)
      operations.touchTopic(TOPIC)
      manager.resetOffsetsForTopic(TOPIC)
    },
    next: (partition = 0) => manager.nextOffset(TOPIC, partition).toString(),
    committedAt: (partition = 0) => cache[TOPIC][partition],
    /**
     * Reads one member's effective baseline independently of the shared cache.
     * @param {number} [partition=0] partition id
     * @param {OffsetManager} [member=manager] member holding the position
     * @returns {string|undefined} held offset
     */
    heldAt: (partition = 0, member = manager) => member.heldOffsetAt(TOPIC, partition),
    owed: (partition = 0) => obligations.isOwed(TOPIC, partition),
  }
}

/**
 * A real `ConsumerGroup` over a fixture's manager, ledger and obligations, with the assignment
 * already applied to its subscription state.
 * @param {object} ctx a fixture from `createScheduledOffsetManager` or `createOffsetsOnTheWire`
 * @param {{topic: string, partitions: number[]}[]} [assignment]
 * @returns {ConsumerGroup}
 */
const groupOver = (ctx, assignment = [{ topic: TOPIC, partitions: [0] }]) => {
  const group = new ConsumerGroup({
    retry: { retries: 0 },
    cluster: ctx.cluster,
    groupId: GROUP_ID,
    topics: [TOPIC],
    topicConfigurations: { [TOPIC]: { fromBeginning: true } },
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
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
    operations: ctx.operations,
    obligations: ctx.obligations,
  })
  group.subscriptionState.assign(assignment)
  group.offsetManager = ctx.manager

  return group
}
module.exports = {
  TOPIC,
  OLD_ID,
  NEW_ID,
  GROUP_ID,
  offsetFetchAnswer,
  listOffsetsAnswer,
  createScheduledOffsetManager,
  groupOver,
}
