const OffsetManager = require('../../index')
const InstrumentationEventEmitter = require('../../../../instrumentation/emitter')
const { Schedule } = require('testHelpers')

const TOPIC = 'test-topic'

/**
 * @param {string} topic
 * @param {number} partition
 * @returns {string} the key a partition has in the broker's committed offsets
 */
const keyOf = (topic, partition) => `${topic}:${partition}`

/**
 * What a case reads of an offset manager's state, each read apart from the others.
 * @typedef {object} OffsetReads
 * @property {(topic: string, partition: number) => string} position where the next fetch of the
 *   partition starts, as a decimal string
 * @property {(topic: string) => boolean} resetOwed whether the topic's recreation still owes its
 *   reset
 */

/**
 * The answers of a coordinator and a cluster whose OffsetFetch, OffsetCommit and ListOffsets calls
 * go through a `Schedule`, so a case can hold any of them and release it when it chooses.
 *
 * The coordinator keeps one committed offset per partition, which an OffsetCommit changes and an
 * OffsetFetch reads (`-1` when none): enough for a case to see what a later resolution reads back,
 * and nothing more than that.
 * @param {Schedule} schedule
 * @param {object} [options]
 * @param {string} [options.committed='99'] what the coordinator holds for a partition nobody
 *   committed to
 * @param {string} [options.earliest='0'] what ListOffsets answers for every partition
 * @returns {{ offsetFetch: Function, offsetCommit: Function, fetchTopicsOffset: Function,
 *   brokerOffsets: Map<string, string> }}
 */
const createOffsetStubs = (schedule, { committed = '99', earliest = '0' } = {}) => {
  const brokerOffsets = new Map()

  return {
    brokerOffsets,
    offsetFetch: (request) =>
      schedule.run({ api: 'OffsetFetch', request }, () => ({
        responses: request.topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            offset: brokerOffsets.get(keyOf(topic, partition)) ?? committed,
            committedLeaderEpoch: -1,
            metadata: null,
            errorCode: 0,
          })),
        })),
      })),
    offsetCommit: (payload) =>
      schedule.run({ api: 'OffsetCommit', payload }, () => {
        for (const { topic, partitions } of payload.topics) {
          for (const { partition, offset } of partitions) {
            brokerOffsets.set(keyOf(topic, partition), offset)
          }
        }

        return { topics: [] }
      }),
    fetchTopicsOffset: (topics) =>
      schedule.run({ api: 'ListOffsets', topics }, () =>
        topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: earliest })),
        }))
      ),
  }
}

/**
 * The answer an OffsetFetch gives, for releasing a held one by hand.
 * @param {Record<number, string>} offsets committed offset by partition (`'-1'` for none)
 * @param {string} [topic]
 * @returns {object}
 */
const offsetFetchAnswer = (offsets, topic = TOPIC) => ({
  responses: [
    {
      topic,
      partitions: Object.entries(offsets).map(([partition, offset]) => ({
        partition: Number(partition),
        offset,
        committedLeaderEpoch: -1,
        metadata: null,
        errorCode: 0,
      })),
    },
  ],
})

/**
 * The answer a ListOffsets gives, for releasing a held one by hand.
 * @param {Record<number, string>} offsets offset by partition
 * @param {string} [topic]
 * @returns {object[]}
 */
const listOffsetsAnswer = (offsets, topic = TOPIC) => [
  {
    topic,
    partitions: Object.entries(offsets).map(([partition, offset]) => ({
      partition: Number(partition),
      offset,
    })),
  },
]

/**
 * A real `OffsetManager` over a stub cluster and coordinator whose calls a `Schedule` can hold,
 * with the state a recreation leaves behind exposed through four reads: `position` (where the next
 * fetch starts), `committed` (the cached committed offset), `resetOwed` (a reset the topic still
 * owes) and `retainedEntries` (what the consumer keeps to tell a recreation apart).
 * @param {object} [options]
 * @param {string[]} [options.topics] assigned topics, each with `partitions` partitions
 * @param {number} [options.partitions=1]
 * @param {boolean} [options.autoCommit=false]
 * @param {string} [options.committed='99'] see `createOffsetStubs`
 * @param {string} [options.earliest='0'] see `createOffsetStubs`
 * @returns {OffsetReads & { manager: OffsetManager, coordinator: object, schedule: Schedule,
 *   brokerOffsets: Map<string, string>,
 *   recreate: (topic: string, newId: string) => void,
 *   committed: (topic: string, partition: number) => string|undefined,
 *   retainedEntries: () => number }} `committed` is the cached committed offset, absent until the
 *   group resolves or commits one; `retainedEntries` counts the recreations the consumer still
 *   remembers; `recreate` deletes the topic and creates it again under `newId`
 */
const createScheduledManager = ({
  topics = [TOPIC],
  partitions = 1,
  autoCommit = false,
  committed = '99',
  earliest = '0',
} = {}) => {
  const schedule = new Schedule()
  const stubs = createOffsetStubs(schedule, { committed, earliest })
  const ids = new Map(topics.map((topic) => [topic, 'id-0']))
  const pendingTopicResets = new Map()
  const topicRecreations = new Map()
  const store = {}
  const coordinator = {
    isConnected: () => true,
    offsetFetch: stubs.offsetFetch,
    offsetCommit: stubs.offsetCommit,
  }
  const cluster = {
    committedOffsets: () => store,
    topicId: (topic) => ids.get(topic),
    fetchTopicsOffset: stubs.fetchTopicsOffset,
    refreshMetadata: async () => {},
    findGroupCoordinator: async () => coordinator,
  }
  const assigned = Array.from({ length: partitions }, (_, partition) => partition)

  const manager = new OffsetManager({
    cluster,
    coordinator,
    memberAssignment: Object.fromEntries(topics.map((topic) => [topic, assigned])),
    autoCommit,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: Object.fromEntries(
      topics.map((topic) => [topic, { fromBeginning: true }])
    ),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 1,
    memberId: 'member-1',
    pendingTopicResets,
    topicRecreations,
  })

  return {
    manager,
    coordinator,
    schedule,
    brokerOffsets: stubs.brokerOffsets,
    /** The topic is deleted and created again under `newId`, as the consumer records it. */
    recreate: (topic, newId) => {
      ids.set(topic, newId)
      pendingTopicResets.set(topic, newId)
      topicRecreations.set(topic, (topicRecreations.get(topic) || 0) + 1)
    },
    position: (topic, partition) => manager.nextOffset(topic, partition).toString(),
    committed: (topic, partition) => store[topic]?.[partition],
    resetOwed: (topic) => pendingTopicResets.has(topic),
    retainedEntries: () => topicRecreations.size,
  }
}

/**
 * Puts a `Schedule` in front of the calls a consumer group built by `createFetchGroup` makes to
 * the coordinator and the cluster for offsets, and empties the committed offsets the harness primed.
 * @param {object} fetchGroup what `createFetchGroup` resolved with
 * @param {object} [options] see `createOffsetStubs`
 * @returns {OffsetReads & { schedule: Schedule, brokerOffsets: Map<string, string> }}
 */
const scheduleOffsets = (fetchGroup, options) => {
  const schedule = new Schedule()
  const stubs = createOffsetStubs(schedule, options)
  const { broker, cluster, offsetManager, offsetCommit } = fetchGroup

  // A group that has resolved nothing holds no committed offset at all (an absent cell is what the
  // fetch loop leaves out), not the primed one `createFetchGroup` starts from.
  for (const [topic, partitions] of Object.entries(offsetManager.memberAssignment)) {
    for (const partition of partitions) {
      delete offsetManager.committedOffsets()[topic][partition]
    }
  }

  jest.spyOn(broker, 'offsetFetch').mockImplementation(stubs.offsetFetch)
  offsetCommit.mockImplementation(stubs.offsetCommit)
  cluster.fetchTopicsOffset = stubs.fetchTopicsOffset

  return {
    schedule,
    brokerOffsets: stubs.brokerOffsets,
    resetOwed: (topic) => offsetManager.pendingTopicResets.has(topic),
    position: (topic, partition) => offsetManager.nextOffset(topic, partition).toString(),
  }
}

module.exports = {
  TOPIC,
  createScheduledManager,
  scheduleOffsets,
  offsetFetchAnswer,
  listOffsetsAnswer,
}
