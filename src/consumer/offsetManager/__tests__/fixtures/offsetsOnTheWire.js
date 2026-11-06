const OffsetManager = require('../../index')
const ResetObligations = require('../../resetObligations')
const InstrumentationEventEmitter = require('../../../../instrumentation/emitter')
const { createScriptedCluster } = require('../../../../../testHelpers/scriptedCluster')
const { createCluster, newLogger } = require('testHelpers')
const { TOPIC, GROUP_ID } = require('./scheduledOffsetManager')

const NODE = { id: 0, host: '127.0.0.1', port: 9092 }
const NO_TOPIC_ID = '00000000-0000-0000-0000-000000000000'
const LOGGED_APIS = ['OffsetFetch', 'OffsetCommit', 'ListOffsets']

/**
 * The fields of an OffsetFetch v9 answer for `offsets`, in the names of the Apache vector.
 * @param {Record<number, string>} offsets committed offset by partition; '-1' is "none"
 * @param {number} [leaderEpoch] the epoch each commit carried, -1 for none
 * @returns {object}
 */
const offsetFetchFields = (offsets, leaderEpoch = -1) => ({
  topics: [
    {
      topic: TOPIC,
      partitions: Object.entries(offsets).map(([partition, offset]) => ({
        partition: Number(partition),
        offset,
        leaderEpoch,
        metadata: null,
        errorCode: 0,
      })),
    },
  ],
})

/**
 * The fields of a ListOffsets v9 answer for `offsets`; this API names a topic `name` and a
 * partition `partitionIndex`.
 * @param {Record<number, string>} offsets log offset by partition
 * @returns {object}
 */
const listOffsetsFields = (offsets) => ({
  topics: [
    {
      name: TOPIC,
      partitions: Object.entries(offsets).map(([partitionIndex, offset]) => ({
        partitionIndex: Number(partitionIndex),
        errorCode: 0,
        timestamp: '-1',
        offset,
        leaderEpoch: 5,
      })),
    },
  ],
})

/**
 * The fields of an OffsetCommit v9 answer that accepts every partition the request named.
 * @param {{ topics: { topic: string, partitions: { partition: number }[] }[] }} request
 * @returns {object}
 */
const offsetCommitFields = ({ topics }) => ({
  topics: topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition }) => ({ partition, errorCode: 0 })),
  })),
})

/**
 * The same handles as `createScheduledOffsetManager`, but every OffsetFetch, ListOffsets and
 * OffsetCommit crosses the real `Cluster`, `BrokerPool`, `Broker`, `ConnectionPool`, `Connection`
 * and the codecs over the scripted cluster. A recreation is a Metadata answer that carries a new
 * topic id, so the cluster's own metadata cache raises the event the Consumer would listen to.
 *
 * The coordinator behaves like a broker: what it holds for a partition is the last OffsetCommit
 * the log shows for it (or `committed`), so it follows the order requests were *sent* in, never the
 * order answers were released in. A request nothing in the schedule holds is answered by the
 * scripted `answer` of its API; one released with no value is answered the same way.
 * Setup gets two minutes to tolerate coverage runner delays; Jest still bounds a hung test.
 *
 * @param {object} [options]
 * @param {number[]} [options.partitions] the assigned partitions of the one topic
 * @param {string} [options.topicId] the topic id the first Metadata answer carries, none by default
 * @param {string} [options.committed] what the coordinator holds before any commit, '-1' for none
 * @param {string} [options.listOffset] what ListOffsets answers for every partition
 * @param {boolean} [options.autoCommit] whether a seek commits
 * @returns {Promise<object>} the manager, `rebuild()`, the schedule, the real cluster, the shared
 *   cache, the ledger and obligations, `setTopicId(id)` and `recreate(id)` (both await a Metadata
 *   refresh), `next`, `committedAt`, `owed`, `requests()` (the decoded request log of the three
 *   offset APIs: `{ api, version, node, correlationId, fields }`), `answers` (field builders for a
 *   held answer) and `disconnect()`
 * @throws {Error} whatever connecting, the first metadata refresh or the coordinator lookup
 *   rejects with; the fixture does not catch them
 */
const createOffsetsOnTheWire = async ({
  partitions = [0],
  topicId,
  committed = '99',
  listOffset = '0',
  autoCommit = false,
} = {}) => {
  const scripted = createScriptedCluster({ nodes: [NODE] })
  const state = { topicId }
  const asked = (topics) => topics.flatMap(({ partitions: ps }) => ps)
  const heldBy = (partition) => {
    const commits = scripted
      .requests()
      .filter(({ api }) => api === 'OffsetCommit')
      .flatMap(({ fields }) => fields.topics.flatMap(({ partitions: ps }) => ps))
      .filter((entry) => entry.partition === partition)
    return commits.length > 0 ? commits[commits.length - 1].offset : committed
  }

  scripted
    .answer('Metadata', () => ({
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: state.topicId || NO_TOPIC_ID,
          isInternal: false,
          partitions: partitions.map((partitionIndex) => ({
            errorCode: 0,
            partitionIndex,
            leaderId: NODE.id,
            leaderEpoch: 5,
            replicas: [NODE.id],
            isr: [NODE.id],
            offlineReplicas: [],
          })),
          topicAuthorizedOperations: -2147483648,
        },
      ],
    }))
    .answer('OffsetFetch', ({ topics }) =>
      offsetFetchFields(
        Object.fromEntries(asked(topics).map(({ partition }) => [partition, heldBy(partition)]))
      )
    )
    .answer('ListOffsets', ({ topics }) =>
      listOffsetsFields(
        Object.fromEntries(asked(topics).map(({ partitionIndex }) => [partitionIndex, listOffset]))
      )
    )
    .answer('OffsetCommit', offsetCommitFields)

  const instrumentationEmitter = new InstrumentationEventEmitter()
  const cluster = createCluster(
    {
      socketFactory: scripted.socketFactory,
      connectionTimeout: 120000,
      requestTimeout: 5000,
      instrumentationEmitter,
    },
    [`${NODE.host}:${NODE.port}`]
  )
  await cluster.connect()
  await cluster.addTargetTopic(TOPIC)
  const coordinator = await cluster.findGroupCoordinator({ groupId: GROUP_ID })

  const operations = cluster.offsetOperations({ groupId: GROUP_ID })
  const obligations = new ResetObligations()
  obligations.attachTo(operations)

  const options = {
    cluster,
    coordinator,
    memberAssignment: { [TOPIC]: partitions },
    autoCommit,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { [TOPIC]: { fromBeginning: true } },
    instrumentationEmitter,
    groupId: GROUP_ID,
    generationId: 1,
    memberId: 'member-a',
    operations,
    obligations,
    logger: newLogger(),
  }
  const manager = new OffsetManager(options)

  cluster.onTopicRecreated(({ payload: { topic, topicId: recreatedId } }) => {
    obligations.owe(topic, recreatedId)
    operations.touchTopic(topic)
    manager.resetOffsetsForTopic(topic)
  })

  return {
    manager,
    rebuild: () => new OffsetManager(options),
    schedule: scripted.schedule,
    scripted,
    cluster,
    cache: cluster.committedOffsets({ groupId: GROUP_ID }),
    operations,
    obligations,
    answers: { offsetFetch: offsetFetchFields, listOffsets: listOffsetsFields },
    setTopicId: async (id) => {
      state.topicId = id
      await cluster.refreshMetadata()
    },
    recreate: async (id) => {
      state.topicId = id
      await cluster.refreshMetadata()
    },
    next: (partition = 0) => manager.nextOffset(TOPIC, partition).toString(),
    committedAt: (partition = 0) =>
      cluster.committedOffsets({ groupId: GROUP_ID })[TOPIC][partition],
    /**
     * Reads one member's effective baseline independently of the shared cache.
     * @param {number} [partition=0] partition id
     * @param {OffsetManager} [member=manager] member holding the position
     * @returns {string|undefined} held offset
     */
    heldAt: (partition = 0, member = manager) => member.heldOffsetAt(TOPIC, partition),
    owed: (partition = 0) => obligations.isOwed(TOPIC, partition),
    requests: () =>
      scripted
        .requests()
        .filter(({ api }) => LOGGED_APIS.includes(api))
        .map(({ api, version, node, correlationId, fields }) => ({
          api,
          version,
          node,
          correlationId,
          fields,
        })),
    disconnect: () => cluster.disconnect(),
  }
}

module.exports = { createOffsetsOnTheWire, offsetFetchFields, listOffsetsFields }
