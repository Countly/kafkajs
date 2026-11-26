const { Kafka, logLevel } = require('../../../../index')
const { MemberMetadata } = require('../../assignerProtocol')
const encodeRows = require('./encodeRows')
const { createScriptedCluster, groupScript } = require('../../../../testHelpers/scriptedCluster')
const { TOPIC } = require('./fetchHarness')

const GROUP_ID = 'repeated-assignment-group'
const ASSIGNER_NAME = 'RowsAssigner'

/** @param {number} ms @returns {Promise<void>} resolves after the real delay */
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * @param {() => boolean} predicate
 * @param {string} label what is awaited, for the failure
 * @returns {Promise<void>} when the predicate holds
 * @throws {Error} when it does not hold within ten seconds
 */
const until = async (predicate, label) => {
  const deadline = Date.now() + 10000
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await pause(5)
  }
}

/**
 * A custom assigner that gives every member the same rows, whatever the group looks like.
 * @param {Array<{ topic: string, partitions: number[] }>} rows
 * @returns {import('types').PartitionAssigner}
 */
const rowsAssigner = (rows) => () => ({
  name: ASSIGNER_NAME,
  version: 0,
  async assign({ members }) {
    return members.map(({ memberId }) => ({ memberId, memberAssignment: encodeRows(rows) }))
  },
  protocol({ topics }) {
    return { name: ASSIGNER_NAME, metadata: MemberMetadata.encode({ version: 0, topics }) }
  },
})

/**
 * Runs the real classic consumer, leader of a group of one, over scripted sockets, with a custom
 * assigner that uploads `rows`, and reports what the group applied and sent.
 *
 * @param {object} options
 * @param {Array<{ topic: string, partitions: number[] }>} options.rows what the assigner uploads
 * @param {boolean} options.fetchSessions whether the consumer uses incremental Fetch sessions
 * @param {number} [options.partitions=3] how many partitions the topic has
 * @returns {Promise<{ joins: object[], crashes: object[], assigned: object[][], warnings: object[],
 *   fetches: number[][], offsetFetches: number[][] }>} the observations; `fetches` and
 *   `offsetFetches` list the partitions each request carried, in the order it carried them. The
 *   consumer is disconnected and the scripted cluster checked clean before it returns
 * @throws {Error} when neither a fetch nor a crash follows the join in time
 */
const applyRows = async ({ rows, fetchSessions, partitions = 3 }) => {
  const cluster = groupScript(
    createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
    { partitions, batches: 0 }
  ).answer('SyncGroup', ({ groupAssignment }) => ({
    memberAssignment: groupAssignment[0].memberAssignment,
  }))
  const lines = []
  const consumer = new Kafka(
    cluster.kafkaOptions({
      logLevel: logLevel.WARN,
      logCreator:
        () =>
        ({ label, log }) =>
          lines.push({ label, ...log }),
    })
  ).consumer({
    groupId: GROUP_ID,
    heartbeatInterval: 50,
    maxWaitTimeInMs: 20,
    retry: { retries: 0 },
    fetchSessions,
    partitionAssigners: [rowsAssigner(rows)],
  })
  const joins = []
  const crashes = []
  const assigned = []
  const remove = [
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload)),
    consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload)),
  ]
  const carried = (api) =>
    cluster
      .requests()
      .filter((request) => request.api === api)
      .map(({ fields }) =>
        (fields.topics ?? []).flatMap((entry) => entry.partitions.map(({ partition }) => partition))
      )
  try {
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC })
    consumer
      .run({
        eachMessage: async () => {},
        rebalanceListener: {
          onPartitionsAssigned: async ({ partitions: taken }) => {
            assigned.push(taken)
          },
        },
      })
      .catch((error) => crashes.push({ error }))
    await until(
      () => crashes.length > 0 || (joins.length === 1 && carried('Fetch').length > 0),
      'a fetch after the join'
    )
    return {
      joins,
      crashes,
      assigned,
      warnings: lines.filter(({ label }) => label === 'WARN'),
      fetches: carried('Fetch'),
      offsetFetches: carried('OffsetFetch'),
    }
  } finally {
    for (const off of remove) off()
    await consumer.disconnect()
    cluster.assertClean()
  }
}

module.exports = { applyRows, ASSIGNER_NAME, GROUP_ID, TOPIC }
