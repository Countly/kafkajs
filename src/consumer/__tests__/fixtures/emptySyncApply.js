const { Kafka, logLevel } = require('../../../../index')
const { createScriptedCluster, groupScript } = require('../../../../testHelpers/scriptedCluster')
const { TOPIC } = require('./fetchHarness')

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
 * Plays a SyncGroup answer through the real classic consumer, over scripted sockets, and reports
 * what the group applied. The assigned callback is held until the test lets it go, so what the
 * client does while that awaited callback is pending is observable.
 *
 * @param {string} memberAssignmentHex the bytes the coordinator answers SyncGroup with
 * @returns {Promise<{ joins: object[], crashes: object[], assigned: object[][], fetchesWhileHeld: number,
 *   fetchesAfter: number, heartbeats: number }>} the observations; the consumer is disconnected and the
 *   scripted cluster checked clean before it returns
 * @throws {Error} when no join is applied in time, or a scripted request was refused
 */
const applySyncAnswer = async (memberAssignmentHex) => {
  const cluster = groupScript(
    createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
    {
      batches: 0,
    }
  ).answer('SyncGroup', () => ({ memberAssignment: memberAssignmentHex }))
  const consumer = new Kafka(cluster.kafkaOptions({ logLevel: logLevel.NOTHING })).consumer({
    groupId: 'empty-assignment-group',
    heartbeatInterval: 50,
    maxWaitTimeInMs: 20,
    retry: { retries: 0 },
  })
  const joins = []
  const crashes = []
  const assigned = []
  let release
  const held = new Promise((resolve) => {
    release = resolve
  })
  const remove = [
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload)),
    consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload)),
  ]
  const count = (api) => cluster.requests().filter((request) => request.api === api).length
  try {
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC })
    const running = consumer.run({
      eachMessage: async () => {},
      rebalanceListener: {
        onPartitionsAssigned: async ({ partitions }) => {
          assigned.push(partitions)
          await held
        },
      },
    })
    await until(() => assigned.length === 1, 'the assigned callback')
    await pause(100)
    const fetchesWhileHeld = count('Fetch')
    release()
    await running
    await until(() => joins.length === 1, 'the group join event')
    const heartbeatsBefore = count('Heartbeat')
    await until(() => count('Heartbeat') > heartbeatsBefore, 'a heartbeat after the apply')
    return {
      joins,
      crashes,
      assigned,
      fetchesWhileHeld,
      fetchesAfter: count('Fetch'),
      heartbeats: count('Heartbeat'),
    }
  } finally {
    release()
    for (const off of remove) off()
    await consumer.disconnect()
    cluster.assertClean()
  }
}

module.exports = { applySyncAnswer }
