const Cluster = require('../../../cluster')
const createConsumer = require('../../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { RecordBatch } = require('../../../protocol/recordBatch/v0')
const RecordEncoder = require('../../../protocol/recordBatch/record/v0')
const { createScriptedCluster, groupScript } = require('../../../../testHelpers/scriptedCluster')
const { newLogger } = require('testHelpers')
const { TOPIC } = require('./fetchHarness')

/** Node 0 is the only bootstrap broker and the partition leader; node 1 is the group coordinator. */
const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9092 },
]
const GROUP = 'two-node-group'

/**
 * A classic consumer over a real `Cluster` and broker pool on a scripted two-node cluster, so the
 * coordinator's broker is a table broker that a cluster disconnect detaches and is not the seed.
 * Fetch answers one record at the offset it is asked for.
 * @param {object} options
 * @param {Function} options.eachMessage
 * @param {object} [options.retry] consumer retry options
 * @returns {Promise<{ scripted: object, cluster: Cluster, consumer: object, events: string[] }>}
 *   `events` orders DISCONNECT, CRASH and STOP
 */
const startTwoNode = async ({ eachMessage, retry }) => {
  const scripted = groupScript(createScriptedCluster({ nodes: NODES }), {
    batches: 0,
    coordinator: 1,
  }).answer('Fetch', async ({ topics }) => {
    const partitions = []
    for (const { partition, fetchOffset } of topics[0].partitions) {
      const batch = await RecordBatch({
        firstOffset: fetchOffset,
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
      })
      partitions.push({ partition, errorCode: 0, records: Buffer.from(batch.buffer) })
    }
    return { partitions }
  })
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const cluster = new Cluster({
    ...scripted.kafkaOptions({ brokers: ['node0:9092'] }),
    logger: newLogger(),
    retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 20 },
    instrumentationEmitter,
  })
  const consumer = createConsumer({
    cluster,
    groupId: GROUP,
    maxWaitTimeInMs: 1,
    logger: newLogger(),
    retry: { retries: 0, initialRetryTime: 10, ...retry },
    instrumentationEmitter,
  })
  const events = []
  for (const name of ['DISCONNECT', 'CRASH', 'STOP'])
    consumer.on(consumer.events[name], () => events.push(name))
  const joined = new Promise((resolve) => consumer.on(consumer.events.GROUP_JOIN, resolve))
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  await consumer.run({ eachMessage })
  await joined
  return { scripted, cluster, consumer, events }
}

module.exports = { startTwoNode, NODES, GROUP }
