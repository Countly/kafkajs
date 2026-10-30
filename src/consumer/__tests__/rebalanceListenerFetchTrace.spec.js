const { Kafka, logLevel } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('./fixtures/fetchHarness')
const { traceFetches } = require('./fixtures/fetchTrace')

const NODE = { id: 0, host: 'node-0', port: 9092 }
const BUDGET_MS = 10000

/** @param {() => boolean} predicate @returns {Promise<void>} resolves when it holds */
const until = async (predicate) => {
  const deadline = Date.now() + BUDGET_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('No fetch was recorded in time')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

describe('Consumer > the live listener spec fetch trace', () => {
  let spy
  let consumer

  afterEach(async () => {
    spy?.mockRestore()
    await consumer?.disconnect()
  })

  test('names the member that sent each Fetch, in the order the requests were sent', async () => {
    const cluster = groupScript(createScriptedCluster({ nodes: [NODE] }), {
      partitions: 3,
    }).answer('Fetch', async ({ maxWaitMs, topics }) => {
      await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      return {
        partitions: topics[0].partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
          records: false,
          abortedTransactions: [],
        })),
      }
    })
    const clientId = 'e2e-p9c-listener-abc123-member-a'
    const kafka = new Kafka(
      cluster.kafkaOptions({ clientId, logLevel: logLevel.NOTHING, retry: { retries: 0 } })
    )
    const entries = []
    let clock = 0
    spy = traceFetches({ topic: TOPIC, sink: entries, tick: () => ++clock })

    consumer = kafka.consumer({ maxWaitTimeInMs: 20 })
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0, 2] }], { offsetReset: 'earliest' })
    await consumer.run({ eachMessage: async () => {} })
    await until(() => entries.length >= 2)

    expect(entries.every((entry) => entry.clientId === clientId)).toBe(true)
    expect(new Set(entries.map(({ partition }) => partition))).toEqual(new Set([0, 2]))
    expect(entries.map(({ ordinal }) => ordinal)).toEqual(entries.map((_, index) => index + 1))
  })
})
