/**
 * Untimed coverage control of the `consumeThroughput.js` corpus: produces the same corpus with the
 * same code on a fresh topic, proves from the broker's offsets that every partition holds data,
 * then consumes it once with that driver's consume loop while counting, apart from the driver's
 * own counters, the partitions whose handler received data. Not a measurement.
 *
 * Usage: node --expose-gc scripts/benchmarks/consumeCoverage.js --partitions 1000 [corpus flags]
 * The corpus flags are those of `consumeThroughput.js`, which reads them from this process.
 * Prints one JSON line `{ partitions, messages, populatedPartitions, partitionsWithData }`; exit 1
 * (with the reason on stderr) unless both counts equal the partitions.
 */

/**
 * Counts the partitions a consume loop delivered data from. It has the shape of the driver's
 * sizing observer (`collect`, `start`, `observe`, `finish`, `dispose`) and samples nothing.
 */
class PartitionCoverage {
  /**
   * @param {number} partitions the topic's partition count
   */
  constructor(partitions) {
    this.partitions = partitions
    this.seen = new Set()
  }

  /** @returns {Promise<void>} nothing to collect */
  async collect() {}

  /** @returns {void} */
  start() {}

  /**
   * @param {number} partition the partition of a nonempty delivery
   * @returns {void}
   * @throws {Error} for a partition the topic does not have
   */
  observe(partition) {
    if (!Number.isInteger(partition) || partition < 0 || partition >= this.partitions)
      throw new Error(`Unexpected partition ${partition}`)
    this.seen.add(partition)
  }

  /** @returns {Promise<{ partitionsWithData: number }>} the partitions seen */
  async finish() {
    return { partitionsWithData: this.seen.size }
  }

  /** @returns {void} */
  dispose() {}
}

/**
 * @param {{ partitions: number, populatedPartitions: number, partitionsWithData: number }} result
 * @returns {void}
 * @throws {Error} unless the broker holds data on, and the consumer received data from, every
 *   partition
 */
const verifyCoverage = ({ partitions, populatedPartitions, partitionsWithData }) => {
  if (populatedPartitions !== partitions)
    throw new Error(`The corpus populated ${populatedPartitions} of ${partitions} partitions`)
  if (partitionsWithData !== partitions)
    throw new Error(`Data reached ${partitionsWithData} of ${partitions} partitions`)
}

/**
 * Runs the control against the benchmark cluster and prints its line.
 * @returns {Promise<void>}
 * @throws {Error} a client failure or a coverage shortfall, after the topic is deleted
 */
const main = async () => {
  const i = process.argv.indexOf('--partitions')
  const partitions = i === -1 ? 6 : Number(process.argv[i + 1])
  const topic = `bench-consume-coverage-${Date.now()}`
  process.env.BENCH_TOPIC = topic
  const { Kafka, logLevel } = require('../../index')
  const { Clients } = require('../clients')
  const throughput = require('./consumeThroughput')
  const kafka = new Kafka({
    clientId: 'bench-consume',
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    logLevel: logLevel.NOTHING,
  })
  /**
   * Runs `work` with a connected admin client that is disconnected afterwards.
   * @param {(admin: object) => Promise<void>} work
   * @returns {Promise<void>}
   */
  const withAdmin = (work) =>
    Clients.use(kafka, async (clients) => {
      const admin = clients.admin()
      await admin.connect()
      await work(admin)
    })
  try {
    await throughput.createTopic(kafka)
    await throughput.produceCorpus(kafka)
    let populatedPartitions
    await withAdmin(async (admin) => {
      const offsets = await admin.fetchTopicOffsets(topic)
      populatedPartitions = offsets.filter(({ high, low }) => BigInt(high) > BigInt(low)).length
    })
    const coverage = new PartitionCoverage(partitions)
    const observed = await throughput.consumeOnce(kafka, 1, undefined, { sizing: coverage })
    const result = {
      partitions,
      messages: observed.deliveredMessages,
      populatedPartitions,
      partitionsWithData: observed.sizing.partitionsWithData,
    }
    verifyCoverage(result)
    console.log(JSON.stringify(result))
  } finally {
    await withAdmin((admin) => admin.deleteTopics({ topics: [topic] })).catch((error) =>
      console.error(`could not delete ${topic}: ${error.message}`)
    )
  }
}

module.exports = { PartitionCoverage, verifyCoverage }

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack)
    process.exitCode = 1
  })
}
