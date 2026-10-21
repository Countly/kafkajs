const Broker = require('../src/broker')
const ConnectionPool = require('../src/network/connectionPool')
const defaultSocketFactory = require('../src/network/socketFactory')
const ISOLATION_LEVEL = require('../src/protocol/isolationLevel')
const { createLogger, LEVELS } = require('../src/loggers')
const LoggerConsole = require('../src/loggers/console')

/** The control record type of an ABORT marker, in the key's second int16. */
const ABORT = 0
/** The bytes one Fetch may return for a partition: 1 MiB. */
const PARTITION_MAX_BYTES = 1048576

/**
 * One broker at one address, with its own connection: no client, no retry, no metadata cache.
 * @param {{ host: string, port: number }} address
 * @returns {Promise<Broker>} connected
 */
const openBroker = async ({ host, port }) => {
  const logger = createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole })
  const broker = new Broker({
    connectionPool: new ConnectionPool({
      host,
      port,
      logger,
      socketFactory: defaultSocketFactory(),
      connectionTimeout: 3000,
      requestTimeout: 30000,
    }),
    logger,
  })
  await broker.connect()
  return broker
}

/**
 * The offset after the last one a record's batch holds.
 * @param {{ batchContext: { firstOffset: string, lastOffsetDelta: number } }} record
 * @returns {bigint}
 */
const batchEnd = ({ batchContext }) =>
  BigInt(batchContext.firstOffset) + BigInt(batchContext.lastOffsetDelta) + 1n

/**
 * Reads one partition from offset 0 to `end` with `Broker#fetch`. Records the broker returns from
 * before the position (the start of a batch the position falls in) or from `end` on are left out.
 * @param {Broker} broker the partition's leader
 * @param {object} read
 * @returns {Promise<{ records: object[], abortedTransactions: object[] }>}
 * @throws {Error} when `maxIdleFetches` fetches in a row make no progress
 */
const readPartition = async (
  broker,
  { topic, partition, end, isolationLevel, maxWaitTime, maxIdleFetches }
) => {
  const records = []
  const aborted = new Map()
  let position = 0n
  let idle = 0
  while (position < end) {
    const { responses } = await broker.fetch({
      isolationLevel,
      maxWaitTime,
      topics: [
        {
          topic,
          partitions: [{ partition, fetchOffset: String(position), maxBytes: PARTITION_MAX_BYTES }],
        },
      ],
    })
    const { messages, abortedTransactions } = responses[0].partitions[0]
    for (const transaction of abortedTransactions || []) {
      aborted.set(`${transaction.producerId}|${transaction.firstOffset}`, transaction)
    }
    let next = position
    for (const record of messages) {
      const offset = BigInt(record.offset)
      if (offset >= position && offset < end) records.push(record)
      if (offset + 1n > next) next = offset + 1n
    }
    if (next === position && ++idle >= maxIdleFetches) {
      throw new Error(`Raw read of ${topic}-${partition} stalled at ${position}, below ${end}`)
    }
    if (next !== position) idle = 0
    position = next
  }
  return { records, abortedTransactions: [...aborted.values()] }
}

/**
 * Opens the first seed broker, looks the topic's partition leaders up once, runs `work` for each
 * partition in partition order on its leader, and disconnects every broker it opened, whatever
 * happened.
 * @param {{ brokers: string[], topic: string, open?: Function }} options
 * @param {(partition: number, leader: object) => Promise<*>} work
 * @returns {Promise<Map<number, *>>} what `work` returned, by partition
 * @throws {Error} when the topic has no metadata or a partition's leader is unknown
 */
const forEachLeader = async ({ brokers, topic, open = openBroker }, work) => {
  const opened = new Map()
  const connect = (key, address) => {
    if (!opened.has(key)) opened.set(key, open(address))
    return opened.get(key)
  }
  try {
    const [host, port] = brokers[0].split(':')
    const seed = await connect('seed', { host, port: Number(port) })
    const { brokers: nodes, topicMetadata } = await seed.metadata([topic])
    const metadata = topicMetadata.find(({ topic: name }) => name === topic)
    if (!metadata || metadata.topicErrorCode) {
      throw new Error(`No metadata for ${topic}: ${JSON.stringify(metadata?.topicErrorCode)}`)
    }
    const results = new Map()
    const partitions = [...metadata.partitionMetadata].sort((a, b) => a.partitionId - b.partitionId)
    for (const { partitionId: partition, leader } of partitions) {
      const node = nodes.find(({ nodeId }) => nodeId === leader)
      if (!node) throw new Error(`${topic}-${partition} has no known leader (${leader})`)
      results.set(partition, await work(partition, await connect(leader, node)))
    }
    return results
  } finally {
    await Promise.all(
      [...opened.values()].map((pending) =>
        pending.then((broker) => broker.disconnect()).catch(() => {})
      )
    )
  }
}

/**
 * The offset each partition's leader reports as the end of the log: the high watermark under
 * `READ_UNCOMMITTED`, the last stable offset under `READ_COMMITTED`.
 * @param {Broker} leader
 * @param {string} topic
 * @param {number} partition
 * @param {number} isolationLevel
 * @returns {Promise<string>}
 */
const endOffsetOf = async (leader, topic, partition, isolationLevel) => {
  const { responses } = await leader.listOffsets({
    replicaId: -1,
    isolationLevel,
    topics: [{ topic, partitions: [{ partition, timestamp: -1 }] }],
  })
  return String(responses[0].partitions[0].offset)
}

/**
 * The end offset of every partition of a topic, read from its leader with `Broker#listOffsets`.
 * @param {object} options
 * @param {string[]} options.brokers seed addresses, `host:port`; the first one is asked
 * @param {string} options.topic
 * @param {number} [options.isolationLevel] `ISOLATION_LEVEL.READ_UNCOMMITTED` by default
 * @param {Function} [options.open] connects one broker; the unit spec supplies fakes
 * @returns {Promise<Map<number, string>>} by partition
 * @throws {Error} as `readLog`
 */
const endOffsets = ({ isolationLevel = ISOLATION_LEVEL.READ_UNCOMMITTED, ...options }) =>
  forEachLeader(options, (partition, leader) =>
    endOffsetOf(leader, options.topic, partition, isolationLevel)
  )

/**
 * Reads a topic's whole log straight from each partition's leader, through `Broker#listOffsets`
 * and `Broker#fetch` alone: no consumer group, no client-side filtering, no retry helper. The end
 * of each partition is what its leader reports when the read starts: the high watermark under
 * `READ_UNCOMMITTED`, the last stable offset under `READ_COMMITTED`. Control records and the
 * records of aborted transactions are returned as the broker sent them; `committedRecords` is the
 * consumer's view. Leaders are looked up once, so read a quiet cluster.
 *
 * @param {object} options
 * @param {string[]} options.brokers seed addresses, `host:port`; the first one is asked
 * @param {string} options.topic
 * @param {number} [options.isolationLevel] `ISOLATION_LEVEL.READ_UNCOMMITTED` by default
 * @param {(address: { host: string, port: number }) => Promise<object>} [options.open] connects
 *   one broker; the unit spec supplies fakes
 * @param {number} [options.maxWaitTime=100] each Fetch's wait, ms
 * @param {number} [options.maxIdleFetches=50] fetches in a row without progress before giving up
 * @returns {Promise<Map<number, { endOffset: string, records: object[],
 *   abortedTransactions: object[] }>>} by partition, in partition order
 * @throws {Error} when the topic has no metadata, a leader is unknown, or a read stalls; every
 *   broker it opened is disconnected either way
 */
const readLog = ({
  isolationLevel = ISOLATION_LEVEL.READ_UNCOMMITTED,
  maxWaitTime = 100,
  maxIdleFetches = 50,
  ...options
}) =>
  forEachLeader(options, async (partition, leader) => {
    const endOffset = await endOffsetOf(leader, options.topic, partition, isolationLevel)
    const read = { topic: options.topic, partition, end: BigInt(endOffset), isolationLevel }
    const log = await readPartition(leader, { ...read, maxWaitTime, maxIdleFetches })
    return { endOffset, ...log }
  })

/**
 * The records a `read_committed` consumer is given, decided by Java's rules and by none of this
 * client's filtering code: going batch by batch, an aborted transaction starts counting at its
 * first offset; a batch of a transactional producer it names is skipped until that producer's
 * ABORT control record; control batches are never given; everything else is.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CompletedFetch.java
 * @param {{ records: object[], abortedTransactions: { producerId: string, firstOffset: string }[] }} log
 *   one partition of `readLog` under `READ_COMMITTED`
 * @returns {object[]}
 */
const committedRecords = ({ records, abortedTransactions }) => {
  const pending = [...abortedTransactions].sort((a, b) =>
    Number(BigInt(a.firstOffset) - BigInt(b.firstOffset))
  )
  const aborted = new Set()
  return records.filter((record) => {
    const { producerId, inTransaction, isControlBatch } = record.batchContext
    while (pending.length && BigInt(pending[0].firstOffset) < batchEnd(record)) {
      aborted.add(pending.shift().producerId)
    }
    if (isControlBatch) {
      if (record.key && record.key.length >= 4 && record.key.readInt16BE(2) === ABORT) {
        aborted.delete(producerId)
      }
      return false
    }
    return !(inTransaction && aborted.has(producerId))
  })
}

module.exports = { readLog, endOffsets, committedRecords }
