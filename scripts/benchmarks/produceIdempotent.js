/**
 * Produce-throughput probe for the idempotent and transactional producers. Records are sent
 * without keys, one per `send()`, by `--concurrency` loops that each await their send, or - with
 * `--rate` - fired at that many sends per second without waiting. With `--transactional 1` the
 * records go in transactions of 100, sent concurrently and then committed. Uses the public API
 * only, so the same file runs unchanged inside an older tree. `--linger` and `--batch-size` reach
 * the producer only when given, and the JSON reports exactly what the producer received: without
 * them the producer's own defaults are measured, and `--linger 0` hands every batch on without a
 * wait. One untimed record per partition is sent first, so metadata and the leader connections are
 * paid for outside the timed window. With `--subscribe-channels 1` a subscriber that does nothing
 * listens on the diagnostics channel of every public event, so the run measures what subscribing
 * costs; that flag exists only in this tree. A bad argument exits 2.
 * Usage: node scripts/benchmarks/produceIdempotent.js [--idempotent 1] [--transactional 0]
 *          [--linger <ms>] [--batch-size 16384] [--partitions 6] [--messages 20000] [--size 200]
 *          [--concurrency 200] [--rate 0] [--subscribe-channels 0]
 * Prints one JSON line: { idempotent, transactional, linger, batchSize, partitions, messages,
 *   size, concurrency, rate, subscribeChannels, elapsedMs, messagesPerSecond, produceRequests,
 *   recordsPerRequest }
 * `produceRequests` counts the Produce requests the producer sent, from its `REQUEST` events.
 */
const { Kafka, logLevel } = require('../../index')
const subscribeChannels = require('./subscribeChannels')

/**
 * Prints `message` and exits 2, the code of a usage error.
 * @param {string} message
 * @returns {never}
 */
const usageError = (message) => {
  console.error(message)
  process.exit(2)
}

/**
 * Reads the numeric flag `--<name>`.
 * @param {string} name
 * @param {number|undefined} fallback returned when the flag is absent
 * @returns {number|undefined}
 */
const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  if (i === -1) {
    return fallback
  }
  const value = Number(process.argv[i + 1])
  return Number.isFinite(value) ? value : usageError(`--${name} needs a number`)
}

/**
 * Reads the switch `--<name>`, which is 0 or 1.
 * @param {string} name
 * @param {0|1} fallback
 * @returns {boolean}
 */
const switchArg = (name, fallback) => {
  const value = arg(name, fallback)
  return value === 0 || value === 1 ? value === 1 : usageError(`--${name} must be 0 or 1`)
}

const IDEMPOTENT = switchArg('idempotent', 1)
const TRANSACTIONAL = switchArg('transactional', 0)
const LINGER = arg('linger', undefined)
const BATCH_SIZE = arg('batch-size', undefined)
const PARTITIONS = arg('partitions', 6)
const MESSAGES = arg('messages', 20000)
const SIZE = arg('size', 200)
const CONCURRENCY = arg('concurrency', 200)
const RATE = arg('rate', 0)
const SUBSCRIBE_CHANNELS = switchArg('subscribe-channels', 0)
const TRANSACTION_RECORDS = 100
if (TRANSACTIONAL && MESSAGES % TRANSACTION_RECORDS !== 0) {
  usageError(`--messages must be a multiple of ${TRANSACTION_RECORDS} with --transactional 1`)
}
const TOPIC = `bench-produce-idempotent-${Date.now()}`
// A cold cluster answers the first transactional InitProducerId with a retriable error until the
// transaction state topic is ready: 10 retries, 100 ms doubling to a 1 s cap, about 7.5 s.
const RETRY = { retries: 10, initialRetryTime: 100, multiplier: 2, maxRetryTime: 1000, factor: 0 }

/**
 * Creates the benchmark topic, replicated on every broker, and waits for its leaders.
 * @param {import('../../index').Kafka} kafka
 * @returns {Promise<void>}
 */
const createTopic = async (kafka) => {
  const admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: PARTITIONS, replicationFactor: 3 }],
    waitForLeaders: true,
  })
  await admin.disconnect()
}

/**
 * Sends `count` one-record sends at once through `target`.
 * @param {{ send: Function }} target a producer or a transaction
 * @param {object} message
 * @param {number} count
 * @returns {Promise<object[][]>}
 */
const sendBurst = (target, message, count) =>
  Promise.all(Array.from({ length: count }, () => target.send(message)))

/**
 * Sends one record per partition, in a transaction when the producer is transactional, so the
 * topic's metadata, the leader connections and the producer id are in place before the timed run.
 * @param {object} producer a connected producer
 * @param {object} message
 * @returns {Promise<void>}
 */
const warmUp = async (producer, message) => {
  if (!TRANSACTIONAL) {
    await sendBurst(producer, message, PARTITIONS)
    return
  }
  const transaction = await producer.transaction()
  await sendBurst(transaction, message, PARTITIONS)
  await transaction.commit()
}

/**
 * Sends `MESSAGES` records, one per send, as the flags say.
 * @param {object} producer a connected producer
 * @param {object} message
 * @returns {Promise<void>}
 */
const sendAll = async (producer, message) => {
  if (TRANSACTIONAL) {
    for (let sent = 0; sent < MESSAGES; sent += TRANSACTION_RECORDS) {
      const transaction = await producer.transaction()
      await sendBurst(transaction, message, TRANSACTION_RECORDS)
      await transaction.commit()
    }
    return
  }

  if (RATE > 0) {
    // Each send's failure is caught as it happens and the first one rethrown at the end, so no
    // rejection waits unhandled while later sends are still being paced out.
    const failures = []
    const sends = []
    const started = Date.now()
    for (let n = 0; n < MESSAGES; n++) {
      const due = started + (n * 1000) / RATE
      if (due > Date.now()) {
        await new Promise((resolve) => setTimeout(resolve, due - Date.now()))
      }
      sends.push(producer.send(message).catch((e) => failures.push(e)))
    }
    await Promise.all(sends)
    if (failures.length > 0) {
      throw failures[0]
    }
    return
  }

  let next = 0
  const loop = async () => {
    while (next < MESSAGES) {
      next += 1
      await producer.send(message)
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, loop))
}

/**
 * Creates the topic, connects the producer, warms it up, times the sends and prints the JSON line.
 * @returns {Promise<void>}
 */
const main = async () => {
  const kafka = new Kafka({
    clientId: 'bench-produce-idempotent',
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    retry: RETRY,
    logLevel: logLevel.NOTHING,
  })
  if (SUBSCRIBE_CHANNELS) {
    subscribeChannels(kafka)
  }
  await createTopic(kafka)

  const producer = kafka.producer({
    idempotent: IDEMPOTENT || TRANSACTIONAL,
    ...(TRANSACTIONAL ? { transactionalId: `${TOPIC}-producer` } : {}),
    ...(LINGER === undefined ? {} : { linger: LINGER }),
    ...(BATCH_SIZE === undefined ? {} : { batchSize: BATCH_SIZE }),
  })
  let produceRequests = 0
  producer.on(producer.events.REQUEST, ({ payload }) => {
    if (payload.apiName === 'Produce') {
      produceRequests += 1
    }
  })
  await producer.connect()

  const message = { topic: TOPIC, messages: [{ value: Buffer.alloc(SIZE, 'x') }] }
  await warmUp(producer, message)
  produceRequests = 0

  const started = process.hrtime.bigint()
  await sendAll(producer, message)
  const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6

  console.log(
    JSON.stringify({
      idempotent: IDEMPOTENT,
      transactional: TRANSACTIONAL,
      linger: LINGER ?? null,
      batchSize: BATCH_SIZE ?? null,
      partitions: PARTITIONS,
      messages: MESSAGES,
      size: SIZE,
      concurrency: CONCURRENCY,
      rate: RATE,
      subscribeChannels: SUBSCRIBE_CHANNELS,
      elapsedMs,
      messagesPerSecond: (MESSAGES / elapsedMs) * 1000,
      produceRequests,
      recordsPerRequest: MESSAGES / produceRequests,
    })
  )

  await producer.disconnect()
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
