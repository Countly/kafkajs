/**
 * Honest bounded teardown for live scenarios: a deadline that fails when it is missed, and a
 * cleanup that tries every client before it reports.
 *
 * Unlike `withTimeout` in `liveScenario.js`, which resolves when its deadline wins, a missed
 * deadline here is an error, so a client that failed to close cannot pass a hook.
 */

/**
 * Awaits `promise` but rejects with `Disconnect exceeded <ms> ms` when `ms` elapses first. The
 * deadline timer is always cleared, so it never outlives the call. A rejection of `promise` is
 * passed through unchanged.
 * @param {Promise<unknown>} promise
 * @param {number} ms
 * @returns {Promise<void>}
 * @throws {Error} when the deadline wins, or whatever `promise` rejects with
 */
const withDeadline = async (promise, ms) => {
  let timer
  try {
    await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Disconnect exceeded ${ms} ms`)), ms)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Disconnects every client in order, each under its own deadline, and keeps going past a failure.
 * Falsy entries (a client that was never allocated) are skipped.
 * @param {Array<{disconnect: () => Promise<unknown>}|null|undefined>} clients
 * @param {number} ms deadline for each disconnect
 * @returns {Promise<void>}
 * @throws {AggregateError} carrying every disconnect failure, once all clients have been tried
 */
const disconnectAll = async (clients, ms) => {
  const failures = []

  for (const client of clients) {
    if (!client) {
      continue
    }
    try {
      await withDeadline(
        Promise.resolve().then(() => client.disconnect()),
        ms
      )
    } catch (error) {
      failures.push(error)
    }
  }

  if (failures.length > 0) {
    throw new AggregateError(
      failures,
      `${failures.length} cleanup failure(s): ${failures.map((error) => error.message).join('; ')}`
    )
  }
}

/**
 * Allocates and starts a live member, retaining it for cleanup even when setup fails.
 * Registers the allocation before event wiring, connect and subscribe; setup failures propagate
 * unchanged. Run rejections are recorded in the shared crash log.
 * @param {object} options
 * @param {{consumer: Function}} options.kafka client factory
 * @param {object} options.consumerOptions options passed unchanged to the consumer factory
 * @param {string} options.label member label used in event and crash logs
 * @param {object[]} options.members cleanup ownership list
 * @param {object[]} options.crashes shared crash log
 * @param {string[]} options.received shared partition/offset log
 * @param {string} options.topic subscribed topic
 * @returns {Promise<object>} member with its consumer, event log and batch log
 * @throws {Error} if allocation, event wiring, connect, subscribe or synchronous run setup fails
 */
const startTrackedMember = async ({
  kafka,
  consumerOptions,
  label,
  members,
  crashes,
  received,
  topic,
}) => {
  const consumer = kafka.consumer(consumerOptions)

  const member = { label, consumer, events: [], batches: [] }
  members.push(member)

  for (const type of [consumer.events.REBALANCING, consumer.events.GROUP_JOIN]) {
    consumer.on(type, ({ timestamp, payload }) => member.events.push({ type, timestamp, payload }))
  }
  consumer.on(consumer.events.START_BATCH_PROCESS, ({ timestamp, payload }) =>
    member.batches.push({ timestamp, payload })
  )
  consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push({ label, ...payload }))

  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  consumer
    .run({
      eachBatch: async ({ batch }) => {
        for (const message of batch.messages) {
          received.push(`${batch.partition}:${message.offset}`)
        }
      },
    })
    .catch((e) => crashes.push({ label, error: e }))

  return member
}

module.exports = { withDeadline, disconnectAll, startTrackedMember }
