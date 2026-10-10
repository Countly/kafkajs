#!/usr/bin/env node
/**
 * A transactional producer that dies between Produce and EndTxn, for the live crash-recovery
 * scenario: it begins a transaction, produces `--records` records, prints one `REQUEST <apiName>`
 * line for every request it sent and then `PRODUCED <n>` once the records are acknowledged, and
 * blocks without ending the transaction until it is killed - or for 120 s, the longest a live case
 * takes, so a member whose parent died first does not linger. Its own request log is how the
 * parent proves no EndTxn was sent before the kill.
 *
 * Usage: node scripts/e2e/transactionCrashMember.js --brokers <host:port,...> --topic <topic>
 *          --transactional-id <id> --records <n>
 * Exit code 2 on bad arguments, 1 on any client failure, 0 when it was never killed.
 */
const { parseArgs } = require('util')
const { Kafka, logLevel } = require('../../index')

// A cold cluster answers the first transactional InitProducerId with a retriable error until the
// transaction state topic is ready: 10 retries, 100 ms doubling to a 1 s cap, about 7.5 s in all.
const RETRY = { retries: 10, initialRetryTime: 100, multiplier: 2, maxRetryTime: 1000, factor: 0 }
// The longest a live case takes: the parent kills the member long before, unless it died first.
const KEEP_ALIVE_MS = 120000

/**
 * @param {string[]} argv the arguments after the script name
 * @returns {{ brokers: string[], topic: string, transactionalId: string, records: number }}
 * @throws {Error} naming the first missing or invalid flag
 */
const parseOptions = (argv) => {
  const { values } = parseArgs({
    args: argv,
    options: {
      brokers: { type: 'string' },
      topic: { type: 'string' },
      'transactional-id': { type: 'string' },
      records: { type: 'string' },
    },
    strict: true,
  })
  for (const flag of ['brokers', 'topic', 'transactional-id', 'records']) {
    if (!values[flag]) {
      throw new Error(`--${flag} is required`)
    }
  }
  const records = Number(values.records)
  if (!Number.isInteger(records) || records < 1) {
    throw new Error(`--records must be a positive integer, got ${values.records}`)
  }
  return {
    brokers: values.brokers.split(','),
    topic: values.topic,
    transactionalId: values['transactional-id'],
    records,
  }
}

/** @param {string} line written with its newline at once, so a reader never sees half of it */
const print = (line) => process.stdout.write(`${line}\n`)

/**
 * Begins a transaction, produces, reports, and waits to be killed.
 * @param {ReturnType<typeof parseOptions>} options
 * @returns {Promise<void>} after `KEEP_ALIVE_MS`, with the transaction still open
 */
const run = async ({ brokers, topic, transactionalId, records }) => {
  const kafka = new Kafka({
    clientId: `transaction-crash-member-${process.pid}`,
    brokers,
    retry: RETRY,
    logLevel: logLevel.NOTHING,
  })
  const producer = kafka.producer({ transactionalId, retry: RETRY })
  producer.on(producer.events.REQUEST, ({ payload }) => print(`REQUEST ${payload.apiName}`))
  await producer.connect()

  const transaction = await producer.transaction()
  await transaction.send({
    topic,
    messages: Array.from({ length: records }, (_, index) => ({ value: `orphan-${index}` })),
  })
  print(`PRODUCED ${records}`)
  await new Promise((resolve) => setTimeout(resolve, KEEP_ALIVE_MS))
}

let options
try {
  options = parseOptions(process.argv.slice(2))
} catch (e) {
  process.stderr.write(`${e.message}\n`)
  process.exit(2)
}

run(options).then(
  () => process.exit(0),
  (e) => {
    process.stderr.write(`${e.stack}\n`)
    process.exit(1)
  }
)
