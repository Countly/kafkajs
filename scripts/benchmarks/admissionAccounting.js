/**
 * The producer's buffer charge against what the held sends really retain. For each record shape,
 * sends go through the real `Accumulator` and its `Admission` to a send path that never answers, as
 * a stalled partition leaves them, and the script compares the bytes the buffer counts (its
 * reservations and batch debits) with the retained storage they cost (`heapUsed` plus
 * `arrayBuffers` after `gc()`, against a baseline taken the same way). A shape is within the
 * measurement tolerance when counted over measured is between 0.9 and 3.0; that calibrates the
 * constants of `src/producer/state/recordSize.js`, and proves nothing about every heap or about
 * RSS. The positive control repeats the shape over a send path that answers and releases at once:
 * it must retain next to nothing, which shows the measured arm holds the records.
 * An internal-API benchmark: it requires the accumulator by tree path.
 *
 * Usage: node --expose-gc scripts/benchmarks/admissionAccounting.js [--target <bytes>]
 *          [--shape <name>]
 * `--target` (default 33554432) is roughly what each shape's sends are charged in total.
 * Prints one JSON line per shape: { shape, sends, records, counted, measured, ratio, control,
 *   within }, then { ok }. Exits 0 when every shape is within [0.9, 3.0] and every control below
 * 0.1 of its count, 1 otherwise, and 2 for a bad argument or a missing `--expose-gc`.
 */
const Accumulator = require('../../src/producer/state/accumulator')
const { Admission } = require('../../src/producer/state/admission')
const { chargeOf, BATCH_DEBIT } = require('../../src/producer/state/recordSize')

const TOPIC = 'bench-topic'
const WINDOW = [0.9, 3.0]
const CONTROL_LIMIT = 0.1

/** The eight shapes: `records(i)` is the records of send `i`, owned by that send. */
const SHAPES = [
  ['empty record', () => [{ value: '' }]],
  ['small strings', (i) => [{ key: `key-${i}`, value: `value-${i}` }]],
  ['small Buffers', () => [{ key: Buffer.alloc(8), value: Buffer.alloc(8) }]],
  ['1 KiB', (i) => [{ value: String(i).padEnd(1024, 'x') }]],
  ['64 KiB', () => [{ value: Buffer.alloc(65536) }]],
  [
    'many headers',
    (i) => [
      {
        value: `value-${i}`,
        headers: Object.fromEntries(Array.from({ length: 20 }, (_, j) => [`h${j}`, `${i}-${j}`])),
      },
    ],
  ],
  ['one record per partition', (i) => [{ value: `value-${i}`, partition: i }]],
  ['Buffer views over their own slabs', () => [{ value: Buffer.alloc(4096).subarray(0, 512) }]],
]

/**
 * @param {string} name
 * @param {string} fallback
 * @returns {string}
 */
const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : process.argv[i + 1]
}

/** Lets the queued joins and hand-offs run. */
const settle = async () => {
  for (let i = 0; i < 3; i += 1) {
    await new Promise((resolve) => setImmediate(resolve))
  }
}

/** @returns {Promise<number>} `heapUsed` plus `arrayBuffers` once the garbage is collected */
const retained = async () => {
  await settle()
  global.gc()
  global.gc()
  const { heapUsed, arrayBuffers } = process.memoryUsage()
  return heapUsed + arrayBuffers
}

/**
 * Sends `count` sends of a shape through an accumulator, one partition per record unless the
 * shape names one, and measures what they retain.
 * @param {(i: number) => object[]} records
 * @param {number} count
 * @param {boolean} answer whether the send path answers and releases at once (the control)
 * @returns {Promise<{ counted: number, measured: number, records: number }>}
 */
const run = async (records, count, answer) => {
  const admission = new Admission({ capacity: Number.MAX_SAFE_INTEGER, maxBlockMs: 0 })
  const kept = []
  const accumulator = new Accumulator({
    linger: 5,
    batchSize: 16384,
    holds: () => true,
    admission,
    partition: async (send) =>
      send.records.flatMap(({ topic, messages }) =>
        messages.map((message) => ({
          topic,
          partition: message.partition ?? 0,
          messages: [message],
        }))
      ),
    sendPartitioned: async ({ batches }) => {
      for (const batch of batches) {
        if (answer) {
          batch.settlers.forEach((s) =>
            s.resolve({ topicName: TOPIC, partition: 0, baseOffset: '0' })
          )
          batch.retire()
        } else {
          kept.push(batch)
        }
      }
    },
  })

  const before = await retained()
  let sends = []
  let total = 0
  for (let i = 0; i < count; i += 1) {
    const topicMessages = Accumulator.snapshot([{ topic: TOPIC, messages: records(i) }])
    total += topicMessages[0].messages.length
    sends.push(accumulator.append({ acks: -1, topicMessages }).catch(() => {}))
  }
  // Hands every pending batch on; awaited only by the control, whose batches settle.
  const flushed = accumulator.flush()
  if (answer) {
    await Promise.all([flushed, ...sends])
    sends = []
  }
  const after = await retained()
  const counted = admission.reserved + admission.waitingBytes + admission.debts
  // Read after the measurement, so the sends and the batches are alive while it is taken.
  return {
    counted,
    measured: after - before,
    records: total,
    sends: sends.length,
    kept: kept.length,
  }
}

/**
 * Measures one shape and its control.
 * @param {string} shape
 * @param {(i: number) => object[]} records
 * @param {number} target bytes the sends are charged in total, roughly
 * @returns {Promise<object>} the JSON line of the shape
 */
const measure = async (shape, records, target) => {
  const perSend =
    chargeOf([{ topic: TOPIC, messages: records(0) }], Number.MAX_SAFE_INTEGER).total + BATCH_DEBIT
  const sends = Math.max(1, Math.floor(target / perSend))
  const measured = await run(records, sends, false)
  const control = await run(records, sends, true)
  const ratio = measured.counted / measured.measured
  return {
    shape,
    sends,
    records: measured.records,
    counted: measured.counted,
    measured: measured.measured,
    ratio: Number(ratio.toFixed(3)),
    control: Number((control.measured / measured.counted).toFixed(3)),
    within:
      ratio >= WINDOW[0] &&
      ratio <= WINDOW[1] &&
      control.measured < CONTROL_LIMIT * measured.counted,
  }
}

const main = async () => {
  if (typeof global.gc !== 'function') {
    console.error('run with node --expose-gc')
    process.exit(2)
  }
  const target = Number(arg('target', '33554432'))
  const only = arg('shape', null)
  const shapes = SHAPES.filter(([name]) => only === null || name === only)
  if (!(target > 0) || shapes.length === 0) {
    console.error(
      `--target must be a positive number and --shape one of: ${SHAPES.map(([n]) => n).join(', ')}`
    )
    process.exit(2)
  }

  let ok = true
  for (const [shape, records] of shapes) {
    const line = await measure(shape, records, target)
    ok = ok && line.within
    console.log(JSON.stringify(line))
  }
  console.log(JSON.stringify({ ok }))
  process.exit(ok ? 0 : 1)
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error)
    process.exit(1)
  })
}

module.exports = { SHAPES, WINDOW, measure }
