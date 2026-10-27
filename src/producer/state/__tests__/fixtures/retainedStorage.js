const Accumulator = require('../../accumulator')
const { Admission } = require('../../admission')
const Cluster = require('../../../../cluster')
const Generation = require('../../../generation')
const { createLogger, LEVELS } = require('../../../../loggers')
const { collect, countersOf, flushAll } = require('../../../__tests__/fixtures/bufferHarness')

const TOPIC = 'topic-a'

/** The buffer capacities every shape is measured at. */
const CAPACITIES = [1048576, 4194304]

/** The shapes of send the bound must hold for: `records(i, shared)` gives the records of send i. */
const SHAPES = [
  [
    'varying records per send',
    (i) =>
      Array.from({ length: 1 + (i % 7) }, (_, j) => ({ key: `k${j}`, value: 'v'.repeat(20 + j) })),
  ],
  ['one record per partition', (i) => [{ value: `fan-${i}`, partition: i }]],
  [
    'many empty header values',
    () => [
      {
        value: '',
        headers: Object.fromEntries(Array.from({ length: 20 }, (_, j) => [`h${j}`, ''])),
      },
    ],
  ],
  ['independent 16 KiB slabs', (i) => [{ value: Buffer.alloc(16384, i % 256) }]],
  [
    'Buffer-valued headers and a shared store',
    (i, shared) => [
      {
        value: shared.subarray((i % 64) * 16, (i % 64) * 16 + 16),
        headers: { a: Buffer.alloc(32), b: [Buffer.alloc(8), shared.subarray(1000, 1008)] },
      },
    ],
  ],
]

/** How many populations one measurement takes. */
const POPULATION_COUNT = CAPACITIES.length * SHAPES.length

/** What each measurement scenario adds to a plain buffer: topic pins, and an open flush snapshot. */
const SCENARIOS = {
  held: { pinned: false, snapshot: false },
  pinned: { pinned: true, snapshot: false },
  tracked: { pinned: true, snapshot: true },
}

/**
 * A producer cluster that never connects, for sends to pin their topics on.
 * @returns {Cluster}
 */
const pinCluster = () =>
  new Cluster({
    brokers: ['localhost:9092'],
    clientId: 'retained-storage',
    logger: createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} }),
    metadataMaxIdle: 300000,
    retry: { retries: 0 },
  })

/**
 * The heap and the backing stores retained once the garbage is collected.
 * @returns {Promise<number>} bytes
 */
const retained = async () => {
  await collect()
  const { heapUsed, arrayBuffers } = process.memoryUsage()
  return heapUsed + arrayBuffers
}

/**
 * An accumulator over a buffer whose send path never answers: every batch it is handed is kept in
 * `batches` until `drain()` settles and retires it. `pinner`, when given, pins the sends' topics;
 * `getGeneration`, when given, is the producer lifecycle each send checks.
 * @param {number} capacity the buffer's byte budget
 * @param {object} [pinner]
 * @param {() => Generation} [getGeneration]
 * @returns {{ accumulator: Accumulator, admission: Admission, batches: object[] }}
 */
const holding = (capacity, pinner, getGeneration) => {
  const admission = new Admission({ capacity, maxBlockMs: 60000 })
  const batches = []
  const accumulator = new Accumulator({
    linger: 0,
    batchSize: 16384,
    admission,
    partition: async (send) =>
      send.records.flatMap(({ topic, messages }) =>
        messages.map((message) => ({
          topic,
          partition: message.partition ?? 0,
          messages: [message],
        }))
      ),
    sendPartitioned: async ({ batches: handed }) => {
      batches.push(...handed)
    },
    pinner,
    getGeneration,
  })
  return { accumulator, admission, batches }
}

/**
 * Sends a shape's sends, all in one turn, until the buffer refuses one as queue-full, and lets
 * their joins run.
 * @param {number} capacity
 * @param {(i: number, shared: Buffer) => object[]} records the records of send i
 * @param {object} [pinner]
 * @param {() => Generation} [getGeneration]
 * @returns {Promise<object>} the population: its buffer, its batches, its sends' outcomes, and the
 *   counters when the first send had to wait
 */
const fill = async (capacity, records, pinner, getGeneration) => {
  const population = holding(capacity, pinner, getGeneration)
  const { accumulator, admission } = population
  let shared = Buffer.alloc(1024)
  const sends = []
  let wave = null
  for (let i = 0; ; i += 1) {
    const mark = admission.reserved + admission.waitingBytes
    sends.push(
      accumulator
        .append({ acks: -1, topicMessages: [{ topic: TOPIC, messages: records(i, shared) }] })
        .catch((error) => error)
    )
    if (admission.reserved + admission.waitingBytes === mark) break
    if (admission.waitingBytes > 0 && wave === null) wave = countersOf(admission)
  }
  shared = null
  await flushAll()
  return { ...population, sends, wave }
}

/**
 * Answers and retires every batch of a population, the waiting sends' included once they are
 * admitted, until none is left.
 * @param {{ batches: object[], sends: Promise<unknown>[] }} population
 * @returns {Promise<object[]>} the outcome of every send
 */
const drain = async ({ batches, sends }) => {
  for (let round = 0; round < 1000; round += 1) {
    await flushAll()
    if (batches.length === 0) break
    for (const batch of batches.splice(0)) {
      batch.settlers.forEach((settler) =>
        settler.resolve({ topicName: TOPIC, partition: batch.partition, baseOffset: '0' })
      )
      batch.retire?.()
    }
  }
  return Promise.all(sends)
}

/**
 * Each capacity with each shape, after one unmeasured population that also compiles the path.
 * @param {(capacity: number, records: Function, shape: string) => Promise<void>} each
 * @returns {Promise<void>}
 */
const populations = async (each) => {
  await drain(await fill(1048576, SHAPES[0][1]))
  for (const capacity of CAPACITIES) {
    for (const [shape, records] of SHAPES) {
      await each(capacity, records, shape)
    }
  }
}

/**
 * Measures what each population of a scenario retains against what its buffer counts for it.
 * @param {string} scenario a key of `SCENARIOS`
 * @returns {Promise<{ shape: string, capacity: number, measured: number, counted: number }[]>}
 *   one sample per population, in bytes
 * @throws {Error} for an unknown scenario
 */
const measure = async (scenario) => {
  const { pinned, snapshot } = SCENARIOS[scenario] ?? {}
  if (pinned === undefined) {
    throw new Error(`unknown scenario "${scenario}"`)
  }
  const pinner = pinned ? pinCluster() : undefined
  const generation = new Generation()
  const samples = []
  await populations(async (capacity, records, shape) => {
    const before = await retained()
    const population = await fill(
      capacity,
      records,
      pinner,
      snapshot ? () => generation : undefined
    )
    const { accumulator, admission } = population
    const open = snapshot ? accumulator.unsettled() : []
    const after = await retained()
    const counted = admission.reserved + admission.waitingBytes + admission.debts
    samples.push({ shape, capacity, measured: after - before, counted })
    await Promise.all([drain(population), ...open])
  })
  return samples
}

module.exports = { POPULATION_COUNT, TOPIC, drain, fill, measure, pinCluster, populations }

// Run as a script by the memory spec: one scenario's samples as JSON on stdout.
if (require.main === module) {
  measure(process.argv[2]).then(
    (samples) => process.stdout.write(JSON.stringify(samples)),
    (error) => {
      process.stderr.write(`${error.stack}\n`)
      process.exitCode = 1
    }
  )
}
