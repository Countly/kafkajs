/**
 * Record-batch decode microbenchmark: decodes one committed record batch N times and reports
 * ns/op, so the cost of the record decoder is a measured number rather than an assumption.
 * Usage: node scripts/benchmarks/recordBatchDecode.js [--iterations 20000]
 * Prints one JSON line: { iterations, nsPerOp, opsPerSecond, records }
 */
const { decode } = require('../../src/protocol/requests/fetch/v11/response')

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : Number(process.argv[i + 1])
}

const ITERATIONS = arg('iterations', 20000)
const WARMUP = 2000
const ROUNDS = 5

// `Decoder` only advances an offset over `this.buffer`; it never writes to it (src/protocol/decoder.js),
// so one Buffer can be reused across every iteration instead of copied per decode.
const FIXTURE = Buffer.from(require('../../src/protocol/requests/fetch/fixtures/v11_response.json'))
const OPTIONS = { checkCrcs: false }

const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * Counts the records the fixture carries, so ns/op can be read per record as well as per response.
 * @param {object} data a decoded fetch response
 * @returns {number} total records across every partition of every topic; a partition that carries
 *   an error instead of messages counts as zero
 */
const countRecords = (data) =>
  data.responses.reduce(
    (total, { partitions }) =>
      total + partitions.reduce((sum, { messages }) => sum + (messages ? messages.length : 0), 0),
    0
  )

/**
 * Decodes the fixture `iterations` times (after an untimed warmup), split into equal rounds, and
 * reports the median round so a single scheduling hiccup does not set the result.
 * @param {number} iterations timed iterations to run
 * @returns {Promise<{ nsPerOp: number, opsPerSecond: number }>} per-op cost and throughput
 */
const run = async (iterations) => {
  for (let i = 0; i < WARMUP; i++) {
    await decode(FIXTURE, OPTIONS)
  }

  const perRound = Math.max(1, Math.floor(iterations / ROUNDS))
  const roundNsPerOp = []

  for (let round = 0; round < ROUNDS; round++) {
    const started = process.hrtime.bigint()
    for (let i = 0; i < perRound; i++) {
      await decode(FIXTURE, OPTIONS)
    }
    roundNsPerOp.push(Number(process.hrtime.bigint() - started) / perRound)
  }

  const nsPerOp = median(roundNsPerOp)
  return { nsPerOp, opsPerSecond: 1e9 / nsPerOp }
}

;(async () => {
  const records = countRecords(await decode(FIXTURE, OPTIONS))
  const { nsPerOp, opsPerSecond } = await run(ITERATIONS)

  console.log(JSON.stringify({ iterations: ITERATIONS, nsPerOp, opsPerSecond, records }))
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
