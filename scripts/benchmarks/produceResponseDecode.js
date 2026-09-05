/**
 * Produce response decode microbenchmark: the classic v7 decoder against the flexible v11 one,
 * over the committed fixtures, so the cost of moving the produce response onto compact encoding
 * is a measured number rather than an assumption.
 * Usage: node scripts/benchmarks/produceResponseDecode.js [--iterations 20000]
 * Prints one JSON line: { v7: { opsPerSecond, nsPerOp }, v11: { ... }, ratio }
 */
const { decode: decodeV7 } = require('../../src/protocol/requests/produce/v7/response')
const { decode: decodeV11 } = require('../../src/protocol/requests/produce/v11/response')

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : Number(process.argv[i + 1])
}

const ITERATIONS = arg('iterations', 20000)
const WARMUP = 2000

// `Decoder` only advances an offset over `this.buffer`; it never writes to it (src/protocol/decoder.js),
// so one Buffer per fixture can be reused across every iteration instead of copied per decode.
const v7Fixture = Buffer.from(
  require('../../src/protocol/requests/produce/fixtures/v7_response.json')
)
const v11Fixture = Buffer.from(
  require('../../src/protocol/requests/produce/fixtures/v11_response.json')
)

/**
 * Runs `decode` over `fixture` `iterations` times (after an untimed warmup) and times the run with
 * a monotonic high-resolution clock.
 * @param {(buf: Buffer) => Promise<unknown>} decode - the response decoder under test
 * @param {Buffer} fixture - the pre-built input buffer, reused unmutated on every call
 * @param {number} iterations - timed iterations to run
 * @returns {Promise<{ opsPerSecond: number, nsPerOp: number }>} throughput and per-op cost
 */
const run = async (decode, fixture, iterations) => {
  for (let i = 0; i < WARMUP; i++) {
    await decode(fixture)
  }

  const started = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) {
    await decode(fixture)
  }
  const elapsedNs = Number(process.hrtime.bigint() - started)

  const nsPerOp = elapsedNs / iterations
  return {
    opsPerSecond: 1e9 / nsPerOp,
    nsPerOp,
  }
}

;(async () => {
  const v7 = await run(decodeV7, v7Fixture, ITERATIONS)
  const v11 = await run(decodeV11, v11Fixture, ITERATIONS)

  console.log(
    JSON.stringify({
      v7,
      v11,
      ratio: v11.nsPerOp / v7.nsPerOp,
    })
  )
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
