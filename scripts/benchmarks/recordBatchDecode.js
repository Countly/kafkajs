/**
 * Record-batch decode microbenchmark: decodes one committed record batch N times and reports
 * ns/op, so the cost of the record decoder is a measured number rather than an assumption. An
 * internal-API benchmark: it requires the Fetch response module by tree path, so one file runs in
 * the tree it is started from and in a previous tree.
 *
 * `--arm uncompressed` (the default) decodes the committed v11 fixture. The other arms decode a
 * Fetch v11 response with one batch of 20 records compressed by `gzip`, `zstd`, `lz4` or `snappy`,
 * with the decompression bound set; a tree without the bound ignores it, and a tree without the
 * codec reports `{ "available": 0 }`.
 *
 * Usage: node scripts/benchmarks/recordBatchDecode.js [--iterations 20000] [--arm <name>]
 * Prints one JSON line: { available, iterations, nsPerOp, opsPerSecond, records, decompressCalls }.
 * `decompressCalls` is how many times the codec's `decompress` ran in 10 untimed decodes, counted by
 * wrapping it: 10 for a compressed arm, 0 for the uncompressed one.
 */
const { decode } = require('../../src/protocol/requests/fetch/v11/response')
const Encoder = require('../../src/protocol/encoder')

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : process.argv[i + 1]
}

const ITERATIONS = Number(arg('iterations', 20000))
const ARM = arg('arm', 'uncompressed')
const WARMUP = 2000
const ROUNDS = 5
const COUNTED = 10
const BOUND = 4194304

const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * A Fetch v11 response body with a single partition whose record set is the given bytes.
 * @param {Buffer} batch
 * @returns {Buffer}
 */
const responseOf = (batch) =>
  new Encoder()
    .writeInt32(0) // clientSideThrottleTime
    .writeInt16(0) // errorCode
    .writeInt32(0) // sessionId
    .writeArray(
      [
        new Encoder().writeString('bench-topic').writeArray(
          [
            new Encoder()
              .writeInt32(0) // partition
              .writeInt16(0) // errorCode
              .writeInt64(100) // highWatermark
              .writeInt64(100) // lastStableOffset
              .writeInt64(0) // logStartOffset
              .writeArray([], 'object') // abortedTransactions
              .writeInt32(-1) // preferredReadReplica
              .writeBytes(batch),
          ],
          'object'
        ),
      ],
      'object'
    ).buffer

/**
 * @param {string} arm
 * @returns {Promise<?{ fixture: Buffer, codec: ?object }>} the response to decode and the codec it
 *   uses; `null` when this tree has no such codec
 */
const prepare = async (arm) => {
  if (arm === 'uncompressed') {
    return {
      fixture: Buffer.from(require('../../src/protocol/requests/fetch/fixtures/v11_response.json')),
      codec: null,
    }
  }

  const { Types, lookupCodec } = require('../../src/protocol/message/compression')
  const { RecordBatch } = require('../../src/protocol/recordBatch/v0')
  const RecordEncoder = require('../../src/protocol/recordBatch/record/v0')
  const type = { gzip: Types.GZIP, zstd: Types.ZSTD, lz4: Types.LZ4, snappy: Types.Snappy }[arm]

  if (type === undefined) {
    return null
  }

  try {
    const codec = lookupCodec(type)
    const records = Array.from({ length: 20 }, (_, n) =>
      RecordEncoder({ offsetDelta: n, key: Buffer.from(`key-${n}`), value: Buffer.alloc(100, n) })
    )
    const batch = Buffer.from((await RecordBatch({ compression: type, records })).buffer)
    return { fixture: responseOf(batch), codec }
  } catch (error) {
    return null
  }
}

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
 * @param {Buffer} fixture
 * @param {object} options the decode options
 * @param {number} iterations timed iterations to run
 * @returns {Promise<{ nsPerOp: number, opsPerSecond: number }>} per-op cost and throughput
 */
const run = async (fixture, options, iterations) => {
  for (let i = 0; i < WARMUP; i++) {
    await decode(fixture, options)
  }

  const perRound = Math.max(1, Math.floor(iterations / ROUNDS))
  const roundNsPerOp = []

  for (let round = 0; round < ROUNDS; round++) {
    const started = process.hrtime.bigint()
    for (let i = 0; i < perRound; i++) {
      await decode(fixture, options)
    }
    roundNsPerOp.push(Number(process.hrtime.bigint() - started) / perRound)
  }

  const nsPerOp = median(roundNsPerOp)
  return { nsPerOp, opsPerSecond: 1e9 / nsPerOp }
}

/**
 * Decodes the fixture a few times with the codec's `decompress` wrapped, then puts it back.
 * @param {Buffer} fixture
 * @param {object} options
 * @param {?object} codec
 * @returns {Promise<number>} how many times `decompress` ran
 */
const countDecompressCalls = async (fixture, options, codec) => {
  if (codec === null) {
    return 0
  }

  const original = codec.decompress
  let calls = 0
  codec.decompress = (...args) => {
    calls += 1
    return original.apply(codec, args)
  }

  try {
    for (let i = 0; i < COUNTED; i++) {
      await decode(fixture, options)
    }
  } finally {
    codec.decompress = original
  }

  return calls
}

;(async () => {
  const prepared = await prepare(ARM)

  if (prepared === null) {
    console.log(JSON.stringify({ available: 0 }))
    return
  }

  const { fixture, codec } = prepared
  const options = { checkCrcs: false, maxDecompressedBatchBytes: BOUND }
  const records = countRecords(await decode(fixture, options))
  const decompressCalls = await countDecompressCalls(fixture, options, codec)
  const { nsPerOp, opsPerSecond } = await run(fixture, options, ITERATIONS)

  console.log(
    JSON.stringify({
      available: 1,
      iterations: ITERATIONS,
      nsPerOp,
      opsPerSecond,
      records,
      decompressCalls,
    })
  )
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
