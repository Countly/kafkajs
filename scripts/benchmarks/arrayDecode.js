/**
 * Array-reader decode microbenchmark: decodes one real response, built once in setup, whose widest
 * array is read by a given `Decoder` array reader, and reports ns/op. Where `frameDecode.js` times
 * the frame loop and `recordBatchDecode.js` decodes responses whose arrays hold a handful of
 * elements, every arm here is dominated by one array of thousands of elements, so a change to a
 * reader's count check or element loop is a measured number rather than an assumption.
 *
 * Each arm goes through the production decoder of one codec, never through the harness:
 *
 * | arm                     | codec          | widest array                                      |
 * | ----------------------- | -------------- | ------------------------------------------------- |
 * | `readArray`             | Produce v7     | one topic, 2000 partitions                        |
 * | `readVarIntArray`       | Fetch v11      | 4 records of 1000 headers each, in one batch      |
 * | `readUVarIntArray`      | Produce v11    | one topic, 2000 partitions (flexible)             |
 * | `readArrayAsync`        | Fetch v11      | one topic, 2000 partitions with empty record sets |
 * | `readUVarIntArrayAsync` | Fetch v12      | one topic, 2000 partitions (flexible)             |
 *
 * An internal-API benchmark: it requires the codecs by tree path, so one copy of the file runs in
 * the tree it sits in. The comparison copies it into the baseline tree, so both trees run the same
 * bytes.
 *
 * Usage: node scripts/benchmarks/arrayDecode.js --arm <name> [--iterations 300]
 * Prints one JSON line: { available, arm, iterations, nsPerOp, opsPerSecond, elements }, where
 * `elements` is how many elements of the widest array one decode returns.
 */
const Encoder = require('../../src/protocol/encoder')
const { decode: decodeProduceV7 } = require('../../src/protocol/requests/produce/v7/response')
const { decode: decodeProduceV11 } = require('../../src/protocol/requests/produce/v11/response')
const { decode: decodeFetchV11 } = require('../../src/protocol/requests/fetch/v11/response')
const { decode: decodeFetchV12 } = require('../../src/protocol/requests/fetch/v12/response')
const { RecordBatch } = require('../../src/protocol/recordBatch/v0')
const RecordEncoder = require('../../src/protocol/recordBatch/record/v0')

const PARTITIONS = 2000
const RECORDS = 4
const HEADERS_PER_RECORD = 1000
const TOPIC = 'bench-topic'
const OPTIONS = { checkCrcs: false, maxDecompressedBatchBytes: 4194304 }
const DEFAULT_ITERATIONS = 300
const WARMUP = 50
const ROUNDS = 5

const USAGE = 'usage: node scripts/benchmarks/arrayDecode.js --arm <name> [--iterations <n>]'

/** @param {number} length @returns {number[]} 0 to length - 1 */
const range = (length) => Array.from({ length }, (_, n) => n)

/** @param {number} n @returns {Encoder} one Produce v5-layout partition response */
const produceClassicPartition = (n) =>
  new Encoder().writeInt32(n).writeInt16(0).writeInt64(n).writeInt64(-1).writeInt64(0)

/** @returns {Buffer} a Produce v7 response body: one topic with every partition */
const produceClassic = () =>
  new Encoder()
    .writeArray(
      [
        new Encoder()
          .writeString(TOPIC)
          .writeArray(range(PARTITIONS).map(produceClassicPartition), 'object'),
      ],
      'object'
    )
    .writeInt32(0).buffer

/** @param {number} n @returns {Encoder} one flexible Produce partition response */
const produceCompactPartition = (n) =>
  produceClassicPartition(n)
    .writeUVarIntArray([], 'object') // RecordErrors
    .writeUVarIntString(null) // ErrorMessage
    .writeTaggedFields()

/** @returns {Buffer} a Produce v11 response body: one topic with every partition */
const produceCompact = () =>
  new Encoder()
    .writeUVarIntArray(
      [
        new Encoder()
          .writeUVarIntString(TOPIC)
          .writeUVarIntArray(range(PARTITIONS).map(produceCompactPartition), 'object')
          .writeTaggedFields(),
      ],
      'object'
    )
    .writeInt32(0) // ThrottleTimeMs
    .writeTaggedFields().buffer

/** @param {number} n @returns {Encoder} the Fetch partition header every version starts with */
const fetchPartitionHeader = (n) =>
  new Encoder().writeInt32(n).writeInt16(0).writeInt64(100).writeInt64(100).writeInt64(0)

/**
 * @param {Encoder[]} partitions
 * @returns {Buffer} a Fetch v11 response body of one topic
 */
const fetchClassic = (partitions) =>
  new Encoder()
    .writeInt32(0) // ThrottleTimeMs
    .writeInt16(0) // ErrorCode
    .writeInt32(0) // SessionId
    .writeArray([new Encoder().writeString(TOPIC).writeArray(partitions, 'object')], 'object')
    .buffer

/**
 * @param {Buffer} [records] the partition's record set, empty by default
 * @returns {(n: number) => Encoder} a Fetch v11 partition with the given record set
 */
const fetchClassicPartition =
  (records = Buffer.alloc(0)) =>
  (n) =>
    fetchPartitionHeader(n)
      .writeArray([], 'object') // AbortedTransactions
      .writeInt32(-1) // PreferredReadReplica
      .writeBytes(records)

/** @returns {Buffer} a Fetch v11 response body: one topic with every partition, no records */
const fetchClassicWide = () => fetchClassic(range(PARTITIONS).map(fetchClassicPartition()))

/** @returns {Promise<Buffer>} a Fetch v11 response body: one partition whose batch has wide headers */
const fetchHeaders = async () => {
  const headers = Object.fromEntries(range(HEADERS_PER_RECORD).map((n) => [`h${n}`, `v${n}`]))
  const records = range(RECORDS).map((n) =>
    RecordEncoder({
      offsetDelta: n,
      key: Buffer.from(`k${n}`),
      value: Buffer.alloc(16, n),
      headers,
    })
  )
  const batch = Buffer.from((await RecordBatch({ records })).buffer)
  return fetchClassic([fetchClassicPartition(batch)(0)])
}

/** @param {number} n @returns {Encoder} one flexible Fetch partition with an empty record set */
const fetchCompactPartition = (n) =>
  fetchPartitionHeader(n)
    .writeUVarIntArray([], 'object') // AbortedTransactions
    .writeInt32(-1) // PreferredReadReplica
    .writeUVarInt(1) // empty COMPACT_RECORDS
    .writeTaggedFields()

/** @returns {Buffer} a Fetch v12 response body: one topic with every partition, no records */
const fetchCompactWide = () =>
  new Encoder()
    .writeInt32(0) // ThrottleTimeMs
    .writeInt16(0) // ErrorCode
    .writeInt32(0) // SessionId
    .writeUVarIntArray(
      [
        new Encoder()
          .writeUVarIntString(TOPIC)
          .writeUVarIntArray(range(PARTITIONS).map(fetchCompactPartition), 'object')
          .writeTaggedFields(),
      ],
      'object'
    )
    .writeTaggedFields().buffer

/**
 * @typedef {object} ArrayArm
 * @property {() => Buffer|Promise<Buffer>} build the response body, run once in setup
 * @property {(raw: Buffer, options: object) => Promise<object>} decode the production decoder
 * @property {(decoded: object) => number} count the elements of the widest array in a decode
 * @property {number} elements what `count` must return, or the arm is not built as described
 * @property {number} widest elements of the widest single array the arm's reader returns in a decode
 */

/** @type {Record<string, ArrayArm>} keyed by the `Decoder` reader the widest array goes through */
const ARMS = {
  readArray: {
    build: produceClassic,
    decode: decodeProduceV7,
    count: (decoded) => decoded.topics[0].partitions.length,
    elements: PARTITIONS,
    widest: PARTITIONS,
  },
  readVarIntArray: {
    build: fetchHeaders,
    decode: decodeFetchV11,
    count: (decoded) =>
      decoded.responses[0].partitions[0].messages.reduce(
        (total, { headers }) => total + Object.keys(headers).length,
        0
      ),
    elements: RECORDS * HEADERS_PER_RECORD,
    widest: HEADERS_PER_RECORD,
  },
  readUVarIntArray: {
    build: produceCompact,
    decode: decodeProduceV11,
    count: (decoded) => decoded.topics[0].partitions.length,
    elements: PARTITIONS,
    widest: PARTITIONS,
  },
  readArrayAsync: {
    build: fetchClassicWide,
    decode: decodeFetchV11,
    count: (decoded) => decoded.responses[0].partitions.length,
    elements: PARTITIONS,
    widest: PARTITIONS,
  },
  readUVarIntArrayAsync: {
    build: fetchCompactWide,
    decode: decodeFetchV12,
    count: (decoded) => decoded.responses[0].partitions.length,
    elements: PARTITIONS,
    widest: PARTITIONS,
  },
}

/**
 * @param {string[]} argv
 * @returns {{ arm: string, iterations: number }}
 * @throws {Error} the usage line for any argument it does not accept
 */
const parseArgs = (argv) => {
  const options = { iterations: DEFAULT_ITERATIONS }
  for (let i = 0; i < argv.length; i += 2) {
    const [flag, value] = [argv[i], argv[i + 1]]
    if (flag === '--arm' && Object.hasOwn(ARMS, value)) {
      options.arm = value
    } else if (
      flag === '--iterations' &&
      Number.isInteger(Number(value)) &&
      Number(value) >= ROUNDS
    ) {
      options.iterations = Number(value)
    } else {
      throw new Error(USAGE)
    }
  }

  if (!options.arm) {
    throw new Error(USAGE)
  }

  return options
}

/** @returns {number} the median of the values: the upper middle of an even count */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * Builds an arm's response and checks it decodes to the shape the arm claims.
 * @param {string} name a key of `ARMS`
 * @returns {Promise<{ raw: Buffer, arm: ArrayArm }>}
 * @throws {Error} when one decode returns another element count than `arm.elements`
 */
const prepare = async (name) => {
  const arm = ARMS[name]
  const raw = await arm.build()
  const counted = arm.count(await arm.decode(raw, OPTIONS))
  if (counted !== arm.elements) {
    throw new Error(`${name} decoded ${counted} elements, expected ${arm.elements}`)
  }

  return { raw, arm }
}

/**
 * Decodes the response `iterations` times (after an untimed warmup), split into equal rounds, and
 * reports the median round so a single scheduling hiccup does not set the result.
 * @param {Buffer} raw
 * @param {ArrayArm} arm
 * @param {number} iterations timed iterations to run
 * @returns {Promise<{ nsPerOp: number, opsPerSecond: number }>}
 */
const run = async (raw, { decode }, iterations) => {
  for (let i = 0; i < WARMUP; i++) {
    await decode(raw, OPTIONS)
  }

  const perRound = Math.floor(iterations / ROUNDS)
  const roundNsPerOp = []
  for (let round = 0; round < ROUNDS; round++) {
    const started = process.hrtime.bigint()
    for (let i = 0; i < perRound; i++) {
      await decode(raw, OPTIONS)
    }
    roundNsPerOp.push(Number(process.hrtime.bigint() - started) / perRound)
  }

  const nsPerOp = median(roundNsPerOp)
  return { nsPerOp, opsPerSecond: 1e9 / nsPerOp }
}

if (require.main === module) {
  ;(async () => {
    const { arm: name, iterations } = parseArgs(process.argv.slice(2))
    const { raw, arm } = await prepare(name)
    const { nsPerOp, opsPerSecond } = await run(raw, arm, iterations)
    console.log(
      JSON.stringify({
        available: 1,
        arm: name,
        iterations,
        nsPerOp,
        opsPerSecond,
        elements: arm.elements,
      })
    )
  })().catch((e) => {
    console.error(e)
    process.exit(1)
  })
}

module.exports = { ARMS, OPTIONS, parseArgs, prepare }
