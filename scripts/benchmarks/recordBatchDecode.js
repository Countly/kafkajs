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
 * Usage: node scripts/benchmarks/recordBatchDecode.js [--iterations 20000] [--arm <name>] [--work-multiplier <k>]
 * Prints one JSON line: { available, iterations, nsPerOp, roundNsPerOp, opsPerSecond, records,
 * decompressCalls }.
 * `decompressCalls` is how many times the codec's `decompress` ran in 10 untimed decodes, counted
 * through a wrapping codec registered for its type: 10 for a compressed arm, 0 for the uncompressed
 * one.
 *
 * `--checksum [--arms uncompressed,gzip,snappy]` is not a measurement: it decodes each arm's
 * response once, untimed, and prints `{ checksums: { <arm>: { available, records, sha256 } } }`,
 * the SHA-256 over every decoded record's partition, offset, key and value, so trees can be
 * compared on decoded content and not only on the record count.
 */
const { createHash } = require('crypto')
const { decode } = require('../../src/protocol/requests/fetch/v11/response')
const Encoder = require('../../src/protocol/encoder')

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : process.argv[i + 1]
}

const ITERATIONS = Number(arg('iterations', 20000))
const WORK_MULTIPLIER = Number(arg('work-multiplier', 1))
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
 * @returns {Promise<?{ fixture: Buffer, codec: ?object, type?: number }>} the response to decode,
 *   the codec it uses and that codec's type; `null` when this tree has no such codec
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
    return { fixture: responseOf(batch), codec, type }
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
 * @param {object} data a decoded fetch response
 * @returns {{ records: number, sha256: string }} the record count and the SHA-256 over each
 *   record's partition, offset, key and value, each length-prefixed (a null key or value is -1)
 */
const checksumOf = (data) => {
  const hash = createHash('sha256')
  let records = 0
  /**
   * Hashes one length-prefixed key or value; a null one is length -1.
   * @param {Buffer|null|undefined} bytes
   * @returns {void}
   */
  const field = (bytes) => {
    hash.update(`${bytes === null || bytes === undefined ? -1 : bytes.length}:`)
    if (bytes) hash.update(bytes)
  }
  for (const { partitions } of data.responses) {
    for (const { partition, messages } of partitions) {
      for (const { offset, key, value } of messages ?? []) {
        records += 1
        hash.update(`${partition}:${offset}:`)
        field(key)
        field(value)
      }
    }
  }
  return { records, sha256: hash.digest('hex') }
}

/**
 * Decodes the fixture `iterations` times (after an untimed warmup), split into equal rounds, and
 * reports the median round so a single scheduling hiccup does not set the result. Repetitions
 * increase operation count within each round while preserving nanoseconds per decode.
 * @param {Buffer} fixture
 * @param {object} options the decode options
 * @param {number} iterations timed iterations to run
 * @param {number} [workMultiplier] repetitions per operation, including warmup
 * @throws {Error} when the multiplier is not a positive integer
 * @returns {Promise<{ nsPerOp: number, roundNsPerOp: number[], opsPerSecond: number }>} per-op cost and throughput
 */
const run = async (fixture, options, iterations, workMultiplier = 1) => {
  if (!Number.isSafeInteger(workMultiplier) || workMultiplier < 1)
    throw new Error('invalid work multiplier')
  for (let i = 0; i < WARMUP * workMultiplier; i++) {
    await decode(fixture, options)
  }

  const perRound = Math.max(1, Math.floor(iterations / ROUNDS)) * workMultiplier
  const roundNsPerOp = []

  for (let round = 0; round < ROUNDS; round++) {
    const started = process.hrtime.bigint()
    for (let i = 0; i < perRound; i++) {
      await decode(fixture, options)
    }
    roundNsPerOp.push(Number(process.hrtime.bigint() - started) / perRound)
  }

  const nsPerOp = median(roundNsPerOp)
  return { nsPerOp, roundNsPerOp, opsPerSecond: 1e9 / nsPerOp }
}

/**
 * Decodes the fixture a few times with a counting codec registered for `type`, then puts the
 * registered factory back. The codec object itself is not changed: built-in codecs are frozen.
 * @param {Buffer} fixture
 * @param {object} options
 * @param {?object} codec
 * @param {number} [type] the codec's type in the registry
 * @returns {Promise<number>} how many times `decompress` ran
 */
const countDecompressCalls = async (fixture, options, codec, type) => {
  if (codec === null) {
    return 0
  }

  const { Codecs } = require('../../src/protocol/message/compression')
  const factory = Codecs[type]
  let calls = 0
  const counting = {
    ...codec,
    decompress: (...args) => {
      calls += 1
      return codec.decompress(...args)
    },
  }
  Codecs[type] = () => counting

  try {
    for (let i = 0; i < COUNTED; i++) {
      await decode(fixture, options)
    }
  } finally {
    Codecs[type] = factory
  }

  return calls
}

/**
 * @param {string[]} arms
 * @returns {Promise<object>} each arm's decoded-content checksum, `{ available: 0 }` without codec
 */
const checksums = async (arms) => {
  const out = {}
  for (const arm of arms) {
    const prepared = await prepare(arm)
    out[arm] = prepared
      ? {
          available: 1,
          ...checksumOf(
            await decode(prepared.fixture, { checkCrcs: false, maxDecompressedBatchBytes: BOUND })
          ),
        }
      : { available: 0 }
  }
  return { checksums: out }
}

if (require.main === module) {
  ;(async () => {
    if (process.argv.includes('--checksum')) {
      const arms = arg('arms', 'uncompressed,gzip,snappy').split(',')
      console.log(JSON.stringify(await checksums(arms)))
      return
    }
    const prepared = await prepare(ARM)

    if (prepared === null) {
      console.log(JSON.stringify({ available: 0 }))
      return
    }

    const { fixture, codec, type } = prepared
    const options = { checkCrcs: false, maxDecompressedBatchBytes: BOUND }
    const records = countRecords(await decode(fixture, options))
    const decompressCalls = await countDecompressCalls(fixture, options, codec, type)
    const { nsPerOp, roundNsPerOp, opsPerSecond } = await run(
      fixture,
      options,
      ITERATIONS,
      WORK_MULTIPLIER
    )

    console.log(
      JSON.stringify({
        available: 1,
        iterations: ITERATIONS,
        workMultiplier: WORK_MULTIPLIER,
        nsPerOp,
        roundNsPerOp,
        opsPerSecond,
        records,
        decompressCalls,
      })
    )
  })().catch((e) => {
    console.error(e)
    process.exit(1)
  })
}

module.exports = { run, checksumOf }
