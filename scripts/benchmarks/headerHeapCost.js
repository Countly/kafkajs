/**
 * The heap a decoded record header retains, against `HEADER_CEILING_BYTES`, the bytes of
 * `maxDecompressedBatchBytes` that the batch decoder lets pay for one header. For each shape, one
 * record batch holding one record with as many headers as the header ceiling allows at a fixed
 * bound goes through the real batch decoder; the script keeps the decoded batch alive and measures
 * `heapUsed` after `gc()`, less what the same batch with a header-less record retains, divided by
 * the header count. Header values are views of the batch's bytes, whose store is outside the heap
 * and bounded by the Fetch size, so only `heapUsed` is counted. `heapAtDecodeEnd` is the heap the
 * decode added before any collection, temporary arrays included: a lower bound for its peak, since
 * a collection during the decode lowers it. An internal-API measurement: it requires the decoder
 * by tree path.
 *
 * Usage: node --expose-gc scripts/benchmarks/headerHeapCost.js [--assert]
 * Prints one JSON line per shape: { shape, headers, bytesPerHeader, retained, heapAtDecodeEnd },
 * then { ok, worstBytesPerHeader, headerCeilingBytes }. With `--assert` it exits 1 when the worst
 * shape retains more than `HEADER_CEILING_BYTES` per header; it exits 2 without `--expose-gc`.
 */
const Decoder = require('../../src/protocol/decoder')
const decodeBatch = require('../../src/protocol/recordBatch/v0/decoder')
const {
  batch,
  record,
} = require('../../src/protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')

const { HEADER_CEILING_BYTES } = decodeBatch
const BOUND = 16777216
const HEADERS = Math.max(1, Math.floor(BOUND / HEADER_CEILING_BYTES))
const OPTIONS = { checkCrcs: false, maxDecompressedBatchBytes: BOUND }
const REPEATS = 3

const VALUES = [
  ['null', () => null],
  ['empty', () => Buffer.alloc(0)],
  ['16-byte', (n) => Buffer.alloc(16, n)],
]

// A letter first: a digits-only key would be stored as a cheaper array index.
const KEYS = [
  ['distinct short keys', (n) => `k${n.toString(36)}`],
  ['one repeated key', () => 'k'],
]

/** Every value kind with every key kind, each as `[name, header(n)]`. */
const SHAPES = VALUES.flatMap(([valueName, value]) =>
  KEYS.map(([keyName, key]) => [`${valueName} values, ${keyName}`, (n) => [key(n), value(n)]])
)

/** @returns {number} `heapUsed` once the garbage is collected */
const heapAfterGc = () => {
  global.gc()
  global.gc()
  return process.memoryUsage().heapUsed
}

/**
 * Decodes `bytes` and measures what the decoded batch retains.
 * @param {Buffer} bytes one record batch
 * @returns {Promise<{ retained: number, atDecodeEnd: number, decoded: object }>} the retained heap,
 *   the heap added at the end of the decode before any collection, and the decoded batch
 */
const measureOnce = async (bytes) => {
  const before = heapAfterGc()
  const decoded = await decodeBatch(new Decoder(bytes), OPTIONS)
  const atDecodeEnd = process.memoryUsage().heapUsed - before
  const retained = heapAfterGc() - before
  return { retained, atDecodeEnd, decoded }
}

/**
 * @param {Buffer} bytes one record batch
 * @param {(decoded: object) => void} [check] throws when the decoded batch is not the expected one
 * @returns {Promise<{ retained: number, atDecodeEnd: number }>} the medians of `REPEATS` decodes
 */
const measure = async (bytes, check = () => {}) => {
  const runs = []
  for (let i = 0; i < REPEATS; i++) {
    const { retained, atDecodeEnd, decoded } = await measureOnce(bytes)
    check(decoded)
    runs.push({ retained, atDecodeEnd })
  }
  const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]
  return {
    retained: median(runs.map((run) => run.retained)),
    atDecodeEnd: median(runs.map((run) => run.atDecodeEnd)),
  }
}

/**
 * @param {object} decoded a decoded batch
 * @returns {number} the headers its one record carries, repeated keys counted once per value
 */
const headerCount = (decoded) =>
  Object.values(decoded.records[0].headers).reduce(
    (total, value) => total + (Array.isArray(value) ? value.length : 1),
    0
  )

const main = async () => {
  if (typeof global.gc !== 'function') {
    console.error('run with node --expose-gc')
    process.exit(2)
  }

  const baseline = await measure(batch({ records: [record()] }))
  let worst = 0
  for (const [shape, header] of SHAPES) {
    const bytes = batch({
      records: [record({ headers: Array.from({ length: HEADERS }, (_, n) => header(n)) })],
    })
    const { retained, atDecodeEnd } = await measure(bytes, (decoded) => {
      if (headerCount(decoded) !== HEADERS) throw new Error(`${shape}: not ${HEADERS} headers`)
    })
    const bytesPerHeader = (retained - baseline.retained) / HEADERS
    worst = Math.max(worst, bytesPerHeader)
    console.log(
      JSON.stringify({
        shape,
        headers: HEADERS,
        bytesPerHeader: Number(bytesPerHeader.toFixed(1)),
        retained,
        heapAtDecodeEnd: atDecodeEnd,
      })
    )
  }

  const ok = worst <= HEADER_CEILING_BYTES
  console.log(
    JSON.stringify({
      ok,
      worstBytesPerHeader: Number(worst.toFixed(1)),
      headerCeilingBytes: HEADER_CEILING_BYTES,
    })
  )
  process.exit(ok || !process.argv.includes('--assert') ? 0 : 1)
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error)
    process.exit(1)
  })
}

module.exports = { SHAPES, HEADERS, BOUND }
