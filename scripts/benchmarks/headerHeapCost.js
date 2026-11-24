/**
 * The heap decoded record headers take, against `HEADER_CEILING_BYTES`, the bytes of
 * `maxDecompressedBatchBytes` that the batch decoder lets pay for one header. Every layout goes
 * through the real batch decoder in two passes:
 * - calibration: `CALIBRATION_HEADERS` headers whatever the constant, under the largest bound. The
 *   worst retained cost, rounded up to a power of two, is `calibratedCeilingBytes`, the constant
 *   these layouts call for.
 * - acceptance: as many headers as the header ceiling of a 16 MiB bound allows with the shipped
 *   constant.
 *
 * Retained cost: `heapUsed` after `gc()` with the decoded batch alive, less what the same records
 * without headers retain, per header. Sampled peak: the highest `heapUsed` read, without a
 * collection, when a sampled record's header array is complete, halfway through and at the last
 * step of its fold into the headers object, and after the decode, less the same for the records
 * without headers, per header. It is a sample, not the peak: a collection or a temporary between
 * two reads is not seen. It is reported, never asserted; the assertion is on the retained cost,
 * which is the calibration rule for these layouts and not a bound on peak heap. Header values are
 * views of the batch's bytes, whose store is outside the heap, so only `heapUsed` counts.
 * The sampler wraps `Decoder#readVarIntArrayBytesBounded` in this process only. An internal-API
 * measurement: it requires the decoder by tree path.
 *
 * Usage: node --expose-gc scripts/benchmarks/headerHeapCost.js [--assert]
 * Prints one JSON line per pass and layout: { pass, layout, headers, records, retainedPerHeader,
 * sampledPeakPerHeader }, then { ok, calibratedCeilingBytes, headerCeilingBytes,
 * worstCalibrationPerHeader, worstAcceptancePerHeader, worstSampledPeakPerHeader }. With `--assert`
 * it exits 1 when either pass retains more than `HEADER_CEILING_BYTES` per header; it exits 2
 * without `--expose-gc`.
 */
const Decoder = require('../../src/protocol/decoder')
const decodeBatch = require('../../src/protocol/recordBatch/v0/decoder')
const {
  batch,
  record,
} = require('../../src/protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')

const { HEADER_CEILING_BYTES } = decodeBatch
const CALIBRATION_HEADERS = 262144
const CALIBRATION_OPTIONS = { checkCrcs: false, maxDecompressedBatchBytes: 2147483647 }
const ACCEPTANCE_BOUND = 16777216
const ACCEPTANCE_HEADERS = Math.max(1, Math.floor(ACCEPTANCE_BOUND / HEADER_CEILING_BYTES))
const ACCEPTANCE_OPTIONS = { checkCrcs: false, maxDecompressedBatchBytes: ACCEPTANCE_BOUND }
const REPEATS = 3
// Records sampled per decode: each sample allocates, so a many-record layout samples a subset.
const SAMPLED_RECORDS = 64

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

/**
 * Each layout as `{ name, perRecord, header(n) }`: `perRecord` headers per record (all of them in
 * one record when absent), header `n` of the batch as `[key, value]`.
 */
const LAYOUTS = [
  ...VALUES.flatMap(([valueName, value]) =>
    KEYS.map(([keyName, key]) => ({
      name: `${valueName} values, ${keyName}`,
      header: (n) => [key(n), value(n)],
    }))
  ),
  {
    name: 'empty values, distinct 16-character keys',
    header: (n) => [n.toString(36).padStart(16, 'k'), Buffer.alloc(0)],
  },
  {
    name: 'empty values, 8 headers per record in pairs of one key',
    perRecord: 8,
    header: (n) => [`g${(n % 8) >> 1}`, Buffer.alloc(0)],
  },
]

let peak = 0
let stride = 1
let arrays = 0

const sample = () => {
  peak = Math.max(peak, process.memoryUsage().heapUsed)
}

/**
 * Makes the fold sample the heap when it reads entry `at`'s value.
 * @param {Array<{ key: string, value: Buffer|null }>} headers a record's decoded header array
 * @param {number} at
 */
const tap = (headers, at) => {
  const { key, value } = headers[at]
  headers[at] = {
    key,
    get value() {
      sample()
      return value
    },
  }
}

/**
 * Wraps the header array reader so that every `stride`-th array samples the heap once complete,
 * and has its middle and last entries tapped for the fold.
 */
const installSampler = () => {
  const readHeaders = Decoder.prototype.readVarIntArrayBytesBounded
  Decoder.prototype.readVarIntArrayBytesBounded = function (reader, budget) {
    const headers = readHeaders.call(this, reader, budget)
    if (arrays++ % stride === 0) {
      sample()
      const last = headers.length - 1
      if (last > 0) tap(headers, last >> 1)
      if (last >= 0) tap(headers, last)
    }
    return headers
  }
}

/** @returns {number} `heapUsed` once the garbage is collected */
const heapAfterGc = () => {
  global.gc()
  global.gc()
  return process.memoryUsage().heapUsed
}

/**
 * Decodes `bytes` and measures what the decoded batch retains and the heap it was sampled at.
 * @param {Buffer} bytes one record batch
 * @param {object} options the batch decoder's options
 * @returns {Promise<{ retained: number, sampledPeak: number, decoded: object }>}
 */
const measureOnce = async (bytes, options) => {
  const before = heapAfterGc()
  peak = before
  arrays = 0
  const decoded = await decodeBatch(new Decoder(bytes), options)
  sample()
  const sampledPeak = peak - before
  const retained = heapAfterGc() - before
  return { retained, sampledPeak, decoded }
}

/**
 * @param {Buffer} bytes one record batch
 * @param {object} options the batch decoder's options
 * @param {(decoded: object) => void} [check] throws when the decoded batch is not the expected one
 * @returns {Promise<{ retained: number, sampledPeak: number }>} the medians of `REPEATS` decodes
 */
const measure = async (bytes, options, check = () => {}) => {
  const runs = []
  for (let i = 0; i < REPEATS; i++) {
    const { retained, sampledPeak, decoded } = await measureOnce(bytes, options)
    check(decoded)
    runs.push({ retained, sampledPeak })
  }
  const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]
  return {
    retained: median(runs.map((run) => run.retained)),
    sampledPeak: median(runs.map((run) => run.sampledPeak)),
  }
}

/**
 * @param {object} decoded a decoded batch
 * @returns {number} the headers its records carry, repeated keys counted once per value
 */
const headerCount = (decoded) =>
  decoded.records.reduce(
    (total, { headers }) =>
      Object.values(headers).reduce(
        (sum, value) => sum + (Array.isArray(value) ? value.length : 1),
        total
      ),
    0
  )

/**
 * @param {{ perRecord?: number, header: (n: number) => Array }} layout
 * @param {number} headers the headers of the batch
 * @returns {{ bytes: Buffer, bare: Buffer, records: number }} the batch, the same records without
 *   headers, and the record count
 */
const build = ({ perRecord, header }, headers) => {
  perRecord = perRecord ?? headers
  const full = []
  const bare = []
  for (let first = 0; first < headers; first += perRecord) {
    const count = Math.min(perRecord, headers - first)
    const offsetDelta = full.length
    full.push(
      record({ offsetDelta, headers: Array.from({ length: count }, (_, i) => header(first + i)) })
    )
    bare.push(record({ offsetDelta }))
  }
  return { bytes: batch({ records: full }), bare: batch({ records: bare }), records: full.length }
}

/**
 * Measures one layout at `headers` headers.
 * @param {object} layout one of `LAYOUTS`
 * @param {number} headers
 * @param {object} options the batch decoder's options
 * @returns {Promise<{ layout: string, headers: number, records: number, retainedPerHeader: number,
 *   sampledPeakPerHeader: number }>}
 */
const measureLayout = async (layout, headers, options) => {
  const { bytes, bare, records } = build(layout, headers)
  stride = Math.max(1, Math.ceil(records / SAMPLED_RECORDS))
  const without = await measure(bare, options)
  const withHeaders = await measure(bytes, options, (decoded) => {
    if (headerCount(decoded) !== headers) throw new Error(`${layout.name}: not ${headers} headers`)
  })
  const perHeader = (bytesUsed) => Number((bytesUsed / headers).toFixed(1))
  return {
    layout: layout.name,
    headers,
    records,
    retainedPerHeader: perHeader(withHeaders.retained - without.retained),
    sampledPeakPerHeader: perHeader(withHeaders.sampledPeak - without.sampledPeak),
  }
}

/**
 * Runs every layout in one pass and prints a line for each.
 * @param {string} pass
 * @param {number} headers
 * @param {object} options the batch decoder's options
 * @returns {Promise<object[]>} the results of `measureLayout`
 */
const runPass = async (pass, headers, options) => {
  const results = []
  for (const layout of LAYOUTS) {
    const result = await measureLayout(layout, headers, options)
    console.log(JSON.stringify({ pass, ...result }))
    results.push(result)
  }
  return results
}

/**
 * Runs the calibration and acceptance passes, prints the summary, and exits 1 under `--assert`
 * when either pass retains more than `HEADER_CEILING_BYTES` per header.
 * @returns {Promise<void>}
 */
const main = async () => {
  if (typeof global.gc !== 'function') {
    console.error('run with node --expose-gc')
    process.exit(2)
  }

  installSampler()
  const calibration = await runPass('calibration', CALIBRATION_HEADERS, CALIBRATION_OPTIONS)
  const acceptance = await runPass('acceptance', ACCEPTANCE_HEADERS, ACCEPTANCE_OPTIONS)
  const worst = (results, field) => Math.max(...results.map((result) => result[field]))
  const worstCalibration = worst(calibration, 'retainedPerHeader')
  const worstAcceptance = worst(acceptance, 'retainedPerHeader')
  const ok = worstCalibration <= HEADER_CEILING_BYTES && worstAcceptance <= HEADER_CEILING_BYTES

  console.log(
    JSON.stringify({
      ok,
      calibratedCeilingBytes: 2 ** Math.ceil(Math.log2(Math.max(1, worstCalibration))),
      headerCeilingBytes: HEADER_CEILING_BYTES,
      worstCalibrationPerHeader: worstCalibration,
      worstAcceptancePerHeader: worstAcceptance,
      worstSampledPeakPerHeader: worst([...calibration, ...acceptance], 'sampledPeakPerHeader'),
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
