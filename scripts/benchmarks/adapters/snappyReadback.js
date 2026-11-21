/**
 * Report adapter of the `snappyCodec.js --session` row: one session per report arm, a warm-up and
 * eight measured runs of the eight codec arms and of an A/A copy, which prints one session record.
 * The arm shows the median and range of its eight measured runs, `~AA` those of its copy. Before a
 * session is shown, the entry's `readback` receipt (the log of `GenerateSnappyVectors --read-back`
 * over the committed encoder artifacts, written by the workflow) must show `read-back ok` for
 * exactly the seven artifacts; otherwise the row is an `error`. A session that decodes other bytes
 * than its corpus or counts other than seven artifacts is `invalid`, a void A/A copy (more than 3 %
 * apart in decode or encode throughput) `inconclusive`, a session outside its own workflow job
 * `omitted`. The driver's declared floors and limits are counted and shown as information; they
 * never decide the status.
 */
const fs = require('node:fs')
const path = require('node:path')
const { lastJsonLine, tail } = require('../report')
const { SESSION } = require('../snappyCodec')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const ENCODER_DIR = path.join(ROOT, 'src/protocol/message/compression/fixtures/encoder')
/** Bytes the eight arms decode and compare (3 MiB, 232000, 1, 64, 64, 32 MiB, 1000000, 1 MiB). */
const CHECKED_BYTES = 174247040
const ARTIFACTS = 7
const LABELS = { snappy: '', 'snappy~AA': '~AA' }
const EXIT = { accepted: 0, omitted: 0, void: 3 }

/**
 * @param {import('../report').ManifestEntry & { readback?: string }} entry
 * @returns {string[]} the artifacts the receipt reads back, in order
 * @throws {Error} without a receipt, or unless it reads back exactly the committed artifacts
 */
const readbacks = ({ readback }) => {
  if (typeof readback !== 'string' || !readback) throw new Error('the entry names no readback')
  const file = path.resolve(ROOT, readback)
  if (!fs.existsSync(file)) throw new Error(`no Java read-back receipt at ${file}`)
  const names = [...fs.readFileSync(file, 'utf8').matchAll(/^read-back ok (\S+)$/gm)].map(
    (match) => match[1]
  )
  const expected = fs
    .readdirSync(ENCODER_DIR)
    .filter((name) => name.endsWith('.json'))
    .sort()
  if (names.length !== expected.length || expected.length !== ARTIFACTS) {
    throw new Error(
      `${names.length} Java read-backs, expected the ${expected.length} encoder artifacts`
    )
  }
  const missing = expected.filter((name) => !names.includes(name))
  if (missing.length) throw new Error(`the Java read-backs are missing ${missing.join(', ')}`)
  return names
}

/**
 * Reads the session record, then, unless the session is `omitted`, the read-back receipt.
 * @param {{status:number|null,signal?:string,error?:Error,stdout?:string,stderr?:string}} result
 * @param {import('../report').ManifestEntry} entry
 * @returns {object} the session record with the read-back artifacts as `receipt`
 * @throws {Error} for a spawn error, a signal, no session record, a session error, an exit that
 *   disagrees with the session, or a bad receipt
 */
const read = (result, entry) => {
  if (result.error) throw result.error
  if (result.status === null) throw new Error(`killed by ${result.signal}`)
  let record
  try {
    record = lastJsonLine(result.stdout ?? '')
  } catch {
    throw new Error(`exit ${result.status}: ${tail(result.stderr) || 'no session record'}`)
  }
  if (record.schema !== 1 || record.session !== 'snappy') {
    throw new Error(`exit ${result.status}: the last JSON line is not a Snappy session record`)
  }
  if (record.status === 'error') throw new Error(`session error: ${record.reason}`)
  if (EXIT[record.status] !== result.status) {
    throw new Error(`exit ${result.status} disagrees with the ${record.status} session`)
  }
  if (record.status === 'omitted') return record
  return { ...record, receipt: readbacks(entry) }
}

/**
 * Turns the session record into one row per arm and metric, with the read-back, byte and A/A
 * controls.
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm the report's arm, `fork`
 * @param {import('../report').Outcome} outcome
 * @returns {{ rows: import('../report').Row[], detail?: import('../report').Detail }}
 */
const rows = (entry, arm, { outputs, error }) => {
  const one = (status, reason, runs = outputs.length) => ({
    rows: [{ benchmark: entry.name, arm: '-', metric: '-', unit: '-', runs, status, reason }],
  })
  if (error) return one('error', error)
  const [record] = outputs
  if (record.status === 'omitted') return one('omitted', record.reason)
  const stats = Object.keys(LABELS).map((label) => record.arms?.[label])
  if (!stats.every(Boolean) || record.runs !== SESSION.runs) {
    return one('error', `the session lacks an arm or its ${SESSION.runs} measured runs`)
  }
  const all = (key, value) => stats.every((s) => s[key]?.min === value && s[key]?.max === value)
  const controls = [
    {
      arm,
      name: 'seven Java read-backs',
      passed: record.receipt.length === ARTIFACTS,
      detail: `read-back ok ${record.receipt.join(', ')}`,
    },
    {
      arm,
      name: 'seven encoder artifacts',
      passed: all('readbacks', ARTIFACTS),
      detail: stats.map((s) => `${s.readbacks?.min} to ${s.readbacks?.max}`).join(' and '),
    },
    {
      arm,
      name: 'decoded bytes = corpus',
      passed: all('checkedBytes', CHECKED_BYTES),
      detail: `${CHECKED_BYTES} bytes in every observation of both arms`,
    },
  ]
  const spread = Object.entries(record.control?.percent ?? {})
    .map(([key, value]) => `${key} ${value.toFixed(2)} %`)
    .join(', ')
  const notes = [
    `A/A copy against the codec: ${spread} (void over 3 %)`,
    `declared floors and limits missed in ${record.missed} of ${2 * SESSION.runs} measured runs (information; it gates nothing)`,
    `driver sha256 ${record.driver?.sha256}`,
    `node ${record.node}`,
    'every child attempt and every observation are in the raw evidence',
  ]
  const failed = controls.filter(({ passed }) => !passed)
  if (failed.length) {
    const reason = `failed controls: ${failed.map(({ name }) => `${arm} ${name}`).join('; ')}`
    return { ...one('invalid', reason, 1), detail: { status: 'invalid', reason, controls, notes } }
  }
  const status = record.void ? 'inconclusive' : 'valid'
  const reason = record.void ? `the A/A control is void: ${spread}` : undefined
  const lines = Object.entries(LABELS).flatMap(([label, suffix]) =>
    entry.metrics.map(({ key, unit }) => {
      const stat = record.arms[label][key]
      const row = { benchmark: entry.name, arm: `${arm}${suffix}`, metric: key, unit }
      return { ...row, runs: stat?.n, status, median: stat?.median, min: stat?.min, max: stat?.max }
    })
  )
  return { rows: lines, detail: { status, ...(reason && { reason }), controls, notes } }
}

/** One session per report arm; its limit is the session budget plus ten minutes. */
module.exports = { runs: 1, timeoutMs: SESSION.budgetMs + 10 * 60000, read, rows }
