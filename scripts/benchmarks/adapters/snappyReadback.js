/**
 * Report adapter of the `snappyCodec.js` row. Before a run is read, the entry's `readback` receipt
 * (the log of `GenerateSnappyVectors --read-back` over the committed encoder artifacts, written by
 * the workflow) must show `read-back ok` for exactly the seven artifacts; otherwise the run is an
 * `error`. A run that decodes other bytes than its corpus or counts other than seven artifacts is
 * `invalid`. The driver's declared floors and limits exit 1 with their line; they are shown as
 * information and never decide the status.
 */
const fs = require('node:fs')
const path = require('node:path')
const { lastJsonLine, tail } = require('../report')
const { median } = require('../session')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const ENCODER_DIR = path.join(ROOT, 'src/protocol/message/compression/fixtures/encoder')
/** Bytes the eight arms decode and compare (3 MiB, 232000, 1, 64, 64, 32 MiB, 1000000, 1 MiB). */
const CHECKED_BYTES = 174247040
const ARTIFACTS = 7

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
 * Reads one run after the receipt. Exit 1 with a line that counts failed gates is a finished run.
 * @param {{status:number|null,signal?:string,error?:Error,stdout?:string,stderr?:string}} result
 * @param {import('../report').ManifestEntry} entry
 * @returns {object} the run's line, its exit and the read-back artifacts
 * @throws {Error} for a bad receipt, a spawn error, a signal, or a failure without a gate line
 */
const read = (result, entry) => {
  const receipt = readbacks(entry)
  if (result.error) throw result.error
  if (result.status === null) throw new Error(`killed by ${result.signal}`)
  let line = null
  try {
    line = lastJsonLine(result.stdout ?? '')
  } catch {
    // Judged below with the exit code.
  }
  const finished =
    line && (result.status === 0 ? line.failures === 0 : result.status === 1 && line.failures > 0)
  if (!finished) {
    throw new Error(`exit ${result.status}: ${tail(result.stderr) || 'no snappyCodec line'}`)
  }
  return { ...line, exit: result.status, receipt }
}

/**
 * Turns the runs into one row per metric, with the read-back and byte controls.
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
  const controls = [
    {
      arm,
      name: 'seven Java read-backs',
      passed: outputs.every(({ receipt }) => receipt.length === ARTIFACTS),
      detail: `read-back ok ${outputs[0].receipt.join(', ')}`,
    },
    {
      arm,
      name: 'seven encoder artifacts',
      passed: outputs.every(({ readbacks: count }) => count === ARTIFACTS),
      detail: `${outputs.map(({ readbacks: count }) => count).join(', ')} counted`,
    },
    {
      arm,
      name: 'decoded bytes = corpus',
      passed: outputs.every(({ checkedBytes }) => checkedBytes === CHECKED_BYTES),
      detail: `${CHECKED_BYTES} bytes in every run`,
    },
  ]
  const missed = outputs.filter(({ failures }) => failures > 0).length
  const notes = [
    `declared floors and limits ${missed ? `missed in ${missed} of ${outputs.length} runs` : 'held'} (information; it gates nothing)`,
  ]
  const failed = controls.filter(({ passed }) => !passed)
  if (failed.length) {
    const reason = `failed controls: ${failed.map(({ name }) => `${arm} ${name}`).join('; ')}`
    return { ...one('invalid', reason, 1), detail: { status: 'invalid', reason, controls, notes } }
  }
  const lines = entry.metrics.map(({ key, unit }) => {
    const values = outputs.map((output) => output[key])
    const row = { benchmark: entry.name, arm, metric: key, unit, runs: values.length }
    if (!values.every(Number.isFinite))
      return { ...row, status: 'error', reason: `${key} is not a finite number` }
    return {
      ...row,
      status: 'valid',
      median: median(values),
      min: Math.min(...values),
      max: Math.max(...values),
    }
  })
  return { rows: lines, detail: { status: 'valid', controls, notes } }
}

module.exports = { timeoutMs: 90 * 60 * 1000, read, rows }
