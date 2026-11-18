/**
 * Report adapter of `shareRecordWalk.js`: one run holds two identical-tree sessions of seven
 * interleaved observations of the walk and its expansion control. The walk and control arms show
 * the first session, their `~AA` copies the second. A void A/A comparison (more than 3 % apart in
 * time, heap or total) is `inconclusive`, never a pass and never an error; a failed correctness
 * control is `invalid`; a line that contradicts itself is an `error`. The driver's fixed gates are
 * shown as information and never decide the status.
 */
const { lastJsonLine, tail } = require('../report')
const { DELIVERY_CORRUPTIONS } = require('../shareRecordWalk')

const ARMS = ['walk', 'control']
const AA_KEYS = ['nsPerOffset', 'retainedHeap', 'retainedTotal']
const AA_LIMIT = 0.03
const OBSERVATIONS = 7
const SPREAD = 'A/A spread'

/**
 * @param {string} status the driver's session status
 * @returns {'void'|'failed'|'candidate'|undefined} its kind
 */
const kindOf = (status) =>
  status === 'void' || status === 'failed'
    ? status
    : String(status).startsWith('candidate')
      ? 'candidate'
      : undefined

/**
 * Reads one run. A void or failed session prints its line and exits 1, a candidate exits 0.
 * @param {{status:number|null,signal?:string,error?:Error,stdout?:string,stderr?:string}} result
 * @returns {object} the session line
 * @throws {Error} for a spawn error, a signal, no session line, or an exit that disagrees with it
 */
const read = (result) => {
  if (result.error) throw result.error
  if (result.status === null) throw new Error(`killed by ${result.signal}`)
  let line
  try {
    line = lastJsonLine(result.stdout ?? '')
  } catch {
    throw new Error(`exit ${result.status}: ${tail(result.stderr) || 'no session line'}`)
  }
  const kind = kindOf(line.status)
  if (line.internalAPI !== true || line.timed !== true || !kind) {
    throw new Error(`exit ${result.status}: the last JSON line is not a timed session`)
  }
  if ((kind === 'candidate' ? 0 : 1) !== result.status) {
    throw new Error(`exit ${result.status} disagrees with the ${kind} session`)
  }
  return line
}

/**
 * @param {number[]} values
 * @returns {number} the driver's median: the middle of the sorted values
 */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * @param {number} a first session median
 * @param {number} b second session median
 * @returns {number} the driver's A/A spread, over the smaller median; Infinity without a denominator
 */
const spreadOf = (a, b) => (!a || !b ? Infinity : Math.abs(a - b) / Math.min(a, b))

/**
 * @param {import('../report').ManifestEntry} entry
 * @returns {number} the acquired offsets after `--offsets`
 * @throws {Error} when the command names no supported size
 */
const offsetsOf = ({ command }) => {
  const offsets = Number(command[command.indexOf('--offsets') + 1])
  if (![100000, 1000000].includes(offsets)) throw new Error('the command names no --offsets')
  return offsets
}

/**
 * Checks that a line has seven finite observations per arm and session and that its medians follow
 * from them.
 * @param {object} line the session line
 * @param {number} offsets the declared corpus
 * @param {import('../report').ManifestEntry['metrics']} metrics
 * @returns {string|undefined} why the line cannot be shown, if it cannot
 */
const malformed = (line, offsets, metrics) => {
  if (line.offsets !== offsets)
    return `the session walked ${line.offsets} offsets, expected ${offsets}`
  const keys = new Set([...metrics.map(({ key }) => key), ...AA_KEYS, 'deliveries'])
  for (const session of ['first', 'second'])
    for (const arm of ARMS) {
      const rows = line[session]?.observations?.[arm]
      if (!Array.isArray(rows) || rows.length !== OBSERVATIONS) {
        return `${session} ${arm} has ${rows?.length} observations, expected ${OBSERVATIONS}`
      }
      for (const key of keys) {
        const values = rows.map((row) => row[key])
        if (!values.every(Number.isFinite)) return `${session} ${arm} has no finite ${key}`
        if (line[session].medians?.[arm]?.[key] !== median(values)) {
          return `${session} ${arm} ${key} median disagrees with its observations`
        }
      }
    }
  return undefined
}

/**
 * Turns one run into the row's lines and details.
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
  let offsets
  try {
    offsets = offsetsOf(entry)
  } catch (failure) {
    return one('error', failure.message)
  }
  const [line] = outputs
  const problem = malformed(line, offsets, entry.metrics)
  if (problem) return one('error', problem)

  const spreads = ARMS.flatMap((name) =>
    AA_KEYS.map((key) => ({
      name,
      key,
      spread: spreadOf(line.first.medians[name][key], line.second.medians[name][key]),
    }))
  )
  const over = spreads.filter(({ spread }) => !(spread <= AA_LIMIT))
  const isVoid = line.status === 'void'
  const percent = (spread) => `${(spread * 100).toFixed(2)} %`
  if (isVoid !== over.length > 0) {
    const worst = over[0] ?? spreads.reduce((a, b) => (b.spread > a.spread ? b : a))
    return one('error', `void ${isVoid} disagrees with an A/A spread of ${percent(worst.spread)}`)
  }
  const all = ['first', 'second'].flatMap((session) =>
    ARMS.flatMap((name) => line[session].observations[name])
  )
  const { validArms, rejectedControls } = line.correctness ?? {}
  const controls = [
    {
      arm: 'session',
      name: 'corrupt outputs rejected',
      passed: validArms === 2 && rejectedControls === DELIVERY_CORRUPTIONS.length,
      detail: `${rejectedControls} of ${DELIVERY_CORRUPTIONS.length}`,
    },
    {
      arm: 'session',
      name: 'corpus identity',
      passed: all.every((row) => row.deliveries === offsets / 2),
      detail: `${offsets} acquired offsets, ${offsets / 2} deliveries in every observation`,
    },
    ...ARMS.map((name) => {
      const off = over.filter((row) => row.name === name)
      return {
        arm: name,
        name: SPREAD,
        passed: off.length === 0,
        detail: off.length
          ? off.map(({ key, spread }) => `${key} ${percent(spread)}`).join(', ')
          : `within ${AA_LIMIT * 100} %`,
      }
    }),
  ]
  const failed = controls.filter(({ name, passed }) => !passed && name !== SPREAD)
  const gates = Array.isArray(line.failed) ? line.failed : []
  const notes = [
    `fixed gates ${gates.length ? `missed: ${gates.join('; ')}` : 'held'} (information; it gates nothing)`,
    `node ${line.host?.node}, load ${line.host?.loadBefore?.[0]} before, ${line.host?.loadAfter?.[0]} after`,
  ]
  let status = 'valid'
  let reason
  if (failed.length) {
    status = 'invalid'
    reason = `failed controls: ${failed.map(({ arm: label, name }) => `${label} ${name}`).join('; ')}`
  } else if (isVoid) {
    const { name, key, spread } = over[0]
    status = 'inconclusive'
    reason = `the A/A control is void: ${name} ${key} differs by ${percent(spread)} between sessions`
  }
  const detail = { status, ...(reason && { reason }), controls, notes }
  if (status === 'invalid') return { ...one(status, reason, 1), detail }
  const lines = [
    ...ARMS.map((name) => [name, line.first.observations[name]]),
    ...ARMS.map((name) => [`${name}~AA`, line.second.observations[name]]),
  ].flatMap(([shown, series]) =>
    entry.metrics.map(({ key, unit }) => {
      const values = series.map((row) => row[key])
      return {
        benchmark: entry.name,
        arm: shown,
        metric: key,
        unit,
        runs: values.length,
        status,
        median: median(values),
        min: Math.min(...values),
        max: Math.max(...values),
      }
    })
  )
  return { rows: lines, detail }
}

/** One run holds both sessions. */
module.exports = { runs: 1, timeoutMs: 20 * 60 * 1000, read, rows }
