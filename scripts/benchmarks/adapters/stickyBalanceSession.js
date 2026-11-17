/**
 * Report adapter of the `stickyBalance.js --session` rows: one session per arm of the report,
 * which runs every partitioner arm of its scenario and an A/A copy of the reference in its own
 * processes and prints one JSON line (`scenario`, `arms`, `control`, `void`, `verdict`,
 * `reasons`). The row shows each partitioner arm's median and range of the manifest metrics and
 * the controls of every arm. A session line that is incomplete or contradicts itself is an `error`,
 * a failed control makes it `invalid`, a void A/A control or an inconclusive session
 * `inconclusive`. The session's verdict is shown and never decides the status.
 */
const { lastJsonLine, tail } = require('../report')

/** Each scenario's arms, its reference, its records per run and the arms that must split evenly. */
const SCENARIOS = {
  healthy: {
    arms: ['round-robin', 'sticky', 'adaptive'],
    reference: 'round-robin',
    records: 100000,
    even: ['round-robin', 'round-robin~AA'],
  },
  'slow-broker': { arms: ['sticky', 'adaptive'], reference: 'sticky', records: 120000, even: [] },
}
/** The session's void limit (`session.js`), in percent of the reference's p99 median. */
const VOID_OVER = 3
/** The most a round-robin split may stray from even: its positive control. */
const EVEN_BALANCE = 1.05
/** The stats the controls read, checked like the manifest metrics. */
const CONTROL_KEYS = [
  'records',
  'partitionerCalls',
  'keyedIdentical',
  'topologyValid',
  'gcAvailable',
  'valid',
  'balance',
  'p99Ms',
]
/** The exit code `stickyBalance.js --session` gives each verdict. */
const EXIT_OF = { passed: 0, failed: 1, inconclusive: 3 }
const SPREAD = 'A/A spread'

/**
 * @typedef {{ median: number, min: number, max: number, n: number }} Stat
 * @typedef {Record<string, Stat>} ArmStats
 */

/**
 * @param {Stat|undefined} stat
 * @returns {string} `min to max`, or `missing`
 */
const range = (stat) => (stat ? `${stat.min} to ${stat.max}` : 'missing')

/**
 * @param {number} value
 * @returns {string} `value` as a signed percent with two decimals, such as `+0.10 %`
 */
const percent = (value) => `${value >= 0 ? '+' : ''}${value.toFixed(2)} %`

/** Each arm's controls: a name, whether its stats pass it, and what to show. */
const CONTROLS = [
  {
    name: 'partitioner calls = records',
    passed: (s) =>
      Number.isInteger(s.records?.min) &&
      s.partitionerCalls?.min === s.records.min &&
      s.partitionerCalls?.max === s.records.max,
    detail: (s) => `calls ${range(s.partitionerCalls)}, records ${range(s.records)}`,
  },
  { name: 'keyed mapping identical', passed: (s) => s.keyedIdentical?.min === 1 },
  { name: 'topology valid', passed: (s) => s.topologyValid?.min === 1 },
  { name: 'GC available', passed: (s) => s.gcAvailable?.min === 1 },
  { name: 'every observation valid', passed: (s) => s.valid?.min === 1 },
]

/**
 * @typedef {{ name: string, arms: string[], reference: string, records: number, even: string[],
 *   runs: number }} Scenario
 */

/**
 * @param {import('../report').ManifestEntry} entry
 * @returns {Scenario} the scenario named after `--scenario`, with the measured runs after `--runs`
 * @throws {Error} when the command names no known scenario or no positive `--runs`
 */
const scenarioOf = ({ command }) => {
  const after = (flag) => (command.includes(flag) ? command[command.indexOf(flag) + 1] : undefined)
  const name = after('--scenario')
  if (!Object.hasOwn(SCENARIOS, name ?? '')) {
    throw new Error('the command names no --scenario healthy or slow-broker')
  }
  const runs = Number(after('--runs'))
  if (!Number.isInteger(runs) || runs < 1) throw new Error('the command names no --runs')
  return { name, ...SCENARIOS[name], runs }
}

/**
 * Reads one session's result. Exit 1 and 3 are a finished session that failed or was
 * inconclusive, so they are read like exit 0; any other exit, or no session line, is an error.
 * @param {{ status: number|null, signal?: string, error?: Error, stdout?: string,
 *   stderr?: string }} result what `spawnSync` returned
 * @returns {object} the session's JSON line
 * @throws {Error} for a spawn error, a signal, no session line, or an exit that disagrees with
 *   the verdict
 */
const read = (result) => {
  if (result.error) throw result.error
  if (result.status === null) throw new Error(`killed by ${result.signal}`)
  const failure = [`exit ${result.status}`, tail(result.stderr) || 'no session report']
  let output
  try {
    output = lastJsonLine(result.stdout ?? '')
  } catch {
    throw new Error(failure.join(': '))
  }
  if (!Object.hasOwn(EXIT_OF, output.verdict) || typeof output.arms !== 'object' || !output.arms) {
    throw new Error(`exit ${result.status}: the last JSON line is not a session report`)
  }
  if (EXIT_OF[output.verdict] !== result.status) {
    throw new Error(`exit ${result.status} disagrees with the ${output.verdict} verdict`)
  }
  return output
}

/**
 * Checks that a session line describes the requested session and agrees with itself: its
 * scenario, a boolean `void`, every arm, every shown or controlled stat finite and ordered over
 * exactly the declared runs, the scenario's records per run, and an A/A spread and `void` that
 * follow from the p99 medians by the session's rule.
 * @param {object} report a session line
 * @param {Scenario} scenario
 * @param {string[]} labels the arms it must have, A/A copy included
 * @param {import('../report').ManifestEntry['metrics']} metrics
 * @returns {string|undefined} why the line cannot be shown, if it cannot
 */
const malformed = (report, { name, reference, records, runs }, labels, metrics) => {
  if (report.scenario !== name) {
    return `the session reports scenario ${report.scenario}, expected ${name}`
  }
  if (typeof report.void !== 'boolean') {
    return 'the session does not say whether its A/A control is void'
  }
  const found = Object.keys(report.arms).sort()
  if (found.join() !== [...labels].sort().join()) {
    return `the session has arms ${found.join(', ')}, expected ${labels.join(', ')}`
  }
  const keys = new Set([...metrics.map(({ key }) => key), ...CONTROL_KEYS])
  for (const label of labels) {
    for (const key of keys) {
      const { median, min, max, n } = report.arms[label][key] ?? {}
      if (![median, min, max].every(Number.isFinite) || min > median || median > max) {
        return `${label} has no ordered finite ${key}`
      }
      if (n !== runs) return `${label} ${key} has ${n} runs, expected ${runs}`
      if (key === 'records' && (min !== records || max !== records)) {
        return `${label} records ${range({ min, max })}, expected ${records}`
      }
    }
  }
  const { control } = report
  if (
    control?.label !== `${reference}~AA` ||
    control.against !== reference ||
    !Number.isFinite(control.percent?.p99Ms)
  ) {
    return 'the session has no A/A comparison on p99Ms'
  }
  const ref = report.arms[reference].p99Ms.median
  const copy = report.arms[control.label].p99Ms.median
  const spread = (copy / ref - 1) * 100
  if (!(Math.abs(spread - control.percent.p99Ms) <= 1e-6)) {
    return `the A/A spread ${percent(control.percent.p99Ms)} disagrees with the medians (${percent(spread)})`
  }
  if (report.void !== Math.abs(copy - ref) > Math.abs(ref) * (VOID_OVER / 100 + 1e-9)) {
    return `void ${report.void} disagrees with an A/A spread of ${percent(spread)}`
  }
  return undefined
}

/**
 * Turns one session into the row's lines and details.
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm the report's arm (the package), `fork`
 * @param {import('../report').Outcome} outcome
 * @returns {{ rows: import('../report').Row[], detail?: import('../report').Detail }}
 */
const rows = (entry, arm, { outputs, error }) => {
  const one = (status, reason, runs = outputs.length) => ({
    rows: [{ benchmark: entry.name, arm: '-', metric: '-', unit: '-', runs, status, reason }],
  })
  if (error) return one('error', error)
  let scenario
  try {
    scenario = scenarioOf(entry)
  } catch (failure) {
    return one('error', failure.message)
  }
  const [report] = outputs
  const { arms, reference, even } = scenario
  const labels = [...arms, `${reference}~AA`]
  const problem = malformed(report, scenario, labels, entry.metrics)
  if (problem) return one('error', problem)

  const controls = labels.flatMap((label) =>
    CONTROLS.map(({ name, passed, detail }) => ({
      arm: label,
      name,
      passed: passed(report.arms[label]),
      ...(detail && { detail: detail(report.arms[label]) }),
    }))
  )
  for (const label of even) {
    const { median } = report.arms[label].balance
    controls.push({
      arm: label,
      name: 'records split evenly',
      passed: median <= EVEN_BALANCE,
      detail: `balance median ${median}, at most ${EVEN_BALANCE}`,
    })
  }
  controls.push({
    arm: report.control.label,
    name: SPREAD,
    passed: !report.void,
    detail: `p99Ms ${percent(report.control.percent.p99Ms)} from ${reference}`,
  })
  const failed = controls.filter(({ name, passed }) => !passed && name !== SPREAD)
  const reasons = Array.isArray(report.reasons) ? report.reasons : []
  const notes = [
    `session verdict ${report.verdict}${reasons.length ? `: ${reasons.join('; ')}` : ''}` +
      ' (information; it gates nothing)',
  ]
  let status = 'valid'
  let reason
  if (failed.length) {
    status = 'invalid'
    reason = `failed controls: ${failed.map(({ arm: label, name }) => `${label} ${name}`).join('; ')}`
  } else if (report.void) {
    status = 'inconclusive'
    reason = `the A/A control is void: ${controls.at(-1).detail}`
  } else if (report.verdict === 'inconclusive') {
    status = 'inconclusive'
    reason = `the session is inconclusive: ${reasons.join('; ')}`
  }
  const detail = { status, ...(reason && { reason }), controls, notes }
  if (status === 'invalid') return { ...one(status, reason, 1), detail }
  return {
    rows: labels.flatMap((label) =>
      entry.metrics.map(({ key, unit }) => {
        const { median, min, max, n } = report.arms[label][key]
        return {
          benchmark: entry.name,
          arm: label,
          metric: key,
          unit,
          runs: n,
          status,
          median,
          min,
          max,
        }
      })
    ),
    detail,
  }
}

/** One session per report arm: the session repeats every partitioner arm itself. */
module.exports = { runs: 1, timeoutMs: 3 * 60 * 60 * 1000, read, rows }
