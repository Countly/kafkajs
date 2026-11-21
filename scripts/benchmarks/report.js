/**
 * Benchmark report: runs every entry of `manifest.json` once per arm and per run, takes the median
 * and the range of each metric, and writes `bench-report.md` and `bench-report.json`. Benchmarks are
 * reported and never judged: a crash, a non-zero exit, bad output or a bad metric is an `error` row,
 * a report is always written, and the exit code stays 0.
 *
 * Every row has a status: `valid`, `inconclusive` (a noisy or void control), `invalid` (a control
 * failed), `error` (the workload failed), `omitted` (not run, with its reason) or `not selected`
 * (left out by `--only`). Only `valid` and `inconclusive` rows carry numbers. An entry's `adapter` names a module in `adapters/` that reads
 * its output instead of the plain reader; an entry's `omitted` (`reason`, `decision`,
 * `evidenceRun`) reports it without running it. Each run's arguments, exit, stdout and stderr, and
 * the manifest entry, are written under `<out>/bench-raw/<entry>/` as the run ends, so the evidence
 * survives a report that never finishes; `{raw}` in a command is a file in that directory.
 *
 * Usage: node scripts/benchmarks/report.js --runs 3 --fork-dir <dir> --baseline-dir <dir> --out <dir>
 *          [--only <name>[,<name>...]]
 * `--fork-dir` and `--baseline-dir` are npm install prefixes: the fork's packed tarball and
 * kafkajs@2.2.4. `--only` runs just the named manifest entries; an empty value runs them all.
 * Exit codes: 0 a report was written, 2 a bad argument (an unknown `--only` name included).
 */
const fs = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { median, setupFailure } = require('./session')
const defaultManifest = require('./manifest.json')

const ROOT = path.resolve(__dirname, '..', '..')
const RUN_TIMEOUT_MS = 15 * 60 * 1000
const MAX_BUFFER = 16 * 1024 * 1024
const BASELINE = 'kafkajs@2.2.4'
const BASELINE_VERSION = '2.2.4'
const PACKAGES = { fork: '@countly/kafkajs', [BASELINE]: 'kafkajs' }
const HEADER = ['benchmark', 'arm', 'metric', 'status', 'median', 'range', 'unit', 'runs', 'ratio']
const STATUSES = ['valid', 'inconclusive', 'invalid', 'error', 'omitted', 'not selected']
const NUMBERED = ['valid', 'inconclusive']
const ADAPTERS = path.join(__dirname, 'adapters')
const USAGE =
  'usage: node scripts/benchmarks/report.js --runs <n> --fork-dir <dir> --baseline-dir <dir> ' +
  '--out <dir> [--only <name>[,<name>...]]'

/**
 * @typedef {'valid'|'inconclusive'|'invalid'|'error'|'omitted'|'not selected'} Status
 * @typedef {{ benchmark: string, arm: string, metric: string, unit: string, runs: number,
 *   status: Status, reason?: string, median?: number, min?: number, max?: number,
 *   ratio?: number }} Row
 * @typedef {{ benchmark?: string, status: Status, reason?: string, raw?: string,
 *   controls: { arm: string, name: string, passed: boolean, detail?: string }[],
 *   notes: string[], table?: { caption: string, header: string[], rows: string[][] } }} Detail
 *   what an adapter shows beside its rows: controls, notes and an optional table of text cells,
 *   shown whatever the row's status
 * @typedef {{ outputs: object[], error?: string }} Outcome what the runs of one arm gave
 * @typedef {{ runs?: number, timeoutMs?: number,
 *   read: (result: object, entry: ManifestEntry, arm: string) => object,
 *   rows: (entry: ManifestEntry, arm: string, outcome: Outcome, runs: number) =>
 *     { rows: Row[], detail?: Detail } }} Adapter `read` turns one spawn result into an output
 *   or throws; `rows` turns an arm's outputs into rows; `runs` and `timeoutMs` override the
 *   report's for every arm
 * @typedef {{ name: string, command: string[], broker: boolean,
 *   metrics: { key: string, unit: string }[], arms: ('fork'|'kafkajs@2.2.4')[],
 *   adapter?: string, omitted?: { reason: string, decision: string, evidenceRun: string } }}
 *   ManifestEntry
 */

/**
 * @param {string|string[]} stdout the output of a benchmark, or its lines
 * @returns {object} the last line that parses as a JSON object
 * @throws {Error} when no line does
 */
const lastJsonLine = (stdout) => {
  const lines = Array.isArray(stdout) ? stdout : String(stdout).split('\n')
  for (let i = lines.length - 1; i >= 0; i--) {
    try {
      const value = JSON.parse(lines[i])
      if (value !== null && typeof value === 'object' && !Array.isArray(value)) return value
    } catch {
      // Not JSON: a log line.
    }
  }
  throw new Error('no line of the output is a JSON object')
}

/**
 * @param {object} obj a benchmark's JSON line
 * @param {string} dotted a dotted path, such as `sizes.16.nsPerOp`
 * @returns {number|undefined} the number there; undefined unless it is an own, finite number
 */
const pick = (obj, dotted) => {
  let value = obj
  for (const key of dotted.split('.')) {
    if (value === null || typeof value !== 'object' || !Object.hasOwn(value, key)) return undefined
    value = value[key]
  }
  return Number.isFinite(value) ? value : undefined
}

/**
 * @param {string[]} command manifest arguments
 * @param {{ client: string, package: string, baselinePackageDir: string, raw: string }} values
 *   placeholder values
 * @returns {string[]} the arguments with `{client}`, `{package}`, `{baselinePackageDir}` and
 *   `{raw}` replaced
 */
const fill = (command, values) =>
  command.map((arg) =>
    arg.replace(/\{(client|package|baselinePackageDir|raw)\}/g, (_, name) => values[name])
  )

/**
 * @param {string} file
 * @param {unknown} value
 * @returns {void} `value` written to `file` as indented JSON
 */
const writeJson = (file, value) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`)

/**
 * @param {string|undefined} stderr
 * @returns {string} the last 300 characters of `stderr` on one line
 */
const tail = (stderr) =>
  String(stderr ?? '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(-300)

/**
 * @param {object} output the JSON line of a two-arm benchmark
 * @param {string} arm the arm that was run
 * @throws {Error} when the package the benchmark loaded is not the one the arm names
 */
const checkLoaded = (output, arm) => {
  const version = arm === 'fork' ? '' : `@${BASELINE_VERSION}`
  const matches =
    output.package === PACKAGES[arm] && (!version || output.version === BASELINE_VERSION)
  if (!matches) {
    throw new Error(
      `loaded ${output.package ?? 'no package'}@${output.version ?? 'no version'}, expected ${PACKAGES[arm]}${version}`
    )
  }
}

/**
 * @param {object} result what `spawn` returned
 * @param {ManifestEntry} entry
 * @param {string} arm
 * @returns {object} the benchmark's JSON line
 * @throws {Error} for a spawn error or timeout, a signal, a non-zero exit, no JSON line or a wrong package
 */
const readRun = (result, entry, arm) => {
  if (result.error) throw result.error
  if (result.status === null) throw new Error(`killed by ${result.signal}`)
  if (result.status !== 0) {
    throw new Error(['exit ' + result.status, tail(result.stderr)].filter(Boolean).join(': '))
  }
  const output = lastJsonLine(result.stdout)
  if (entry.arms.length === 2) checkLoaded(output, arm)
  return output
}

/**
 * Runs `entry` for one arm, `runs` times in a row, and stops at the first run that fails. After
 * each run, every run so far (arguments, exit, stdout, stderr) is rewritten to `<rawDir>/<arm>.json`.
 * @param {ManifestEntry} entry
 * @param {string} arm
 * @param {Adapter} adapter reads each run's result
 * @param {{ runs: number, forkDir: string, baselineDir: string, spawn: Function,
 *   rawDir: string }} options `rawDir` is the entry's evidence directory
 * @returns {Outcome} what the runs that succeeded gave
 */
const runArm = (entry, arm, adapter, { runs, forkDir, baselineDir, spawn, rawDir }) => {
  const evidence = { arm, runs: [] }
  const outputs = []
  for (let n = 1; n <= runs; n++) {
    const args = fill(entry.command, {
      client: arm === 'fork' ? forkDir : baselineDir,
      package: PACKAGES[arm],
      baselinePackageDir: path.join(baselineDir, 'node_modules', 'kafkajs'),
      raw: path.join(rawDir, `${arm}-run-${n}.jsonl`),
    })
    const timeout = adapter.timeoutMs ?? RUN_TIMEOUT_MS
    const options = { cwd: ROOT, encoding: 'utf8', timeout, maxBuffer: MAX_BUFFER }
    const result = spawn(process.execPath, args, options)
    evidence.runs.push({
      run: n,
      args,
      status: result.status,
      signal: result.signal ?? null,
      error: result.error?.message ?? null,
      stdout: result.stdout ?? '',
      stderr: result.stderr ?? '',
    })
    writeJson(path.join(rawDir, `${arm}.json`), evidence)
    try {
      outputs.push(adapter.read(result, entry, arm))
    } catch (error) {
      return { outputs, error: `run ${n} of ${runs}: ${error.message}` }
    }
  }
  return { outputs }
}

/**
 * @param {ManifestEntry} entry
 * @param {string} arm
 * @param {Outcome} outcome
 * @param {number} runs
 * @returns {Row[]} one row per metric: `valid` with numbers, or `error` with its reason
 */
const rowsOf = (entry, arm, { outputs, error }, runs) =>
  entry.metrics.map(({ key, unit }) => {
    const row = { benchmark: entry.name, arm, metric: key, unit, runs: outputs.length }
    if (error) return { ...row, status: 'error', reason: error }
    const values = outputs.map((output) => pick(output, key))
    const bad = values.indexOf(undefined)
    if (bad !== -1) {
      const reason = `run ${bad + 1} of ${runs}: ${key} is not a finite number`
      return { ...row, status: 'error', reason }
    }
    const stats = { median: median(values), min: Math.min(...values), max: Math.max(...values) }
    return { ...row, status: 'valid', ...stats }
  })

/** The reader of an entry without an `adapter`: one JSON line of numbers per run. */
const plain = {
  read: readRun,
  rows: (entry, arm, outcome, runs) => ({ rows: rowsOf(entry, arm, outcome, runs) }),
}

/**
 * @param {string|undefined} name an entry's `adapter`
 * @returns {Adapter} `adapters/<name>.js`, or the plain reader without a name
 * @throws {Error} for a name that is not a plain module name or names no adapter
 */
const loadAdapter = (name) => {
  if (name === undefined) return plain
  const file = path.join(ADAPTERS, `${name}.js`)
  if (!/^[A-Za-z0-9]+$/.test(name) || !fs.existsSync(file)) {
    throw new Error(`no adapter ${name} in scripts/benchmarks/adapters`)
  }
  return require(file)
}

/**
 * @param {ManifestEntry} entry
 * @param {Status} status
 * @param {string} reason
 * @param {number} [runs]
 * @returns {Row} the one row of an entry that has no numbers to show
 */
const entryRow = (entry, status, reason, runs = 0) => ({
  benchmark: entry.name,
  arm: '-',
  metric: '-',
  unit: '-',
  runs,
  status,
  reason,
})

/**
 * @param {ManifestEntry} entry an entry with `omitted`
 * @returns {Row} its `omitted` row, or an `error` row when it lacks its reason, decision or
 *   evidence run
 */
const omittedRow = (entry) => {
  const { reason, decision, evidenceRun } = entry.omitted
  const named = [reason, decision, evidenceRun].every((text) => typeof text === 'string' && text)
  return named
    ? entryRow(entry, 'omitted', `${reason} under ${decision}, evidence run ${evidenceRun}`)
    : entryRow(entry, 'error', 'an omitted entry needs its reason, decision and evidenceRun')
}

/**
 * Keeps the status rules whatever an adapter returned: a known status, and numbers only on a
 * `valid` or `inconclusive` row, which must have a finite median. An `inconclusive` row with a
 * reason and no numbers at all stays as it is: a session whose observations formed no
 * measurement.
 * @param {Row} row
 * @returns {Row}
 */
const finalize = (row) => {
  const { median: m, min, max, ratio, ...bare } = row
  if (!STATUSES.includes(row.status)) {
    return { ...bare, status: 'error', reason: `unknown status ${row.status}` }
  }
  if (!NUMBERED.includes(row.status)) return bare
  const bareInconclusive = row.status === 'inconclusive' && row.reason
  if (bareInconclusive && [m, min, max, ratio].every((n) => n === undefined)) return bare
  if (![m, min, max].every(Number.isFinite)) {
    return { ...bare, status: 'error', reason: `a ${row.status} row without its numbers` }
  }
  return row
}

/**
 * Sets `ratio` (fork median over the 2.2.4 median) on the fork rows that have a 2.2.4 counterpart.
 * It stays unset when either median is missing or the 2.2.4 median is 0.
 * @param {Row[]} rows the rows of one entry
 * @returns {void}
 */
const addRatios = (rows) => {
  const baseline = new Map(
    rows.filter((row) => row.arm === BASELINE).map((row) => [row.metric, row])
  )
  for (const row of rows) {
    const other = row.arm === 'fork' ? baseline.get(row.metric) : undefined
    if (other?.median && row.median !== undefined) row.ratio = row.median / other.median
  }
}

/**
 * Runs one manifest entry on each of its arms, unless it is `omitted`, and writes the entry to
 * `<rawDir>/<name>/entry.json` first.
 * `{client}` is the arm's install prefix, `{package}` its package name, `{baselinePackageDir}`
 * `<baselineDir>/node_modules/kafkajs`, the installed 2.2.4 package (not the fork's scoped one),
 * and `{raw}` a file of the run in the entry's evidence directory.
 * A two-arm entry's output must name the package it loaded, the arm's own.
 * @param {ManifestEntry} entry
 * @param {{ runs: number, forkDir: string, baselineDir: string, spawn: Function,
 *   rawDir: string }} options `spawn` has the signature of `child_process.spawnSync`; the
 *   directories are install prefixes; `rawDir` holds every entry's evidence
 * @returns {{ rows: Row[], details: Detail[] }} the rows, one per (arm, metric) unless the
 *   adapter says otherwise, and what the adapter shows beside them
 */
const runEntry = (entry, options) => {
  const rawDir = path.join(options.rawDir, entry.name)
  fs.mkdirSync(rawDir, { recursive: true })
  writeJson(path.join(rawDir, 'entry.json'), entry)
  if (entry.omitted !== undefined) return { rows: [omittedRow(entry)], details: [] }
  let adapter
  try {
    adapter = loadAdapter(entry.adapter)
  } catch (error) {
    return { rows: [entryRow(entry, 'error', error.message)], details: [] }
  }
  const runs = adapter.runs ?? options.runs
  const results = entry.arms.map((arm) => {
    const outcome = runArm(entry, arm, adapter, { ...options, runs, rawDir })
    try {
      return adapter.rows(entry, arm, outcome, runs)
    } catch (error) {
      return { rows: [entryRow(entry, 'error', `the ${arm} rows failed: ${error.message}`)] }
    }
  })
  const rows = results.flatMap((result) => result.rows).map(finalize)
  addRatios(rows)
  const raw = `bench-raw/${entry.name}/`
  const details = results.flatMap(({ detail }) =>
    detail ? [{ benchmark: entry.name, ...detail, raw }] : []
  )
  return { rows, details }
}

/**
 * The row of an entry its workflow job could not prepare: written to its evidence directory with
 * the setup outcomes, and never run.
 * @param {ManifestEntry} entry
 * @param {string} reason what `setupFailure` returned
 * @param {string} setup the job's `BENCH_SETUP`
 * @param {string} rawDir every entry's evidence directory
 * @returns {{ rows: Row[], details: Detail[] }}
 */
const setupRow = (entry, reason, setup, rawDir) => {
  const dir = path.join(rawDir, entry.name)
  fs.mkdirSync(dir, { recursive: true })
  writeJson(path.join(dir, 'entry.json'), entry)
  writeJson(path.join(dir, 'setup.json'), { setup, reason })
  return { rows: [entryRow(entry, 'error', reason)], details: [] }
}

/**
 * @param {string|number} text
 * @returns {string} `text` on one line with its pipes escaped, safe in a table cell or list item
 */
const cell = (text) => String(text).replace(/\s+/g, ' ').replace(/\|/g, '\\|')

/**
 * @param {number} n
 * @returns {string} `n` to six significant digits
 */
const fmt = (n) => String(Number(n.toPrecision(6)))

/**
 * @param {Detail} detail
 * @returns {string[]} the Markdown section of one adapter's controls, notes and table
 */
const renderDetail = ({ benchmark, status, reason, raw, controls, notes, table }) => [
  '',
  `### ${cell(benchmark)}: ${status}`,
  '',
  ...(reason ? [`- ${cell(reason)}`] : []),
  `- raw evidence: ${raw}`,
  ...controls.map(
    ({ arm, name, passed, detail }) =>
      `- ${cell(`control ${arm} ${name}: ${passed ? 'passed' : 'FAILED'}${detail ? ` (${detail})` : ''}`)}`
  ),
  ...notes.map((note) => `- ${cell(note)}`),
  ...(table
    ? [
        '',
        cell(table.caption),
        '',
        ...[table.header, table.header.map(() => '---'), ...table.rows].map(
          (cells) => `| ${cells.map(cell).join(' | ')} |`
        ),
      ]
    : []),
]

/**
 * @param {Row[]} rows
 * @param {Record<string, string|number>} meta lines shown above the table
 * @param {Detail[]} [details] adapters' controls and notes
 * @returns {string} the report as Markdown: metadata, the table with each row's status (numbers
 *   only beside `valid` and `inconclusive`), every reason, and the adapters' details
 */
const renderMarkdown = (rows, meta, details = []) => {
  const line = (cells) => `| ${cells.map(cell).join(' | ')} |`
  const body = rows.map((row) => {
    const numbered = NUMBERED.includes(row.status) && row.median !== undefined
    return [
      row.benchmark,
      row.arm,
      row.metric,
      row.status,
      numbered ? fmt(row.median) : '-',
      numbered ? `${fmt(row.min)} to ${fmt(row.max)}` : '-',
      row.unit,
      row.runs,
      row.ratio === undefined ? '-' : fmt(row.ratio),
    ]
  })
  const reasons = rows
    .filter((row) => row.reason)
    .map((row) => {
      const where =
        row.arm === '-' ? row.benchmark : `${row.benchmark} / ${row.arm} / ${row.metric}`
      return `- ${cell(`${where}: ${row.status}: ${row.reason}`)}`
    })
  return [
    '# Benchmark report',
    '',
    ...Object.entries(meta).map(([key, value]) => `- ${key}: ${value}`),
    '',
    line(HEADER),
    line(HEADER.map(() => '---')),
    ...body.map(line),
    ...(reasons.length ? ['', '## Reasons', '', ...reasons] : []),
    ...(details.length ? ['', '## Controls and notes', ...details.flatMap(renderDetail)] : []),
    '',
  ].join('\n')
}

/**
 * @param {string[]} argv
 * @returns {{ runs: number, forkDir: string, baselineDir: string, out: string,
 *   only: string[]|null }} `only` is null when `--only` is absent or empty
 * @throws {Error} the usage line for a missing, repeated or unknown flag or a bad `--runs`
 */
const parseArgs = (argv) => {
  const flags = {}
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i]
    const known = ['--runs', '--fork-dir', '--baseline-dir', '--out', '--only'].includes(name)
    if (!known || argv[i + 1] === undefined || Object.hasOwn(flags, name)) throw new Error(USAGE)
    flags[name] = argv[i + 1]
  }
  const runs = Number(flags['--runs'])
  const missing = ['--fork-dir', '--baseline-dir', '--out'].some((name) => !flags[name])
  if (!Number.isInteger(runs) || runs < 1 || missing) throw new Error(USAGE)
  return {
    runs,
    forkDir: path.resolve(flags['--fork-dir']),
    baselineDir: path.resolve(flags['--baseline-dir']),
    out: path.resolve(flags['--out']),
    only: flags['--only'] ? flags['--only'].split(',') : null,
  }
}

/**
 * Runs the manifest, or the entries `--only` names, and writes `bench-report.md` and
 * `bench-report.json` into `--out`, with each run entry's raw evidence under `--out/bench-raw/`.
 * An entry `--only` leaves out is a `not selected` row and is neither run nor given evidence.
 * In a workflow job whose setup steps did not all succeed (`BENCH_SETUP`, space-separated
 * `<step>=<outcome>` pairs), every selected entry is an `error` row naming those steps and is not
 * run; its evidence directory keeps the entry and a `setup.json` receipt.
 * @param {string[]} argv `--runs`, `--fork-dir`, `--baseline-dir`, `--out` and `--only`
 * @param {object} [deps]
 * @param {Function} [deps.spawn] `child_process.spawnSync`, replaceable in tests
 * @param {{ entries: ManifestEntry[] }} [deps.manifest] the benchmarks to run
 * @param {(line: string) => void} [deps.log] receives the usage line and one line per entry
 * @param {Record<string, string|undefined>} [deps.env] the environment, `process.env` by default
 * @returns {number} 0 once the report is written, whatever its rows hold; 2 for a bad argument
 *   or an `--only` name that is no manifest entry
 */
const main = (
  argv,
  { spawn = spawnSync, manifest = defaultManifest, log = console.error, env = process.env } = {}
) => {
  let args
  try {
    args = parseArgs(argv)
  } catch (error) {
    log(error.message)
    return 2
  }
  const unknown = args.only?.find((name) => !manifest.entries.some((entry) => entry.name === name))
  if (unknown !== undefined) {
    log(`--only names no manifest row ${unknown}`)
    return 2
  }
  const rawDir = path.join(args.out, 'bench-raw')
  fs.mkdirSync(rawDir, { recursive: true })
  const setup = setupFailure(env.BENCH_SETUP)
  const results = manifest.entries.map((entry) => {
    if (args.only && !args.only.includes(entry.name)) {
      return { rows: [entryRow(entry, 'not selected', 'left out by --only')], details: [] }
    }
    log(entry.name)
    if (setup) return setupRow(entry, setup, env.BENCH_SETUP, rawDir)
    return runEntry(entry, { ...args, spawn, rawDir })
  })
  const rows = results.flatMap((result) => result.rows)
  const details = results.flatMap((result) => result.details)
  const meta = {
    runs: args.runs,
    ...(args.only && { only: args.only }),
    node: process.version,
    ...(process.env.KAFKA_VERSION && { kafka: process.env.KAFKA_VERSION }),
    ...(process.env.GITHUB_SHA && { commit: process.env.GITHUB_SHA }),
    date: new Date().toISOString(),
  }
  writeJson(path.join(args.out, 'bench-report.json'), { meta, rows, details })
  fs.writeFileSync(path.join(args.out, 'bench-report.md'), renderMarkdown(rows, meta, details))
  return 0
}

module.exports = { lastJsonLine, pick, tail, runEntry, renderMarkdown, main }

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2))
}
