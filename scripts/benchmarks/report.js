/**
 * Benchmark report: runs every entry of `manifest.json` once per arm and per run, takes the median
 * and the range of each metric, and writes `bench-report.md` and `bench-report.json`. Benchmarks are
 * reported and never judged: a crash, a non-zero exit, bad output or a bad metric is an `error` row,
 * a report is always written, and the exit code stays 0.
 *
 * Usage: node scripts/benchmarks/report.js --runs 3 --fork-dir <dir> --baseline-dir <dir> --out <dir>
 * `--fork-dir` and `--baseline-dir` are npm install prefixes: the fork's packed tarball and
 * kafkajs@2.2.4. Exit codes: 0 a report was written, 2 a bad argument.
 */
const fs = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { median } = require('./session')
const defaultManifest = require('./manifest.json')

const ROOT = path.resolve(__dirname, '..', '..')
const RUN_TIMEOUT_MS = 15 * 60 * 1000
const MAX_BUFFER = 16 * 1024 * 1024
const BASELINE = 'kafkajs@2.2.4'
const BASELINE_VERSION = '2.2.4'
const PACKAGES = { fork: '@countly/kafkajs', [BASELINE]: 'kafkajs' }
const HEADER = ['benchmark', 'arm', 'metric', 'median', 'range', 'unit', 'runs', 'ratio']
const USAGE =
  'usage: node scripts/benchmarks/report.js --runs <n> --fork-dir <dir> --baseline-dir <dir> --out <dir>'

/**
 * @typedef {{ benchmark: string, arm: string, metric: string, unit: string, runs: number,
 *   median?: number, min?: number, max?: number, ratio?: number, error?: string }} Row
 * @typedef {{ name: string, command: string[], broker: boolean,
 *   metrics: { key: string, unit: string }[], arms: ('fork'|'kafkajs@2.2.4')[] }} ManifestEntry
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
 * @param {{ client: string, package: string, baselinePackageDir: string }} values placeholder values
 * @returns {string[]} the arguments with `{client}`, `{package}` and `{baselinePackageDir}` replaced
 */
const fill = (command, values) =>
  command.map((arg) =>
    arg.replace(/\{(client|package|baselinePackageDir)\}/g, (_, name) => values[name])
  )

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
 * Runs `entry` for one arm, `runs` times in a row, and stops at the first run that fails.
 * @param {ManifestEntry} entry
 * @param {string} arm
 * @param {{ runs: number, forkDir: string, baselineDir: string, spawn: Function }} options
 * @returns {{ outputs: object[], error?: string }} the JSON lines of the runs that succeeded
 */
const runArm = (entry, arm, { runs, forkDir, baselineDir, spawn }) => {
  const args = fill(entry.command, {
    client: arm === 'fork' ? forkDir : baselineDir,
    package: PACKAGES[arm],
    baselinePackageDir: path.join(baselineDir, 'node_modules', 'kafkajs'),
  })
  const outputs = []
  for (let n = 1; n <= runs; n++) {
    const options = { cwd: ROOT, encoding: 'utf8', timeout: RUN_TIMEOUT_MS, maxBuffer: MAX_BUFFER }
    try {
      outputs.push(readRun(spawn(process.execPath, args, options), entry, arm))
    } catch (error) {
      return { outputs, error: `run ${n} of ${runs}: ${error.message}` }
    }
  }
  return { outputs }
}

/**
 * @param {ManifestEntry} entry
 * @param {string} arm
 * @param {{ outputs: object[], error?: string }} outcome
 * @param {number} runs
 * @returns {Row[]} one row per metric
 */
const rowsOf = (entry, arm, { outputs, error }, runs) =>
  entry.metrics.map(({ key, unit }) => {
    const row = { benchmark: entry.name, arm, metric: key, unit, runs: outputs.length }
    if (error) return { ...row, error }
    const values = outputs.map((output) => pick(output, key))
    const bad = values.indexOf(undefined)
    if (bad !== -1)
      return { ...row, error: `run ${bad + 1} of ${runs}: ${key} is not a finite number` }
    return { ...row, median: median(values), min: Math.min(...values), max: Math.max(...values) }
  })

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
 * Runs one manifest entry on each of its arms.
 * `{client}` is the arm's install prefix, `{package}` its package name and `{baselinePackageDir}`
 * `<baselineDir>/node_modules/kafkajs`, the installed 2.2.4 package (not the fork's scoped one).
 * A two-arm entry's output must name the package it loaded, the arm's own.
 * @param {ManifestEntry} entry
 * @param {{ runs: number, forkDir: string, baselineDir: string, spawn: Function }} options `spawn`
 *   has the signature of `child_process.spawnSync`; the directories are install prefixes
 * @returns {Row[]} one row per (arm, metric); a failed run or metric gives an `error` row
 */
const runEntry = (entry, options) => {
  const rows = entry.arms.flatMap((arm) =>
    rowsOf(entry, arm, runArm(entry, arm, options), options.runs)
  )
  addRatios(rows)
  return rows
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
 * @param {Row[]} rows
 * @param {Record<string, string|number>} meta lines shown above the table
 * @returns {string} the report as Markdown: metadata, the table and the reason of every error row
 */
const renderMarkdown = (rows, meta) => {
  const line = (cells) => `| ${cells.map(cell).join(' | ')} |`
  const body = rows.map((row) =>
    row.error
      ? [row.benchmark, row.arm, row.metric, 'error', '-', row.unit, row.runs, '-']
      : [
          row.benchmark,
          row.arm,
          row.metric,
          fmt(row.median),
          `${fmt(row.min)} to ${fmt(row.max)}`,
          row.unit,
          row.runs,
          row.ratio === undefined ? '-' : fmt(row.ratio),
        ]
  )
  const errors = rows
    .filter((row) => row.error)
    .map((row) => `- ${cell(`${row.benchmark} / ${row.arm} / ${row.metric}: ${row.error}`)}`)
  return [
    '# Benchmark report',
    '',
    ...Object.entries(meta).map(([key, value]) => `- ${key}: ${value}`),
    '',
    line(HEADER),
    line(HEADER.map(() => '---')),
    ...body.map(line),
    ...(errors.length ? ['', '## Errors', '', ...errors] : []),
    '',
  ].join('\n')
}

/**
 * @param {string[]} argv
 * @returns {{ runs: number, forkDir: string, baselineDir: string, out: string }}
 * @throws {Error} the usage line for a missing, repeated or unknown flag or a bad `--runs`
 */
const parseArgs = (argv) => {
  const flags = {}
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i]
    const known = ['--runs', '--fork-dir', '--baseline-dir', '--out'].includes(name)
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
  }
}

/**
 * Runs the whole manifest and writes `bench-report.md` and `bench-report.json` into `--out`.
 * @param {string[]} argv `--runs`, `--fork-dir`, `--baseline-dir` and `--out`
 * @param {object} [deps]
 * @param {Function} [deps.spawn] `child_process.spawnSync`, replaceable in tests
 * @param {{ entries: ManifestEntry[] }} [deps.manifest] the benchmarks to run
 * @param {(line: string) => void} [deps.log] receives the usage line and one line per entry
 * @returns {number} 0 once the report is written, whatever its rows hold; 2 for a bad argument
 */
const main = (
  argv,
  { spawn = spawnSync, manifest = defaultManifest, log = console.error } = {}
) => {
  let args
  try {
    args = parseArgs(argv)
  } catch (error) {
    log(error.message)
    return 2
  }
  const rows = manifest.entries.flatMap((entry) => {
    log(entry.name)
    return runEntry(entry, { ...args, spawn })
  })
  const meta = {
    runs: args.runs,
    node: process.version,
    ...(process.env.KAFKA_VERSION && { kafka: process.env.KAFKA_VERSION }),
    ...(process.env.GITHUB_SHA && { commit: process.env.GITHUB_SHA }),
    date: new Date().toISOString(),
  }
  fs.mkdirSync(args.out, { recursive: true })
  fs.writeFileSync(
    path.join(args.out, 'bench-report.json'),
    `${JSON.stringify({ meta, rows }, null, 2)}\n`
  )
  fs.writeFileSync(path.join(args.out, 'bench-report.md'), renderMarkdown(rows, meta))
  return 0
}

module.exports = { lastJsonLine, pick, runEntry, renderMarkdown, main }

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2))
}
