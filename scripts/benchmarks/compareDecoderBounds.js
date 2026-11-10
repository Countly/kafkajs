/**
 * Decoder performance gate: compares the decode benchmarks of this tree (the candidate) with a
 * frozen baseline tree, on one machine and one Node version, and fails on a slowdown above 5%.
 *
 * Usage:
 *   node scripts/benchmarks/compareDecoderBounds.js --run --baseline <40-hex sha>
 *        [--out .tmp/decoder-comparison.json] [--raw .tmp/decoder-comparison/raw]
 *   node scripts/benchmarks/compareDecoderBounds.js --input <file>
 *
 * `--run` extracts the baseline with `git archive` (a full-history checkout is required) into
 * `.tmp/decoder-comparison/baseline`, links this tree's `node_modules` into it, copies the
 * benchmark scripts the baseline predates over from this tree, runs every arm's benchmark from
 * each tree in alternating old/new order, writes every raw invocation under `--raw`, validates and
 * compares the medians, writes the summary to `--out` and removes the baseline tree again.
 * `--input` validates and compares an existing summary.
 *
 * The input is
 * `{ baselineSha, candidateSha, node, arms: [{ name, oldNsPerOp, newNsPerOp }], unavailable }`
 * with at least 5 positive finite samples per side. Every known arm is either in `arms` or, when
 * it is not required, named in `unavailable` (the arms no tree has a codec for); an arm in neither
 * or in both, a required arm or an unknown name in `unavailable`, fails. A slowdown above 5%
 * passes only with an entry `{ arm, maxSlowdownPercent, ruling }` in `decoderBoundsExceptions.json`,
 * and only up to that bound. Missing, incomplete or incomparable data never passes, with or
 * without an exception. Exit code 0 only when every required arm is complete and within its
 * limit, otherwise 1.
 */
const fs = require('fs')
const path = require('path')
const { execFileSync, spawnSync } = require('child_process')

const REPO_ROOT = path.resolve(__dirname, '..', '..')
const EXCEPTIONS_FILE = path.join(__dirname, 'decoderBoundsExceptions.json')
const WORK_DIR = path.join('.tmp', 'decoder-comparison')
const DEFAULT_OUT = path.join('.tmp', 'decoder-comparison.json')
const DEFAULT_RAW = path.join(WORK_DIR, 'raw')

const SHA = /^[0-9a-f]{40}$/
const MIN_SAMPLES = 5
const PAIRS = 7
const MAX_SLOWDOWN_PERCENT = 5
const INVOCATION_TIMEOUT_MS = 10 * 60 * 1000
const USAGE =
  'usage: node scripts/benchmarks/compareDecoderBounds.js ' +
  '(--run --baseline <40-hex sha> [--out <file>] [--raw <dir>] | --input <file>)'

const FRAME_SHAPES = ['64b', '64k', '1m', 'coalesced', 'byte-chunks']
const COMPRESSED_ARMS = ['gzip', 'zstd', 'lz4', 'snappy']
const ARRAY_DECODE_SCRIPT = 'scripts/benchmarks/arrayDecode.js'
const ARRAY_READERS = [
  'readArray',
  'readVarIntArray',
  'readUVarIntArray',
  'readArrayAsync',
  'readUVarIntArrayAsync',
]

/**
 * @typedef {object} Arm
 * @property {string} name unique arm name
 * @property {boolean} required a missing or unavailable required arm fails the gate
 * @property {string[]} argv command and arguments, run with the tree as working directory
 * @property {string} metric key of the benchmark's JSON line that holds nanoseconds per operation
 * @property {string} [overlay] script of this tree that the baseline tree may predate: it is copied
 *   into the baseline tree, so both trees run the same bytes
 */

/** @type {Arm[]} */
const ARMS = [
  ...FRAME_SHAPES.map((shape) => ({
    name: `frame-decode:${shape}`,
    required: true,
    argv: ['npm', 'run', '--silent', 'bench:frame-decode', '--', '--shape', shape],
    metric: 'nsPerFrame',
  })),
  ...['uncompressed', ...COMPRESSED_ARMS].map((arm) => ({
    name: `record-batch:${arm}`,
    required: arm === 'uncompressed',
    argv: [
      'node',
      'scripts/benchmarks/recordBatchDecode.js',
      '--iterations',
      '20000',
      '--arm',
      arm,
    ],
    metric: 'nsPerOp',
  })),
  ...ARRAY_READERS.map((reader) => ({
    name: `array-decode:${reader}`,
    required: true,
    argv: ['node', ARRAY_DECODE_SCRIPT, '--arm', reader],
    metric: 'nsPerOp',
    overlay: ARRAY_DECODE_SCRIPT,
  })),
]

const OVERLAY_FILES = [...new Set(ARMS.flatMap(({ overlay }) => overlay ?? []))]

/**
 * @param {number[]} values at least one number
 * @returns {number} the median; the mean of the two middle values for an even count
 */
const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2
}

/** @param {*} sample @returns {boolean} a positive finite number */
const validSample = (sample) => typeof sample === 'number' && Number.isFinite(sample) && sample > 0

/** @param {*} samples @returns {boolean} an array of at least 5 positive finite numbers */
const validSamples = (samples) =>
  Array.isArray(samples) && samples.length >= MIN_SAMPLES && samples.every(validSample)

/**
 * Checks that an input can be compared at all.
 * @param {*} input the parsed input
 * @returns {string[]} one message per problem; empty when the input is complete and comparable
 */
const validateInput = (input) => {
  if (input === null || typeof input !== 'object') {
    return ['the input is not an object']
  }

  const problems = []
  for (const key of ['baselineSha', 'candidateSha']) {
    if (typeof input[key] !== 'string' || !SHA.test(input[key])) {
      problems.push(`${key} must be a full 40-hex commit SHA`)
    }
  }
  if (input.baselineSha === input.candidateSha && SHA.test(input.baselineSha ?? '')) {
    problems.push('baselineSha and candidateSha are the same commit; nothing is being compared')
  }
  if (typeof input.node !== 'string' || input.node === '') {
    problems.push('node must name the Node version both sides ran on')
  }
  if (!Array.isArray(input.arms)) {
    return [...problems, 'arms must be an array']
  }

  const seen = new Set()
  for (const arm of input.arms) {
    if (typeof arm?.name !== 'string' || arm.name === '' || seen.has(arm.name)) {
      problems.push(`arm name ${JSON.stringify(arm?.name)} is missing or duplicated`)
      continue
    }
    seen.add(arm.name)
    for (const side of ['oldNsPerOp', 'newNsPerOp']) {
      if (!validSamples(arm[side])) {
        problems.push(`${arm.name}: ${side} needs at least ${MIN_SAMPLES} positive finite samples`)
      }
    }
  }

  const declared = declaredUnavailable(input.unavailable, seen, problems)
  for (const { name, required } of ARMS) {
    if (seen.has(name) || (!required && declared.has(name))) {
      continue
    }
    problems.push(
      required
        ? `required arm ${name} is missing`
        : `${name} has neither measurements nor an unavailable disposition`
    )
  }

  return problems
}

/**
 * Checks the arms an input declares unavailable: each must be a known, non-required arm that is
 * not also measured and not declared twice.
 * @param {*} unavailable the input's `unavailable`; absent means none
 * @param {Set<string>} measured the names of the arms the input measures
 * @param {string[]} problems receives one message per violation
 * @returns {Set<string>} the declared names that are valid
 */
const declaredUnavailable = (unavailable = [], measured, problems) => {
  if (!Array.isArray(unavailable)) {
    problems.push('unavailable must be an array of arm names')
    return new Set()
  }

  const declared = new Set()
  for (const name of unavailable) {
    const arm = ARMS.find((known) => known.name === name)
    if (!arm) {
      problems.push(`${String(name)} is not a known arm and cannot be unavailable`)
    } else if (arm.required) {
      problems.push(`required arm ${name} cannot be unavailable`)
    } else if (measured.has(name)) {
      problems.push(`${name} is both measured and unavailable`)
    } else if (declared.has(name)) {
      problems.push(`${name} is declared unavailable twice`)
    } else {
      declared.add(name)
    }
  }

  return declared
}

/**
 * @param {*} entries the parsed exceptions file
 * @returns {Map<string, { maxSlowdownPercent: number, ruling: string }>} the entries by arm
 * @throws {Error} when the file is not an array of `{ arm, maxSlowdownPercent, ruling }`
 */
const parseExceptions = (entries) => {
  if (!Array.isArray(entries)) {
    throw new Error('the exceptions file must hold an array')
  }

  const byArm = new Map()
  for (const entry of entries) {
    const valid =
      typeof entry?.arm === 'string' &&
      Number.isFinite(entry.maxSlowdownPercent) &&
      entry.maxSlowdownPercent > MAX_SLOWDOWN_PERCENT &&
      typeof entry.ruling === 'string' &&
      entry.ruling !== ''
    if (!valid || byArm.has(entry.arm)) {
      throw new Error(`invalid or duplicate exception ${JSON.stringify(entry)}`)
    }
    byArm.set(entry.arm, entry)
  }

  return byArm
}

/**
 * @typedef {object} ArmResult
 * @property {string} name
 * @property {number} oldMedian nanoseconds per operation in the baseline
 * @property {number} newMedian nanoseconds per operation in the candidate
 * @property {number} slowdownPercent `(new / old - 1) * 100`; negative when faster
 * @property {'pass'|'exception'|'fail'} verdict
 */

/**
 * @param {number} oldMedian
 * @param {number} newMedian
 * @param {number} percent allowed slowdown
 * @returns {boolean} whether `newMedian` is at most `percent` above `oldMedian`; compared by
 *   cross-multiplication so that 105 against 100 is exactly 5%
 */
const within = (oldMedian, newMedian, percent) => newMedian * 100 <= oldMedian * (100 + percent)

/**
 * Compares the medians of every arm and applies the 5% limit and any recorded exception.
 * @param {object} input see the file comment
 * @param {Map<string, { maxSlowdownPercent: number }>} exceptions from {@link parseExceptions}
 * @returns {{ pass: boolean, problems: string[], unavailable: string[], arms: ArmResult[] }}
 *   `pass` is false whenever `problems` is not empty, whatever the exceptions say
 */
const compare = (input, exceptions) => {
  const problems = validateInput(input)
  const arms = []
  for (const arm of Array.isArray(input?.arms) ? input.arms : []) {
    if (!validSamples(arm?.oldNsPerOp) || !validSamples(arm?.newNsPerOp)) {
      continue
    }

    const oldMedian = median(arm.oldNsPerOp)
    const newMedian = median(arm.newNsPerOp)
    const exception = exceptions.get(arm.name)
    let verdict = 'fail'
    if (within(oldMedian, newMedian, MAX_SLOWDOWN_PERCENT)) {
      verdict = 'pass'
    } else if (exception && within(oldMedian, newMedian, exception.maxSlowdownPercent)) {
      verdict = 'exception'
    }
    arms.push({
      name: arm.name,
      oldMedian,
      newMedian,
      slowdownPercent: (newMedian / oldMedian - 1) * 100,
      verdict,
    })
  }

  const unavailable = Array.isArray(input?.unavailable) ? input.unavailable : []
  const pass = problems.length === 0 && arms.every((arm) => arm.verdict !== 'fail')
  return { pass, problems, unavailable, arms }
}

/**
 * @param {ReturnType<typeof compare>} result
 * @returns {string} the per-arm report, problems and unavailable arms included
 */
const formatReport = ({ pass, problems, unavailable, arms }) =>
  [
    ...arms.map(
      (arm) =>
        `${arm.verdict.toUpperCase().padEnd(9)}${arm.name.padEnd(28)}` +
        `old ${arm.oldMedian.toFixed(1)} ns  new ${arm.newMedian.toFixed(1)} ns  ` +
        `${arm.slowdownPercent >= 0 ? '+' : ''}${arm.slowdownPercent.toFixed(2)}%`
    ),
    ...unavailable.map((name) => `UNAVAILABLE ${name} (no such codec in either tree)`),
    ...problems.map((problem) => `PROBLEM  ${problem}`),
    pass ? 'decoder performance: PASS' : 'decoder performance: FAIL',
  ].join('\n')

/**
 * Runs every arm in the baseline and candidate trees, alternating which goes first each round so
 * that machine drift lands on both sides, and keeps every raw invocation.
 */
class MeasurementCollector {
  /**
   * @param {object} options
   * @param {{ old: string, new: string }} options.trees the working directory of each side
   * @param {string} options.rawDir directory that receives one file per invocation
   * @param {(cwd: string, argv: string[]) => { status: number, stdout: string, stderr: string }} options.run
   * @param {(file: string, text: string) => void} options.save writes a raw file, creating its directory
   * @param {number} [options.pairs] paired invocations per arm
   */
  constructor({ trees, rawDir, run, save, pairs = PAIRS }) {
    this.trees = trees
    this.rawDir = rawDir
    this.run = run
    this.save = save
    this.pairs = pairs
  }

  /**
   * @returns {{ arms: Array<{ name: string, oldNsPerOp: number[], newNsPerOp: number[] }>, unavailable: string[] }}
   *   the samples per arm; an arm whose codec is missing in both trees is named in `unavailable`
   *   instead, and one missing in a single tree keeps fewer than the required samples there
   * @throws {Error} when an invocation fails or prints no usable number
   */
  collect() {
    const samples = new Map(ARMS.map((arm) => [arm.name, { old: [], new: [] }]))
    const missing = { old: new Set(), new: new Set() }

    for (let pair = 0; pair < this.pairs; pair++) {
      const sides = pair % 2 === 0 ? ['old', 'new'] : ['new', 'old']
      for (const arm of ARMS) {
        for (const side of sides) {
          if (missing[side].has(arm.name)) continue
          const value = this.measure(arm, side, pair)
          if (value === null) missing[side].add(arm.name)
          else samples.get(arm.name)[side].push(value)
        }
      }
    }

    const unavailable = ARMS.map((arm) => arm.name).filter(
      (name) => missing.old.has(name) && missing.new.has(name)
    )
    const arms = ARMS.filter((arm) => !unavailable.includes(arm.name)).map(({ name }) => ({
      name,
      oldNsPerOp: samples.get(name).old,
      newNsPerOp: samples.get(name).new,
    }))
    return { arms, unavailable }
  }

  /**
   * @param {Arm} arm
   * @param {'old'|'new'} side
   * @param {number} pair
   * @returns {number|null} nanoseconds per operation, or null when the tree has no such codec
   * @throws {Error} when the process fails or its last line is not JSON with a positive number
   */
  measure(arm, side, pair) {
    const { status, stdout, stderr } = this.run(this.trees[side], arm.argv)
    const file = path.join(this.rawDir, arm.name.replace(':', '-'), `${pair}-${side}.txt`)
    this.save(
      file,
      `$ ${arm.argv.join(' ')}\nexit ${status}\n--- stdout\n${stdout}\n--- stderr\n${stderr}\n`
    )
    if (status !== 0) {
      throw new Error(`${arm.name} failed in the ${side} tree (exit ${status}); see ${file}`)
    }

    const line = stdout.trim().split('\n').pop()
    let parsed
    try {
      parsed = JSON.parse(line)
    } catch {
      throw new Error(`${arm.name} printed no JSON line in the ${side} tree; see ${file}`)
    }
    if (parsed.available === 0) {
      return null
    }
    if (!validSample(parsed[arm.metric])) {
      throw new Error(`${arm.name} reported no usable ${arm.metric} in the ${side} tree`)
    }

    return parsed[arm.metric]
  }
}

/**
 * Copies files of the candidate tree over the same paths of another tree, so that a benchmark
 * script the other tree predates runs there as the same bytes.
 * @param {object} options
 * @param {string} options.repoRoot the candidate tree
 * @param {string} options.dir the tree to copy into
 * @param {string[]} options.files paths relative to the tree roots
 * @returns {void}
 * @throws {Error} when a file is missing from the candidate tree
 */
const overlayFiles = ({ repoRoot, dir, files }) => {
  for (const file of files) {
    fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true })
    fs.copyFileSync(path.join(repoRoot, file), path.join(dir, file))
  }
}

/**
 * Extracts a commit into an empty directory, links this tree's dependencies into it and copies the
 * overlay files of this tree over it.
 * @param {object} options
 * @param {string} options.repoRoot the candidate tree
 * @param {string} options.sha the 40-hex baseline commit
 * @param {string} options.dir the directory to fill; anything already there is removed
 * @param {string[]} [options.overlay] files of the candidate tree to copy into the extracted tree
 * @returns {void}
 * @throws {Error} when the commit is not in this checkout, such as a shallow one
 */
const prepareBaselineTree = ({ repoRoot, sha, dir, overlay = [] }) => {
  try {
    execFileSync('git', ['cat-file', '-e', `${sha}^{commit}`], { cwd: repoRoot, stdio: 'pipe' })
  } catch {
    throw new Error(`baseline ${sha} is not in this checkout; a full-history checkout is required`)
  }

  fs.rmSync(dir, { recursive: true, force: true })
  fs.mkdirSync(dir, { recursive: true })
  const tarball = `${dir}.tar`
  execFileSync('git', ['archive', '--format=tar', `--output=${tarball}`, sha], { cwd: repoRoot })
  execFileSync('tar', ['-xf', tarball, '-C', dir])
  fs.rmSync(tarball)
  fs.symlinkSync(path.join(repoRoot, 'node_modules'), path.join(dir, 'node_modules'), 'dir')
  overlayFiles({ repoRoot, dir, files: overlay })
}

/**
 * @param {string} cwd
 * @param {string[]} argv command and arguments
 * @returns {{ status: number, stdout: string, stderr: string }} a timeout or spawn failure has a
 *   non-zero status
 */
const runCommand = (cwd, argv) => {
  const [command, ...args] = argv
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    timeout: INVOCATION_TIMEOUT_MS,
  })
  return { status: result.status ?? 1, stdout: result.stdout ?? '', stderr: result.stderr ?? '' }
}

/**
 * @param {string[]} argv the arguments after the script name
 * @returns {{ mode: 'run', baseline: string, out: string, raw: string } | { mode: 'input', input: string }}
 * @throws {Error} the usage line for anything it does not accept
 */
const parseArgs = (argv) => {
  const options = {}
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i]
    if (flag === '--run') {
      options.run = true
    } else if (['--baseline', '--out', '--raw', '--input'].includes(flag) && argv[i + 1]) {
      options[flag.slice(2)] = argv[++i]
    } else {
      throw new Error(USAGE)
    }
  }

  if (options.run && !options.input && SHA.test(options.baseline ?? '')) {
    return {
      mode: 'run',
      baseline: options.baseline,
      out: options.out ?? DEFAULT_OUT,
      raw: options.raw ?? DEFAULT_RAW,
    }
  }
  if (!options.run && options.input && !options.baseline && !options.out && !options.raw) {
    return { mode: 'input', input: options.input }
  }

  throw new Error(USAGE)
}

/**
 * @param {string} file
 * @param {string} text
 * @returns {void}
 */
const saveFile = (file, text) => {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, text)
}

/**
 * @param {string[]} argv the arguments after the script name
 * @param {(line: string) => void} [log]
 * @returns {number} the exit code
 */
const main = (argv, log = console.log) => {
  const baselineDir = path.join(REPO_ROOT, WORK_DIR, 'baseline')
  let extracted = false
  try {
    const options = parseArgs(argv)
    const exceptions = parseExceptions(JSON.parse(fs.readFileSync(EXCEPTIONS_FILE, 'utf8')))
    let input
    if (options.mode === 'input') {
      input = JSON.parse(fs.readFileSync(options.input, 'utf8'))
    } else {
      extracted = true
      prepareBaselineTree({
        repoRoot: REPO_ROOT,
        sha: options.baseline,
        dir: baselineDir,
        overlay: OVERLAY_FILES,
      })
      const collector = new MeasurementCollector({
        trees: { old: baselineDir, new: REPO_ROOT },
        rawDir: options.raw,
        run: runCommand,
        save: saveFile,
      })
      input = {
        baselineSha: options.baseline,
        candidateSha: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: REPO_ROOT })
          .toString()
          .trim(),
        node: runCommand(REPO_ROOT, ['node', '--version']).stdout.trim(),
        commands: Object.fromEntries(ARMS.map((arm) => [arm.name, arm.argv.join(' ')])),
        ...collector.collect(),
      }
    }

    const result = compare(input, exceptions)
    if (options.mode === 'run') {
      saveFile(options.out, `${JSON.stringify({ ...input, result }, null, 2)}\n`)
    }
    log(formatReport(result))
    return result.pass ? 0 : 1
  } catch (error) {
    log(error.message)
    return 1
  } finally {
    if (extracted) fs.rmSync(baselineDir, { recursive: true, force: true })
  }
}

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2))
}

module.exports = {
  ARMS,
  MeasurementCollector,
  compare,
  formatReport,
  main,
  overlayFiles,
  parseArgs,
  parseExceptions,
  prepareBaselineTree,
  validateInput,
}
