/**
 * Decoder performance gate: one fixed collection of 30 paired A/C/B blocks per registered row.
 * A is baseline, C is another baseline invocation and B is candidate. All six orders occur five
 * times in a seeded schedule saved before measurement. Identical benchmark scripts overlay both
 * trees; every invocation and its internal rounds remain in the output and raw files.
 *
 * Usage: node scripts/benchmarks/compareDecoderBounds.js --run --baseline <40-hex sha>
 *          --partition frame-1m|frame-other|batch-array [--seed <uint32>] [--out <file>] [--raw <dir>]
 *        node scripts/benchmarks/compareDecoderBounds.js --combine --input <file> --input <file> --input <file>
 *          [--out <file>]
 *        node scripts/benchmarks/compareDecoderBounds.js --input <file>
 *
 * Live and offline modes use the same validation and verdict. Each row needs 30 positive finite
 * observations on A, C and B, paired by its invocation block/side and the recorded schedule.
 * Frame invocations also need three positive finite warm-up rounds. Unavailable codecs need all
 * 90 invocations to report absence in both trees, without errors.
 * Bonferroni median intervals use all 15 registered rows, including unavailable optional codecs.
 * A/A interval and median calibration and all three spreads determine validity. PASS requires
 * both the paired upper candidate bound and the unpaired median ratio at most 1.05. A valid lower
 * bound above 1.05 is REGRESSION; a failed median check is FAIL; otherwise INCONCLUSIVE. Invalid,
 * inconclusive, fail, regression and timeout all exit 1 for ordinary rows. D167 information-only
 * rows retain their verdicts but only a valid REGRESSION fails the gate; structural errors and
 * timeouts still fail. Exceptions must remain empty.
 *
 * The summary holds { baselineSha, candidateSha, node, registeredRows, seed, schedule, arms,
 * unavailable, unavailableInvocations, timedOut, result }. The result records informationOnlyRows
 * with their reasons. Arms retain oldNsPerOp, newNsPerOp,
 * baselineControlNsPerOp and invocations with explicit block and A/C/B side. The old unpaired
 * delta is printed as information only. Each partition has its own 25-minute deadline; no retries or
 * adaptive extension. A timeout kills the running subprocess group and preserves partial data.
 */
const fs = require('fs')
const path = require('path')
const { randomBytes } = require('crypto')
const { performance } = require('perf_hooks')
const { execFileSync, spawn } = require('child_process')

const REPO_ROOT = path.resolve(__dirname, '..', '..')
const EXCEPTIONS_FILE = path.join(__dirname, 'decoderBoundsExceptions.json')
const WORK_DIR = path.join('.tmp', 'decoder-comparison')
const DEFAULT_OUT = path.join('.tmp', 'decoder-comparison.json')
const DEFAULT_RAW = path.join(WORK_DIR, 'raw')
const SHA = /^[0-9a-f]{40}$/
const BLOCKS = 30
const COLLECTION_DEADLINE_MS = 25 * 60 * 1000
const MAX_RATIO = 1.05
const MIN_CONTROL_RATIO = 0.95
const MAX_SPREAD = 0.15
/** D167 rows remain measured and reported; only noise is waived, a valid FAIL or regression still fails. */
const INFORMATION_ONLY_ROWS = Object.freeze({
  'frame-decode:1m': 'D167: GitHub-hosted runner noise; A/A unstable and C as noisy as B.',
  'frame-decode:64b': 'D167: GitHub-hosted runner noise; A/A unstable and C as noisy as B.',
  'frame-decode:coalesced': 'D167: GitHub-hosted runner noise; A/A unstable and C as noisy as B.',
})
const SIDES = { A: 'oldNsPerOp', C: 'baselineControlNsPerOp', B: 'newNsPerOp' }
const ORDERS = ['ACB', 'ABC', 'CAB', 'CBA', 'BAC', 'BCA']
const USAGE =
  'usage: node scripts/benchmarks/compareDecoderBounds.js ' +
  '(--run --baseline <40-hex sha> --partition frame-1m|frame-other|batch-array ' +
  '[--seed <uint32>] [--out <file>] [--raw <dir>] | [--combine] --input <file> [--input <file>] [--out <file>])'

/** Fixed work per operation; the overlay and command use the same value on A, C and B. */
const WORK_MULTIPLIERS = {
  'frame-decode:64b': 16,
  'frame-decode:64k': 16,
  'frame-decode:1m': 64,
  'frame-decode:coalesced': 16,
  'frame-decode:byte-chunks': 16,
  'record-batch:uncompressed': 16,
  'record-batch:gzip': 2,
  'record-batch:zstd': 2,
  'record-batch:lz4': 1,
  'record-batch:snappy': 1,
  'array-decode:readArray': 1,
  'array-decode:record-headers': 1,
  'array-decode:readUVarIntArray': 1,
  'array-decode:readArrayAsync': 1,
  'array-decode:readUVarIntArrayAsync': 1,
}

/**
 * @typedef {object} Arm
 * @property {string} name unique registered row
 * @property {boolean} required unavailable required rows fail
 * @property {string[]} argv command and arguments, run with the tree as working directory
 * @property {string} metric nanoseconds per operation in the benchmark JSON
 * @property {string} overlay copied unchanged into the baseline tree
 */
/** @type {Arm[]} */
const ARMS = [
  ...['64b', '64k', '1m', 'coalesced', 'byte-chunks'].map((shape) => ({
    name: `frame-decode:${shape}`,
    required: true,
    argv: ['node', '--expose-gc', 'scripts/benchmarks/frameDecode.js', '--shape', shape],
    metric: 'nsPerFrame',
    overlay: 'scripts/benchmarks/frameDecode.js',
  })),
  ...['uncompressed', 'gzip', 'zstd', 'lz4', 'snappy'].map((arm) => ({
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
    overlay: 'scripts/benchmarks/recordBatchDecode.js',
  })),
  ...[
    'readArray',
    'record-headers',
    'readUVarIntArray',
    'readArrayAsync',
    'readUVarIntArrayAsync',
  ].map((arm) => ({
    name: `array-decode:${arm}`,
    required: true,
    argv: ['node', 'scripts/benchmarks/arrayDecode.js', '--arm', arm],
    metric: 'nsPerOp',
    overlay: 'scripts/benchmarks/arrayDecode.js',
  })),
].map((arm) => ({
  ...arm,
  argv: [...arm.argv, '--work-multiplier', String(WORK_MULTIPLIERS[arm.name])],
}))
const REGISTERED_ROWS = ARMS.map(({ name }) => name)
const PARTITIONS = {
  'frame-1m': ['frame-decode:1m'],
  'frame-other': REGISTERED_ROWS.filter(
    (name) => name.startsWith('frame-decode:') && name !== 'frame-decode:1m'
  ),
  'batch-array': REGISTERED_ROWS.filter((name) => !name.startsWith('frame-decode:')),
}
const OVERLAY_FILES = [...new Set(ARMS.map(({ overlay }) => overlay))]

/** @param {number[]} values @returns {number} median, averaging the middle two for even counts */
const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2
    ? sorted[middle]
    : sorted[middle - 1] + (sorted[middle] - sorted[middle - 1]) / 2
}
/** @param {*} value @returns {boolean} positive finite number */
const validSample = (value) => typeof value === 'number' && Number.isFinite(value) && value > 0
/** @param {*} values @param {number} count @returns {boolean} complete positive finite samples */
const validSamples = (values, count = BLOCKS) =>
  Array.isArray(values) && values.length === count && Array.from(values).every(validSample)
/** @param {*} seed @returns {boolean} a reproducible unsigned 32-bit seed */
const validSeed = (seed) => Number.isInteger(seed) && seed >= 0 && seed <= 0xffffffff

/**
 * Fisher-Yates shuffle of five copies of each order, using a uint32 seeded generator.
 * @param {number} seed unsigned 32-bit seed
 * @returns {string[][]} all 30 orders, in collection order
 * @throws {Error} when the seed is not a uint32
 */
const createSchedule = (seed) => {
  if (!validSeed(seed)) throw new Error('seed must be a uint32')
  let state = seed
  const schedule = ORDERS.flatMap((order) => Array.from({ length: 5 }, () => order.split('')))
  for (let i = schedule.length - 1; i > 0; i--) {
    state = (state + 0x6d2b79f5) >>> 0
    let value = Math.imul(state ^ (state >>> 15), state | 1)
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61)
    const j = Math.floor((((value ^ (value >>> 14)) >>> 0) / 4294967296) * (i + 1))
    ;[schedule[i], schedule[j]] = [schedule[j], schedule[i]]
  }
  return schedule
}

/**
 * Exact two-sided binomial order-statistic median interval, Bonferroni corrected for R and Q.
 * k = max{j: P[Binomial(30, .5) <= j-1] <= alpha/2}; endpoints are X(k), X(31-k).
 * @param {number[]} samples exactly 30 positive finite ratios
 * @param {number} m registered rows fixed before collection, including optional rows
 * @returns {{ alpha: number, k: number, lower: number, upper: number }} one-based index and bounds
 * @throws {Error} when samples or registration count are invalid
 */
const medianInterval = (samples, m) => {
  if (!validSamples(samples) || !Number.isInteger(m) || m < 1)
    throw new Error('invalid interval input')
  const alpha = 0.05 / (2 * m)
  let probability = 2 ** -BLOCKS
  let cumulative = 0
  let k = 0
  for (let j = 1; j <= BLOCKS / 2; j++) {
    cumulative += probability
    if (cumulative <= alpha / 2) k = j
    probability *= (BLOCKS - j + 1) / j
  }
  const sorted = [...samples].sort((a, b) => a - b)
  return { alpha, k, lower: sorted[k - 1] ?? 0, upper: sorted[BLOCKS - k] ?? Infinity }
}

/**
 * @param {object} row measured row
 * @param {string[][]} schedule pre-recorded order of each block
 * @param {boolean} [requireWork] require revised work accounting in each invocation
 * @returns {string[]} errors in identity, samples, pairing or internal rounds
 */
const rowProblems = (row, schedule, requireWork = false) => {
  const problems = []
  const arm = ARMS.find(({ name }) => name === row?.name)
  if (!arm) return [`unknown arm ${String(row?.name)}`]
  for (const field of Object.values(SIDES)) {
    if (!validSamples(row[field]))
      problems.push(`${row.name}: ${field} needs 30 positive finite samples`)
  }
  if (!Array.isArray(row.invocations) || row.invocations.length !== BLOCKS * 3) {
    return [...problems, `${row.name}: needs 90 paired invocations`]
  }
  Array.from(row.invocations).forEach((invocation, index) => {
    const block = Math.floor(index / 3)
    const side = schedule?.[block]?.[index % 3]
    const rounds = arm.metric === 'nsPerFrame' ? 7 : 5
    if (
      !invocation ||
      invocation.block !== block ||
      !Object.hasOwn(SIDES, side ?? '') ||
      invocation.side !== side ||
      invocation.error ||
      (requireWork && invocation.workMultiplier !== WORK_MULTIPLIERS[row.name]) ||
      invocation.available === 0 ||
      !validSample(invocation[arm.metric]) ||
      invocation[arm.metric] !== row[SIDES[side]]?.[block] ||
      !validSamples(invocation.roundNsPerOp, rounds) ||
      (arm.metric === 'nsPerFrame' && !validSamples(invocation.warmupRoundNsPerOp, 3)) ||
      median(invocation.roundNsPerOp) !== invocation[arm.metric]
    )
      problems.push(`${row.name}: invalid invocation ${index} in block ${block}`)
  })
  return problems
}

/**
 * @param {*} input parsed summary
 * @returns {string[]} structural problems; no malformed or partial evidence is acceptance eligible
 */
const validateInput = (input) => {
  if (input === null || typeof input !== 'object') return ['the input is not an object']
  const problems = []
  for (const key of ['baselineSha', 'candidateSha']) {
    if (!SHA.test(input[key] ?? '')) problems.push(`${key} must be a full 40-hex commit SHA`)
  }
  if (input.baselineSha === input.candidateSha)
    problems.push('baselineSha and candidateSha are the same commit')
  if (typeof input.node !== 'string' || !input.node)
    problems.push('node must name the shared Node version')
  if (JSON.stringify(input.registeredRows) !== JSON.stringify(REGISTERED_ROWS))
    problems.push('registeredRows must include every registered row in order')
  if (
    !validSeed(input.seed) ||
    JSON.stringify(input.schedule) !== JSON.stringify(createSchedule(input.seed))
  )
    problems.push('schedule must match its seed and contain six orders five times each')
  if (input.timedOut) problems.push('collection timed out')
  if (!Array.isArray(input.arms)) return [...problems, 'arms must be an array']
  const partitioned = input.partition !== undefined
  const names =
    partitioned && Object.hasOwn(PARTITIONS, input.partition)
      ? PARTITIONS[input.partition]
      : REGISTERED_ROWS
  if (partitioned) {
    if (!Object.hasOwn(PARTITIONS, input.partition)) problems.push('unknown partition')
    if (JSON.stringify(input.workMultipliers) !== JSON.stringify(WORK_MULTIPLIERS))
      problems.push('workMultipliers must match the registered work table')
  }
  const seen = new Set()
  for (const row of input.arms) {
    if (!row || seen.has(row.name)) problems.push('arm name is missing or duplicated')
    seen.add(row?.name)
    if (partitioned && !names.includes(row?.name)) problems.push('row is outside its partition')
    problems.push(...rowProblems(row, input.schedule, partitioned))
  }
  const declared = declaredUnavailable(input.unavailable, seen, problems)
  if (partitioned && declared.size)
    problems.push('every partition row must PASS; unavailable rows are non-green')
  if (declared.size > 0 || input.unavailableInvocations !== undefined) {
    const evidence = input.unavailableInvocations
    if (
      !Array.isArray(evidence) ||
      evidence.length !== declared.size ||
      new Set(evidence.map((row) => row?.name)).size !== declared.size
    ) {
      problems.push('unavailable codec evidence is incomplete or duplicated')
    } else {
      for (const row of evidence) {
        if (
          !declared.has(row?.name) ||
          !Array.isArray(row.invocations) ||
          row.invocations.length !== BLOCKS * 3 ||
          Array.from(row.invocations).some(
            (invocation, index) =>
              !invocation ||
              invocation.block !== Math.floor(index / 3) ||
              invocation.side !== input.schedule?.[Math.floor(index / 3)]?.[index % 3] ||
              invocation.available !== 0 ||
              invocation.error
          )
        ) {
          problems.push('unavailable codec evidence contains a measurement or control error')
        }
      }
    }
  }
  for (const { name, required } of ARMS.filter(({ name }) => names.includes(name))) {
    if (!seen.has(name) && (required || !declared.has(name))) {
      problems.push(
        required
          ? `required arm ${name} is missing`
          : `${name} has neither measurements nor an unavailable disposition`
      )
    }
  }
  return problems
}

/**
 * @param {*} unavailable optional codec names, absent means none
 * @param {Set<string>} measured measured names
 * @param {string[]} problems receives disposition violations
 * @returns {Set<string>} permitted unavailable names
 */
const declaredUnavailable = (unavailable = [], measured, problems) => {
  if (!Array.isArray(unavailable)) {
    problems.push('unavailable must be an array of arm names')
    return new Set()
  }
  const declared = new Set()
  for (const name of unavailable) {
    const arm = ARMS.find((known) => known.name === name)
    if (!arm) problems.push(`${String(name)} is not a known arm and cannot be unavailable`)
    else if (arm.required) problems.push(`required arm ${name} cannot be unavailable`)
    else if (measured.has(name)) problems.push(`${name} is both measured and unavailable`)
    else if (declared.has(name)) problems.push(`${name} is declared unavailable twice`)
    else declared.add(name)
  }
  return declared
}

/**
 * @param {*} entries parsed exceptions file
 * @returns {Map} empty exception set; exceptions never alter confidence or validity
 * @throws {Error} when exceptions are nonempty or malformed
 */
const parseExceptions = (entries) => {
  if (!Array.isArray(entries) || entries.length)
    throw new Error('decoder performance exceptions must remain empty')
  return new Map()
}

/** @param {number[]} samples @returns {number} second-highest minus second-lowest over median */
const spread = (samples) => {
  const sorted = [...samples].sort((a, b) => a - b)
  return (sorted[28] - sorted[1]) / median(sorted)
}
/** @param {number} a @param {number} b @returns {boolean} inclusive unpaired 5% upper check */
const medianWithin = (a, b) => {
  const scaled = b * 100
  const bound = a * 105
  return Number.isFinite(scaled) && Number.isFinite(bound) ? scaled <= bound : b / a <= MAX_RATIO
}

/**
 * @param {object} row structurally paired observations
 * @returns {object} validity calibration, paired intervals and the strict verdict
 */
const judgeRow = (row) => {
  const A = row.oldNsPerOp
  const B = row.newNsPerOp
  const C = row.baselineControlNsPerOp
  const oldMedian = median(A)
  const newMedian = median(B)
  const controlMedian = median(C)
  const R = B.map((b, i) => b / A[i])
  const Q = C.map((c, i) => c / A[i])
  if (!validSamples(R) || !validSamples(Q))
    return { name: row.name, verdict: 'invalid', problems: ['non-finite paired ratio'] }
  const ratioInterval = medianInterval(R, ARMS.length)
  const controlInterval = medianInterval(Q, ARMS.length)
  const spreads = { A: spread(A), C: spread(C), B: spread(B) }
  const problems = []
  if (controlInterval.lower < MIN_CONTROL_RATIO || controlInterval.upper > MAX_RATIO)
    problems.push('A/A interval outside [0.95, 1.05]')
  if (!medianWithin(oldMedian, controlMedian) || controlMedian / oldMedian < MIN_CONTROL_RATIO)
    problems.push('A/A median outside five percent')
  if (Object.values(spreads).some((value) => value > MAX_SPREAD))
    problems.push('spread exceeds fifteen percent')
  let verdict = 'invalid'
  if (!problems.length) {
    if (ratioInterval.upper <= MAX_RATIO && medianWithin(oldMedian, newMedian)) verdict = 'pass'
    else if (ratioInterval.lower > MAX_RATIO) verdict = 'regression'
    else if (!medianWithin(oldMedian, newMedian)) verdict = 'fail'
    else verdict = 'inconclusive'
  }
  return {
    name: row.name,
    oldMedian,
    newMedian,
    controlMedian,
    slowdownPercent: (newMedian / oldMedian - 1) * 100,
    ratioInterval,
    controlInterval,
    spreads,
    verdict,
    problems,
  }
}

/** Verdicts D167 waives on its information-only rows: noise, never valid slowdown evidence. */
const WAIVED_NOISE = Object.freeze(['invalid', 'inconclusive'])

/**
 * @param {object} row computed row verdict; structural problems remain separate gate failures
 * @returns {boolean} PASS, or INVALID/INCONCLUSIVE on a D167 information-only row
 */
const rowPassesGate = ({ name, verdict }) =>
  verdict === 'pass' ||
  (Object.hasOwn(INFORMATION_ONLY_ROWS, name) && WAIVED_NOISE.includes(verdict))

/**
 * @param {*} input summary; live and offline use this same decision path
 * @returns {{ pass: boolean, problems: string[], unavailable: string[], arms: object[], informationOnlyRows: object }} gate verdict and D167 reasons
 */
const compare = (input) => {
  const problems = validateInput(input)
  const arms = (Array.isArray(input?.arms) ? input.arms : []).map((row) => {
    const issues = rowProblems(row, input.schedule, input.partition !== undefined)
    return issues.length ? { name: row?.name, verdict: 'invalid', problems: issues } : judgeRow(row)
  })
  const unavailable = Array.isArray(input?.unavailable) ? input.unavailable : []
  return {
    pass: !problems.length && arms.every(rowPassesGate),
    problems,
    unavailable,
    arms,
    informationOnlyRows: INFORMATION_ONLY_ROWS,
  }
}

/**
 * Combines exactly the three complete, disjoint partitions. Seeds and clocks are independent;
 * tree identities, Node version, registration and work must agree. No unavailable row is a PASS.
 * @param {object[]} inputs partition evidence files, in any order
 * @returns {ReturnType<typeof compare>} all fifteen verdicts, with D167 information-only treatment
 */
const combine = (inputs) => {
  if (!Array.isArray(inputs))
    return {
      pass: false,
      problems: ['partitions must be an array'],
      unavailable: [],
      arms: [],
      informationOnlyRows: INFORMATION_ONLY_ROWS,
    }
  const problems = []
  const seen = new Set()
  const results = inputs.map((input) => {
    if (!Object.hasOwn(PARTITIONS, input?.partition ?? '') || seen.has(input.partition))
      problems.push('missing, unknown or duplicate partition')
    seen.add(input?.partition)
    for (const key of ['baselineSha', 'candidateSha', 'node']) {
      if (input?.[key] !== inputs[0]?.[key]) problems.push(`${key} differs across partitions`)
    }
    return compare(input)
  })
  if (
    inputs.length !== Object.keys(PARTITIONS).length ||
    Object.keys(PARTITIONS).some((id) => !seen.has(id))
  )
    problems.push('all three partitions are required')
  const arms = results.flatMap((result) => result.arms)
  const names = arms.map(({ name }) => name)
  if (
    names.length !== REGISTERED_ROWS.length ||
    new Set(names).size !== REGISTERED_ROWS.length ||
    REGISTERED_ROWS.some((name) => !names.includes(name))
  )
    problems.push('every registered row must occur exactly once')
  problems.push(...results.flatMap((result) => result.problems))
  return {
    pass: !problems.length && results.every((result) => result.pass) && arms.every(rowPassesGate),
    problems,
    unavailable: results.flatMap((result) => result.unavailable),
    arms,
    informationOnlyRows: INFORMATION_ONLY_ROWS,
  }
}

/** @param {ReturnType<typeof compare>} result @returns {string} per-row bounds and diagnostic delta */
const formatReport = ({ pass, problems, unavailable, arms }) =>
  [
    ...arms.map(
      (arm) =>
        `${arm.verdict.toUpperCase().padEnd(13)}${String(arm.name).padEnd(29)}` +
        (Object.hasOwn(INFORMATION_ONLY_ROWS, arm.name) ? 'information only (D167)  ' : '') +
        (arm.ratioInterval
          ? `R [${arm.ratioInterval.lower.toFixed(4)}, ${arm.ratioInterval.upper.toFixed(4)}]  ` +
            `Q [${arm.controlInterval.lower.toFixed(4)}, ${arm.controlInterval.upper.toFixed(4)}]  ` +
            `unpaired ${arm.slowdownPercent >= 0 ? '+' : ''}${arm.slowdownPercent.toFixed(2)}% (information only)`
          : '') +
        (arm.problems.length ? `  ${arm.problems.join('; ')}` : '')
    ),
    ...unavailable.map((name) => `UNAVAILABLE ${name} (no such codec in either tree)`),
    ...problems.map((problem) => `PROBLEM  ${problem}`),
    pass ? 'decoder performance: PASS' : 'decoder performance: FAIL',
  ].join('\n')

/** One fixed seeded collection. Errors remain evidence; no measurement is retried or extended. */
class MeasurementCollector {
  /**
   * @param {object} options dependencies
   * @param {{ old: string, new: string }} options.trees tree directories
   * @param {string} options.rawDir invocation evidence directory
   * @param {Function} options.run asynchronous subprocess runner with a remaining timeout
   * @param {Function} options.save synchronous evidence writer
   * @param {string} [options.partition] one fixed subset; omitted only for legacy offline fixtures
   * @param {number} [options.seed] unsigned seed, random by default
   * @param {Function} [options.now] monotonic milliseconds
   */
  constructor({
    trees,
    rawDir,
    run,
    save,
    partition,
    seed = randomBytes(4).readUInt32LE(0),
    now = () => performance.now(),
  }) {
    if (partition !== undefined && !Object.hasOwn(PARTITIONS, partition))
      throw new Error('unknown partition')
    Object.assign(this, { trees, rawDir, run, save, seed, now, partition })
    this.arms =
      partition === undefined
        ? ARMS
        : ARMS.filter(({ name }) => PARTITIONS[partition].includes(name))
    this.schedule = createSchedule(seed)
    this.deadlineMs = COLLECTION_DEADLINE_MS
  }

  /**
   * Records the schedule before the first invocation, then collects each block once.
   * @returns {Promise<object>} complete or partial observations, including timeout and error evidence
   */
  async collect() {
    const started = this.now()
    const registration = {
      ...(this.partition !== undefined && { partition: this.partition }),
      workMultipliers: { ...WORK_MULTIPLIERS },
      seed: this.seed,
      registeredRows: [...REGISTERED_ROWS],
      schedule: this.schedule,
    }
    this.save(path.join(this.rawDir, 'schedule.json'), `${JSON.stringify(registration, null, 2)}\n`)
    const rows = this.arms.map(({ name }) => ({
      name,
      oldNsPerOp: [],
      newNsPerOp: [],
      baselineControlNsPerOp: [],
      invocations: [],
    }))
    let timedOut = false
    for (let block = 0; block < BLOCKS && !timedOut; block++) {
      for (const [index, arm] of this.arms.entries()) {
        if (timedOut) break
        const row = rows[index]
        for (const side of this.schedule[block]) {
          const remaining = this.deadlineMs - (this.now() - started)
          if (remaining <= 0) {
            timedOut = true
            break
          }
          try {
            const measurement = await this.measure(arm, side, block, remaining)
            row.invocations.push({ ...measurement, block, side })
            if (measurement.available !== 0) row[SIDES[side]].push(measurement[arm.metric])
          } catch (error) {
            row.invocations.push({ block, side, error: error.message })
            if (error.timedOut) {
              timedOut = true
              break
            }
          }
        }
      }
    }
    if (this.now() - started >= this.deadlineMs) timedOut = true
    const unavailableRows = rows.filter(
      (row) =>
        !ARMS.find(({ name }) => name === row.name).required &&
        row.invocations.length === BLOCKS * 3 &&
        row.invocations.every((invocation) => invocation.available === 0 && !invocation.error)
    )
    const unavailable = unavailableRows.map(({ name }) => name)
    return {
      ...registration,
      timedOut,
      arms: rows.filter(({ name }) => !unavailable.includes(name)),
      unavailable,
      unavailableInvocations: unavailableRows,
    }
  }

  /**
   * @param {Arm} arm registered row
   * @param {'A'|'C'|'B'} side C and A run in the baseline tree
   * @param {number} block zero-based block identity
   * @param {number} timeoutMs remaining collection time
   * @returns {Promise<object>} unmodified parsed benchmark JSON, including every internal round
   * @throws {Error} failed process, invalid metric or timeout, always after preserving raw evidence
   */
  async measure(arm, side, block, timeoutMs) {
    const result = await this.run(this.trees[side === 'B' ? 'new' : 'old'], arm.argv, { timeoutMs })
    const { status, stdout, stderr } = result
    const file = path.join(this.rawDir, arm.name.replace(':', '-'), `${block}-${side}.txt`)
    this.save(
      file,
      `$ ${arm.argv.join(' ')}\nexit ${status}\ntimeout ${Boolean(result.timedOut)}\n--- stdout\n${stdout}\n--- stderr\n${stderr}\n`
    )
    if (result.timedOut || status !== 0) {
      const error = new Error(`${arm.name} failed in ${side} (exit ${status}); see ${file}`)
      error.timedOut = Boolean(result.timedOut)
      throw error
    }
    let parsed
    try {
      parsed = JSON.parse(stdout.trim().split('\n').pop())
    } catch {
      throw new Error(`${arm.name} printed no JSON line in ${side}; see ${file}`)
    }
    if (parsed?.available === 0) return parsed
    if (!validSample(parsed?.[arm.metric]))
      throw new Error(`${arm.name} reported no usable ${arm.metric} in ${side}`)
    return parsed
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
 * Runs a direct command in its own process group. The deadline kills the group, including any
 * descendants, with SIGKILL; it is not a graceful wait that could overrun the collection budget.
 * @param {string} cwd working directory
 * @param {string[]} argv command and arguments
 * @param {object} [options] subprocess dependencies and budget
 * @param {number} [options.timeoutMs] remaining collection milliseconds
 * @param {Function} [options.spawn] process factory
 * @param {Function} [options.kill] process-group signal sender
 * @returns {Promise<{ status: number, stdout: string, stderr: string, timedOut: boolean }>} evidence
 */
const runCommand = (
  cwd,
  argv,
  {
    timeoutMs = COLLECTION_DEADLINE_MS,
    spawn: start = spawn,
    kill = process.kill.bind(process),
  } = {}
) =>
  new Promise((resolve) => {
    const child = start(argv[0], argv.slice(1), {
      cwd,
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    let timedOut = false
    const timer = setTimeout(
      () => {
        timedOut = true
        try {
          kill(-child.pid, 'SIGKILL')
        } catch (error) {
          if (error.code !== 'ESRCH') stderr += error.message
        }
      },
      Math.max(1, Math.ceil(timeoutMs))
    )
    child.stdout.on('data', (chunk) => {
      stdout += chunk
    })
    child.stderr.on('data', (chunk) => {
      stderr += chunk
    })
    child.on('error', (error) => {
      stderr += error.message
    })
    child.on('close', (status) => {
      clearTimeout(timer)
      resolve({ status: timedOut ? 1 : (status ?? 1), stdout, stderr, timedOut })
    })
  })

/**
 * @param {string[]} argv arguments after the script name
 * @returns {object} strict run or offline options
 * @throws {Error} usage for invalid options
 */
const parseArgs = (argv) => {
  const options = { inputs: [] }
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i]
    if (flag === '--run' || flag === '--combine') options[flag.slice(2)] = true
    else if (flag === '--input' && argv[i + 1]) options.inputs.push(argv[++i])
    else if (
      ['--baseline', '--out', '--raw', '--seed', '--partition'].includes(flag) &&
      argv[i + 1] &&
      options[flag.slice(2)] === undefined
    )
      options[flag.slice(2)] = argv[++i]
    else throw new Error(USAGE)
  }
  if (
    options.seed !== undefined &&
    (!/^\d+$/.test(options.seed) || !validSeed(Number(options.seed)))
  )
    throw new Error(USAGE)
  if (
    options.run &&
    !options.combine &&
    !options.inputs.length &&
    SHA.test(options.baseline ?? '') &&
    Object.hasOwn(PARTITIONS, options.partition ?? '')
  ) {
    return {
      mode: 'run',
      partition: options.partition,
      baseline: options.baseline,
      out: options.out ?? DEFAULT_OUT,
      raw: options.raw ?? DEFAULT_RAW,
      seed: options.seed === undefined ? randomBytes(4).readUInt32LE(0) : Number(options.seed),
    }
  }
  if (
    !options.run &&
    options.inputs.length &&
    !options.baseline &&
    !options.raw &&
    options.seed === undefined &&
    options.partition === undefined
  ) {
    if (options.combine || options.inputs.length > 1)
      return { mode: 'combine', inputs: options.inputs, out: options.out }
    if (!options.out) return { mode: 'input', input: options.inputs[0] }
  }
  throw new Error(USAGE)
}

/** @param {string} file @param {string} text @returns {void} writes evidence with parent directories */
const saveFile = (file, text) => {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, text)
}

/**
 * @param {string[]} argv arguments after the script name
 * @param {Function} [log] report sink
 * @returns {Promise<number>} zero for complete accepted evidence with no proven regression
 */
const main = async (argv, log = console.log) => {
  const baselineDir = path.join(REPO_ROOT, WORK_DIR, 'baseline')
  let extracted = false
  try {
    const options = parseArgs(argv)
    parseExceptions(JSON.parse(fs.readFileSync(EXCEPTIONS_FILE, 'utf8')))
    let input
    if (options.mode === 'combine')
      input = options.inputs.map((file) => JSON.parse(fs.readFileSync(file, 'utf8')))
    else if (options.mode === 'input') input = JSON.parse(fs.readFileSync(options.input, 'utf8'))
    else {
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
        partition: options.partition,
        seed: options.seed,
        run: runCommand,
        save: saveFile,
      })
      input = {
        baselineSha: options.baseline,
        candidateSha: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: REPO_ROOT })
          .toString()
          .trim(),
        node: process.version,
        commands: Object.fromEntries(ARMS.map((arm) => [arm.name, arm.argv.join(' ')])),
        ...(await collector.collect()),
      }
    }
    const result = options.mode === 'combine' ? combine(input) : compare(input)
    if (options.mode === 'combine' && options.out)
      saveFile(options.out, `${JSON.stringify({ partitions: input, result }, null, 2)}\n`)
    if (options.mode === 'run')
      saveFile(options.out, `${JSON.stringify({ ...input, result }, null, 2)}\n`)
    log(formatReport(result))
    return result.pass ? 0 : 1
  } catch (error) {
    log(error.message)
    return 1
  } finally {
    if (extracted) fs.rmSync(baselineDir, { recursive: true, force: true })
  }
}

if (require.main === module)
  main(process.argv.slice(2)).then((code) => {
    process.exitCode = code
  })

module.exports = {
  ARMS,
  INFORMATION_ONLY_ROWS,
  PARTITIONS,
  WORK_MULTIPLIERS,
  MeasurementCollector,
  combine,
  compare,
  formatReport,
  main,
  medianInterval,
  createSchedule,
  overlayFiles,
  parseArgs,
  parseExceptions,
  prepareBaselineTree,
  runCommand,
  validateInput,
}
