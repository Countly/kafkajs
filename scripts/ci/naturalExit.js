const fs = require('fs')
const { isDeepStrictEqual } = require('util')
const { spawnSync } = require('child_process')
const {
  TITLE,
  PROFILES,
  childArgs,
  childEnvironment,
  environmentIdentity,
} = require('./shareSoakContract')
const { verifySharePrimingLink } = require('../evidence/sharePrimingLink')

const LANES = {
  soak: {
    ...PROFILES.short,
    spec: 'shareSoakLive.spec.js',
    cases: ['preserves share identities and resources through the short soak'],
  },
  'soak-full': { ...PROFILES.full, cases: [TITLE] },
  shutdown: {
    stem: 'share-shutdown',
    // Case budgets 40000 + 50000 + 30000 + 177000 (the admission case's summed bounds), plus the
    // wrapper: beforeAll 10000, four Jest case joins 4 * 2000, Jest startup 10000, exit 10000.
    totalMs: 120000 + 177000 + 38000,
    spec: 'src/consumer/share/__tests__/shareShutdownLive.spec.js',
    cases: [
      'sends four decisions and releases six held records before lock expiry',
      'writes no share frame after a fresh timeout zero call',
      'stops cleanly and releases unstarted records',
      'connect during disconnect reconnects only after DISCONNECT',
    ],
  },
  ordinary: {
    stem: 'natural-exit-ordinary',
    // Case budgets 4 * 60000 (consumers) + 3 * 30000 (producers, admin), plus the wrapper:
    // seven Jest case joins 7 * 2000, Jest startup 10000, exit 10000.
    totalMs: 240000 + 90000 + 34000,
    spec: 'src/__tests__/naturalExitLive.spec.js',
    cases: [
      'classic dynamic consumer consumes one message and disconnects',
      'classic static consumer keeps its group slot after disconnect',
      'consumer-protocol dynamic consumer consumes one message and disconnects',
      'consumer-protocol static consumer consumes one message and disconnects',
      'idempotent producer sends one message and disconnects',
      'transactional producer commits one message and disconnects',
      'admin creates and describes a topic and disconnects',
    ],
    /** Selected by `NATURAL_EXIT_CONTROL=1`: the same child, which must not exit on its own. */
    control: {
      stem: 'natural-exit-ordinary-control',
      // Case budget 20000, plus one Jest case join 2000, Jest startup 10000, exit 10000.
      totalMs: 20000 + 22000,
      cases: ['leaves one interval alive after its producer disconnects'],
    },
  },
}

/** @param {string} lane Fixed CI child selection. @returns {boolean} Whether it is a share soak. */
const isSoak = (lane) => lane === 'soak' || lane === 'soak-full'

/** @returns {number} Absolute OS monotonic milliseconds, shared with the fixed child specs. */
const monotonicMs = () => Number(process.hrtime.bigint() / 1000000n)

/**
 * Selects completed cleanup in original case order on the parent's clock.
 * Shutdown's fixed child stamps hrtime directly; soak also records its explicit clock origin.
 * @param {string} lane Fixed CI child selection.
 * @param {string} text Original child evidence JSONL.
 * @param {object} observation Parent entry and actual child-exit observations.
 * @param {string[]} [cases] Expected case order; the lane's own by default.
 * @returns {number} The last completed cleanup timestamp.
 */
const cleanupTimestamp = (lane, text, { startedAt, exitedAt }, cases = LANES[lane].cases) => {
  const soak = isSoak(lane)
  const body = text.trim()
  const rows = body ? body.split('\n').map(JSON.parse) : []
  if (soak && verifySharePrimingLink(rows).length)
    throw new Error('soak: incomplete or invalid priming cleanup evidence')
  const done = rows.filter((row) => row?.event === 'case-complete')
  let previousAt = startedAt
  if (
    rows.some((row) => row?.event === 'case-failed') ||
    done.length !== cases.length ||
    done.some((row, index) => {
      const cleanup = row.cleanup
      const at = cleanup?.completedAt
      const clock = cleanup?.clock
      const invalid =
        row.case !== cases[index] ||
        row.status !== 'passed' ||
        row.tainted !== false ||
        !Number.isSafeInteger(at) ||
        at < previousAt ||
        (index > 0 && at === previousAt) ||
        at > exitedAt ||
        (clock !== undefined && (clock?.origin !== 'hrtime' || clock?.completedAt !== at)) ||
        (soak &&
          (row.profileMs !== LANES[lane].profileMs ||
            row.selector !== LANES[lane].selector ||
            row.soakProfile !== LANES[lane].profile ||
            row.stem !== LANES[lane].stem ||
            row.schedule?.slotMs !== LANES[lane].slotMs ||
            row.schedule?.totalMs !== LANES[lane].totalMs ||
            cleanup?.groupsAbsent !== true ||
            cleanup?.topicsAbsent !== true ||
            clock?.origin !== 'hrtime'))
      previousAt = at
      return invalid
    })
  ) {
    throw new Error(`${lane}: incomplete or invalid cleanup evidence`)
  }
  return previousAt
}

/**
 * Builds the fixed Jest child argv for a direct lane, without forced exit.
 * @param {string} spec Repository path of the live spec.
 * @param {string} [reportStem] Adds a Jest JSON report at `.tmp/e2e/<reportStem>-jest.json`.
 * @returns {string[]} Child argv.
 */
const jestArgs = (spec, reportStem) => [
  'node_modules/jest/bin/jest.js',
  '--ci',
  '--detectOpenHandles',
  '--runInBand',
  '--reporters=./testHelpers/quietReporter.js',
  '--runTestsByPath',
  spec,
  ...(reportStem ? ['--json', `--outputFile=.tmp/e2e/${reportStem}-jest.json`] : []),
]

/**
 * Requires the fixed, actually executed Jest inventory: one suite of the given spec whose
 * cases, in file order, are exactly the skipped ones and then the passed ones. `success` is
 * false on any failed test or suite runtime error. The suite status is the one Jest derives for
 * that inventory: `passed` when every case ran, `focused` when some were skipped. The defaults
 * are the short share soak's.
 * @param {object} report Original Jest JSON report.
 * @param {object} [expected] Expected inventory.
 * @param {string} [expected.label] Error prefix.
 * @param {string} [expected.spec] Repository path of the spec.
 * @param {string[]} [expected.passed] Titles that must pass.
 * @param {string[]} [expected.skipped] Titles that must be skipped.
 * @returns {void}
 * @throws {Error} On missing or changed test inventory.
 */
const validateJestReport = (
  report,
  {
    label = 'soak',
    spec = 'src/consumer/share/__tests__/shareSoakLive.spec.js',
    passed = [TITLE],
    skipped = [],
  } = {}
) => {
  const suites = report?.testResults
  const suite = suites?.[0]
  const expected = [
    ...skipped.map((title) => ({ title, status: 'pending' })),
    ...passed.map((title) => ({ title, status: 'passed' })),
  ]
  if (
    report?.success !== true ||
    report.numTotalTests !== expected.length ||
    report.numPassedTests !== passed.length ||
    report.numFailedTests !== 0 ||
    report.numPendingTests !== skipped.length ||
    report.numTodoTests !== 0 ||
    suites?.length !== 1 ||
    !suite.name?.endsWith(`/${spec}`) ||
    suite.status !== (skipped.length ? 'focused' : 'passed') ||
    !isDeepStrictEqual(
      suite.assertionResults?.map(({ title, status }) => ({ title, status })),
      expected
    )
  )
    throw new Error(`${label}: invalid Jest inventory`)
}

/**
 * Observes actual child exit under a watchdog, archives parent facts even on rejection, and
 * enforces both inclusive bounds using original cleanup evidence. Child argv is fixed: callers
 * cannot request forced exit or substitute a child with a different cleanup clock contract.
 * The ordinary lane with `NATURAL_EXIT_CONTROL=1` runs its seeded-timer control: the same parent
 * contract with the control's cases, budget and artifacts, which CI requires to be rejected.
 * @param {'soak'|'soak-full'|'shutdown'|'ordinary'} lane Fixed CI child selection.
 * @param {object} [ports] OS operations, injectable for broker-free parent controls.
 * @param {Function} [ports.spawn] Synchronous child spawn with timeout/status observation.
 * @param {Function} [ports.now] Absolute OS monotonic milliseconds.
 * @param {Function} [ports.readFile] Original JSONL evidence reader.
 * @param {Function} [ports.writeFile] Parent exit artifact writer.
 * @param {object} [ports.env] Environment that selects the soak child and the ordinary control.
 * @returns {object} Accepted original parent observations and last cleanup timestamp.
 * @throws {Error} On abnormal exit, invalid evidence/clock order, or either exceeded bound.
 */
const runNaturalExit = (
  lane,
  {
    spawn = spawnSync,
    now = monotonicMs,
    readFile = fs.readFileSync,
    writeFile = fs.writeFileSync,
    env = process.env,
  } = {}
) => {
  if (!Object.hasOwn(LANES, lane))
    throw new Error('Expected natural-exit lane: soak, soak-full, shutdown or ordinary')
  const soak = isSoak(lane)
  const control = lane === 'ordinary' ? env.NATURAL_EXIT_CONTROL === '1' : undefined
  const { totalMs, spec, stem, cases } = control
    ? { ...LANES[lane], ...LANES[lane].control }
    : LANES[lane]
  const exitMs = 10000
  if (
    soak &&
    process.execArgv.some((arg) =>
      /^(--require|--import|--loader|--experimental-loader|--eval|-r$|-e$)/.test(arg)
    )
  )
    throw new Error('Forbidden parent preload or eval override')
  const profile = lane === 'soak' ? 'short' : 'full'
  const childEnv = soak ? childEnvironment(profile, env) : undefined
  const args = soak ? childArgs(profile) : jestArgs(spec, control ? stem : undefined)
  const startedAt = now()
  let result
  try {
    result = spawn(process.execPath, args, {
      stdio: 'inherit',
      timeout: totalMs,
      killSignal: 'SIGKILL',
      ...(childEnv && { env: childEnv }),
    })
  } catch (error) {
    result = { error, status: null, signal: null }
  }
  const exitedAt = now()
  const observation = {
    parentPid: process.pid,
    childPid: result.pid ?? null,
    executable: process.execPath,
    argv: args,
    ...(childEnv && { environment: environmentIdentity(childEnv) }),
    lane,
    ...(control !== undefined && { control }),
    origin: 'hrtime',
    startedAt,
    exitedAt,
    status: result.status,
    signal: result.signal,
    totalMs,
    exitMs,
    watchdogTerminated: result.error?.code === 'ETIMEDOUT',
    error: result.error?.message,
  }
  const exitFile = `.tmp/e2e/${stem}-exit.json`
  writeFile(exitFile, JSON.stringify(observation) + '\n')
  try {
    if (result.error || result.status !== 0 || result.signal !== null) {
      throw new Error(`${lane}: unsuccessful child exit`, { cause: result.error })
    }
    if (
      ![startedAt, exitedAt].every(Number.isSafeInteger) ||
      startedAt < 0 ||
      exitedAt < startedAt
    ) {
      throw new Error(`${lane}: missing or unordered parent clock observations`)
    }
    if (lane === 'soak')
      validateJestReport(JSON.parse(readFile('.tmp/e2e/share-soak-jest.json', 'utf8')))
    const cleanupAt = cleanupTimestamp(
      lane,
      readFile(`.tmp/e2e/${stem}.jsonl`, 'utf8'),
      observation,
      cases
    )
    if (exitedAt - startedAt > totalMs) throw new Error(`${lane}: inclusive process bound exceeded`)
    if (exitedAt - cleanupAt > exitMs) throw new Error(`${lane}: post-cleanup bound exceeded`)
    observation.cleanupAt = cleanupAt
    observation.accepted = true
    return { ...observation, cleanupAt }
  } catch (error) {
    observation.accepted = false
    observation.rejection = error.message
    throw error
  } finally {
    writeFile(exitFile, JSON.stringify(observation) + '\n')
  }
}

/**
 * Credits the seeded-timer control only for its intended failure: the parent's watchdog killed
 * the fixed control child after Jest reported the control case passed with every ordinary case
 * skipped, and after that case's single clean cleanup row, stamped within this parent's own
 * observation. A natural exit, any other exit, a failed or timed-out Jest case (even when its
 * body later wrote a passed row) and stale, wrong-case, tainted or contradictory evidence all
 * refuse the credit.
 * @param {object} [ports] Artifact access, injectable for broker-free controls.
 * @param {Function} [ports.readFile] Reader of the control's `.tmp/e2e` artifacts.
 * @returns {void}
 * @throws {Error} Prefixed `control: ` when the control cannot be credited.
 */
const verifyControlFailure = ({ readFile = fs.readFileSync } = {}) => {
  const { spec, cases: skipped } = LANES.ordinary
  const { stem, totalMs, cases } = LANES.ordinary.control
  const read = (file, parse = (text) => text) => {
    try {
      return parse(readFile(`.tmp/e2e/${file}`, 'utf8'))
    } catch {
      throw new Error(`control: missing or unreadable ${file}`)
    }
  }
  const exit = read(`${stem}-exit.json`, JSON.parse)
  if (
    exit?.lane !== 'ordinary' ||
    exit.control !== true ||
    !isDeepStrictEqual(exit.argv, jestArgs(spec, stem)) ||
    exit.totalMs !== totalMs ||
    exit.watchdogTerminated !== true ||
    exit.status !== null ||
    exit.signal !== 'SIGKILL' ||
    exit.accepted !== false ||
    exit.rejection !== 'ordinary: unsuccessful child exit'
  )
    throw new Error('control: the child was not stopped by the watchdog')
  if (
    ![exit.startedAt, exit.exitedAt].every(Number.isSafeInteger) ||
    exit.startedAt < 0 ||
    exit.exitedAt < exit.startedAt
  )
    throw new Error('control: missing or unordered parent clock observations')
  validateJestReport(read(`${stem}-jest.json`, JSON.parse), {
    label: 'control',
    spec,
    passed: cases,
    skipped,
  })
  try {
    cleanupTimestamp('ordinary', read(`${stem}.jsonl`), exit, cases)
  } catch (error) {
    throw new Error(`control: ${error.message}`, { cause: error })
  }
}

module.exports = {
  runNaturalExit,
  cleanupTimestamp,
  validateJestReport,
  verifyControlFailure,
  monotonicMs,
}

if (require.main === module) {
  try {
    if (process.argv.length !== 3) throw new Error('Usage: node scripts/ci/naturalExit.js <lane>')
    const lane = process.argv[2]
    const direct = lane === 'shutdown' || lane === 'ordinary'
    if (!direct && process.env.KAFKAJS_SOAK_ORIGIN_FILE) {
      const { ShareSoakJob } = require('./shareSoakJob')
      const job = new ShareSoakJob()
      const profile = lane === 'soak' ? 'short' : lane === 'soak-full' ? 'full' : null
      if (!profile) throw new Error('Unknown fixed parent')
      const state = JSON.parse(fs.readFileSync('.tmp/e2e/lifecycle.json', 'utf8'))
      if (state.profile !== profile) throw new Error('Parent differs from admitted fixed profile')
      const out = fs.openSync('.tmp/e2e/stdout.log', 'a')
      const err = fs.openSync('.tmp/e2e/stderr.log', 'a')
      try {
        job.stage(
          'parent',
          () =>
            job.parent(() =>
              runNaturalExit(lane, {
                spawn: (executable, argv, options) =>
                  spawnSync(executable, argv, { ...options, stdio: ['ignore', out, err] }),
              })
            ),
          PROFILES[profile].parentMs
        )
      } finally {
        fs.closeSync(out)
        fs.closeSync(err)
      }
    } else if (direct) runNaturalExit(lane)
    else throw new Error('Share soak requires its frozen CI lifecycle')
  } catch (error) {
    console.error(error)
    process.exitCode = 1
  }
}
