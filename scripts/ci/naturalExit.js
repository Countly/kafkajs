const fs = require('fs')
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
    totalMs: 120000,
    spec: 'shareShutdownLive.spec.js',
    cases: [
      'sends four decisions and releases six held records before lock expiry',
      'writes no share frame after a fresh timeout zero call',
      'stops cleanly and releases unstarted records',
    ],
  },
}

/** @returns {number} Absolute OS monotonic milliseconds, shared with the fixed child specs. */
const monotonicMs = () => Number(process.hrtime.bigint() / 1000000n)

/**
 * Selects completed cleanup in original case order on the parent's clock.
 * Shutdown's fixed child stamps hrtime directly; soak also records its explicit clock origin.
 * @param {string} lane Fixed CI child selection.
 * @param {string} text Original child evidence JSONL.
 * @param {object} observation Parent entry and actual child-exit observations.
 * @returns {number} The last completed cleanup timestamp.
 */
const cleanupTimestamp = (lane, text, { startedAt, exitedAt }) => {
  const body = text.trim()
  const rows = body ? body.split('\n').map(JSON.parse) : []
  if (lane !== 'shutdown' && verifySharePrimingLink(rows).length)
    throw new Error('soak: incomplete or invalid priming cleanup evidence')
  const done = rows.filter((row) => row?.event === 'case-complete')
  const cases = LANES[lane].cases
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
        (lane !== 'shutdown' &&
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
 * Requires the fixed, actually executed Jest inventory with no skipped or failed cases.
 * @param {object} report Original Jest JSON report.
 * @returns {void}
 * @throws {Error} On missing or changed test inventory.
 */
const validateJestReport = (report) => {
  const suites = report.testResults
  const suite = suites?.[0]
  const cases = suite?.assertionResults
  if (
    report.success !== true ||
    report.numTotalTests !== 1 ||
    report.numPassedTests !== 1 ||
    report.numFailedTests !== 0 ||
    report.numPendingTests !== 0 ||
    report.numTodoTests !== 0 ||
    suites?.length !== 1 ||
    !suite.name?.endsWith('/src/consumer/share/__tests__/shareSoakLive.spec.js') ||
    suite.status !== 'passed' ||
    cases?.length !== 1 ||
    cases[0].title !== TITLE ||
    cases[0].status !== 'passed'
  )
    throw new Error('soak: invalid Jest inventory')
}

/**
 * Observes actual child exit under a watchdog, archives parent facts even on rejection, and
 * enforces both inclusive bounds using original cleanup evidence. Child argv is fixed: callers
 * cannot request forced exit or substitute a child with a different cleanup clock contract.
 * @param {'soak'|'soak-full'|'shutdown'} lane Fixed CI child selection.
 * @param {object} [ports] OS operations, injectable for broker-free parent controls.
 * @param {Function} [ports.spawn] Synchronous child spawn with timeout/status observation.
 * @param {Function} [ports.now] Absolute OS monotonic milliseconds.
 * @param {Function} [ports.readFile] Original JSONL evidence reader.
 * @param {Function} [ports.writeFile] Parent exit artifact writer.
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
    throw new Error('Expected natural-exit lane: soak, soak-full or shutdown')
  const { totalMs, spec } = LANES[lane]
  const exitMs = 10000
  if (
    lane !== 'shutdown' &&
    process.execArgv.some((arg) =>
      /^(--require|--import|--loader|--experimental-loader|--eval|-r$|-e$)/.test(arg)
    )
  )
    throw new Error('Forbidden parent preload or eval override')
  const profile = lane === 'soak' ? 'short' : 'full'
  const childEnv = lane === 'shutdown' ? undefined : childEnvironment(profile, env)
  const args =
    lane !== 'shutdown'
      ? childArgs(profile)
      : [
          ...(lane === 'soak' ? ['--expose-gc'] : []),
          'node_modules/jest/bin/jest.js',
          '--ci',
          '--detectOpenHandles',
          '--runInBand',
          '--runTestsByPath',
          `src/consumer/share/__tests__/${spec}`,
        ]
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
  writeFile(`.tmp/e2e/share-${lane}-exit.json`, JSON.stringify(observation) + '\n')
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
      readFile(`.tmp/e2e/share-${lane}.jsonl`, 'utf8'),
      observation
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
    writeFile(`.tmp/e2e/share-${lane}-exit.json`, JSON.stringify(observation) + '\n')
  }
}

module.exports = { runNaturalExit, cleanupTimestamp, validateJestReport, monotonicMs }

if (require.main === module) {
  try {
    if (process.argv.length !== 3) throw new Error('Usage: node scripts/ci/naturalExit.js <lane>')
    const lane = process.argv[2]
    if (lane !== 'shutdown' && process.env.KAFKAJS_SOAK_ORIGIN_FILE) {
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
    } else if (lane === 'shutdown') runNaturalExit(lane)
    else throw new Error('Share soak requires its frozen CI lifecycle')
  } catch (error) {
    console.error(error)
    process.exitCode = 1
  }
}
