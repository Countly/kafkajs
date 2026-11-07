const fs = require('fs')
const { spawnSync } = require('child_process')

const LANES = {
  soak: {
    totalMs: 540000,
    spec: 'shareSoakLive.spec.js',
    cases: ['preserves share identities and resources through the short soak'],
  },
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
  const rows = text.trim().split('\n').map(JSON.parse)
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
        (lane === 'soak' &&
          (row.profileMs !== 480000 ||
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
 * Observes actual child exit under a watchdog, archives parent facts even on rejection, and
 * enforces both inclusive bounds using original cleanup evidence. Child argv is fixed: callers
 * cannot request forced exit or substitute a child with a different cleanup clock contract.
 * @param {'soak'|'shutdown'} lane Fixed CI child selection.
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
  } = {}
) => {
  if (!Object.hasOwn(LANES, lane)) throw new Error('Expected natural-exit lane: soak or shutdown')
  const { totalMs, spec } = LANES[lane]
  const exitMs = 10000
  const args = [
    ...(lane === 'soak' ? ['--expose-gc'] : []),
    'node_modules/jest/bin/jest.js',
    '--ci',
    '--detectOpenHandles',
    '--runInBand',
    '--runTestsByPath',
    `src/consumer/share/__tests__/${spec}`,
  ]
  const startedAt = now()
  const result = spawn(process.execPath, args, {
    stdio: 'inherit',
    timeout: totalMs,
    killSignal: 'SIGKILL',
  })
  const exitedAt = now()
  const observation = {
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
  if (result.error || result.status !== 0 || result.signal !== null) {
    throw new Error(`${lane}: unsuccessful child exit`, { cause: result.error })
  }
  if (![startedAt, exitedAt].every(Number.isSafeInteger) || startedAt < 0 || exitedAt < startedAt) {
    throw new Error(`${lane}: missing or unordered parent clock observations`)
  }
  const cleanupAt = cleanupTimestamp(
    lane,
    readFile(`.tmp/e2e/share-${lane}.jsonl`, 'utf8'),
    observation
  )
  if (exitedAt - startedAt > totalMs) throw new Error(`${lane}: inclusive process bound exceeded`)
  if (exitedAt - cleanupAt > exitMs) throw new Error(`${lane}: post-cleanup bound exceeded`)
  return { ...observation, cleanupAt }
}

if (require.main === module) {
  try {
    if (process.argv.length !== 3) throw new Error('Usage: node scripts/ci/naturalExit.js <lane>')
    runNaturalExit(process.argv[2])
  } catch (error) {
    console.error(error)
    process.exitCode = 1
  }
}

module.exports = { runNaturalExit }
