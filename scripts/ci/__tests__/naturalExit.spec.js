const { runNaturalExit, validateJestReport, verifyControlFailure } = require('../naturalExit')
const REAL_CONTROL_REPORT = require('./fixtures/naturalExitControlJest.json')
const { PROFILES, childArgs, childEnvironment } = require('../shareSoakContract')
const { primingReceiptHash } = require('../../evidence/sharePrimingLink')

const SOAK_CASE = 'preserves share identities and resources through the short soak'
const SHUTDOWN_CASES = [
  'sends four decisions and releases six held records before lock expiry',
  'writes no share frame after a fresh timeout zero call',
  'stops cleanly and releases unstarted records',
  'connect during disconnect reconnects only after DISCONNECT',
]
const SHUTDOWN_TOTAL_MS = 335000
const ORDINARY_SPEC = 'src/__tests__/naturalExitLive.spec.js'
const ORDINARY_CASES = [
  'classic dynamic consumer consumes one message and disconnects',
  'classic static consumer keeps its group slot after disconnect',
  'consumer-protocol dynamic consumer consumes one message and disconnects',
  'consumer-protocol static consumer consumes one message and disconnects',
  'idempotent producer sends one message and disconnects',
  'transactional producer commits one message and disconnects',
  'admin creates and describes a topic and disconnects',
]
const ORDINARY_TOTAL_MS = 364000
const CONTROL_CASES = ['leaves one interval alive after its producer disconnects']
const CONTROL_TOTAL_MS = 42000
const CONTROL_REPORT = '.tmp/e2e/natural-exit-ordinary-control-jest.json'
const STEMS = {
  soak: 'share-soak',
  'soak-full': 'share-soak-full',
  shutdown: 'share-shutdown',
  ordinary: 'natural-exit-ordinary',
}
/** @param {string} spec Repository path of the live spec. @returns {string[]} The fixed child argv. */
const jestChild = (spec) => [
  'node_modules/jest/bin/jest.js',
  '--ci',
  '--detectOpenHandles',
  '--runInBand',
  '--reporters=./testHelpers/quietReporter.js',
  '--runTestsByPath',
  spec,
]
const CONTROL_ARGV = [...jestChild(ORDINARY_SPEC), '--json', `--outputFile=${CONTROL_REPORT}`]

/**
 * The Jest JSON report a hosted control run really wrote (Jest 30.5.1, Node 24): the control case
 * passed, the seven ordinary cases were skipped, and Jest named the suite `focused`. Only the
 * runner's checkout path is replaced. The Node 22 and 26 reports differ in timestamps only.
 * @returns {object} A mutable copy.
 */
const controlReport = () => JSON.parse(JSON.stringify(REAL_CONTROL_REPORT))

/**
 * The three artifacts of a control run that the watchdog killed after its case passed.
 * @returns {{exit: object, report: object, rows: object[], files: Map<string, string>, read: Function}}
 *   Mutable artifacts and a reader over their serialized form.
 */
const controlArtifacts = () => {
  const artifacts = {
    exit: {
      lane: 'ordinary',
      control: true,
      argv: CONTROL_ARGV,
      startedAt: 1000,
      exitedAt: 1000 + CONTROL_TOTAL_MS,
      status: null,
      signal: 'SIGKILL',
      totalMs: CONTROL_TOTAL_MS,
      watchdogTerminated: true,
      accepted: false,
      rejection: 'ordinary: unsuccessful child exit',
    },
    report: controlReport(),
    rows: [
      {
        event: 'case-complete',
        case: CONTROL_CASES[0],
        status: 'passed',
        tainted: false,
        cleanup: { completedAt: 6000 },
      },
    ],
    files: new Map(),
  }
  artifacts.read = (file, encoding) => {
    if (encoding !== 'utf8') throw new Error('Wrong encoding')
    const text = artifacts.files.has(file)
      ? artifacts.files.get(file)
      : {
          '.tmp/e2e/natural-exit-ordinary-control-exit.json': JSON.stringify(artifacts.exit),
          [CONTROL_REPORT]: JSON.stringify(artifacts.report),
          '.tmp/e2e/natural-exit-ordinary-control.jsonl': artifacts.rows
            .map((row) => JSON.stringify(row) + '\n')
            .join(''),
        }[file]
    if (text === undefined) throw Object.assign(new Error(`ENOENT: ${file}`), { code: 'ENOENT' })
    return text
  }
  return artifacts
}

/**
 * Adds the independently sealed first-use completion before the original scored cleanup row.
 * Parent tests exercise completion linkage; complete source reconstruction has separate controls.
 * @param {object[]} rows Original scored case rows.
 * @returns {object[]} Serialized first-use linkage followed by original scored observations.
 */
const withPriming = (rows) => {
  const prime = {
    event: 'priming-complete',
    scenario: 'prime',
    scope: 'priming',
    status: 'passed',
    tainted: false,
    schedule: { slotMs: 32000 },
    profileMs: 960000,
    elapsedProfileMs: 960000,
    protocolSha256: 'a'.repeat(64),
    timing: { startedAt: 0, workDue: 1010000, observedAt: 970000 },
    cleanup: {
      groupsAbsent: true,
      topicsAbsent: true,
      completedAt: 971000,
      clock: { origin: 'hrtime', completedAt: 971000 },
    },
  }
  const link = {
    scenario: 'prime',
    status: 'passed',
    passes: 1,
    cycles: 30,
    resets: 10,
    verifiers: 31,
    completedAt: 970000,
    boundaryAt: 971000,
    receiptSha256: primingReceiptHash(prime),
  }
  return [
    { event: 'priming-start', startedAt: 0, totalMs: 1020000, passes: 1, cycles: 30, resets: 10 },
    prime,
    { event: 'priming-boundary', priming: link },
    ...rows.map((row) =>
      row.event !== 'case-complete'
        ? row
        : {
            ...row,
            scenario: 'score',
            scope: 'scored',
            priming: link,
            timing: { startedAt: 0, stages: [{ name: 'setup', startedAt: 972000 }] },
          }
    ),
  ]
}

/**
 * Supplies only the OS boundary: the production parent still parses and validates the JSONL.
 * @param {'soak'|'soak-full'|'shutdown'|'ordinary'} lane CI lane.
 * @param {{control?: boolean}} [options] Runs the ordinary lane's seeded-timer control.
 * @returns {object} Mutable original timestamps, child result, evidence and captured artifacts.
 */
const fixture = (lane, { control = false } = {}) => {
  const soak = lane === 'soak' || lane === 'soak-full'
  const profile = lane === 'soak' ? PROFILES.short : PROFILES.full
  const stem = control ? 'natural-exit-ordinary-control' : STEMS[lane]
  const totalMs =
    lane === 'shutdown'
      ? SHUTDOWN_TOTAL_MS
      : lane === 'ordinary'
        ? control
          ? CONTROL_TOTAL_MS
          : ORDINARY_TOTAL_MS
        : profile.totalMs
  const completedAt = 1000 + totalMs - 10000
  const cases =
    lane === 'shutdown'
      ? SHUTDOWN_CASES
      : lane === 'ordinary'
        ? control
          ? CONTROL_CASES
          : ORDINARY_CASES
        : [SOAK_CASE]
  const state = {
    startedAt: 1000,
    exitedAt: 1000 + totalMs,
    result: { status: 0, signal: null },
    rows: cases.map((title, index) => {
      const at = completedAt - (cases.length - index - 1) * 20000
      return {
        event: 'case-complete',
        case: title,
        status: 'passed',
        tainted: false,
        ...(soak && {
          profileMs: profile.profileMs,
          selector: profile.selector,
          soakProfile: profile.profile,
          stem: profile.stem,
          schedule: { slotMs: profile.slotMs, totalMs },
        }),
        cleanup: {
          groupsAbsent: true,
          topicsAbsent: true,
          completedAt: at,
          ...(soak && { clock: { origin: 'hrtime', completedAt: at } }),
        },
      }
    }),
  }
  const artifacts = new Map()
  let childReturned = false
  const spawn = jest.fn(() => {
    childReturned = true
    return state.result
  })
  const run = () =>
    runNaturalExit(lane, {
      spawn,
      env: control ? { NATURAL_EXIT_CONTROL: '1' } : {},
      now: () => (childReturned ? state.exitedAt : state.startedAt),
      readFile: (file, encoding) => {
        if (file === '.tmp/e2e/share-soak-jest.json')
          return JSON.stringify({
            success: true,
            numTotalTests: 1,
            numPassedTests: 1,
            numFailedTests: 0,
            numPendingTests: 0,
            numTodoTests: 0,
            testResults: [
              {
                name: '/repo/src/consumer/share/__tests__/shareSoakLive.spec.js',
                status: 'passed',
                assertionResults: [{ title: SOAK_CASE, status: 'passed' }],
              },
            ],
          })
        if (file !== `.tmp/e2e/${stem}.jsonl` || encoding !== 'utf8')
          throw new Error('Wrong evidence input')
        return (
          state.text ??
          (soak ? withPriming(state.rows) : state.rows)
            .map((row) => JSON.stringify(row))
            .join('\n') + '\n'
        )
      },
      writeFile: (file, text) => artifacts.set(file, JSON.parse(text)),
    })
  return { state, artifacts, spawn, run }
}

describe.each(['soak', 'soak-full', 'shutdown'])('CI natural exit > %s', (lane) => {
  test('accepts cleanup +10000 ms and the inclusive total exactly', () => {
    const { state, artifacts, run } = fixture(lane)
    state.rows.unshift({ event: 'progress', cleanup: { completedAt: state.exitedAt } })

    expect(run()).toMatchObject({
      startedAt: 1000,
      exitedAt:
        lane === 'soak' ? 2041000 : lane === 'soak-full' ? 3001000 : 1000 + SHUTDOWN_TOTAL_MS,
      cleanupAt:
        lane === 'soak' ? 2031000 : lane === 'soak-full' ? 2991000 : SHUTDOWN_TOTAL_MS - 9000,
      status: 0,
      signal: null,
      origin: 'hrtime',
      totalMs: lane === 'soak' ? 2040000 : lane === 'soak-full' ? 3000000 : SHUTDOWN_TOTAL_MS,
      exitMs: 10000,
      watchdogTerminated: false,
    })
    expect(artifacts.get(`.tmp/e2e/share-${lane}-exit.json`)).toMatchObject({
      startedAt: 1000,
      exitedAt: state.exitedAt,
      watchdogTerminated: false,
    })
  })

  test('rejects cleanup +10001 ms while still inside the inclusive total', () => {
    const { state, run } = fixture(lane)
    const cleanup = state.rows.at(-1).cleanup
    cleanup.completedAt--
    if (cleanup.clock) cleanup.clock.completedAt--
    expect(run).toThrow(/cleanup bound/)
  })

  test('rejects 1 ms past total while inside the post-cleanup allowance', () => {
    const { state, run } = fixture(lane)
    state.exitedAt++
    const cleanup = state.rows.at(-1).cleanup
    cleanup.completedAt++
    if (cleanup.clock) cleanup.clock.completedAt++
    expect(run).toThrow(/process bound/)
  })

  test('uses the fixed child, a selector-specific watchdog and no forced exit', () => {
    const { spawn, run } = fixture(lane)
    run()
    expect(spawn).toHaveBeenCalledWith(
      process.execPath,
      lane !== 'shutdown'
        ? childArgs(lane === 'soak' ? 'short' : 'full')
        : [
            ...(lane === 'soak' ? ['--expose-gc'] : []),
            'node_modules/jest/bin/jest.js',
            '--ci',
            '--detectOpenHandles',
            '--runInBand',
            '--reporters=./testHelpers/quietReporter.js',
            '--runTestsByPath',
            `src/consumer/share/__tests__/share${lane === 'soak' ? 'Soak' : 'Shutdown'}Live.spec.js`,
          ],
      {
        stdio: 'inherit',
        timeout: lane === 'soak' ? 2040000 : lane === 'soak-full' ? 3000000 : SHUTDOWN_TOTAL_MS,
        killSignal: 'SIGKILL',
        ...(lane !== 'shutdown' && {
          env: childEnvironment(lane === 'soak' ? 'short' : 'full', {}),
        }),
      }
    )
  })

  test.each([
    [
      'watchdog',
      {
        status: 0,
        signal: null,
        error: Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' }),
      },
    ],
    ['spawn error', { status: 0, signal: null, error: new Error('spawn failed') }],
    ['nonzero', { status: 1, signal: null }],
    ['signal', { status: 0, signal: 'SIGKILL' }],
    ['missing status', { signal: null }],
    ['missing signal observation', { status: 0 }],
  ])('rejects %s despite valid timestamps and preserves exit facts', (name, result) => {
    const { state, artifacts, run } = fixture(lane)
    state.result = result
    expect(run).toThrow(/child exit/)
    expect(artifacts.get(`.tmp/e2e/share-${lane}-exit.json`)).toMatchObject({
      startedAt: 1000,
      exitedAt: state.exitedAt,
      watchdogTerminated: name === 'watchdog',
    })
  })

  test.each([
    ['missing', (rows) => rows.pop()],
    ['duplicate', (rows) => rows.push(rows[0])],
    [
      'wrong case',
      (rows) => {
        rows[0].case = 'unrelated case'
      },
    ],
    ['failed case', (rows) => rows.push({ event: 'case-failed' })],
    [
      'failed status',
      (rows) => {
        rows[0].status = 'failed'
      },
    ],
    [
      'tainted',
      (rows) => {
        rows[0].tainted = true
      },
    ],
    [
      'absent cleanup',
      (rows) => {
        delete rows[0].cleanup
      },
    ],
    [
      'missing cleanup timestamp',
      (rows) => {
        delete rows[0].cleanup.completedAt
      },
    ],
    [
      'fractional cleanup timestamp',
      (rows) => {
        rows[0].cleanup.completedAt += 0.5
      },
    ],
    [
      'cleanup before entry',
      (rows) => {
        rows[0].cleanup.completedAt = 999
      },
    ],
    [
      'cleanup after exit',
      (rows) => {
        rows.at(-1).cleanup.completedAt += 10001
      },
    ],
    [
      'foreign clock',
      (rows) => {
        rows[0].cleanup.clock = {
          origin: 'performance-ms',
          completedAt: rows[0].cleanup.completedAt,
        }
      },
    ],
    [
      'uncorrelated clock timestamp',
      (rows) => {
        rows[0].cleanup.clock = { origin: 'hrtime', completedAt: rows[0].cleanup.completedAt - 1 }
      },
    ],
  ])('rejects %s cleanup evidence', (name, mutate) => {
    const { state, run } = fixture(lane)
    mutate(state.rows)
    expect(run).toThrow(/cleanup evidence/)
  })

  test.each([
    ['missing entry', undefined, 1000],
    ['nonfinite entry', Infinity, 1000],
    ['fractional entry', 0.5, 1000],
    ['negative entry', -1, 1000],
    ['missing exit', 1000, undefined],
    ['nonfinite exit', 1000, NaN],
    ['reversed parent clock', 2000, 1000],
    ['unsafe exit', 1000, Number.MAX_SAFE_INTEGER + 1],
  ])('rejects %s clock observations', (name, startedAt, exitedAt) => {
    const { state, run } = fixture(lane)
    Object.assign(state, { startedAt, exitedAt })
    expect(run).toThrow(/parent clock/)
  })

  test.each(['', '{', 'null\n', '{}\n'])('rejects unusable JSONL %j', (text) => {
    const { state, run } = fixture(lane)
    state.text = text
    expect(run).toThrow()
  })
})

describe('CI natural exit > soak cleanup contract', () => {
  test.each([
    ['historical short', { profileMs: 480000 }],
    [
      'historical full with matching durations',
      {
        selector: 'SHARE_SOAK_FULL',
        soakProfile: 'share-consumer',
        stem: 'share-soak-full',
      },
    ],
    ['unbound selector', { selector: undefined }],
    ['old slot', { schedule: { slotMs: 16000, totalMs: 2040000 } }],
    ['old total', { schedule: { slotMs: 32000, totalMs: 540000 } }],
  ])('rejects %s evidence', (_name, change) => {
    const { state, run } = fixture('soak')
    Object.assign(state.rows[0], change)
    expect(run).toThrow(/cleanup evidence/)
  })

  test.each([
    [
      'missing clock origin',
      (row) => {
        delete row.cleanup.clock
      },
    ],
    [
      'wrong profile',
      (row) => {
        row.profileMs = 120000
      },
    ],
    [
      'group remains',
      (row) => {
        row.cleanup.groupsAbsent = false
      },
    ],
    [
      'topic remains',
      (row) => {
        row.cleanup.topicsAbsent = false
      },
    ],
  ])('rejects %s', (name, mutate) => {
    const { state, run } = fixture('soak')
    mutate(state.rows[0])
    expect(run).toThrow(/cleanup evidence/)
  })
})

describe('CI natural exit > shutdown cleanup order', () => {
  test('rejects four rows with a repeated case instead of the missing case', () => {
    const { state, run } = fixture('shutdown')
    state.rows[1].case = state.rows[0].case
    expect(run).toThrow(/cleanup evidence/)
  })

  test('rejects cleanup timestamps that move backwards despite valid final cleanup', () => {
    const { state, run } = fixture('shutdown')
    state.rows[1].cleanup.completedAt = state.rows[0].cleanup.completedAt - 1
    expect(run).toThrow(/cleanup evidence/)
  })

  test('rejects repeated cleanup timestamps despite four distinct completed cases', () => {
    const { state, run } = fixture('shutdown')
    state.rows[1].cleanup.completedAt = state.rows[0].cleanup.completedAt
    expect(run).toThrow(/cleanup evidence/)
  })

  test('rejects reversed completion rows instead of sorting them into a pass', () => {
    const { state, run } = fixture('shutdown')
    state.rows.reverse()
    expect(run).toThrow(/cleanup evidence/)
  })

  test('shutdown lane accepts four ordered cleanup records including the close-admission case', () => {
    const { state, run } = fixture('shutdown')
    expect(state.rows.map((row) => row.case)).toEqual(SHUTDOWN_CASES)
    expect(state.rows).toHaveLength(4)
    expect(run()).toMatchObject({
      accepted: true,
      totalMs: SHUTDOWN_TOTAL_MS,
      cleanupAt: state.rows[3].cleanup.completedAt,
    })
  })

  test('shutdown lane rejects a missing or reordered close-admission record', () => {
    const missing = fixture('shutdown')
    missing.state.rows.pop()
    expect(missing.state.rows.map((row) => row.case)).toEqual(SHUTDOWN_CASES.slice(0, 3))
    expect(missing.run).toThrow('shutdown: incomplete or invalid cleanup evidence')

    const reordered = fixture('shutdown')
    const [third, fourth] = reordered.state.rows.slice(2).map((row) => row.case)
    reordered.state.rows[2].case = fourth
    reordered.state.rows[3].case = third
    expect(reordered.run).toThrow('shutdown: incomplete or invalid cleanup evidence')
  })
})

describe('CI natural exit > ordinary clients', () => {
  test('ordinary lane names its spec and cases', () => {
    const { state, artifacts, spawn, run } = fixture('ordinary')
    expect(run()).toMatchObject({
      accepted: true,
      control: false,
      totalMs: ORDINARY_TOTAL_MS,
      cleanupAt: state.rows.at(-1).cleanup.completedAt,
    })
    expect(spawn).toHaveBeenCalledWith(process.execPath, jestChild(ORDINARY_SPEC), {
      stdio: 'inherit',
      timeout: ORDINARY_TOTAL_MS,
      killSignal: 'SIGKILL',
    })
    expect(artifacts.get('.tmp/e2e/natural-exit-ordinary-exit.json')).toMatchObject({
      lane: 'ordinary',
      control: false,
      accepted: true,
      argv: jestChild(ORDINARY_SPEC),
    })

    const live = require('node:fs').readFileSync(ORDINARY_SPEC, 'utf8')
    for (const title of [...ORDINARY_CASES, ...CONTROL_CASES]) expect(live).toContain(`'${title}'`)

    const missingStatic = fixture('ordinary')
    missingStatic.state.rows.splice(1, 1)
    expect(missingStatic.run).toThrow('ordinary: incomplete or invalid cleanup evidence')
  })

  test('ordinary control run is reported as a failure', () => {
    const fs = require('node:fs')
    const os = require('node:os')
    const path = require('node:path')
    const { spawnSync } = require('node:child_process')
    const clean = fixture('ordinary', { control: true })
    expect(clean.run()).toMatchObject({ accepted: true, control: true, totalMs: CONTROL_TOTAL_MS })
    expect(clean.spawn.mock.calls[0][2].timeout).toBe(CONTROL_TOTAL_MS)

    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ordinary-control-'))
    const artifacts = new Map()
    const evidence = path.join(root, 'control.jsonl')
    const report = path.join(root, 'control-jest.json')
    const child = `
      const at = Number(process.hrtime.bigint() / 1000000n)
      const row = { event: 'case-complete', case: ${JSON.stringify(CONTROL_CASES[0])},
        status: 'passed', tainted: false, cleanup: { completedAt: at } }
      require('node:fs').writeFileSync(${JSON.stringify(evidence)}, JSON.stringify(row) + '\\n')
      require('node:fs').writeFileSync(${JSON.stringify(report)}, ${JSON.stringify(
        JSON.stringify(controlReport())
      )})
      setInterval(() => {}, 10)
    `
    try {
      const run = () =>
        runNaturalExit('ordinary', {
          env: { NATURAL_EXIT_CONTROL: '1' },
          // Only the watchdog is shortened; the recorded argv stays the fixed control child.
          spawn: (executable, argv, options) =>
            spawnSync(executable, ['-e', child], { ...options, stdio: 'ignore', timeout: 500 }),
          readFile: () => fs.readFileSync(evidence, 'utf8'),
          writeFile: (name, text) => artifacts.set(name, JSON.parse(text)),
        })
      expect(run).toThrow('ordinary: unsuccessful child exit')
      expect(fs.readFileSync(evidence, 'utf8')).toContain(CONTROL_CASES[0])
      expect(artifacts.get('.tmp/e2e/natural-exit-ordinary-control-exit.json')).toMatchObject({
        lane: 'ordinary',
        control: true,
        accepted: false,
        watchdogTerminated: true,
        totalMs: CONTROL_TOTAL_MS,
        argv: CONTROL_ARGV,
        rejection: 'ordinary: unsuccessful child exit',
      })
      expect(artifacts.has('.tmp/e2e/natural-exit-ordinary-exit.json')).toBe(false)

      const real = {
        '.tmp/e2e/natural-exit-ordinary-control-exit.json': JSON.stringify(
          artifacts.get('.tmp/e2e/natural-exit-ordinary-control-exit.json')
        ),
        [CONTROL_REPORT]: fs.readFileSync(report, 'utf8'),
        '.tmp/e2e/natural-exit-ordinary-control.jsonl': fs.readFileSync(evidence, 'utf8'),
      }
      expect(verifyControlFailure({ readFile: (file) => real[file] })).toBeUndefined()

      const cleanExit = {
        ...real,
        '.tmp/e2e/natural-exit-ordinary-control-exit.json': JSON.stringify(
          clean.artifacts.get('.tmp/e2e/natural-exit-ordinary-control-exit.json')
        ),
      }
      expect(() => verifyControlFailure({ readFile: (file) => cleanExit[file] })).toThrow(
        'control: the child was not stopped by the watchdog'
      )
    } finally {
      fs.rmSync(root, { recursive: true, force: true })
    }
  })

  test('ordinary control is credited only for a passed control case the watchdog killed', () => {
    const accepted = controlArtifacts()
    expect(verifyControlFailure({ readFile: accepted.read })).toBeUndefined()

    const damages = {
      'Jest timeout then a late passed row': (a) => {
        Object.assign(a.report, { success: false, numPassedTests: 0, numFailedTests: 1 })
        a.report.testResults[0].status = 'failed'
        a.report.testResults[0].assertionResults[7].status = 'failed'
      },
      'absent Jest report': (a) => a.files.set(CONTROL_REPORT, undefined),
      'a truncated exit artifact': (a) =>
        a.files.set('.tmp/e2e/natural-exit-ordinary-control-exit.json', '{'),
      'failed Jest suite': (a) => {
        Object.assign(a.report, { success: false, numRuntimeErrorTestSuites: 1 })
        a.report.testResults[0].status = 'failed'
      },
      'an ordinary case ran': (a) => {
        Object.assign(a.report, { numPassedTests: 2, numPendingTests: 6 })
        a.report.testResults[0].assertionResults[0].status = 'passed'
      },
      'a missing skipped case': (a) => {
        Object.assign(a.report, { numTotalTests: 7, numPendingTests: 6 })
        a.report.testResults[0].assertionResults.splice(1, 1)
      },
      'another spec': (a) => (a.report.testResults[0].name = '/repo/src/other.spec.js'),
      'a suite status that contradicts its skipped cases': (a) =>
        (a.report.testResults[0].status = 'passed'),
      'a stale cleanup row': (a) => (a.rows[0].cleanup.completedAt = 999),
      'cleanup after the kill': (a) => (a.rows[0].cleanup.completedAt = a.exit.exitedAt + 1),
      'a wrong-case cleanup row': (a) => (a.rows[0].case = ORDINARY_CASES[0]),
      'a tainted cleanup row': (a) => (a.rows[0].tainted = true),
      'a contradictory failure row': (a) => a.rows.push({ event: 'case-failed', error: 'late' }),
      'no cleanup row': (a) => (a.rows = []),
      'a natural exit': (a) =>
        Object.assign(a.exit, {
          status: 0,
          signal: null,
          watchdogTerminated: false,
          accepted: true,
        }),
      'a nonzero exit': (a) =>
        Object.assign(a.exit, { status: 1, signal: null, watchdogTerminated: false }),
      'the ordinary child': (a) => (a.exit.argv = jestChild(ORDINARY_SPEC)),
      'the ordinary lane': (a) => (a.exit.control = false),
      'unordered parent clock': (a) => (a.exit.exitedAt = a.exit.startedAt - 1),
    }
    const credited = Object.entries(damages).filter(([, damage]) => {
      const artifacts = controlArtifacts()
      damage(artifacts)
      try {
        verifyControlFailure({ readFile: artifacts.read })
        return true
      } catch (error) {
        return !error.message.startsWith('control: ')
      }
    })
    expect(credited.map(([name]) => name)).toEqual([])
  })

  test('ordinary control accepts the suite status Jest reports for skipped cases and no other', () => {
    const report = controlReport()
    expect(report.testResults[0].status).toBe('focused')
    expect(report.numPendingTests).toBe(ORDINARY_CASES.length)
    const control = {
      label: 'control',
      spec: ORDINARY_SPEC,
      passed: CONTROL_CASES,
      skipped: ORDINARY_CASES,
    }
    expect(validateJestReport(report, control)).toBeUndefined()

    for (const status of ['passed', 'failed', 'pending', 'skipped', undefined]) {
      const forged = controlReport()
      forged.testResults[0].status = status
      expect(() => validateJestReport(forged, control)).toThrow('control: invalid Jest inventory')
    }
  })

  test('a fully executed inventory still requires the passed suite status', () => {
    const soakReport = (status) => ({
      success: true,
      numTotalTests: 1,
      numPassedTests: 1,
      numFailedTests: 0,
      numPendingTests: 0,
      numTodoTests: 0,
      testResults: [
        {
          name: '/repo/src/consumer/share/__tests__/shareSoakLive.spec.js',
          status,
          assertionResults: [{ title: SOAK_CASE, status: 'passed' }],
        },
      ],
    })
    expect(validateJestReport(soakReport('passed'))).toBeUndefined()
    expect(() => validateJestReport(soakReport('focused'))).toThrow('soak: invalid Jest inventory')
  })

  test('ordinary lane leaves the four-case shutdown inventory unchanged', () => {
    const shutdown = fixture('shutdown')
    expect(shutdown.run()).toMatchObject({ accepted: true, totalMs: SHUTDOWN_TOTAL_MS })
    expect(shutdown.spawn).toHaveBeenCalledWith(
      process.execPath,
      jestChild('src/consumer/share/__tests__/shareShutdownLive.spec.js'),
      { stdio: 'inherit', timeout: SHUTDOWN_TOTAL_MS, killSignal: 'SIGKILL' }
    )
    expect(shutdown.artifacts.get('.tmp/e2e/share-shutdown-exit.json')).not.toHaveProperty(
      'control'
    )

    const ordinary = fixture('ordinary')
    ordinary.state.rows = ordinary.state.rows.slice(0, 4).map((row, index) => ({
      ...row,
      case: SHUTDOWN_CASES[index],
    }))
    expect(ordinary.run).toThrow('ordinary: incomplete or invalid cleanup evidence')

    const extra = fixture('shutdown')
    extra.state.rows.push({ ...extra.state.rows[3], case: ORDINARY_CASES[0] })
    expect(extra.run).toThrow('shutdown: incomplete or invalid cleanup evidence')
  })
})

test('rejects an unknown lane before spawning any child', () => {
  const spawn = jest.fn()
  expect(() => runNaturalExit('other', { spawn })).toThrow(/lane/)
  expect(spawn).not.toHaveBeenCalled()
})

describe('CI natural exit > sealed priming link', () => {
  test.each([
    'missing',
    'duplicate',
    'failed',
    'false cleanup',
    'wrong seal',
    'late boundary',
    'reordered',
  ])('rejects %s first-use evidence without selecting its cleanup as scored cleanup', (damage) => {
    const { state, run } = fixture('soak')
    const rows = withPriming(state.rows)
    if (damage === 'missing') rows.splice(1, 1)
    if (damage === 'duplicate') rows.splice(1, 0, rows[1])
    if (damage === 'failed') rows[1].status = 'failed'
    if (damage === 'false cleanup') rows[1].cleanup.groupsAbsent = false
    if (damage === 'wrong seal') rows.at(-1).priming.receiptSha256 = 'b'.repeat(64)
    if (damage === 'late boundary') rows.at(-1).priming.boundaryAt = 1020001
    if (damage === 'reordered') [rows[1], rows[2]] = [rows[2], rows[1]]
    state.text = rows.map((row) => JSON.stringify(row)).join('\n')
    expect(run).toThrow(/priming cleanup evidence/)
  })
})

describe('CI natural exit > actual fixed CLI subprocess observation', () => {
  test.each([false, true])(
    'observes the actual fixed child PID and refuses event-loop retention, retained=%s',
    (retained) => {
      const fs = require('node:fs')
      const os = require('node:os')
      const path = require('node:path')
      const { spawnSync } = require('node:child_process')
      const root = fs.mkdtempSync(path.join(os.tmpdir(), 'fixed-soak-parent-'))
      const artifact = new Map()
      try {
        fs.mkdirSync(path.join(root, 'scripts/soak'), { recursive: true })
        fs.mkdirSync(path.join(root, '.tmp/e2e'), { recursive: true })
        fs.writeFileSync(
          path.join(root, 'rows.json'),
          JSON.stringify(withPriming(fixture('soak-full').state.rows))
        )
        fs.writeFileSync(
          path.join(root, 'scripts/soak/soak.js'),
          `
        const fs = require('node:fs')
        const rows = JSON.parse(fs.readFileSync('rows.json'))
        const at = Number(process.hrtime.bigint() / 1000000n)
        const prime = rows.find(r => r.event === 'priming-complete')
        prime.cleanup.completedAt = at
        prime.cleanup.clock.completedAt = at
        const hash = require(${JSON.stringify(require.resolve('../../evidence/sharePrimingLink'))}).primingReceiptHash(prime)
        rows.find(r => r.event === 'priming-boundary').priming.receiptSha256 = hash
        rows.at(-1).priming.receiptSha256 = hash
        rows.at(-1).cleanup.completedAt = at
        rows.at(-1).cleanup.clock.completedAt = at
        fs.writeFileSync('.tmp/e2e/share-soak-full.jsonl', rows.map(JSON.stringify).join('\\n') + '\\n')
        ${retained ? 'setInterval(() => {}, 10)' : 'setImmediate(() => {})'}
      `
        )
        const run = () =>
          runNaturalExit('soak-full', {
            env: {},
            // The shortened OS port is only the bounded failure control; accepted argv/watchdog remain fixed.
            spawn: (executable, argv, options) =>
              spawnSync(executable, argv, {
                ...options,
                cwd: root,
                timeout: retained ? 200 : options.timeout,
              }),
            readFile: (name) => fs.readFileSync(path.join(root, name), 'utf8'),
            writeFile: (name, bytes) => artifact.set(name, JSON.parse(bytes)),
          })
        if (retained) expect(run).toThrow(/child exit/)
        else expect(run()).toMatchObject({ status: 0, signal: null, accepted: true })
        const exit = artifact.get('.tmp/e2e/share-soak-full-exit.json')
        expect(exit.childPid).toBeGreaterThan(0)
        expect(exit.parentPid).toBe(process.pid)
        expect(exit.argv).toEqual(childArgs('full'))
        expect(exit.watchdogTerminated).toBe(retained)
        expect(exit.accepted).toBe(!retained)
      } finally {
        fs.rmSync(root, { recursive: true, force: true })
      }
    }
  )
})

test('CI natural exit > refuses the CLI outside its original frozen lifecycle', () => {
  const { spawnSync } = require('node:child_process')
  const result = spawnSync(process.execPath, ['scripts/ci/naturalExit.js', 'soak-full'], {
    env: {},
    encoding: 'utf8',
    timeout: 3000,
  })
  expect(result.status).toBe(1)
  expect(result.stderr).toContain('Share soak requires its frozen CI lifecycle')
})
