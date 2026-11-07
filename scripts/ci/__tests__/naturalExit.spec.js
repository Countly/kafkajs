const { runNaturalExit } = require('../naturalExit')

const SOAK_CASE = 'preserves share identities and resources through the short soak'
const SHUTDOWN_CASES = [
  'sends four decisions and releases six held records before lock expiry',
  'writes no share frame after a fresh timeout zero call',
  'stops cleanly and releases unstarted records',
]

/**
 * Supplies only the OS boundary: the production parent still parses and validates the JSONL.
 * @param {'soak'|'shutdown'} lane CI lane.
 * @returns {object} Mutable original timestamps, child result, evidence and captured artifacts.
 */
const fixture = (lane) => {
  const totalMs = lane === 'soak' ? 1020000 : 120000
  const completedAt = 1000 + totalMs - 10000
  const cases = lane === 'soak' ? [SOAK_CASE] : SHUTDOWN_CASES
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
        ...(lane === 'soak' && {
          profileMs: 960000,
          selector: 'SHARE_SOAK_E2E',
          soakProfile: 'share-consumer-short',
          stem: 'share-soak',
          schedule: { slotMs: 32000, totalMs },
        }),
        cleanup: {
          groupsAbsent: true,
          topicsAbsent: true,
          completedAt: at,
          ...(lane === 'soak' && { clock: { origin: 'hrtime', completedAt: at } }),
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
      now: () => (childReturned ? state.exitedAt : state.startedAt),
      readFile: (file, encoding) => {
        if (file !== `.tmp/e2e/share-${lane}.jsonl` || encoding !== 'utf8')
          throw new Error('Wrong evidence input')
        return state.text ?? state.rows.map((row) => JSON.stringify(row)).join('\n') + '\n'
      },
      writeFile: (file, text) => artifacts.set(file, JSON.parse(text)),
    })
  return { state, artifacts, spawn, run }
}

describe.each(['soak', 'shutdown'])('CI natural exit > %s', (lane) => {
  test('accepts cleanup +10000 ms and the inclusive total exactly', () => {
    const { state, artifacts, run } = fixture(lane)
    state.rows.unshift({ event: 'progress', cleanup: { completedAt: state.exitedAt } })

    expect(run()).toMatchObject({
      startedAt: 1000,
      exitedAt: lane === 'soak' ? 1021000 : 121000,
      cleanupAt: lane === 'soak' ? 1011000 : 111000,
      status: 0,
      signal: null,
      origin: 'hrtime',
      totalMs: lane === 'soak' ? 1020000 : 120000,
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
      [
        ...(lane === 'soak' ? ['--expose-gc'] : []),
        'node_modules/jest/bin/jest.js',
        '--ci',
        '--detectOpenHandles',
        '--runInBand',
        '--runTestsByPath',
        `src/consumer/share/__tests__/share${lane === 'soak' ? 'Soak' : 'Shutdown'}Live.spec.js`,
      ],
      { stdio: 'inherit', timeout: lane === 'soak' ? 1020000 : 120000, killSignal: 'SIGKILL' }
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
    ['old slot', { schedule: { slotMs: 16000, totalMs: 1020000 } }],
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
  test('rejects three rows with a repeated case instead of the missing case', () => {
    const { state, run } = fixture('shutdown')
    state.rows[1].case = state.rows[0].case
    expect(run).toThrow(/cleanup evidence/)
  })

  test('rejects cleanup timestamps that move backwards despite valid final cleanup', () => {
    const { state, run } = fixture('shutdown')
    state.rows[1].cleanup.completedAt = state.rows[0].cleanup.completedAt - 1
    expect(run).toThrow(/cleanup evidence/)
  })

  test('rejects repeated cleanup timestamps despite three distinct completed cases', () => {
    const { state, run } = fixture('shutdown')
    state.rows[1].cleanup.completedAt = state.rows[0].cleanup.completedAt
    expect(run).toThrow(/cleanup evidence/)
  })

  test('rejects reversed completion rows instead of sorting them into a pass', () => {
    const { state, run } = fixture('shutdown')
    state.rows.reverse()
    expect(run).toThrow(/cleanup evidence/)
  })
})

test('rejects an unknown lane before spawning any child', () => {
  const spawn = jest.fn()
  expect(() => runNaturalExit('other', { spawn })).toThrow(/lane/)
  expect(spawn).not.toHaveBeenCalled()
})
