const { evidenceStatus, CAPTURES } = require('../evidence')
const { ShareCycleOracle } = require('../../soak/soak')

/** Completeness structure only; oracle replay is controlled separately from the real oracle suite. */
const fixture = () => {
  const rows = []
  const add = (event, fields = {}) =>
    rows.push({
      event,
      diagnostic: true,
      acceptanceEligible: false,
      selector: 'DIAG_SHARE_HEAP',
      tree: 'a'.repeat(40),
      node: 'v24.22.2',
      brokerVersion: '4.3.1',
      ...fields,
    })
  const captures = CAPTURES.map((label, i) => ({
    label,
    diagnostic: true,
    acceptanceEligible: false,
    ownerAtMs: i * 100,
    completedOwnerAtMs: i * 100 + 1,
    snapshotMs: 1,
    sizeBytes: 10,
    heapUsed: 100,
  }))
  add('diag-source', {
    harness: 'standalone-short-share',
    sources: ['scripts/diag/shareHeap.js', 'scripts/diag/evidence.js', 'scripts/soak/soak.js'].map(
      (file) => ({ file, sha256: 'a'.repeat(64) })
    ),
  })
  add('diag-envelope', { extraCycles: 10, totalMs: 1955000 })
  add('diag-heap-snapshot', captures[0])
  const slots = Array.from({ length: 30 }, (_, cycle) => ({
    cycle,
    startedAt: cycle * 32000,
    due: (cycle + 1) * 32000,
    completedAt: (cycle + 1) * 32000,
  }))
  add('diag-original-timing', {
    timing: {
      slots,
      phases: [0, 1, 2].map((phase) => ({
        phase,
        startedAt: phase * 320000,
        due: (phase + 1) * 320000,
        completedAt: (phase + 1) * 320000,
      })),
    },
  })
  const prefix = []
  for (let cycle = 0; cycle < 40; cycle++) {
    if ([10, 20, 30].includes(cycle)) add('diag-heap-snapshot', captures[cycle / 10])
    if (cycle === 30)
      add('diag-original-final', {
        final: {
          timing: { kind: 'final', cycle: 29, startedAt: 960000, due: 980000 },
          prefix: [...prefix],
          verifier: { memberId: 'final' },
          positions: [{}, {}],
        },
      })
    const plan = ShareCycleOracle.plan(cycle)
    const produced = plan.records.map(({ sequence, partition }) => ({
      identity: JSON.stringify(['topic', partition, String(sequence)]),
    }))
    prefix.push(...produced.map(({ identity }) => identity))
    const timing = {
      startedAt: cycle * 32000,
      due: (cycle + 1) * 32000,
      completedAt: cycle * 32000 + 1000,
    }
    const observation = {
      produced,
      prefix: [...prefix],
      timing,
      verifier: { memberId: `verifier-${cycle}` },
    }
    if (cycle < 30) {
      add('soak-cycle', {
        cycle,
        status: 'passed',
        violations: [],
        oracles: {
          log: { expected: produced.map(({ identity }) => identity), actual: [] },
          accepted: [],
          broker: [],
        },
        ...observation,
        cleanup: { clientsDisconnected: true },
      })
      if (cycle >= 10 && cycle < 20) add('soak-reset', { cycle })
    } else {
      add('diag-extra-cycle', {
        extra: cycle - 29,
        plan: { ...plan, first: String(plan.first), phase: 'share-steady', reset: false },
        observation,
        violations: [],
        slot: { ...timing, windowStart: cycle * 32000 },
      })
    }
  }
  for (const kind of [
    'baseline',
    'phase-end',
    'phase-end',
    'phase-end',
    'teardown',
    'resource-verdict',
  ]) {
    const phase = CAPTURES[rows.filter((row) => row.kind === 'phase-end').length + 1]
    add('soak-resources', {
      kind,
      phase,
      ok: false,
      violations: ['historical heap screen remains failed'],
    })
  }
  add('diag-repetition-resources', { kind: 'phase-end', phase: 'diag-repeat-10' })
  add('diag-heap-snapshot', captures[4])
  for (const phase of ['share-steady', 'share-reset', 'share-members', 'diag-repeat-10']) {
    for (let i = 0; i < 6; i++)
      add(phase === 'diag-repeat-10' ? 'diag-repetition-resources' : 'soak-resources', {
        kind: 'sample',
        phase,
      })
  }
  add('diag-cleanup', {
    status: 'passed',
    completedAt: 1300000,
    spentMs: 1000,
    budgetMs: 20000,
    baseline: {
      listeners: {},
      sockets: { created: 0, destroyed: 0, open: 0 },
      queueDepth: 0,
      ledgerSize: 0,
      activeResources: {},
    },
    after: {
      listeners: {},
      sockets: { created: 0, destroyed: 0, open: 0 },
      queueDepth: 0,
      ledgerSize: 0,
      activeResources: {},
    },
  })
  add('diag-complete', { extraCycles: 10 })
  return { rows, captures }
}

beforeEach(() => {
  // The structural controls do not fabricate broker success: the unchanged real oracle has its
  // own broker-free suite. Its rejection path is tested below without this stub.
  jest.spyOn(ShareCycleOracle, 'judge').mockReturnValue([])
  for (const method of ['sequence', 'broker', 'verifier', 'position', 'log'])
    jest.spyOn(ShareCycleOracle, method).mockImplementation(() => {})
})
afterEach(() => jest.restoreAllMocks())

test('allows completed diagnostics with archived failed heap verdicts, never acceptance', () => {
  const { rows, captures } = fixture()
  expect(evidenceStatus(rows, captures)).toEqual({ complete: true, violations: [] })
})

test.each([
  [
    'missing original',
    (rows) =>
      rows.splice(
        rows.findIndex(({ cycle }) => cycle === 0),
        1
      ),
  ],
  [
    'missing extra',
    (rows) =>
      rows.splice(
        rows.findIndex(({ extra }) => extra === 1),
        1
      ),
  ],
  ['duplicate extra', (rows) => rows.push(rows.find(({ extra }) => extra === 1))],
  [
    'out of order',
    (rows) => {
      const a = rows.findIndex(({ extra }) => extra === 1)
      ;[rows[a], rows[a + 1]] = [rows[a + 1], rows[a]]
    },
  ],
  [
    'late slot',
    (rows) => {
      rows.find(({ extra }) => extra === 1).slot.completedAt += 32001
    },
  ],
  [
    'oracle failure',
    (rows) => {
      rows.find(({ extra }) => extra === 1).violations.push('missing Accept')
    },
  ],
  [
    'lost plan proof',
    (rows) => {
      rows.find(({ extra }) => extra === 1).plan.records.pop()
    },
  ],
  [
    'no cleanup',
    (rows) =>
      rows.splice(
        rows.findIndex(({ event }) => event === 'diag-cleanup'),
        1
      ),
  ],
  [
    'no final proof',
    (rows) =>
      rows.splice(
        rows.findIndex(({ event }) => event === 'diag-original-final'),
        1
      ),
  ],
  ['no completion', (rows) => rows.pop()],
  ['acceptance row', (rows) => rows.push({ event: 'case-complete' })],
])('rejects false completion: %s', (_, mutate) => {
  const { rows, captures } = fixture()
  mutate(rows)
  expect(evidenceStatus(rows, captures).complete).toBe(false)
})

test('rejects missing or reordered repetition capture and backwards original capture clocks', () => {
  const { rows, captures } = fixture()
  expect(evidenceStatus(rows, captures.slice(0, 4)).complete).toBe(false)
  expect(evidenceStatus(rows, [...captures].reverse()).complete).toBe(false)
  captures[4].ownerAtMs = 0
  expect(evidenceStatus(rows, captures).complete).toBe(false)
})

test('replays the real cycle oracle instead of trusting an empty archived violations list', () => {
  ShareCycleOracle.judge.mockRestore()
  const { rows, captures } = fixture()
  const status = evidenceStatus(rows, captures)
  expect(status.complete).toBe(false)
  expect(status.violations.some((message) => /cycle 0|Cycle 0/.test(message))).toBe(true)
})

test('rejects missing diagnostic source identity, mixed source trees and broker tuples', () => {
  const { rows, captures } = fixture()
  rows[0].tree = 'b'.repeat(40)
  expect(evidenceStatus(rows, captures).complete).toBe(false)
  rows[0].tree = 'a'.repeat(40)
  rows[0].brokerVersion = '4.4.0'
  expect(evidenceStatus(rows, captures).complete).toBe(false)
  rows[0].brokerVersion = '4.3.1'
  rows.splice(
    rows.findIndex(({ event }) => event === 'diag-source'),
    1
  )
  expect(evidenceStatus(rows, captures).complete).toBe(false)
})
