const { verifyShareQuietHeap } = require('../shareQuietHeap')
const { verifyFiles } = require('../verify')

/**
 * Original resource observations, separate from the supplied summary verdict.
 * @param {number} rate retained bytes per elapsed minute
 * @returns {object[]} ordered source rows and a separate matching resource verdict
 */
const evidence = (rate = 98304) => {
  const rows = [
    { kind: 'baseline', snapshot: { heapAtMs: 1200000, heapUsed: 10000000 } },
    ...['share-steady', 'share-reset', 'share-members'].map((phase, i) => ({
      kind: 'phase-end',
      phase,
      snapshot: {
        heapAtMs: 1200000 + [1, 3, 8][i] * 60000,
        heapUsed: 10000000 + rate * [1, 3, 8][i],
      },
    })),
    { kind: 'teardown', snapshot: { heapAtMs: 1800000, heapUsed: 10000000 } },
  ].map((row) => ({ event: 'soak-resources', ...row }))
  const points = rows.slice(1, 4).map(({ phase, snapshot }) => ({ phase, ...snapshot }))
  rows.push({
    event: 'soak-resources',
    kind: 'resource-verdict',
    ok: true,
    violations: [],
    measured: { quietSlope: { points, bytesPerMinute: rate } },
  })
  return rows
}

describe('independent share quiet-heap evidence validation', () => {
  test.each([0, -98304, 98304])('recomputes %s from the original observations', (rate) => {
    expect(verifyShareQuietHeap(evidence(rate))).toMatchObject({
      bytesPerMinute: rate,
      problems: [],
    })
  })

  test('a forged passing summary cannot hide above-bound original evidence', () => {
    const rows = evidence()
    rows[3].snapshot.heapUsed++
    const result = verifyShareQuietHeap(rows)
    expect(Math.round(result.bytesPerMinute)).toBe(98304)
    expect(result.problems).toEqual([
      'phase-end quiet heap slope screen',
      'quiet slope verdict differs from original observations',
    ])
  })

  test.each([131072, 262144, 393216])('rejects %s B/min even with a matching summary', (rate) => {
    expect(verifyShareQuietHeap(evidence(rate)).problems).toContain(
      'phase-end quiet heap slope screen'
    )
  })

  test.each([undefined, null, NaN, Infinity, -1, '1200000'])(
    'rejects absent or invalid original times %s despite a passing verdict',
    (time) => {
      for (let i = 0; i < 5; i++) {
        const rows = evidence()
        rows[i].snapshot.heapAtMs = time
        expect(verifyShareQuietHeap(rows)).toMatchObject({
          bytesPerMinute: null,
          problems: expect.arrayContaining(['phase-end quiet heap slope screen']),
        })
      }
    }
  )

  test.each([
    'duplicate time',
    'reversed time',
    'missing end',
    'duplicate end',
    'reordered ends',
    'wrong phase',
    'missing baseline',
    'missing teardown',
    'missing verdict',
    'duplicate verdict',
    'invalid heap',
    'absent source',
    'failed verdict',
    'forged points',
  ])('rejects %s', (damage) => {
    const rows = evidence()
    if (damage === 'duplicate time') rows[2].snapshot.heapAtMs = rows[1].snapshot.heapAtMs
    if (damage === 'reversed time') rows[2].snapshot.heapAtMs = 0
    if (damage === 'missing end') rows.splice(2, 1)
    if (damage === 'duplicate end') rows.splice(2, 0, rows[1])
    if (damage === 'reordered ends') [rows[1], rows[2]] = [rows[2], rows[1]]
    if (damage === 'wrong phase') rows[2].phase = 'share-steady'
    if (damage === 'missing baseline') rows.shift()
    if (damage === 'missing teardown') rows.splice(4, 1)
    if (damage === 'missing verdict') rows.pop()
    if (damage === 'duplicate verdict') rows.push(rows[5])
    if (damage === 'invalid heap') rows[2].snapshot.heapUsed = -1
    if (damage === 'absent source') delete rows[2].snapshot
    if (damage === 'failed verdict') rows[5].ok = false
    if (damage === 'forged points') rows[5].measured.quietSlope.points[0].heapUsed++
    expect(verifyShareQuietHeap(rows).problems.length).toBeGreaterThan(0)
  })

  test('keeps both historical receipts failed and refuses their missing monotonic chronology', () => {
    for (const receipt of require('../../soak/__tests__/fixtures/shareHeapReplays.json')) {
      const { baseline, phaseEnds, teardown } = receipt.run
      const rows = [
        { kind: 'baseline', snapshot: baseline },
        ...phaseEnds.map((end) => ({ kind: 'phase-end', ...end })),
        { kind: 'teardown', snapshot: teardown },
        {
          kind: 'resource-verdict',
          ok: false,
          violations: ['share-steady: retained-heap screen'],
          measured: receipt.measured,
        },
      ].map((row) => ({ event: 'soak-resources', ...row }))
      expect(verifyShareQuietHeap(rows)).toMatchObject({
        bytesPerMinute: null,
        problems: expect.arrayContaining([
          'phase-end quiet heap slope screen',
          'share resource verdict is absent or failed',
        ]),
      })
    }
  })

  test.each(['SHARE_SOAK_E2E', 'SHARE_SOAK_FULL'])(
    'the stamp verifier invokes the predicate for %s',
    (selector) => {
      const verified = { tree: 'a'.repeat(40), node: process.version, brokerVersion: '4.3.1' }
      const stamps = {
        ...verified,
        at: '2026-11-08T00:00:00Z',
        lane: 'docker',
        step: 'soak',
        wireVersions: {},
        config: {},
      }
      const rows = evidence().map((row) => ({ ...stamps, scenario: 'one', selector, ...row }))
      const verify = () =>
        verifyFiles(['share.jsonl'], verified, () =>
          rows.map((row) => JSON.stringify(row)).join('\n')
        )
      expect(verify().problems).toEqual([])
      rows[2].snapshot.heapAtMs = null
      expect(verify().problems).toContain(
        'share.jsonl: share scenario one: phase-end quiet heap slope screen'
      )
      rows.splice(0, rows.length, {
        ...stamps,
        scenario: 'one',
        selector,
        event: 'case-complete',
        ok: true,
      })
      expect(verify().problems).toContain(
        'share.jsonl: share scenario one: phase-end quiet heap slope screen'
      )
    }
  )

  test('an observation cannot be borrowed from another scenario or unauthenticated row', () => {
    const verified = { tree: 'a'.repeat(40), node: process.version, brokerVersion: '4.3.1' }
    const rows = evidence().map((row) => ({
      ...verified,
      at: '2026-11-08T00:00:00Z',
      lane: 'docker',
      step: 'soak',
      wireVersions: {},
      config: {},
      selector: 'SHARE_SOAK_E2E',
      scenario: 'one',
      ...row,
    }))
    rows[2].scenario = 'another'
    const read = () => rows.map((row) => JSON.stringify(row)).join('\n')
    expect(
      verifyFiles(['share.jsonl'], verified, read).problems.filter((p) =>
        p.includes('phase-end quiet heap slope screen')
      )
    ).toHaveLength(2)
    rows[2].scenario = 'one'
    rows[2].tree = 'b'.repeat(40)
    expect(verifyFiles(['share.jsonl'], verified, read).problems.join(' ')).toContain('verified')
  })
})
