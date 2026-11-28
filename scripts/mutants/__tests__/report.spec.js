const { MutationReport } = require('../report')
const { MutantSelection } = require('../select')
const inventory = require('../inventory.json')

const SHA = 'a'.repeat(40)
const STARTED = '2026-11-17T10:00:00.000Z'

/**
 * @param {object} fields overrides
 * @returns {object} a selection record that runs in a unit shard
 */
const rec = (fields) => ({
  area: 'offsets',
  source: 'generated',
  commandClass: 'unit',
  disposition: 'run',
  target: 'src/consumer/offsetManager/index.js',
  line: 10,
  operator: 'constant',
  original: 'const a = 1',
  mutated: 'const a = 2',
  spec: 'src/consumer/offsetManager/__tests__/commitOffsets.spec.js',
  ...fields,
})

/**
 * @param {object} record a selection record
 * @param {string} verdict the runner's verdict
 * @param {object} [fields] overrides
 * @returns {object} the ledger row the runner writes for it
 */
const row = (record, verdict, fields = {}) => ({
  id: record.id,
  area: record.area,
  target: record.target,
  line: record.line,
  operator: record.operator,
  original: record.original,
  mutated: record.mutated,
  witness: record.spec,
  verdict,
  reason: '',
  durationMs: 5,
  ...fields,
})

/**
 * @param {object[]} records selection records
 * @param {object[]} rows ledger rows
 * @returns {object} the report's JSON
 */
const build = (records, rows) =>
  new MutationReport({
    selection: {
      sha: SHA,
      runId: '7',
      seed: '7',
      perArea: 40,
      areas: 'all',
      shards: 2,
      startedAt: STARTED,
      records,
    },
    ledgers: [{ file: 'mutants-1.jsonl', rows }],
    runId: '7',
    now: () => Date.parse('2026-11-17T11:00:00.000Z'),
  }).build()

describe('scripts/mutants/report.js', () => {
  test('the area table counts killed, survived, stale, invalid, excluded and not-run', () => {
    const records = [
      rec({ id: 'k' }),
      rec({ id: 's' }),
      rec({ id: 't' }),
      rec({ id: 'i' }),
      rec({ id: 'n' }),
      rec({ id: 'sampled-out', disposition: 'not-run: sampling' }),
      rec({ id: 'x', disposition: 'excluded: the mutation edits its own witness spec' }),
      rec({ id: 'other', area: 'share' }),
    ]
    const json = build(records, [
      row(records[0], 'killed'),
      row(records[1], 'survived'),
      row(records[2], 'stale', { reason: 'stale hunk' }),
      row(records[3], 'invalid', { reason: 'the control is failed' }),
      row(records[4], 'not-run', { reason: 'time cap' }),
      row(records[6], 'killed'),
      row(records[7], 'killed'),
    ])
    expect(json.unit.areas).toEqual([
      {
        area: 'offsets',
        severity: 'P1',
        mutants: 7,
        killed: 1,
        survived: 1,
        stale: 1,
        invalid: 1,
        excluded: 1,
        notRun: 2,
        score: 0.5,
      },
      {
        area: 'share',
        severity: 'P1',
        mutants: 1,
        killed: 1,
        survived: 0,
        stale: 0,
        invalid: 0,
        excluded: 0,
        notRun: 0,
        score: 1,
      },
    ])
    expect(json.unit.total).toEqual({
      area: 'total',
      severity: '',
      mutants: 8,
      killed: 2,
      survived: 1,
      stale: 1,
      invalid: 1,
      excluded: 1,
      notRun: 2,
      score: 2 / 3,
    })
    expect(json.records.find((r) => r.id === 'x')).toMatchObject({
      verdict: 'excluded',
      reason: 'the mutation edits its own witness spec',
    })
    const md = MutationReport.markdown(json)
    expect(md).toContain(
      '| Area | Severity | Mutants | Killed | Survived | Stale | Invalid | Excluded | Not run | Score |'
    )
    expect(md).toContain('| offsets | P1 | 7 | 1 | 1 | 1 | 1 | 1 | 2 | 50.0% |')
    expect(md).toContain('| **total** |  | 8 | 2 | 1 | 1 | 1 | 1 | 2 | 66.7% |')
  })

  test('the score excludes stale, invalid and not-run records', () => {
    const records = ['a', 'b', 'c', 'd', 'e'].map((id) => rec({ id }))
    const json = build(records, [
      row(records[0], 'killed'),
      row(records[1], 'stale'),
      row(records[2], 'invalid'),
      row(records[3], 'not-run', { reason: 'time cap' }),
      row(records[4], 'bogus'),
    ])
    expect(json.unit.areas[0]).toMatchObject({
      killed: 1,
      survived: 0,
      stale: 1,
      invalid: 2,
      notRun: 1,
      score: 1,
    })
    const onlyUndecided = build(records.slice(1, 4), [
      row(records[1], 'stale'),
      row(records[2], 'invalid'),
    ])
    expect(onlyUndecided.unit.areas[0]).toMatchObject({ killed: 0, survived: 0, score: null })
    expect(MutationReport.markdown(onlyUndecided)).toContain(
      '| offsets | P1 | 3 | 0 | 0 | 1 | 1 | 0 | 1 | — |'
    )
  })

  test('a selected record without a ledger row is not-run, never killed, and the report says what was not run', () => {
    const records = [
      rec({ id: 'ran' }),
      rec({ id: 'cancelled' }),
      rec({ id: 'late' }),
      rec({ id: 'sampled-out', disposition: 'not-run: sampling' }),
    ]
    const json = build(records, [
      row(records[0], 'killed'),
      row(records[2], 'not-run', { reason: 'time cap' }),
    ])
    expect(json.unit.areas[0]).toMatchObject({ mutants: 4, killed: 1, notRun: 3, score: 1 })
    expect(json.records.find((r) => r.id === 'cancelled')).toMatchObject({
      verdict: 'not-run',
      reason: 'no ledger row: its shard was cancelled, cut off or failed',
    })
    expect(json.provenance.ledgers).toEqual({ found: 1, expected: 2 })
    const notRun = MutationReport.markdown(json).split('## What was not run')[1].split('\n## ')[0]
    expect(notRun).toContain('Unit ledgers found: 1 of 2 shards.')
    expect(notRun).toContain('- 1 × no ledger row: its shard was cancelled, cut off or failed')
    expect(notRun).toContain('- 1 × time cap')
    expect(notRun).toContain('- 1 × sampling')
  })

  test('the hot paths every record passes through are P1 areas; aged metadata ranks with cluster', () => {
    const { AREAS } = require('../generate')
    const severities = Object.fromEntries(
      AREAS.map(({ name }) => [name, MutationReport.severity(name)])
    )
    expect(severities).toEqual({
      transactions: 'P1',
      offsets: 'P1',
      'group-timing': 'P1',
      share: 'P1',
      protocol: 'P1',
      'producer-send': 'P1',
      network: 'P1',
      'fetch-loop': 'P1',
      compression: 'P1',
      'aged-metadata': 'P2',
      cluster: 'P2',
      admin: 'P2',
    })
  })

  test('survivors are ranked by area severity then operator class', () => {
    const records = [
      rec({ id: 'p2-removed', area: 'cluster', operator: 'removed-call', mutated: '' }),
      rec({ id: 'p1-constant', operator: 'constant' }),
      rec({ id: 'p1-hand', operator: 'patch', source: 'historical' }),
      rec({ id: 'p1-boundary', operator: 'boundary', original: 'a < b', mutated: 'a <= b' }),
      rec({ id: 'p1-removed', operator: 'negated-condition' }),
      rec({ id: 'p2-history', area: 'C2', operator: 'early-return' }),
    ]
    const json = build(
      records,
      records.map((r) => row(r, 'survived', { reason: 'the witness still passes' }))
    )
    expect(json.survivors.map((s) => s.id)).toEqual([
      'p1-removed',
      'p1-boundary',
      'p1-constant',
      'p1-hand',
      'p2-history',
      'p2-removed',
    ])
    expect(json.survivors.map((s) => [s.severity, s.operatorClass, s.highRisk])).toEqual([
      ['P1', 'removed check', true],
      ['P1', 'boundary', true],
      ['P1', 'constant', false],
      ['P1', 'hand-written', false],
      ['P2', 'removed check', false],
      ['P2', 'removed check', false],
    ])
  })

  test('every survivor line has file:line, original, mutated, witness and a missing-assertion sentence', () => {
    const records = [
      rec({
        id: 'b',
        operator: 'boundary',
        line: 42,
        original: '  if (a < b) {',
        mutated: '  if (a <= b) {',
      }),
      rec({ id: 'r', operator: 'removed-call', line: 7, original: '  this.flush()', mutated: '' }),
    ]
    const json = build(records, [
      row(records[0], 'survived', { line: 44 }),
      row(records[1], 'survived'),
    ])
    expect(json.survivors).toEqual([
      expect.objectContaining({
        id: 'r',
        file: 'src/consumer/offsetManager/index.js',
        line: 7,
        original: '  this.flush()',
        mutated: '',
        witness: 'src/consumer/offsetManager/__tests__/commitOffsets.spec.js',
        missingAssertion:
          'No selected witness produced a valid kill when `this.flush()` is removed.',
      }),
      expect.objectContaining({
        id: 'b',
        line: 44,
        missingAssertion:
          'No selected witness produced a valid kill when `if (a < b) {` becomes `if (a <= b) {`.',
      }),
    ])
    const md = MutationReport.markdown(json)
    expect(md).toContain('`src/consumer/offsetManager/index.js:44`')
    expect(md).toContain('original: `  if (a < b) {`')
    expect(md).toContain('mutated: `  if (a <= b) {`')
    expect(md).toContain('mutated: (line removed)')
    expect(md).toContain('witness: `src/consumer/offsetManager/__tests__/commitOffsets.spec.js`')
    expect(md).toContain(
      'candidate missing assertion: No selected witness produced a valid kill when `if (a < b) {` becomes `if (a <= b) {`.'
    )
    expect(md).toContain('runs only its selected witness specs')
    expect(md).toContain('another spec may already guard it')
  })

  test('an area-filtered run still reports every inventory exclusion with its reason', () => {
    const { selection } = MutantSelection.select({
      historical: inventory.records,
      excluded: inventory.excluded,
      generated: [],
      perArea: 40,
      seed: '7',
      areas: 'offsets',
      sha: SHA,
      runId: '7',
      shards: 2,
      now: () => Date.parse(STARTED),
    })
    const json = new MutationReport({
      selection,
      ledgers: [],
      runId: '7',
      now: () => Date.parse('2026-11-17T11:00:00.000Z'),
    }).build()
    const excluded = json.records.filter((r) => r.verdict === 'excluded')
    expect(excluded.map((r) => r.id)).toEqual(inventory.excluded.map((e) => e.id))
    expect(excluded.map((r) => r.reason)).toEqual(inventory.excluded.map((e) => e.reason))
    expect(json.unit.total.excluded).toBe(inventory.excluded.length)
    const notRun = MutationReport.markdown(json).split('## What was not run')[1].split('\n## ')[0]
    expect(notRun).toContain(
      `Excluded from the inventory, by reason:\n- ${inventory.excluded.length} × the mutation edits its own witness spec, which the runner refuses`
    )
  })

  test('the header explains a mutation run in three lines', () => {
    const md = MutationReport.markdown(build([rec({ id: 'x' })], []))
    const header = md
      .split('\n## ')[0]
      .split('\n')
      .filter((l) => l.startsWith('- '))
    expect(header).toHaveLength(3)
    expect(header[0]).toMatch(/only its selected witness specs.*not the whole suite/i)
    expect(header[1]).toMatch(
      /killed.*a witness noticed.*survivor.*no witness produced a valid kill.*at least one still passed.*another spec may already guard it/i
    )
    expect(header[2]).toMatch(/stale, invalid, excluded and not-run.*never counted as killed/i)
  })

  test('a selection from another run is refused', () => {
    expect(() =>
      new MutationReport({
        selection: { sha: SHA, runId: '6', startedAt: STARTED, shards: 1, records: [] },
        ledgers: [],
        runId: '7',
      }).build()
    ).toThrow('the selection belongs to run 6, not 7')
  })
})
