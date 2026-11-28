const { MutantSelection } = require('../select')

/**
 * @param {string} area
 * @param {number} files target files
 * @param {string[]} operators
 * @param {number} perBucket records per file and operator
 * @returns {object[]} generated records
 */
const generated = (area, files, operators, perBucket) => {
  const out = []
  for (let f = 0; f < files; f++)
    for (const operator of operators)
      for (let n = 0; n < perBucket; n++)
        out.push({
          id: `${area}/f${f}.js:${n}:${operator}`,
          area,
          target: `src/${area}/f${f}.js`,
          line: n + 1,
          operator,
          original: 'a',
          mutated: 'b',
          spec: `src/${area}/f.spec.js`,
          commandClass: 'unit',
        })
  return out
}

const HISTORICAL = [
  {
    id: 'D/D01',
    area: 'D',
    target: 'src/a.js',
    spec: 'src/a.spec.js',
    title: 't',
    commandClass: 'unit',
    patchSha: 'f'.repeat(64),
  },
  {
    id: 'D/D02',
    area: 'D',
    target: 'src/a.js',
    spec: 'src/a.spec.js',
    title: 't',
    commandClass: 'unit',
    stale: 'stale hunk',
  },
  {
    id: 'D/D12',
    area: 'D',
    target: 'src/b.js',
    spec: 'src/b.spec.js',
    title: 't',
    commandClass: 'live',
    patchSha: 'e'.repeat(64),
  },
  {
    id: 'G/G01',
    area: 'G',
    target: 'src/g.js',
    spec: 'src/g.spec.js',
    title: 't',
    commandClass: 'unit',
    patchSha: 'd'.repeat(64),
  },
]

const EXCLUDED = [
  {
    id: 'J/J12',
    target: 'src/j.spec.js',
    reason: 'the mutation edits its own witness spec, which the runner refuses',
  },
]

const OPERATORS = ['boundary', 'constant', 'removed-call']

/**
 * @param {object} [options] overrides
 * @returns {object} the selection
 */
const select = (options = {}) =>
  MutantSelection.select({
    historical: HISTORICAL,
    excluded: EXCLUDED,
    generated: [
      ...generated('offsets', 3, OPERATORS, 10),
      ...generated('admin', 1, ['constant'], 2),
    ],
    perArea: 12,
    seed: '42',
    areas: 'all',
    sha: 'a'.repeat(40),
    runId: '7',
    shards: 8,
    now: () => Date.parse('2026-11-17T10:00:00.000Z'),
    ...options,
  })

describe('scripts/mutants/select.js', () => {
  test('samples each area evenly across target files and operators, and names every unsampled record', () => {
    const { selection, unit, live } = select()
    const offsets = unit.records.filter((r) => r.area === 'offsets')
    expect(offsets).toHaveLength(12)
    const buckets = {}
    for (const r of offsets)
      buckets[`${r.target} ${r.operator}`] = (buckets[`${r.target} ${r.operator}`] || 0) + 1
    expect(Object.keys(buckets)).toHaveLength(9)
    expect(
      Math.max(...Object.values(buckets)) - Math.min(...Object.values(buckets))
    ).toBeLessThanOrEqual(1)
    expect(unit.records.filter((r) => r.area === 'admin')).toHaveLength(2)
    expect(unit.records.filter((r) => r.area === 'D').map((r) => r.id)).toEqual(['D/D01', 'D/D02'])
    expect(live.records.map((r) => r.id)).toEqual(['D/D12'])
    const byDisposition = {}
    for (const r of selection.records)
      byDisposition[r.disposition] = (byDisposition[r.disposition] || 0) + 1
    expect(byDisposition).toEqual({
      run: 12 + 2 + 3,
      live: 1,
      'not-run: sampling': 90 - 12,
      'excluded: the mutation edits its own witness spec, which the runner refuses': 1,
    })
    expect(selection.records).toHaveLength(HISTORICAL.length + 92 + EXCLUDED.length)
    expect(selection).toMatchObject({
      sha: 'a'.repeat(40),
      runId: '7',
      seed: '42',
      perArea: 12,
      areas: 'all',
      shards: 8,
      startedAt: '2026-11-17T10:00:00.000Z',
    })
  })

  test('the sample is fixed by its seed and moves with another seed', () => {
    const ids = (seed) => select({ seed }).unit.records.map((r) => r.id)
    expect(ids('42')).toEqual(ids('42'))
    expect(ids('43')).not.toEqual(ids('42'))
  })

  test('an area filter keeps the other records as not-run: area', () => {
    const { selection, unit, live } = select({ areas: 'G,admin' })
    expect(unit.records.map((r) => r.area).sort()).toEqual(['G', 'admin', 'admin'])
    expect(live.records).toEqual([])
    expect(selection.records.filter((r) => r.disposition === 'not-run: area')).toHaveLength(3 + 90)
  })

  test('an inventory exclusion keeps its area and reason under any area filter and never runs', () => {
    const { selection, unit, live } = select()
    expect(selection.records.find((r) => r.id === 'J/J12')).toMatchObject({
      area: 'J',
      source: 'historical',
      target: 'src/j.spec.js',
      disposition: 'excluded: the mutation edits its own witness spec, which the runner refuses',
    })
    expect([...unit.records, ...live.records].map((r) => r.id)).not.toContain('J/J12')
    const filtered = select({ areas: 'offsets' })
    expect(filtered.selection.records.find((r) => r.id === 'J/J12')).toMatchObject({
      area: 'J',
      disposition: 'excluded: the mutation edits its own witness spec, which the runner refuses',
    })
    expect([...filtered.unit.records, ...filtered.live.records].map((r) => r.id)).not.toContain(
      'J/J12'
    )
  })

  test('the plan gives each shard its cap plus the longest record and cleanup, within the 6-hour job limit', () => {
    expect(MutantSelection.plan({ shards: '8', capMinutes: '40' })).toEqual({
      shards: [1, 2, 3, 4, 5, 6, 7, 8],
      capMinutes: 40,
      shardTimeout: 40 + 26 + 10,
    })
    const longest = MutantSelection.plan({ shards: '32', capMinutes: '300' }).shardTimeout
    expect(longest).toBe(300 + 26 + 10)
    expect(longest).toBeLessThanOrEqual(360)
    expect(() => MutantSelection.plan({ shards: '0', capMinutes: '40' })).toThrow('shards')
    expect(() => MutantSelection.plan({ shards: '8', capMinutes: '301' })).toThrow('time cap')
  })
})
