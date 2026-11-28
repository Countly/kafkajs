const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { readSnapshot } = require('../../e2e/telemetry/retained/heapDiff')
const { writeSyntheticSnapshot, NODE_TYPES } = require('./fixtures/syntheticSnapshot')
const heapSummary = require('../heapSummary')

const ROOT = path.join(__dirname, '../../..')
const MIB = 1024 * 1024
const dirs = []
const tmp = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'heap-summary-'))
  dirs.push(dir)
  return dir
}
afterAll(() => dirs.forEach((dir) => fs.rmSync(dir, { recursive: true, force: true })))
/** @param {string} out @param {object} r @returns {object} `r` with its file's size */
const sized = (out, r) => ({ ...r, bytes: fs.statSync(path.join(out, r.file)).size })

/** @returns {object} a receipt of `slot` at hour `h`; `bytes` is set once its file is written */
const receipt = (slot, h, extra = {}) => {
  const boundary = `h${String(h).padStart(2, '0')}`
  return {
    slot,
    boundary,
    file: `heap/${slot}.${boundary}.heapsnapshot`,
    at: h * 60,
    ms: 1,
    bytes: null,
    error: null,
    ...extra,
  }
}

describe('heap snapshot summary', () => {
  test('excludes exactly the code node type, whatever its name, and judges every other type, a type V8 does not have included', () => {
    const dir = tmp()
    const nodes = [
      { type: 'code', name: '(instruction stream for f)', bytes: 1000 },
      { type: 'code', name: 'system / BytecodeArray', bytes: 200 },
      { type: 'code', name: 'someFunction', bytes: 30 },
      { type: 'code', name: '/src/cluster/metadataCache.js', bytes: 4 },
      { type: 'hidden', name: 'system / BytecodeArray', bytes: 50000 },
      { type: 'object shape', name: 'system / Map', bytes: 600000 },
      { type: 'string', name: 'code', bytes: 7000000 },
      { type: 'brand new type', name: '(code)', bytes: 80000000 },
    ]
    const totals = heapSummary.totalsOf(
      readSnapshot(writeSyntheticSnapshot(path.join(dir, 's.heapsnapshot'), nodes))
    )
    expect(heapSummary.EXCLUDED_TYPE).toBe('code')
    expect(totals).toEqual({
      nodes: 9,
      totalBytes: 87651234,
      judgedBytes: 87650000,
      excludedBytes: 1234,
      excluded: {
        '(instruction stream)': 1000,
        'system / BytecodeArray': 200,
        '(function)': 30,
        '(script)': 4,
      },
      judgedByType: {
        synthetic: 0,
        hidden: 50000,
        'object shape': 600000,
        string: 7000000,
        'brand new type': 80000000,
      },
    })
    expect(
      NODE_TYPES.filter((t) => t !== 'code').every((t) => t !== heapSummary.EXCLUDED_TYPE)
    ).toBe(true)
  })

  test('the runbook lists exactly the excluded node type and the name categories the summary uses', () => {
    const runbook = fs.readFileSync(path.join(ROOT, 'docs/FinalSoakRunbook.md'), 'utf8')
    const rule = /^Excluded node types: (.+)\.$/m.exec(runbook)
    expect(rule?.[1].match(/`[^`]+`/g)).toEqual([`\`${heapSummary.EXCLUDED_TYPE}\``])
    for (const name of [
      '(instruction stream for f)',
      '(code for f)',
      '(BASELINE code)',
      '(shared function info)',
      '(constant pool)',
      'system / FeedbackVector',
      'system / LoadHandler',
      'system / TrustedByteArray',
      '(DeoptimizationEntry_Eager builtin code)',
      '/home/run/src/index.js',
      'node:internal/url',
      'someFunction',
      '',
    ])
      expect([name, runbook.includes(`| \`${heapSummary.categoryOf(name)}\` |`)]).toEqual([
        name,
        true,
      ])
  })

  test('a run summary round-trips through its file and keeps every snapshot file it summarised', () => {
    const out = tmp()
    const judged = [9e6, 9e6 + 1.5 * MIB, 9e6 + 0.25 * MIB, 9e6 + 0.125 * MIB]
    const receipts = judged.map((bytes, h) => {
      const r = receipt('p04', h)
      writeSyntheticSnapshot(path.join(out, r.file), [
        { type: 'object', name: 'Judged', bytes },
        { type: 'code', name: '(code)', bytes: 3e6 + h * MIB },
      ])
      return sized(out, r)
    })
    receipts.push(receipt('p05', 0, { at: null, ms: null, error: 'not taken: step h00q2 late' }))
    const written = heapSummary.summarizeRun(out, receipts)
    const read = JSON.parse(fs.readFileSync(path.join(out, heapSummary.SUMMARY_FILE), 'utf8'))
    expect(read).toEqual(written)
    expect(read.excludedType).toBe('code')
    expect(read.records.map((r) => [r.slot, r.boundary, r.error, r.judgedBytes])).toEqual([
      ['p04', 'h00', null, judged[0]],
      ['p04', 'h01', null, judged[1]],
      ['p04', 'h02', null, judged[2]],
      ['p04', 'h03', null, judged[3]],
      ['p05', 'h00', 'not taken: step h00q2 late', undefined],
    ])
    expect(fs.readdirSync(path.join(out, 'heap')).sort()).toEqual([
      'p04.h00.heapsnapshot',
      'p04.h01.heapsnapshot',
      'p04.h02.heapsnapshot',
      'p04.h03.heapsnapshot',
      'summary.json',
    ])
    for (const r of read.records.filter((x) => x.error === null)) {
      const totals = heapSummary.totalsOf(readSnapshot(path.join(out, r.file)))
      expect(totals).toEqual({
        nodes: r.nodes,
        totalBytes: r.totalBytes,
        judgedBytes: r.judgedBytes,
        excludedBytes: r.excludedBytes,
        excluded: r.excluded,
        judgedByType: r.judgedByType,
      })
    }
    expect(read.records[3]).toMatchObject({ excludedBytes: 3e6 + 3 * MIB, growingSince: 'h02' })
  })

  test('a snapshot that cannot be read is recorded with its error, its file left in place, and judges nothing', () => {
    const out = tmp()
    const r = receipt('p06', 0)
    fs.mkdirSync(path.join(out, 'heap'))
    fs.writeFileSync(path.join(out, r.file), '{"snapshot":')
    const { records } = heapSummary.summarizeRun(out, [sized(out, r)])
    expect(Object.keys(records[0])).not.toContain('judgedBytes')
    expect(records[0].error).toMatch(/^unreadable snapshot: Malformed heap snapshot/)
    expect(fs.existsSync(path.join(out, r.file))).toBe(true)
  })

  test.each([
    [
      'a path outside the heap directory',
      { file: '../escape.heapsnapshot' },
      'is not heap/p04.h00.heapsnapshot',
    ],
    [
      'another process’s snapshot',
      { file: 'heap/p05.h00.heapsnapshot' },
      'is not heap/p04.h00.heapsnapshot',
    ],
    [
      'another boundary’s snapshot',
      { file: 'heap/p04.h01.heapsnapshot' },
      'is not heap/p04.h00.heapsnapshot',
    ],
    ['a size other than the file’s', { bytes: 1 }, 'bytes, its receipt 1'],
  ])(
    'a receipt naming %s is summarised as an error, never read as that snapshot',
    (_, change, why) => {
      const out = tmp()
      for (const [slot, h] of [
        ['p04', 0],
        ['p05', 0],
        ['p04', 1],
      ])
        writeSyntheticSnapshot(path.join(out, receipt(slot, h).file), [
          { type: 'object', name: 'Judged', bytes: 9e6 },
        ])
      const r = { ...sized(out, receipt('p04', 0)), ...change }
      const [record] = heapSummary.summarizeRun(out, [r]).records
      expect(Object.keys(record)).not.toContain('judgedBytes')
      expect(record.error).toContain(why)
    }
  )

  test('summarises real snapshots: 7 MiB of new bytecode and feedback is excluded, and the growing constructor of a harness array is named with its retainer path', () => {
    const fixture = path.join(__dirname, 'fixtures/heapJudged.js')
    const totals = {}
    for (const kind of ['code', 'array']) {
      const out = tmp()
      const child = spawnSync(process.execPath, [fixture, path.join(out, 'heap'), 'p04', kind], {
        encoding: 'utf8',
      })
      expect([child.status, child.stderr]).toEqual([0, ''])
      const receipts = [0, 1, 2].map((h) => sized(out, receipt('p04', h)))
      totals[kind] = heapSummary.summarizeRun(out, receipts).records
    }
    const [code0, , code2] = totals.code
    expect(code2.excludedBytes - code0.excludedBytes).toBeGreaterThan(6 * MIB)
    expect(code2.judgedBytes - code0.judgedBytes).toBeLessThan(64 * 1024)
    const [, , array2] = totals.array
    expect(array2.growing[0]).toMatchObject({
      type: 'object',
      name: 'HarnessMarker',
      ownerClass: 'harness',
    })
    expect(array2.growing[0].path).toMatch(/HarnessMarker$/)
  }, 60000)
})
