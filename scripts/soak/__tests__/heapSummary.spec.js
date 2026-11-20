const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { readSnapshot } = require('../../e2e/telemetry/retained/heapDiff')
const { writeSyntheticSnapshot, NODE_TYPES } = require('./fixtures/syntheticSnapshot')
const heapSummary = require('../heapSummary')

const ROOT = path.join(__dirname, '../../..')
const MIB = 1024 * 1024
const BOUND = 2097152
const dirs = []
const tmp = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'heap-summary-'))
  dirs.push(dir)
  return dir
}
afterAll(() => dirs.forEach((dir) => fs.rmSync(dir, { recursive: true, force: true })))

/** @returns {object} a receipt of `slot` at hour `h` */
const receipt = (slot, h, extra = {}) => {
  const boundary = `h${String(h).padStart(2, '0')}`
  return {
    slot,
    boundary,
    file: `heap/${slot}.${boundary}.heapsnapshot`,
    at: h * 60,
    ms: 1,
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

  test('a run summary round-trips through its file, keeps the first, the last and any snapshot past half the bound, and deletes the rest', () => {
    const out = tmp()
    const judged = [9e6, 9e6 + 1.5 * MIB, 9e6 + 0.25 * MIB, 9e6 + 0.125 * MIB]
    const receipts = judged.map((bytes, h) => {
      const r = receipt('p04', h)
      writeSyntheticSnapshot(path.join(out, r.file), [
        { type: 'object', name: 'Judged', bytes },
        { type: 'code', name: '(code)', bytes: 3e6 + h * MIB },
      ])
      return r
    })
    receipts.push(receipt('p05', 0, { at: null, ms: null, error: 'not taken: step h00q2 late' }))
    const written = heapSummary.summarizeRun(out, receipts, BOUND)
    const read = JSON.parse(fs.readFileSync(path.join(out, heapSummary.SUMMARY_FILE), 'utf8'))
    expect(read).toEqual(written)
    expect(read.excludedType).toBe('code')
    expect(read.records.map((r) => [r.slot, r.boundary, r.kept, r.error, r.judgedBytes])).toEqual([
      ['p04', 'h00', true, null, judged[0]],
      ['p04', 'h01', true, null, judged[1]],
      ['p04', 'h02', false, null, judged[2]],
      ['p04', 'h03', true, null, judged[3]],
      ['p05', 'h00', false, 'not taken: step h00q2 late', undefined],
    ])
    expect(fs.readdirSync(path.join(out, 'heap')).sort()).toEqual([
      'p04.h00.heapsnapshot',
      'p04.h01.heapsnapshot',
      'p04.h03.heapsnapshot',
      'summary.json',
    ])
    for (const r of read.records.filter((x) => x.kept)) {
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

  test('a snapshot that cannot be read is recorded with its error and kept, and judges nothing', () => {
    const out = tmp()
    const r = receipt('p06', 0)
    fs.mkdirSync(path.join(out, 'heap'))
    fs.writeFileSync(path.join(out, r.file), '{"snapshot":')
    const outside = receipt('p07', 0, { file: '../escape.heapsnapshot' })
    const { records } = heapSummary.summarizeRun(out, [r, outside], BOUND)
    expect(records[0]).toMatchObject({ kept: true })
    expect(Object.keys(records[0])).not.toContain('judgedBytes')
    expect(records[0].error).toMatch(/^unreadable snapshot: Malformed heap snapshot/)
    expect(records[1]).toMatchObject({
      kept: false,
      error: 'unreadable snapshot: ../escape.heapsnapshot is not a heap snapshot path',
    })
    expect(fs.existsSync(path.join(out, r.file))).toBe(true)
  })

  test('summarises real snapshots: 7 MiB of new bytecode and feedback is excluded, and the growing constructor of a harness array is named with its retainer path', () => {
    const fixture = path.join(__dirname, 'fixtures/heapJudged.js')
    const totals = {}
    for (const kind of ['code', 'array']) {
      const out = tmp()
      const child = spawnSync(process.execPath, [fixture, path.join(out, 'heap'), 'p04', kind], {
        encoding: 'utf8',
      })
      expect([child.status, child.stderr]).toEqual([0, ''])
      const receipts = [0, 1, 2].map((h) => receipt('p04', h))
      totals[kind] = heapSummary.summarizeRun(out, receipts, BOUND).records
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
