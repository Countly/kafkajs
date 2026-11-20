const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')

const ROOT = path.join(__dirname, '../../..')
const FIXTURE = path.join(__dirname, 'fixtures/heapGrowth.js')
const REPORT = path.join(ROOT, 'scripts/soak/heapSnapshotReport.js')
const MARKERS = 20000

describe('final soak heap report', () => {
  let dir = null

  beforeAll(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-heap-'))
    const child = spawnSync(process.execPath, [FIXTURE, dir, String(MARKERS)], {
      encoding: 'utf8',
    })
    expect([child.status, child.stderr]).toEqual([0, ''])
  }, 60000)

  afterAll(() => dir && fs.rmSync(dir, { recursive: true, force: true }))

  test('two real snapshots of a process that retained a known array name its owner file as the top growing file and constructor, with the retainer path', () => {
    const { report, classOf } = require('../heapSnapshotReport')
    const [result, ...rest] = report(dir)
    expect(rest).toEqual([])
    expect(result).toMatchObject({ slot: 'p03', kind: null, from: 'h00', to: 'h01' })
    const { files, groups, classes } = result.diff
    expect(files[0].owner).toBe(FIXTURE)
    expect(files[0].delta).toBeGreaterThan(MARKERS * 16)
    expect(classOf(FIXTURE)).toBe('harness')
    expect(classes.harness.delta).toBe(
      files.filter((f) => classOf(f.owner) === 'harness').reduce((sum, f) => sum + f.delta, 0)
    )
    expect(groups[0]).toMatchObject({
      type: 'object',
      name: 'GrowthMarker',
      owner: FIXTURE,
      countDelta: MARKERS,
    })
    expect(groups[0].path).toMatch(/\.kept Array > \[\d+\] GrowthMarker$/)
  }, 60000)

  test('the command line prints the owner rows, the growing file and the constructor with its retainer path', () => {
    const cli = spawnSync(process.execPath, [REPORT, dir], { encoding: 'utf8' })
    expect([cli.status, cli.stderr]).toEqual([0, ''])
    const lines = cli.stdout.split('\n')
    expect(lines[0]).toMatch(
      /^p03 h00 -> h01: live self bytes [\d.]+ -> [\d.]+ MiB \(\+[\d,]+ B\)$/
    )
    expect(lines).toContainEqual(expect.stringMatching(/^ {2}harness +[\d,]+ +[\d,]+ +\+[\d,]+/))
    const files = lines.indexOf('  files by growth:')
    expect(lines[files + 1]).toMatch(new RegExp(`^ +\\+[\\d,]+ B {2}harness +${FIXTURE}$`))
    const top = lines.findIndex((l) => /growing constructors and closures:$/.test(l))
    expect(lines[top + 1]).toContain(`+${MARKERS.toLocaleString('en-US')}  object GrowthMarker`)
    expect(lines[top + 2]).toMatch(/^ {8}retained by: .*GrowthMarker$/)
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-heap-empty-'))
    try {
      fs.mkdirSync(path.join(empty, 'heap'))
      const none = spawnSync(process.execPath, [REPORT, empty], { encoding: 'utf8' })
      expect(none.status).toBe(1)
      expect(none.stderr).toContain('no two snapshots of one process')
    } finally {
      fs.rmSync(empty, { recursive: true, force: true })
    }
  }, 60000)
})
