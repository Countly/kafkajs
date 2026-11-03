const fs = require('fs')
const os = require('os')
const path = require('path')
const { NodeTestReport, run } = require('../checkNodeTests')

const tap = ({
  name = 'live',
  child = 'child',
  skip = '',
  fail = 0,
  pass = 1,
  tests = 1,
} = {}) => `TAP version 13
# Subtest: ${name}
    # Subtest: ${child}
    ok 1 - ${child}${skip}
      ---
      type: 'test'
      ...
    1..1
ok 1 - ${name}
  ---
  type: 'suite'
  ...
1..1
# tests ${tests}
# pass ${pass}
# fail ${fail}
# cancelled 0
# skipped ${tests - pass}
# todo 0
# duration_ms 10
`
const contract = {
  blocks: 4,
  skipped: 'none',
  liveTests: [1],
  liveNames: [['live', 'live > child']],
}
const report = (first = tap()) =>
  first + tap({ name: 'loader' }) + tap({ name: 'plain' }) + tap({ name: 'register' })

describe('Node test report', () => {
  it('accepts four completed blocks with the exact qualified live names', () => {
    expect(new NodeTestReport(report()).check(contract)).toEqual([])
  })

  it.each([
    ['missing block', tap()],
    ['missing live test', report(tap({ name: 'other' }))],
    ['renamed live test', report(tap({ child: 'renamed' }))],
    ['failed summary', report(tap({ fail: 1 }))],
    ['empty tests', report(tap({ tests: 0, pass: 0 }))],
    ['skipped test', report(tap({ skip: ' # SKIP', pass: 0 }))],
    ['todo test', report(tap({ skip: ' # TODO' }))],
  ])('rejects %s', (_, text) => {
    expect(new NodeTestReport(text).check(contract).length).toBeGreaterThan(0)
  })

  it('requires the live leaf-test count as well as names', () => {
    expect(new NodeTestReport(report()).check({ ...contract, liveTests: [2] })).not.toEqual([])
  })

  it('permits host skips when other tests passed', () => {
    const text = tap({ skip: ' # SKIP', tests: 2, pass: 1 })
    expect(new NodeTestReport(text).check({ blocks: 1, skipped: 'allowed' })).toEqual([])
    expect(new NodeTestReport(text).check({ blocks: 1, skipped: 'none' })).not.toEqual([])
  })

  it('rejects all-skipped output even when skips are allowed', () => {
    expect(
      new NodeTestReport(tap({ skip: ' # SKIP', pass: 0 })).check({ blocks: 1, skipped: 'allowed' })
    ).not.toEqual([])
  })

  it.each([
    tap().replace('# duration_ms 10', ''),
    tap().replace('# todo 0', ''),
    tap().replace('1..1\n# tests', '1..2\n# tests'),
    tap().replace('ok 1 - live', 'ok 1 - renamed'),
    tap() + 'Bail out! failed\n',
  ])('rejects incomplete or malformed TAP', (text) => {
    expect(() => new NodeTestReport(text)).toThrow()
  })

  it('rejects an empty report and invalid contracts', () => {
    expect(new NodeTestReport('').check(contract)).not.toEqual([])
    expect(new NodeTestReport(report()).check({})).not.toEqual([])
  })

  it('fails closed for missing files, contracts, and CLI arguments', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'node-report-'))
    const io = { out: jest.fn(), err: jest.fn() }
    try {
      const file = path.join(dir, 'report.tap')
      const gate = path.join(dir, 'gate.json')
      fs.writeFileSync(file, report())
      fs.writeFileSync(gate, JSON.stringify({ checks: [{ id: 'one', nodeTest: contract }] }))
      expect(run([file, 'one', gate], io)).toBe(0)
      expect(io.out).toHaveBeenCalledWith('one: ok, 4 Node test blocks verified')
      expect(run([file, 'absent', gate], io)).toBe(1)
      expect(run([path.join(dir, 'absent'), 'one', gate], io)).toBe(1)
      expect(run([], io)).toBe(1)
      fs.writeFileSync(file, '')
      expect(run([file, 'one', gate], io)).toBe(1)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })
})
