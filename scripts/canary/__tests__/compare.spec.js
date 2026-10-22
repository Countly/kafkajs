const { compare } = require('../compare')
const { spawnSync } = require('child_process')
const fs = require('fs')
const os = require('os')
const path = require('path')
/** Builds one reporter test. @param {string} fullTitle @returns {object} test @throws Never. */
const test = (fullTitle) => ({
  title: fullTitle.split(' ').at(-1),
  fullTitle,
  duration: 1,
  err: {},
})
/** Builds a final-status report. @param {string[]} passed @param {string[]} failed @param {string[]} pending @returns {object} report @throws Never. */
const report = (passed = ['suite a'], failed = [], pending = []) => ({
  tests: [...passed, ...failed, ...pending].map(test),
  passes: passed.map(test),
  failures: failed.map(test),
  pending: pending.map(test),
})
describe('canary test identities', () => {
  it('ignores order, durations and aggregate counts', () => {
    const right = report(['suite b', 'suite a'])
    right.tests.reverse()
    right.stats = { passes: 999 }
    expect(compare(report(['suite a', 'suite b']), right)).toEqual([])
  })
  it('shows a renamed test despite unchanged counts', () => {
    expect(compare(report(), report(['suite b']))).toEqual([
      '- ["suite a","passed"]',
      '+ ["suite b","passed"]',
    ])
  })
  it('shows a changed status despite unchanged counts', () => {
    expect(compare(report(), report([], ['suite a']))).toEqual([
      '- ["suite a","passed"]',
      '+ ["suite a","failed"]',
    ])
  })
  it('uses the full title rather than the leaf title', () => {
    expect(compare(report(['left a']), report(['right a']))).toHaveLength(2)
  })
  it('shows added and removed pending tests', () => {
    expect(compare(report(), report(['suite a'], [], ['suite pending']))).toEqual([
      '+ ["suite pending","pending"]',
    ])
    expect(compare(report(['suite a'], [], ['suite pending']), report())).toEqual([
      '- ["suite pending","pending"]',
    ])
  })
  it('keeps a failed hook as its own failed identity', () => {
    const hooked = { ...report(), failures: [test('suite "before all" hook for "x"')] }
    expect(compare(hooked, hooked)).toEqual([])
    expect(compare(report(), hooked)).toEqual([
      '+ ["suite \\"before all\\" hook for \\"x\\"","failed"]',
    ])
  })
  it.each([
    ['missing array', {}],
    ['empty run', report([])],
    ['duplicate identity', report(['suite a', 'suite a'])],
    ['missing identity', { ...report(), tests: [{ title: 'a' }] }],
    ['missing status', { ...report(), passes: [] }],
    ['conflicting status', { ...report(), failures: [test('suite a')] }],
    ['unknown status identity', { ...report(), pending: [test('other')] }],
  ])('rejects %s', (_, value) => {
    expect(() => compare(report(), value)).toThrow()
  })
  it('CLI returns 0, 1 and 2 for equality, differences and invalid inputs', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'canary-compare-'))
    const left = path.join(dir, 'left.json')
    const right = path.join(dir, 'right.json')
    /** Invokes the actual command. @param {...string} args @returns {object} subprocess outcome @throws {Error} when spawn fails. */
    const run = (...args) =>
      spawnSync(process.execPath, [path.resolve(__dirname, '../compare.js'), ...args], {
        encoding: 'utf8',
      })
    try {
      fs.writeFileSync(left, JSON.stringify(report()))
      fs.writeFileSync(right, JSON.stringify(report()))
      const equal = run(left, right)
      expect(equal.status).toBe(0)
      expect(equal.stdout).toContain('identical')
      fs.writeFileSync(right, JSON.stringify(report(['suite b'])))
      const diff = run(left, right)
      expect(diff.status).toBe(1)
      expect(diff.stdout).toContain('- ["suite a","passed"]')
      fs.writeFileSync(right, '{')
      expect(run(left, right).status).toBe(2)
      expect(run(left).status).toBe(2)
      expect(run(left, path.join(dir, 'absent')).status).toBe(2)
      expect(run(left, left, right).status).toBe(2)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })
})
