const fs = require('fs')
const os = require('os')
const path = require('path')
const { check, normalizePath, run } = require('../checkIdentities')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const SPEC = 'src/consumer/__tests__/leaderUnionLive.spec.js'
const ID = (title, file = SPEC) => `${file} › ${title}`

const test_ = (fullName, status = 'passed') => ({ fullName, status })
const suite = (file, ...tests) => ({
  name: path.join(ROOT, file),
  status: 'passed',
  assertionResults: tests,
})
const report = (...suites) => ({ testResults: suites })

describe('scripts > ci > checkIdentities', () => {
  describe('check', () => {
    it('passes a report whose expected titles all passed', () => {
      const result = check(report(suite(SPEC, test_('Consumer a'), test_('Consumer b'))), [
        ID('Consumer a'),
        ID('Consumer b'),
      ])
      expect(result).toEqual({ ok: true, problems: [], passed: 2, total: 2 })
    })

    it('fails a report with no tests', () => {
      expect(check(report(), []).problems).toEqual(['the report has no tests'])
      expect(check(report(suite(SPEC)), []).ok).toBe(false)
    })

    it('fails a report where every test was skipped', () => {
      const result = check(
        report(suite(SPEC, test_('Consumer a', 'pending'), test_('Consumer b', 'skipped'))),
        []
      )
      expect(result.ok).toBe(false)
      expect(result.problems).toHaveLength(2)
      expect(result.problems[0]).toContain('test was not run (pending)')
    })

    it('fails a todo test', () => {
      const result = check(
        report(suite(SPEC, test_('Consumer a'), test_('Consumer todo', 'todo'))),
        []
      )
      expect(result.problems).toEqual([expect.stringContaining('test was not run (todo)')])
    })

    it('fails one failed test among passing ones', () => {
      const result = check(
        report(suite(SPEC, test_('Consumer a'), test_('Consumer b', 'failed'))),
        []
      )
      expect(result.problems).toEqual([`test failed: ${ID('Consumer b')}`])
    })

    it('fails an expected title that is missing', () => {
      const result = check(report(suite(SPEC, test_('Consumer a'))), [
        ID('Consumer a'),
        ID('Consumer gone'),
      ])
      expect(result.problems).toEqual([`expected test is missing: ${ID('Consumer gone')}`])
    })

    it('fails an expected title that did not pass, once', () => {
      const result = check(report(suite(SPEC, test_('Consumer a', 'pending'))), [ID('Consumer a')])
      expect(result.problems).toEqual([`expected test did not pass (pending): ${ID('Consumer a')}`])
    })

    it('fails an expected title when any run of it did not pass', () => {
      const result = check(
        report(suite(SPEC, test_('Consumer a'), test_('Consumer a', 'failed'))),
        [ID('Consumer a')]
      )
      expect(result.ok).toBe(false)
    })

    it('tolerates a skip that is listed and still fails one that is not', () => {
      const tests = [
        test_('Consumer a'),
        test_('Consumer listed', 'pending'),
        test_('Consumer extra', 'pending'),
      ]
      const result = check(report(suite(SPEC, ...tests)), [], {
        allowedSkips: [ID('Consumer listed')],
      })
      expect(result.problems).toEqual([expect.stringContaining(ID('Consumer extra'))])
    })

    it('rejects a report containing only allowed skips', () => {
      expect(
        check(report(suite(SPEC, test_('Consumer s', 'pending'))), [], {
          allowedSkips: [ID('Consumer s')],
        }).ok
      ).toBe(false)
    })

    it('does not tolerate a failure of a listed skip', () => {
      const result = check(report(suite(SPEC, test_('Consumer a', 'failed'))), [], {
        allowedSkips: [ID('Consumer a')],
      })
      expect(result.problems).toEqual([`test failed: ${ID('Consumer a')}`])
    })

    it('fails a suite that could not run', () => {
      const broken = { name: path.join(ROOT, SPEC), status: 'failed', assertionResults: [] }
      const result = check(report(suite('src/a.spec.js', test_('A a')), broken), [])
      expect(result.problems).toEqual([`suite did not run: ${SPEC}`])
    })

    it('matches an absolute and a repo-relative report path to the same title', () => {
      const relative = { name: SPEC, status: 'passed', assertionResults: [test_('Consumer a')] }
      expect(check(report(relative), [ID('Consumer a')]).ok).toBe(true)
      expect(check(report(suite(SPEC, test_('Consumer a'))), [ID('Consumer a')]).ok).toBe(true)
    })
  })

  describe('normalizePath', () => {
    it('makes an absolute path under the root repo-relative', () => {
      expect(normalizePath(path.join(ROOT, 'src', 'a', 'b.spec.js'))).toBe('src/a/b.spec.js')
    })

    it('keeps a repo-relative path and resolves dot segments', () => {
      expect(normalizePath('src/a/b.spec.js')).toBe('src/a/b.spec.js')
      expect(normalizePath('./src/a/../a/b.spec.js')).toBe('src/a/b.spec.js')
    })

    it('leaves a path outside the root untouched', () => {
      expect(normalizePath('/elsewhere/b.spec.js', ROOT)).toBe('/elsewhere/b.spec.js')
    })

    it('uses the root it is given', () => {
      expect(normalizePath('/work/repo/src/b.spec.js', '/work/repo')).toBe('src/b.spec.js')
    })
  })

  describe('run', () => {
    let dir
    const lines = { out: [], err: [] }
    const io = { out: (line) => lines.out.push(line), err: (line) => lines.err.push(line) }
    const write = (name, value) => {
      const file = path.join(dir, name)
      fs.writeFileSync(file, typeof value === 'string' ? value : JSON.stringify(value))
      return file
    }

    beforeEach(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), 'check-identities-'))
      lines.out = []
      lines.err = []
    })

    afterEach(() => fs.rmSync(dir, { recursive: true, force: true }))

    const gate = () =>
      write('gate.json', {
        version: 1,
        checks: [{ id: 'one', titles: [ID('Consumer a')], allowedSkips: [ID('Consumer s')] }],
      })

    it('exits 0 and prints one summary line for a passing report', () => {
      const file = write(
        'report.json',
        report(suite(SPEC, test_('Consumer a'), test_('Consumer s', 'pending')))
      )
      expect(run([file, 'one', gate()], io)).toBe(0)
      expect(lines.out).toEqual(['one: ok, 1/2 tests passed, 1 expected titles found'])
      expect(lines.err).toEqual([])
    })

    it('exits 1 with one line per problem', () => {
      const file = write('report.json', report(suite(SPEC, test_('Consumer x', 'failed'))))
      expect(run([file, 'one', gate()], io)).toBe(1)
      expect(lines.err).toHaveLength(2)
      expect(lines.err.every((line) => line.startsWith('one: '))).toBe(true)
    })

    it('exits 1 for an unknown check id', () => {
      const file = write('report.json', report(suite(SPEC, test_('Consumer a'))))
      expect(run([file, 'nope', gate()], io)).toBe(1)
      expect(lines.err[0]).toContain('nope: unknown check id')
    })

    it('exits 1 for an unreadable report and for missing arguments', () => {
      expect(run([path.join(dir, 'absent.json'), 'one', gate()], io)).toBe(1)
      expect(lines.err[0]).toContain('cannot read report')
      expect(run([write('bad.json', '{'), 'one', gate()], io)).toBe(1)
      expect(run([], io)).toBe(1)
      expect(lines.err[lines.err.length - 1]).toContain('usage')
    })
  })
})
