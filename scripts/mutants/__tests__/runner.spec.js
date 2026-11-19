const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')

const { spawn, spawnSync } = require('child_process')

const { MutantRunner } = require('../runner')
const fakeExec = require('./fakeExec')

const RUNNER = path.resolve(__dirname, '..', 'runner.js')
const FAKE_EXEC = path.resolve(__dirname, 'fakeExec.js')
const NODE_MODULES = path.resolve(require.resolve('jest/package.json'), '..', '..')
const SPEC = 'src/x/w.spec.js'
const TARGET = 'src/x/value.js'
const JEST_CONFIG =
  "module.exports = { testEnvironment: 'node', watchman: false, roots: ['<rootDir>/src'], testRegex: '\\\\.spec\\\\.js$' }\n"
const OTHER_SPEC = 'src/y/w.spec.js'
const ENV_CONFIG = JEST_CONFIG.replace("'node'", "'./env.js'")
const SETTLE_MS = 1000
const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex')

/**
 * A process that ignores SIGTERM and, once `go` exists in the probe directory, keeps writing the
 * target of the tree it was started from, recreating that tree's directories if they were removed.
 * @param {string} probe the test's directory outside the tree
 * @returns {string} the writer's source
 */
const writer = (probe) =>
  [
    "const fs = require('fs')",
    "const path = require('path')",
    "process.on('SIGTERM', () => {})",
    `fs.writeFileSync(${JSON.stringify(path.join(probe, 'orphan.pid'))}, String(process.pid))`,
    `const target = path.join(__dirname, ${JSON.stringify(TARGET)})`,
    'setInterval(() => {',
    `  if (!fs.existsSync(${JSON.stringify(path.join(probe, 'go'))})) return`,
    '  try {',
    '    fs.mkdirSync(path.dirname(target), { recursive: true })',
    "    fs.writeFileSync(target, 'module.exports = 9\\n')",
    '  } catch {}',
    '}, 10)',
    '',
  ].join('\n')

/**
 * @param {string} probe the test's directory outside the tree
 * @param {string} rootExpr an expression for the tree's root in the generated code
 * @param {string} options extra `spawn` options, as source
 * @returns {string} an async statement that starts the writer and waits until it runs
 */
const launch = (probe, rootExpr, options) =>
  `{ const r = ${rootExpr}; require('child_process').spawn(process.execPath, [require('path').join(r, 'writer.js')], { stdio: 'ignore', ...${options} }).unref(); while (!require('fs').existsSync(${JSON.stringify(path.join(probe, 'orphan.pid'))})) await new Promise((t) => setTimeout(t, 10)) }`

/**
 * A Jest environment that, under the mutation of the first spec, starts the writer and then ends as
 * `ending` says.
 * @param {string} probe the test's directory outside the tree
 * @param {string} ending source run after the writer started
 * @param {string} options the writer's extra `spawn` options, as source
 * @returns {string} the environment's source
 */
const environment = (probe, ending, options) =>
  [
    "const fs = require('fs')",
    "const path = require('path')",
    "const { TestEnvironment } = require('jest-environment-node')",
    'module.exports = class extends TestEnvironment {',
    '  constructor(config, context) {',
    '    super(config, context)',
    '    this.file = context.testPath',
    '  }',
    '  async setup() {',
    '    await super.setup()',
    `    if (!this.file.endsWith(${JSON.stringify(SPEC)})) return`,
    `    if (!fs.readFileSync(path.join(__dirname, ${JSON.stringify(TARGET)}), 'utf8').includes('= 2')) return`,
    `    ${launch(probe, '__dirname', options)}`,
    `    ${ending}`,
    '  }',
    '}',
    '',
  ].join('\n')
const CRASH = 'process.exit(2)'
/**
 * @param {string} probe the test's directory outside the tree
 * @returns {string} source that records Jest's pid and hangs
 */
const hang = (probe) =>
  `fs.writeFileSync(${JSON.stringify(path.join(probe, 'hung'))}, String(process.pid)); for (;;) await new Promise((t) => setTimeout(t, 20))`

/**
 * A witness of the first spec's target that waits at a barrier until `release` exists.
 * @param {string} probe the test's directory outside the tree
 * @returns {string} the spec's source
 */
const barrier = (probe) =>
  [
    "test('w', async () => {",
    `  require('fs').writeFileSync(${JSON.stringify(path.join(probe, 'second.ready'))}, '')`,
    `  while (!require('fs').existsSync(${JSON.stringify(path.join(probe, 'release'))}))`,
    '    await new Promise((r) => setTimeout(r, 20))',
    "  expect(require('../x/value')).toBe(1)",
    '}, 120000)',
    '',
  ].join('\n')

describe('scripts/mutants/runner.js', () => {
  const roots = []

  afterAll(() => {
    for (const root of roots) fs.rmSync(root, { recursive: true, force: true })
  })

  /**
   * Creates a scratch tree that runs the repository's own Jest.
   * @param {Record<string, string>} files relative path → content
   * @returns {string} the tree's root
   */
  const tree = (files) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mutants-spec-'))
    roots.push(root)
    fs.symlinkSync(NODE_MODULES, path.join(root, 'node_modules'))
    fs.writeFileSync(path.join(root, 'package.json'), '{"name":"mutant-fixture"}\n')
    fs.writeFileSync(path.join(root, 'jest.config.js'), JEST_CONFIG)
    for (const [file, text] of Object.entries({ [TARGET]: 'module.exports = 1\n', ...files })) {
      fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true })
      fs.writeFileSync(path.join(root, file), text)
    }
    return root
  }

  const read = (root, file) => fs.readFileSync(path.join(root, file), 'utf8')

  /** @returns {string} a directory outside any tree, for files a test and its fixtures share */
  const probeDir = () => {
    const probe = fs.mkdtempSync(path.join(os.tmpdir(), 'mutants-probe-'))
    roots.push(probe)
    return probe
  }

  /**
   * @param {string} root a tree
   * @returns {Record<string, string>} the hash of every file of the checkout: all but `.tmp/` and `node_modules`
   */
  const snapshot = (root) => {
    const files = {}
    const walk = (dir) => {
      for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, e.name)
        const rel = path.relative(root, full)
        if (rel === '.tmp' || rel === 'node_modules') continue
        if (e.isDirectory()) walk(full)
        else files[rel] = sha256(fs.readFileSync(full))
      }
    }
    walk(root)
    return files
  }

  /**
   * @param {string} root a tree
   * @returns {string[]} the record directories left under `.tmp/mutants`
   */
  const leftovers = (root) => fs.readdirSync(path.join(root, '.tmp', 'mutants'))

  /**
   * @param {object} [fields] overrides
   * @returns {object} an inline record that mutates line 1 of the target
   */
  const record = (fields = {}) => ({
    id: 'fixture/M1',
    area: 'fixture',
    target: TARGET,
    line: 1,
    operator: 'constant',
    original: 'module.exports = 1',
    mutated: 'module.exports = 2',
    spec: SPEC,
    title: 'w',
    commandClass: 'unit',
    ...fields,
  })

  const WITNESS = "test('w', () => { expect(require('./value')).toBe(1) })\n"

  test('a mutant whose witness fails by assertion is killed', () => {
    const root = tree({ [SPEC]: WITNESS })
    const entry = new MutantRunner().runOne(record(), root, root)
    expect(entry).toMatchObject({
      id: 'fixture/M1',
      area: 'fixture',
      target: TARGET,
      line: 1,
      operator: 'constant',
      original: 'module.exports = 1',
      mutated: 'module.exports = 2',
      witness: `${SPEC} › w`,
      verdict: 'killed',
      reason: '',
    })
    expect(entry.durationMs).toBeGreaterThanOrEqual(0)
    expect(fs.readFileSync(path.join(root, TARGET), 'utf8')).toBe('module.exports = 1\n')
  }, 120000)

  test('a mutant whose witness passes is a survivor', () => {
    const root = tree({ [SPEC]: WITNESS })
    const entry = new MutantRunner().runOne(record({ mutated: 'module.exports = 1.0' }), root, root)
    expect(entry.verdict).toBe('survived')
    expect(entry.reason).toBe('the witness still passes')
    expect(fs.readFileSync(path.join(root, TARGET), 'utf8')).toBe('module.exports = 1\n')
  }, 120000)

  test('a generated mutant without a witness title is killed by a test of its spec failing by assertion', () => {
    const spec = [
      "test('a', () => { expect(true).toBe(true) })",
      "test('b', () => { expect(require('./value')).toBe(1) })",
      '',
    ].join('\n')
    const root = tree({ [SPEC]: spec })
    const entry = new MutantRunner().runOne(record({ title: '' }), root, root)
    expect(entry.verdict).toBe('killed')
    expect(entry.witness).toBe(`${SPEC} › b`)
  }, 120000)

  test('a stale hunk is stale, never killed', () => {
    const root = tree({ [SPEC]: WITNESS })
    const patch = `--- a/${TARGET}\n+++ b/${TARGET}\n@@ -1 +1 @@\n-module.exports = 7\n+module.exports = 8\n`
    fs.mkdirSync(path.join(root, 'patches/fixture'), { recursive: true })
    fs.writeFileSync(path.join(root, 'patches/fixture/P1.patch'), patch)
    const exec = jest.fn(fakeExec)
    const runner = new MutantRunner({ exec })
    const patched = record({ id: 'fixture/P1', patchSha: sha256(patch) })
    const inline = record({ id: 'fixture/I1', original: 'module.exports = 7' })
    const listed = record({ id: 'fixture/S1', stale: 'the hunk no longer applies' })
    const entries = [patched, inline, listed].map((r) =>
      runner.runOne(r, root, path.join(root, 'patches'))
    )
    expect(entries.map((e) => e.verdict)).toEqual(['stale', 'stale', 'stale'])
    expect(entries[0].reason).toMatch(/hunk/)
    expect(entries[1].reason).toMatch(/line 1 no longer reads the original/)
    expect(entries[2].reason).toBe('the hunk no longer applies')
    expect(exec).not.toHaveBeenCalled()
    expect(fs.readFileSync(path.join(root, TARGET), 'utf8')).toBe('module.exports = 1\n')
  })

  test('an ambiguous witness is invalid', () => {
    const root = tree({ [SPEC]: WITNESS + WITNESS })
    const entry = new MutantRunner().runOne(record(), root, root)
    expect(entry.verdict).toBe('invalid')
    expect(entry.reason).toMatch(/2 assertions have the frozen full name/)
  }, 120000)

  test('an afterEach failure after the body failure is accepted only with recorded hook timeouts', () => {
    const spec = [
      'let value',
      "afterEach(() => { if (value === 2) throw new Error('cleanup fails') })",
      "test('w', () => { value = require('./value'); expect(value).toBe(1) })",
      '',
    ].join('\n')
    const root = tree({ [SPEC]: spec })
    expect(new MutantRunner().runOne(record(), root, root).verdict).toBe('killed')

    const events = [
      'test_start',
      'test_fn_start',
      'test_fn_failure',
      'hook_failure:afterEach',
      'test_done',
    ]
    expect(
      MutantRunner.judgeEvents({ failing: false, hookTimeoutsRecorded: true, events }, []).status
    ).toBe('failed')
    expect(MutantRunner.judgeEvents({ failing: false, events }, []).status).toBe('setup')
    const timedOut = events.map((e) => (e === 'hook_failure:afterEach' ? `${e}:timeout` : e))
    expect(
      MutantRunner.judgeEvents({ failing: false, hookTimeoutsRecorded: true, events: timedOut }, [])
        .status
    ).toBe('error')
  }, 120000)

  test('the target and spec are restored after a crash', () => {
    const root = tree({ [SPEC]: WITNESS })
    const before = snapshot(root)
    const exec = (rec, dir, outFile, run) => {
      if (!fs.readFileSync(path.join(dir, TARGET), 'utf8').includes('= 2'))
        return fakeExec(rec, dir, outFile, run)
      fs.writeFileSync(path.join(dir, SPEC), 'rewritten\n')
      throw new Error('the child died')
    }
    const entry = new MutantRunner({ exec }).runOne(record(), root, root)
    expect(entry.verdict).toBe('invalid')
    expect(entry.reason).toBe('the run crashed: the child died')
    expect(fs.readFileSync(path.join(root, TARGET), 'utf8')).toBe('module.exports = 1\n')
    expect(fs.readFileSync(path.join(root, SPEC), 'utf8')).toBe(WITNESS)
    expect(snapshot(root)).toEqual(before)
    expect(leftovers(root)).toEqual([])
  })

  test('the checkout is never written: the mutation and every execution happen in a disposable copy, removed afterwards', () => {
    const root = tree({ [SPEC]: WITNESS })
    const real = fs.realpathSync(root)
    const before = snapshot(root)
    const seen = []
    const exec = (rec, dir, outFile, run) => {
      seen.push({
        dir,
        inside: path.relative(path.join(real, '.tmp', 'mutants'), dir).split(path.sep)[0],
        copy: read(dir, TARGET),
        checkout: snapshot(root),
      })
      return fakeExec(rec, dir, outFile, run)
    }
    expect(new MutantRunner({ exec }).runOne(record(), root, root).verdict).toBe('killed')
    expect(seen.map((s) => s.copy)).toEqual(['module.exports = 1\n', 'module.exports = 2\n'])
    expect(seen.map((s) => s.checkout)).toEqual([before, before])
    expect(seen[0].dir).toBe(seen[1].dir)
    expect(seen[0].inside).toMatch(/^record-/)
    expect(fs.existsSync(seen[0].dir)).toBe(false)
    expect(snapshot(root)).toEqual(before)
    expect(leftovers(root)).toEqual([])
  })

  /**
   * The default Jest execution, recording the given checkout file's bytes whenever it runs.
   * @param {string} file a checkout file
   * @param {string[]} seen receives the file's bytes at each execution
   * @returns {Function} an `exec` for {@link MutantRunner}
   */
  const watching = (file, seen) => (rec, dir, outFile, run) => {
    seen.push(fs.readFileSync(file, 'utf8'))
    return MutantRunner.jestExec(rec, dir, outFile, run)
  }

  test('a source link that leads back into the checkout is refused before any run, and leaves the checkout and a later record untouched', () => {
    const root = tree({ [SPEC]: WITNESS })
    const real = path.join(root, 'value.js')
    fs.renameSync(path.join(root, TARGET), real)
    fs.symlinkSync(real, path.join(root, TARGET))
    const seen = []
    const runner = new MutantRunner({ exec: watching(real, seen) })
    expect(() => runner.runOne(record(), root, root)).toThrow(
      `the tree links outside its copy: ${TARGET}`
    )
    expect(seen).toEqual([])
    expect(fs.readFileSync(real, 'utf8')).toBe('module.exports = 1\n')
    expect(() => runner.runOne(record({ id: 'fixture/M2' }), root, root)).toThrow(
      `the tree links outside its copy: ${TARGET}`
    )
    expect(seen).toEqual([])
    expect(fs.readFileSync(real, 'utf8')).toBe('module.exports = 1\n')
    expect(leftovers(root)).toEqual([])
  }, 120000)

  test('a dependency that links into the checkout is refused before any run, so no witness reads the original target', () => {
    const spec = "test('w', () => { expect(require('local-package')).toBe(1) })\n"
    const root = tree({
      [SPEC]: spec,
      'local-package/package.json': '{"name":"local-package","main":"index.js"}\n',
      'local-package/index.js': `module.exports = require('../${TARGET}')\n`,
    })
    fs.unlinkSync(path.join(root, 'node_modules'))
    fs.mkdirSync(path.join(root, 'node_modules'))
    fs.symlinkSync(path.join(NODE_MODULES, 'jest'), path.join(root, 'node_modules', 'jest'))
    fs.symlinkSync(
      path.join(root, 'local-package'),
      path.join(root, 'node_modules', 'local-package')
    )
    const before = snapshot(root)
    const seen = []
    const runner = new MutantRunner({ exec: watching(path.join(root, TARGET), seen) })
    const refusal = 'a dependency links into the tree: node_modules/local-package'
    expect(() => runner.runOne(record(), root, root)).toThrow(refusal)
    expect(seen).toEqual([])
    expect(snapshot(root)).toEqual(before)
    expect(() => runner.runOne(record({ id: 'fixture/M2' }), root, root)).toThrow(refusal)
    expect(seen).toEqual([])
    expect(snapshot(root)).toEqual(before)
    expect(leftovers(root)).toEqual([])
  }, 120000)

  /**
   * Runs one record through the synchronous API in a child Node process.
   * @param {string} cwd the child's working directory
   * @param {object} rec the record
   * @param {string} root the root argument, as the caller passes it
   * @param {string} patchDir the patch directory argument, as the caller passes it
   * @returns {object} the report line
   */
  const runFrom = (cwd, rec, root, patchDir) => {
    const r = spawnSync(
      process.execPath,
      [
        '-e',
        `const { MutantRunner } = require(${JSON.stringify(RUNNER)})
         const entry = new MutantRunner().runOne(${JSON.stringify(rec)}, ${JSON.stringify(root)}, ${JSON.stringify(patchDir)})
         console.log(JSON.stringify(entry))`,
      ],
      { cwd, encoding: 'utf8' }
    )
    expect({ status: r.status, stderr: r.stderr }).toEqual({ status: 0, stderr: '' })
    return JSON.parse(r.stdout)
  }

  test("the synchronous API kills a mutant with the root '.' and leaves the checkout pristine", () => {
    const root = tree({ [SPEC]: WITNESS })
    const before = snapshot(root)
    expect(runFrom(root, record(), '.', '.')).toMatchObject({ verdict: 'killed', reason: '' })
    expect(snapshot(root)).toEqual(before)
    expect(leftovers(root)).toEqual([])
  }, 120000)

  test('the synchronous API kills a mutant with a relative subdirectory root and a patch directory relative to the caller', () => {
    const patch = `--- a/${TARGET}\n+++ b/${TARGET}\n@@ -1 +1 @@\n-module.exports = 1\n+module.exports = 2\n`
    const root = tree({ [SPEC]: WITNESS, 'patches/fixture/P1.patch': patch })
    const before = snapshot(root)
    const cwd = path.dirname(root)
    const base = path.basename(root)
    const rec = record({ id: 'fixture/P1', patchSha: sha256(patch) })
    expect(runFrom(cwd, rec, `${base}/src/..`, `${base}/patches`)).toMatchObject({
      verdict: 'killed',
      reason: '',
    })
    expect(snapshot(root)).toEqual(before)
    expect(leftovers(root)).toEqual([])
  }, 120000)

  test('shard 2/3 runs exactly its third of the records', () => {
    const root = tree({ [SPEC]: WITNESS })
    const records = Array.from({ length: 9 }, (_, i) => record({ id: `fixture/M${i}` }))
    const out = path.join(root, 'ledger.jsonl')
    const runner = new MutantRunner({ exec: fakeExec })
    const entries = runner.run(records, {
      root,
      patchDir: root,
      out,
      shard: MutantRunner.parseShard('2/3'),
    })
    expect(entries.map((e) => e.id)).toEqual(['fixture/M1', 'fixture/M4', 'fixture/M7'])
    expect(entries.map((e) => e.verdict)).toEqual(['killed', 'killed', 'killed'])
    const lines = fs
      .readFileSync(out, 'utf8')
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l))
    expect(lines.map((l) => l.id)).toEqual(['fixture/M1', 'fixture/M4', 'fixture/M7'])

    const all = ['1/3', '2/3', '3/3'].flatMap((s) =>
      runner
        .run(records, { root, patchDir: root, shard: MutantRunner.parseShard(s) })
        .map((e) => e.id)
    )
    expect(all.sort()).toEqual(records.map((r) => r.id).sort())
    expect(() => MutantRunner.parseShard('4/3')).toThrow(/--shard/)
    expect(() => MutantRunner.parseShard('0/3')).toThrow(/--shard/)
  })

  test('a time cap reports every record it did not start as not-run', () => {
    const root = tree({ [SPEC]: WITNESS })
    let now = 0
    const exec = (...args) => {
      now += 60000
      return fakeExec(...args)
    }
    const records = Array.from({ length: 4 }, (_, i) => record({ id: `fixture/M${i}` }))
    const runner = new MutantRunner({ exec, now: () => now })
    const entries = runner.run(records, { root, patchDir: root, timeCapMs: 3 * 60000 })
    expect(entries.map((e) => [e.id, e.verdict, e.reason])).toEqual([
      ['fixture/M0', 'killed', ''],
      ['fixture/M1', 'killed', ''],
      ['fixture/M2', 'not-run', 'time cap'],
      ['fixture/M3', 'not-run', 'time cap'],
    ])
  })

  describe('a record with several witnesses', () => {
    const STEADY = 'src/x/steady.js'
    const MISSING = 'src/z/gone.spec.js'

    /**
     * @param {string[]} failing the witness specs that fail once the target is mutated
     * @returns {jest.Mock} a stand-in execution: a witness outside `failing` always passes
     */
    const execFailing = (failing) =>
      jest.fn((rec, dir, outFile, run) =>
        fakeExec(
          { ...rec, title: 'w', target: failing.includes(rec.spec) ? rec.target : STEADY },
          dir,
          outFile,
          run
        )
      )

    const several = (witnesses, fields = {}) =>
      record({ spec: witnesses[0], witnesses, title: '', ...fields })

    const files = () => ({
      [SPEC]: WITNESS,
      [OTHER_SPEC]: WITNESS,
      [STEADY]: 'module.exports = 1\n',
    })

    test('is killed when any one witness fails, names that witness, and runs no witness after a kill', () => {
      const root = tree(files())
      const exec = execFailing([OTHER_SPEC])
      const entry = new MutantRunner({ exec }).runOne(several([SPEC, OTHER_SPEC]), root, root)
      expect(entry).toMatchObject({ verdict: 'killed', reason: '', witness: `${OTHER_SPEC} › w` })
      expect(exec.mock.calls.map(([rec]) => rec.spec)).toEqual([SPEC, SPEC, OTHER_SPEC, OTHER_SPEC])
      expect(read(root, TARGET)).toBe('module.exports = 1\n')
      expect(leftovers(root)).toEqual([])

      const first = execFailing([SPEC, OTHER_SPEC])
      const stopped = new MutantRunner({ exec: first }).runOne(
        several([SPEC, OTHER_SPEC]),
        root,
        root
      )
      expect(stopped).toMatchObject({ verdict: 'killed', witness: `${SPEC} › w` })
      expect(first.mock.calls.map(([rec]) => rec.spec)).toEqual([SPEC, SPEC])
    })

    test('survives only when every witness passes, and reports each of them', () => {
      const root = tree(files())
      const exec = execFailing([])
      const entry = new MutantRunner({ exec }).runOne(several([SPEC, OTHER_SPEC]), root, root)
      expect(entry.verdict).toBe('survived')
      expect(entry.witness).toBe(`${SPEC}, ${OTHER_SPEC}`)
      expect(entry.reason).toBe(
        `${SPEC}: survived (the witness still passes); ${OTHER_SPEC}: survived (the witness still passes)`
      )
      expect(exec).toHaveBeenCalledTimes(4)
    })

    test('a witness that cannot decide does not hide another that can', () => {
      const root = tree(files())
      const killed = new MutantRunner({ exec: execFailing([OTHER_SPEC]) }).runOne(
        several([MISSING, OTHER_SPEC]),
        root,
        root
      )
      expect(killed).toMatchObject({ verdict: 'killed', witness: `${OTHER_SPEC} › w` })
      const survived = new MutantRunner({ exec: execFailing([]) }).runOne(
        several([MISSING, OTHER_SPEC]),
        root,
        root
      )
      expect(survived.verdict).toBe('survived')
      expect(survived.reason).toBe(
        `${MISSING}: invalid (the witness spec is missing); ${OTHER_SPEC}: survived (the witness still passes)`
      )
      const undecided = new MutantRunner({ exec: execFailing([]) }).runOne(
        several([MISSING, 'src/z/also.spec.js']),
        root,
        root
      )
      expect(undecided.verdict).toBe('invalid')
    })

    test('the time cap stops the witnesses still to run, reports the record not-run, and never undoes a kill', () => {
      const root = tree(files())
      let now = 0
      const exec = jest.fn((rec, dir, outFile, run) => {
        now += 60000
        return fakeExec({ ...rec, title: 'w', target: STEADY }, dir, outFile, run)
      })
      const runner = new MutantRunner({ exec, now: () => now })
      const records = [
        several([SPEC, OTHER_SPEC], { id: 'fixture/M0' }),
        several([SPEC, OTHER_SPEC], { id: 'fixture/M1' }),
      ]
      const entries = runner.run(records, { root, patchDir: root, timeCapMs: 2 * 60000 })
      expect(entries.map((e) => [e.id, e.verdict, e.reason])).toEqual([
        [
          'fixture/M0',
          'not-run',
          `time cap: 1 of 2 witnesses ran (${SPEC}: survived (the witness still passes))`,
        ],
        ['fixture/M1', 'not-run', 'time cap'],
      ])
      expect(exec).toHaveBeenCalledTimes(2)

      now = 0
      const three = runner.run([several([SPEC, OTHER_SPEC, MISSING], { id: 'fixture/M2' })], {
        root,
        patchDir: root,
        timeCapMs: 4 * 60000,
      })
      expect(three.map((e) => [e.verdict, e.reason])).toEqual([
        [
          'not-run',
          `time cap: 2 of 3 witnesses ran (${SPEC}: survived (the witness still passes); ${OTHER_SPEC}: survived (the witness still passes))`,
        ],
      ])

      now = 0
      const killing = new MutantRunner({
        exec: jest.fn((rec, dir, outFile, run) => {
          now += 60000
          return fakeExec({ ...rec, title: 'w' }, dir, outFile, run)
        }),
        now: () => now,
      })
      const killed = killing.run([several([SPEC, OTHER_SPEC])], {
        root,
        patchDir: root,
        timeCapMs: 60000,
      })
      expect(killed.map((e) => [e.verdict, e.witness])).toEqual([['killed', `${SPEC} › w`]])
    })

    test('is validated: at least two distinct specs, starting with spec, none of them the target', () => {
      const bad = (witnesses, fields) => () => MutantRunner.validate([several(witnesses, fields)])
      expect(bad([SPEC])).toThrow(/witnesses/)
      expect(bad([SPEC, SPEC])).toThrow(/witnesses/)
      expect(bad([OTHER_SPEC, SPEC], { spec: SPEC })).toThrow(/witnesses/)
      expect(bad([SPEC, 3])).toThrow(/witnesses/)
      expect(bad([SPEC, TARGET])).toThrow(/never edits its witness spec/)
      expect(bad([SPEC, '../escape.spec.js'])).toThrow(/escapes the tree/)
      expect(() => MutantRunner.validate([record({ witnesses: 'src/x/w.spec.js' })])).toThrow(
        /witnesses/
      )
      expect(MutantRunner.validate([several([SPEC, OTHER_SPEC])])).toHaveLength(1)
    })
  })

  test('a live record is located but reported not-run', () => {
    const root = tree({ [SPEC]: WITNESS })
    const exec = jest.fn(fakeExec)
    const entry = new MutantRunner({ exec }).runOne(record({ commandClass: 'live' }), root, root)
    expect(entry).toMatchObject({ verdict: 'not-run', reason: 'live', target: TARGET, line: 1 })
    expect(exec).not.toHaveBeenCalled()
  })

  test('a live record runs only with complete guard evidence for its own tree', () => {
    const root = tree({ [SPEC]: WITNESS })
    const sha = 'a'.repeat(40)
    const guard = {
      at: '2026-11-17T10:30:00.000Z',
      lane: 'docker',
      node: process.version,
      brokerVersion: '4.4.0',
      wireVersions: { Produce: [3, 12] },
      config: {
        compose: 'docker-compose.yml',
        env: { NODE_ENV: 'production', BROKER_BOUNCE_E2E: '1' },
      },
      composeImages: { kafka1: 'apache/kafka:4.4.0@sha256:1' },
      runId: '7',
      sha,
    }
    const file = path.join(probeDir(), 'live-guard.json')
    const run = (evidence) => {
      if (evidence) fs.writeFileSync(file, JSON.stringify(evidence))
      else fs.rmSync(file, { force: true })
      const exec = jest.fn(fakeExec)
      const runner = new MutantRunner({ exec, liveEvidence: file, treeSha: () => sha })
      return { entry: runner.runOne(record({ commandClass: 'live' }), root, root), exec }
    }
    const complete = run(guard)
    expect(complete.entry).toMatchObject({ verdict: 'killed', reason: '' })
    expect(complete.exec).toHaveBeenCalledTimes(2)
    const partial = run({ ...guard, at: '', composeImages: {} })
    expect(partial.entry).toMatchObject({
      verdict: 'invalid',
      reason: 'guard evidence lacks at, composeImages',
    })
    expect(partial.exec).not.toHaveBeenCalled()
    const foreign = run({ ...guard, sha: 'b'.repeat(40) })
    expect(foreign.entry).toMatchObject({
      verdict: 'invalid',
      reason: `guard evidence names another tree (${'b'.repeat(40)})`,
    })
    expect(foreign.exec).not.toHaveBeenCalled()
    const absent = run(null)
    expect(absent.entry).toMatchObject({ verdict: 'invalid', reason: 'guard evidence is missing' })
    expect(absent.exec).not.toHaveBeenCalled()
  })

  test('an inventory record whose target lies under node_modules is refused', () => {
    expect(MutantRunner.validate([record()])).toEqual([record()])
    for (const target of [
      'node_modules/jest/bin/jest.js',
      './node_modules/jest/bin/jest.js',
      'src/x/node_modules/dep/index.js',
    ])
      expect(() => MutantRunner.validate([record({ target })])).toThrow(
        'fixture/M1: a mutation never edits node_modules'
      )
  })

  /**
   * Runs a script in a child Node process.
   * @param {string} script the script's source
   * @returns {import('child_process').SpawnSyncReturns<string>} its result
   */
  const node = (script) => spawnSync(process.execPath, ['-e', script], { encoding: 'utf8' })

  /**
   * @param {number} pid a process id
   * @returns {boolean} whether that process can still run (a zombie cannot)
   */
  const alive = (pid) => {
    try {
      const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8')
      return stat.slice(stat.lastIndexOf(')') + 2)[0] !== 'Z'
    } catch {
      return false
    }
  }

  /**
   * @param {number} pid a process id
   * @returns {number} its parent's pid
   */
  const parentOf = (pid) => {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8')
    return Number(stat.slice(stat.lastIndexOf(')') + 2).split(' ')[1])
  }

  /**
   * Waits until `check` returns a truthy value.
   * @param {() => unknown} check the condition
   * @param {number} ms how long to wait
   * @returns {Promise<unknown>} the value
   */
  const until = async (check, ms) => {
    for (const end = Date.now() + ms; Date.now() < end;) {
      const value = check()
      if (value) return value
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
    throw new Error('timed out waiting')
  }

  /**
   * @param {string} probe the test's probe directory
   * @param {string} name a file the fixtures write there
   * @returns {Promise<number>} the pid the file holds, once it is written
   */
  const pidIn = async (probe, name) =>
    Number(
      await until(
        () =>
          fs.existsSync(path.join(probe, name)) && fs.readFileSync(path.join(probe, name), 'utf8'),
        90000
      )
    )

  /**
   * @param {string} probe the test's probe directory
   * @returns {number|undefined} the writer's pid, once it runs
   */
  const orphan = (probe) =>
    fs.existsSync(path.join(probe, 'orphan.pid')) ? Number(read(probe, 'orphan.pid')) : undefined

  /**
   * Lets any surviving writer write, then waits for the settle window.
   * @param {string} probe the test's probe directory
   */
  const settled = async (probe) => {
    fs.writeFileSync(path.join(probe, 'go'), '')
    await new Promise((resolve) => setTimeout(resolve, SETTLE_MS))
  }

  /** @param {...(number|undefined)} pids processes a test leaves behind, killed if still running */
  const stop = (...pids) => {
    for (const pid of pids) if (pid && alive(pid)) process.kill(pid, 'SIGKILL')
  }

  /**
   * @param {string} root a tree
   * @returns {string[]} the target of every record copy under `.tmp/mutants`, sorted
   */
  const copiedTargets = (root) =>
    leftovers(root)
      .map((dir) => path.join(root, '.tmp', 'mutants', dir, 'tree', TARGET))
      .filter((file) => fs.existsSync(file))
      .map((file) => fs.readFileSync(file, 'utf8'))
      .sort()

  /**
   * {@link copiedTargets}, read again until no copy is caught between the stray writer's truncate and
   * write, which reads as an empty file.
   * @param {string} root a tree
   * @returns {Promise<string[]>} the targets, each read whole
   */
  const wholeTargets = (root) =>
    until(() => {
      const targets = copiedTargets(root)
      return targets.every((text) => text !== '') && targets
    }, 5000)

  /**
   * Starts the CLI on the records of a tree, with the inventory and the ledger in the probe directory.
   * @param {string} root the tree
   * @param {string} probe the test's probe directory
   * @param {object[]} records the inventory
   * @returns {{ child: import('child_process').ChildProcess, exited: Promise<{ code: number|null, signal: string|null }>, ledger: () => object[] }} the CLI
   */
  const cli = (root, probe, records) => {
    const inventory = path.join(probe, 'inventory.json')
    const out = path.join(probe, 'out.jsonl')
    fs.writeFileSync(inventory, JSON.stringify({ records }))
    const args = [RUNNER, inventory, '--root', root, '--patches', root, '--out', out]
    const child = spawn(process.execPath, args, { cwd: probe, stdio: 'ignore' })
    const exited = new Promise((resolve) =>
      child.on('exit', (code, signal) => resolve({ code, signal }))
    )
    const ledger = () =>
      fs
        .readFileSync(out, 'utf8')
        .trim()
        .split('\n')
        .map((l) => JSON.parse(l))
    return { child, exited, ledger }
  }

  /**
   * Runs the CLI over two records of one target. The first record's mutant leaves a writer behind;
   * the second waits at a barrier, so it runs while that writer writes. The checkout's bytes are
   * compared before, during and after the run.
   * @param {string} root a tree holding both specs, the writer and any `env.js`
   * @param {string} probe the test's probe directory
   * @param {() => Promise<void>} [provoke] what the test does while the first mutant runs
   * @returns {Promise<object[]>} the ledger
   */
  const twoRecords = async (root, probe, provoke) => {
    const before = snapshot(root)
    const records = [record(), record({ id: 'fixture/M2', spec: OTHER_SPEC })]
    const { child, exited, ledger } = cli(root, probe, records)
    try {
      if (provoke) await provoke()
      await until(() => fs.existsSync(path.join(probe, 'second.ready')), 90000)
      expect(alive(orphan(probe))).toBe(true)
      expect(snapshot(root)).toEqual(before)
      await settled(probe)
      expect(await wholeTargets(root)).toEqual(['module.exports = 1\n', 'module.exports = 9\n'])
      expect(snapshot(root)).toEqual(before)
      fs.writeFileSync(path.join(probe, 'release'), '')
      expect(await exited).toEqual({ code: 0, signal: null })
      expect(alive(orphan(probe))).toBe(true)
      expect(snapshot(root)).toEqual(before)
      return ledger()
    } finally {
      fs.writeFileSync(path.join(probe, 'release'), '')
      child.kill('SIGKILL')
      stop(orphan(probe))
    }
  }

  /**
   * @param {string} probe the test's probe directory
   * @param {Record<string, string>} files more files: the first spec, or an `env.js`
   * @returns {string} a tree for {@link twoRecords}
   */
  const twoRecordTree = (probe, files) =>
    tree({
      'jest.config.js': files['env.js'] ? ENV_CONFIG : JEST_CONFIG,
      [SPEC]: WITNESS,
      [OTHER_SPEC]: barrier(probe),
      'writer.js': writer(probe),
      ...files,
    })

  /**
   * After a run that did not finish: lets the stray writer write, then checks that the checkout is
   * unchanged and that a later record on the same checkout is decided from untouched bytes.
   * @param {string} root the tree
   * @param {string} probe the test's probe directory
   * @param {Record<string, string>} before the checkout's snapshot before the run
   */
  const laterRunUnaffected = async (root, probe, before) => {
    expect(alive(orphan(probe))).toBe(true)
    await settled(probe)
    expect(await wholeTargets(root)).toContain('module.exports = 9\n')
    expect(snapshot(root)).toEqual(before)
    const seen = []
    const exec = (rec, dir, outFile, run) => {
      seen.push(read(dir, TARGET))
      return fakeExec(rec, dir, outFile, run)
    }
    expect(new MutantRunner({ exec }).runOne(record(), root, root).verdict).toBe('killed')
    expect(seen).toEqual(['module.exports = 1\n', 'module.exports = 2\n'])
    expect(snapshot(root)).toEqual(before)
  }

  /**
   * @param {string} probe the test's probe directory
   * @param {string} options the writer's extra `spawn` options, as source
   * @returns {string} a witness that, under the mutation, starts the writer and then fails
   */
  const launching = (probe, options) =>
    [
      "test('w', async () => {",
      "  const value = require('./value')",
      `  if (value !== 1) ${launch(probe, "require('path').join(__dirname, '..', '..')", options)}`,
      '  expect(value).toBe(1)',
      '})',
      '',
    ].join('\n')

  /**
   * @param {string} probe the test's probe directory
   * @param {string} options the writer's extra `spawn` options, as source
   * @returns {string} a witness that, under the mutation, starts the writer, records Jest's pid and waits
   */
  const lingering = (probe, options) =>
    [
      "test('w', async () => {",
      "  const value = require('./value')",
      '  if (value !== 1) {',
      `    ${launch(probe, "require('path').join(__dirname, '..', '..')", options)}`,
      `    require('fs').writeFileSync(${JSON.stringify(path.join(probe, 'mutated.pid'))}, String(process.pid))`,
      '    await new Promise((resolve) => setTimeout(resolve, 300000))',
      '  }',
      '  expect(value).toBe(1)',
      '}, 400000)',
      '',
    ].join('\n')

  test("a descendant left in its command's process group is killed when the command ends, and no other process", async () => {
    const probe = probeDir()
    const bystander = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], {
      stdio: 'ignore',
    })
    const root = tree({ [SPEC]: launching(probe, '{ env: {} }'), 'writer.js': writer(probe) })
    const before = snapshot(root)
    try {
      expect(new MutantRunner().runOne(record(), root, root).verdict).toBe('killed')
      await until(() => !alive(orphan(probe)), 5000)
      expect(alive(bystander.pid)).toBe(true)
      expect(snapshot(root)).toEqual(before)
      expect(leftovers(root)).toEqual([])
    } finally {
      stop(orphan(probe), bystander.pid)
    }
  }, 120000)

  test('a descendant that leaves its session with an empty environment changes neither the checkout nor a later record', async () => {
    const probe = probeDir()
    const root = twoRecordTree(probe, { [SPEC]: launching(probe, '{ detached: true, env: {} }') })
    const ledger = await twoRecords(root, probe)
    expect(ledger.map((l) => [l.id, l.verdict, l.reason])).toEqual([
      ['fixture/M1', 'killed', ''],
      ['fixture/M2', 'killed', ''],
    ])
  }, 120000)

  test('a descendant of a crashed Jest environment changes neither the checkout nor a later record', async () => {
    const probe = probeDir()
    const root = twoRecordTree(probe, { 'env.js': environment(probe, CRASH, '{ detached: true }') })
    const ledger = await twoRecords(root, probe)
    expect(ledger.map((l) => [l.id, l.verdict])).toEqual([
      ['fixture/M1', 'invalid'],
      ['fixture/M2', 'killed'],
    ])
  }, 120000)

  test('a descendant of a timed-out Jest changes neither the checkout nor a later record', async () => {
    const probe = probeDir()
    const root = twoRecordTree(probe, {
      'env.js': environment(probe, hang(probe), '{ env: {}, detached: true }'),
    })
    const ledger = await twoRecords(root, probe, async () => {
      const timeout = parentOf(await pidIn(probe, 'hung'))
      expect(fs.readFileSync(`/proc/${timeout}/comm`, 'utf8').trim()).toBe('timeout')
      process.kill(timeout, 'SIGALRM')
    })
    expect(ledger.map((l) => [l.id, l.verdict, l.reason])).toEqual([
      ['fixture/M1', 'invalid', 'the mutant run was cut off (exit 124)'],
      ['fixture/M2', 'killed', ''],
    ])
  }, 120000)

  test('a SIGTERM to the runner exits 143 and leaves the checkout unchanged for a later record', async () => {
    const probe = probeDir()
    const root = tree({
      [SPEC]: lingering(probe, '{ detached: true, env: {} }'),
      'writer.js': writer(probe),
    })
    const before = snapshot(root)
    const { child, exited } = cli(root, probe, [record()])
    let jestPid
    try {
      jestPid = await pidIn(probe, 'mutated.pid')
      expect(copiedTargets(root)).toEqual(['module.exports = 2\n'])
      expect(snapshot(root)).toEqual(before)
      child.kill('SIGTERM')
      expect(await exited).toEqual({ code: 143, signal: null })
      expect(snapshot(root)).toEqual(before)
      await laterRunUnaffected(root, probe, before)
    } finally {
      child.kill('SIGKILL')
      stop(jestPid, orphan(probe))
    }
  }, 120000)

  test('a worker killed mid-mutant exits 137 and leaves the checkout unchanged for a later record', async () => {
    const probe = probeDir()
    const root = tree({
      [SPEC]: lingering(probe, '{ detached: true }'),
      'writer.js': writer(probe),
    })
    const before = snapshot(root)
    const { child, exited } = cli(root, probe, [record()])
    let jestPid
    try {
      jestPid = await pidIn(probe, 'mutated.pid')
      const workerPid = parentOf(parentOf(jestPid))
      expect(parentOf(workerPid)).toBe(child.pid)
      expect(snapshot(root)).toEqual(before)
      process.kill(workerPid, 'SIGKILL')
      expect(await exited).toEqual({ code: 137, signal: null })
      expect(snapshot(root)).toEqual(before)
      await laterRunUnaffected(root, probe, before)
    } finally {
      child.kill('SIGKILL')
      stop(jestPid, orphan(probe))
    }
  }, 120000)

  test('a descendant whose Jest, timeout, worker and supervisor were all SIGKILLed changes neither the checkout nor a later record', async () => {
    const probe = probeDir()
    const root = tree({ [SPEC]: lingering(probe, '{ env: {} }'), 'writer.js': writer(probe) })
    const before = snapshot(root)
    const { child, exited } = cli(root, probe, [record()])
    let jestPid
    let timeoutPid
    let workerPid
    try {
      jestPid = await pidIn(probe, 'mutated.pid')
      timeoutPid = parentOf(jestPid)
      workerPid = parentOf(timeoutPid)
      child.kill('SIGKILL')
      expect(await exited).toEqual({ code: null, signal: 'SIGKILL' })
      for (const pid of [workerPid, jestPid, timeoutPid]) process.kill(pid, 'SIGKILL')
      await until(() => ![workerPid, jestPid, timeoutPid].some(alive), 10000)
      expect(snapshot(root)).toEqual(before)
      await laterRunUnaffected(root, probe, before)
    } finally {
      child.kill('SIGKILL')
      stop(workerPid, jestPid, timeoutPid, orphan(probe))
    }
  }, 120000)

  test('the disposable copies lie where the Git and npm ignore rules exclude them', () => {
    const repo = path.resolve(__dirname, '..', '..', '..')
    const root = tree({ [SPEC]: WITNESS })
    const real = fs.realpathSync(root)
    let files
    const exec = (rec, dir, outFile, run) => {
      files ??= [path.join(dir, TARGET), path.join(dir, SPEC), outFile, run.evidence].map((f) =>
        path.relative(real, f)
      )
      return fakeExec(rec, dir, outFile, run)
    }
    new MutantRunner({ exec }).runOne(record(), root, root)
    expect(files.every((f) => f.startsWith(`.tmp${path.sep}mutants${path.sep}`))).toBe(true)
    for (const rules of ['.gitignore', '.npmignore']) {
      const probe = fs.mkdtempSync(path.join(os.tmpdir(), 'mutants-ignore-'))
      roots.push(probe)
      expect(spawnSync('git', ['init', '-q'], { cwd: probe }).status).toBe(0)
      fs.copyFileSync(path.join(repo, rules), path.join(probe, '.git', 'info', 'exclude'))
      const r = spawnSync('git', ['check-ignore', '--no-index', ...files], {
        cwd: probe,
        encoding: 'utf8',
      })
      expect({ rules, ignored: r.stdout.trim().split('\n') }).toEqual({ rules, ignored: files })
    }
  })

  test('a runner killed mid-mutant leaves the checkout unchanged and does not block the next run', () => {
    const root = tree({ [SPEC]: WITNESS })
    const before = snapshot(root)
    const killed = node(`
      const fs = require('fs')
      const path = require('path')
      const { MutantRunner } = require(${JSON.stringify(RUNNER)})
      const fakeExec = require(${JSON.stringify(FAKE_EXEC)})
      const exec = (rec, dir, ...rest) => {
        if (fs.readFileSync(path.join(dir, ${JSON.stringify(TARGET)}), 'utf8').includes('= 2'))
          process.kill(process.pid, 'SIGKILL')
        return fakeExec(rec, dir, ...rest)
      }
      new MutantRunner({ exec }).runOne(${JSON.stringify(record())}, ${JSON.stringify(root)}, ${JSON.stringify(root)})
    `)
    expect(killed.signal).toBe('SIGKILL')
    expect(copiedTargets(root)).toEqual(['module.exports = 2\n'])
    expect(snapshot(root)).toEqual(before)

    const exec = jest.fn(fakeExec)
    expect(new MutantRunner({ exec }).runOne(record(), root, root).verdict).toBe('killed')
    expect(exec).toHaveBeenCalledTimes(2)
    expect(snapshot(root)).toEqual(before)
  })

  test('two runs on the same checkout proceed side by side, each in its own copy', () => {
    const OTHER = 'src/x/other.js'
    const root = tree({ [SPEC]: WITNESS, [OTHER]: 'module.exports = 1\n' })
    const before = snapshot(root)
    const second = (rec) =>
      JSON.parse(
        node(`
          const { MutantRunner } = require(${JSON.stringify(RUNNER)})
          const fakeExec = require(${JSON.stringify(FAKE_EXEC)})
          const entry = new MutantRunner({ exec: fakeExec }).runOne(${JSON.stringify(rec)}, ${JSON.stringify(root)}, ${JSON.stringify(root)})
          console.log(JSON.stringify([entry.id, entry.verdict, entry.reason]))
        `).stdout
      )
    const seen = []
    const exec = (rec, dir, outFile, run) => {
      if (read(dir, TARGET).includes('= 2')) {
        seen.push(second(record({ id: 'fixture/B1' })))
        seen.push(second(record({ id: 'fixture/B2', target: OTHER })))
      }
      return fakeExec(rec, dir, outFile, run)
    }
    expect(new MutantRunner({ exec }).runOne(record(), root, root).verdict).toBe('killed')
    expect(seen).toEqual([
      ['fixture/B1', 'killed', ''],
      ['fixture/B2', 'killed', ''],
    ])
    expect(snapshot(root)).toEqual(before)
    expect(leftovers(root)).toEqual([])
  })

  test('a mutant that does not compile is invalid, never killed', () => {
    const lazy = tree({ [SPEC]: WITNESS })
    const eager = tree({
      [SPEC]: "const value = require('./value')\ntest('w', () => { expect(value).toBe(1) })\n",
    })
    for (const root of [lazy, eager]) {
      const entry = new MutantRunner().runOne(record({ mutated: 'module.exports = {' }), root, root)
      expect(entry.verdict).toBe('invalid')
      expect(entry.reason).toMatch(/^the mutation does not compile: /)
      expect(read(root, TARGET)).toBe('module.exports = 1\n')
    }
  }, 120000)

  test('a module that fails to load inside the witness body is invalid, a runtime error still kills', () => {
    const root = tree({
      [TARGET]: "module.exports = require('./ok')\n",
      'src/x/ok.js': 'module.exports = 1\n',
      'src/x/broken.js': 'module.exports = {\n',
      [SPEC]: WITNESS,
    })
    const original = "module.exports = require('./ok')"
    for (const mutated of [
      "module.exports = require('./broken')",
      "module.exports = require('./missing')",
    ]) {
      const entry = new MutantRunner().runOne(record({ original, mutated }), root, root)
      expect([mutated, entry.verdict, entry.reason]).toEqual([
        mutated,
        'invalid',
        'the witness body did not fail: a hook, import or setup failure is not a kill',
      ])
      expect(read(root, TARGET)).toBe(`${original}\n`)
    }

    const parsing = tree({
      [TARGET]: "module.exports = '1'\n",
      [SPEC]: "test('w', () => { expect(JSON.parse(require('./value'))).toBe(1) })\n",
    })
    const parsed = record({ original: "module.exports = '1'", mutated: "module.exports = '{'" })
    expect(new MutantRunner().runOne(parsed, parsing, parsing).verdict).toBe('killed')
  }, 120000)

  test('a spec-level kill needs a test that passed in the control', () => {
    const root = tree({
      [SPEC]: [
        "const value = require('./value')",
        "if (value === 1) test('control only', () => { expect(value).toBe(1) })",
        "else test('mutant only', () => { expect(value).toBe(1) })",
        '',
      ].join('\n'),
    })
    const entry = new MutantRunner().runOne(record({ title: '' }), root, root)
    expect(entry.verdict).toBe('invalid')
    expect(entry.reason).toBe("the mutant run's tests are not the control's")
  }, 120000)

  test('a witness registered at another line than in the control is not a kill', () => {
    const root = tree({
      [SPEC]: [
        "const value = require('./value')",
        "if (value === 1) test('w', () => { expect(value).toBe(1) })",
        "if (value !== 1) test('w', () => { expect(value).toBe(1) })",
        '',
      ].join('\n'),
    })
    for (const title of ['w', '']) {
      const entry = new MutantRunner().runOne(record({ title }), root, root)
      expect([title, entry.verdict, entry.reason]).toEqual([
        title,
        'invalid',
        "the mutant run's tests are not the control's",
      ])
    }
  }, 120000)

  test('a test the mutant omits or skips makes the run invalid', () => {
    const variants = [
      "if (value === 1) test('b', () => { expect(true).toBe(true) })",
      ";(value === 1 ? test : test.skip)('b', () => { expect(true).toBe(true) })",
    ]
    for (const variant of variants) {
      const root = tree({
        [SPEC]: [
          "const value = require('./value')",
          "test('a', () => { expect(value).toBe(1) })",
          variant,
          '',
        ].join('\n'),
      })
      const entry = new MutantRunner().runOne(record({ title: '' }), root, root)
      expect([variant, entry.verdict, entry.reason]).toEqual([
        variant,
        'invalid',
        "the mutant run's tests are not the control's",
      ])
    }
  }, 120000)
})
