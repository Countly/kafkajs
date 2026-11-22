const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { lastJsonLine, pick, runEntry, renderMarkdown, main } = require('../report')
const manifest = require('../manifest.json')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const BENCHMARKS = path.join(ROOT, 'scripts', 'benchmarks')
const HELPERS = [
  'session.js',
  'sendPathCounters.js',
  'fetchFrameCounter.js',
  'subscribeChannels.js',
  'report.js',
]
const KNOWN_PLACEHOLDERS = ['{client}', '{package}', '{baselinePackageDir}', '{raw}']
const FORK = { package: '@countly/kafkajs', version: '3.0.0-test' }
const BASE = { package: 'kafkajs', version: '2.2.4' }
const forkDir = path.join(os.tmpdir(), 'bench-fork')
const baselineDir = path.join(os.tmpdir(), 'bench-base')
const rawDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bench-raw-'))
afterAll(() => fs.rmSync(rawDir, { recursive: true, force: true }))

const json = (value) => ({ status: 0, stdout: `log line\n${JSON.stringify(value)}\n`, stderr: '' })
const single = (overrides) => ({
  name: 'single',
  command: ['scripts/benchmarks/single.js'],
  broker: false,
  metrics: [{ key: 'v', unit: 'ms' }],
  arms: ['fork'],
  ...overrides,
})
const compared = (overrides) => ({
  name: 'compared',
  command: [
    'scripts/benchmarks/clientCompare.js',
    '--client',
    '{client}',
    '--package',
    '{package}',
  ],
  broker: true,
  metrics: [{ key: 'a', unit: 'msg/s' }],
  arms: ['fork', 'kafkajs@2.2.4'],
  ...overrides,
})
const options = (spawn, runs = 3) => ({ runs, forkDir, baselineDir, spawn, rawDir })
const valueAfter = (args, flag) => args[args.indexOf(flag) + 1]
/** A spawn stub: `respond(args, callNumber)` answers each call; every call is kept in `calls`. */
const stub = (respond) => {
  const calls = []
  const spawn = (command, args, spawnOptions) => {
    calls.push({ command, args, spawnOptions })
    return respond(args, calls.length)
  }
  return { spawn, calls }
}
/** A nested object holding `value` at every metric key of `entry`. */
const outputFor = (entry, value = 1) => {
  const out = {}
  for (const { key } of entry.metrics) {
    const parts = key.split('.')
    let at = out
    for (const part of parts.slice(0, -1)) at = at[part] ??= {}
    at[parts.at(-1)] = value
  }
  return out
}
const identityOf = (args) =>
  valueAfter(args, '--package') === 'kafkajs' ? BASE : valueAfter(args, '--package') ? FORK : {}

describe('Benchmarks > report', () => {
  describe('lastJsonLine', () => {
    test('returns the last line that parses as a JSON object', () => {
      const out = 'log\n{"a":1}\n[1,2]\n"x"\n7\n{"a":2}\nnoise\n'

      expect(lastJsonLine(out)).toEqual({ a: 2 })
      expect(lastJsonLine(out.split('\n'))).toEqual({ a: 2 })
    })

    test('throws when no line is a JSON object', () => {
      expect(() => lastJsonLine('log\n[1]\n"x"\n')).toThrow(/JSON object/)
      expect(() => lastJsonLine('')).toThrow(/JSON object/)
    })
  })

  describe('pick', () => {
    test('reads dotted metric paths', () => {
      const murmur2 = { sizes: { 16: { nsPerOp: 12.5, baselineNsPerOp: 14 } } }

      expect(pick(murmur2, 'sizes.16.nsPerOp')).toBe(12.5)
      expect(pick(murmur2, 'sizes.256.nsPerOp')).toBeUndefined()
      expect(pick({ v11: { nsPerOp: 3 } }, 'v11.nsPerOp')).toBe(3)
    })

    test('returns finite numbers only and never reads inherited properties', () => {
      expect(pick({ a: null }, 'a')).toBeUndefined()
      expect(pick({ a: '5' }, 'a')).toBeUndefined()
      expect(pick({ a: Infinity }, 'a')).toBeUndefined()
      expect(pick({ a: NaN }, 'a')).toBeUndefined()
      expect(pick({ a: { b: 1 } }, 'a')).toBeUndefined()
      expect(pick({}, 'toString.length')).toBeUndefined()
      expect(pick({ a: 0 }, 'a')).toBe(0)
    })
  })

  describe('runEntry', () => {
    test('computes the median and the min to max of three runs', () => {
      const values = [5, 1, 3]
      const { spawn, calls } = stub((args, n) => json({ v: values[n - 1] }))

      const { rows } = runEntry(single(), options(spawn))

      expect(calls).toHaveLength(3)
      expect(rows).toEqual([
        {
          benchmark: 'single',
          arm: 'fork',
          metric: 'v',
          unit: 'ms',
          runs: 3,
          status: 'valid',
          median: 3,
          min: 1,
          max: 5,
        },
      ])
      expect(rows[0]).not.toHaveProperty('ratio')
      expect(rows[0]).not.toHaveProperty('reason')
    })

    test('runs node from the repository root with the manifest arguments', () => {
      const { spawn, calls } = stub(() => json({ v: 1 }))

      runEntry(
        single({ command: ['--expose-gc', 'scripts/benchmarks/single.js', '--x', '1'] }),
        options(spawn, 1)
      )

      expect(calls[0].command).toBe(process.execPath)
      expect(calls[0].args).toEqual(['--expose-gc', 'scripts/benchmarks/single.js', '--x', '1'])
      expect(calls[0].spawnOptions).toMatchObject({ cwd: ROOT, encoding: 'utf8' })
      expect(calls[0].spawnOptions.timeout).toBeGreaterThan(0)
    })

    test('reports a crash, a non-zero exit and unparseable output as error rows and keeps the other rows', () => {
      const { rows: crash } = runEntry(
        single(),
        options(() => ({ error: new Error('spawn ENOENT'), status: null, stdout: '', stderr: '' }))
      )
      const { rows: exit } = runEntry(
        single(),
        options(() => ({ status: 1, stdout: '', stderr: 'line1\nboom\n' }))
      )
      const { rows: garbage } = runEntry(
        single(),
        options(() => ({ status: 0, stdout: '{"v": NaN}\n', stderr: '' }))
      )
      const { rows: killed } = runEntry(
        single(),
        options(() => ({ status: null, signal: 'SIGTERM', stdout: '', stderr: '' }))
      )
      // Only the fork arm crashes; the baseline arm and the second metric keep their rows.
      const { rows: partial } = runEntry(
        compared({
          metrics: [
            { key: 'a', unit: 'msg/s' },
            { key: 'b', unit: 'ms' },
          ],
        }),
        options((_command, args) =>
          valueAfter(args, '--package') === '@countly/kafkajs'
            ? { status: 2, stdout: '', stderr: 'down' }
            : json({ ...BASE, a: 4, b: 6 })
        )
      )

      expect(crash).toEqual([
        expect.objectContaining({
          metric: 'v',
          runs: 0,
          status: 'error',
          reason: expect.stringContaining('spawn ENOENT'),
        }),
      ])
      expect(exit[0].reason).toBe('run 1 of 3: exit 1: line1 boom')
      expect(garbage[0].reason).toMatch(/JSON object/)
      expect(killed[0].reason).toMatch(/SIGTERM/)
      expect(partial.map((row) => [row.arm, row.metric, row.status === 'valid'])).toEqual([
        ['fork', 'a', false],
        ['fork', 'b', false],
        ['kafkajs@2.2.4', 'a', true],
        ['kafkajs@2.2.4', 'b', true],
      ])
      expect(partial[0]).toMatchObject({ status: 'error', reason: 'run 1 of 3: exit 2: down' })
      expect(partial[1]).toMatchObject({ status: 'error', reason: 'run 1 of 3: exit 2: down' })
      expect(partial[2]).toMatchObject({ median: 4, runs: 3 })
    })

    test('stops an arm at its first failed run', () => {
      const { spawn, calls } = stub((args, n) =>
        n === 2 ? { status: 1, stdout: '', stderr: 'x' } : json({ v: 1 })
      )

      const { rows } = runEntry(single(), options(spawn))

      expect(calls).toHaveLength(2)
      expect(rows).toEqual([
        expect.objectContaining({
          runs: 1,
          status: 'error',
          reason: expect.stringContaining('run 2 of 3'),
        }),
      ])
    })

    test('an interleaving adapter alternates the arms run by run and stops only the arm that failed', () => {
      const { spawn, calls } = stub((args, n) =>
        n === 3
          ? { status: 1, stdout: '', stderr: 'boom' }
          : json({ ...identityOf(args), a: n, elapsedMs: 30000 })
      )

      const { rows, details } = runEntry(compared({ adapter: 'clientCompare' }), options(spawn, 1))

      expect(calls.map(({ args }) => valueAfter(args, '--package'))).toEqual([
        '@countly/kafkajs',
        'kafkajs',
        '@countly/kafkajs',
        'kafkajs',
        'kafkajs',
        'kafkajs',
        'kafkajs',
      ])
      expect(
        rows.map(({ arm, metric, status, runs, median }) => ({ arm, metric, status, runs, median }))
      ).toEqual([
        { arm: 'fork', metric: 'a', status: 'error', runs: 1, median: undefined },
        { arm: 'kafkajs@2.2.4', metric: 'a', status: 'valid', runs: 5, median: 5 },
      ])
      expect(rows[0].reason).toBe('run 2 of 5: exit 1: boom')
      expect(details.map(({ benchmark, status }) => [benchmark, status])).toEqual([
        ['compared', 'error'],
        ['compared', 'valid'],
      ])
      const evidence = JSON.parse(
        fs.readFileSync(path.join(rawDir, 'compared', 'kafkajs@2.2.4.json'), 'utf8')
      )
      expect(evidence.runs.map(({ run }) => run)).toEqual([1, 2, 3, 4, 5])
    })

    test('an entry with profile runs each arm that did not fail once more under --cpu-prof, after its measured runs and outside its medians', () => {
      const { spawn, calls } = stub((args, n) =>
        valueAfter(args, '--package') === 'kafkajs'
          ? { status: 1, stdout: '', stderr: 'boom' }
          : json({
              ...identityOf(args),
              a: args.includes('--cpu-prof') ? 999 : n,
              elapsedMs: 30000,
            })
      )

      const { rows, details } = runEntry(
        compared({ name: 'profiled', adapter: 'clientCompare', profile: true }),
        options(spawn)
      )

      const profileDir = path.join(rawDir, 'profiled', 'fork-cpu-profile')
      expect(calls).toHaveLength(7)
      expect(calls[6].args.slice(0, 3)).toEqual(['--cpu-prof', '--cpu-prof-dir', profileDir])
      expect(calls[6].args.slice(3)).toEqual(calls[0].args)
      expect(calls.slice(0, 6).every(({ args }) => !args.includes('--cpu-prof'))).toBe(true)
      expect(rows.find(({ arm }) => arm === 'fork')).toMatchObject({ runs: 5, median: 4 })
      expect(details.find(({ notes }) => notes[0].startsWith('fork')).notes.at(-1)).toBe(
        'CPU profile (one extra run, not in the medians): bench-raw/profiled/fork-cpu-profile/'
      )
      const evidence = JSON.parse(
        fs.readFileSync(path.join(rawDir, 'profiled', 'fork.json'), 'utf8')
      )
      expect(evidence.runs.map(({ run }) => run)).toEqual([1, 2, 3, 4, 5, 'profile'])
    })

    test('reports a missing, null, NaN or non-numeric metric as an error row', () => {
      const keys = ['good', 'missing', 'nothing', 'text', 'huge', 'nested']
      const entry = single({ metrics: keys.map((key) => ({ key, unit: 'ms' })) })
      const stdout = '{"good":4,"nothing":null,"text":"12","huge":1e999,"nested":{"a":1}}\n'

      const { rows } = runEntry(
        entry,
        options(() => ({ status: 0, stdout, stderr: '' }), 1)
      )

      expect(rows.map((row) => row.metric)).toEqual(keys)
      expect(rows[0]).toMatchObject({ median: 4 })
      for (const row of rows.slice(1)) {
        expect(row.status).toBe('error')
        expect(row.reason).toContain(row.metric)
        expect(row).not.toHaveProperty('median')
      }
    })

    test('omits the ratio when the baseline median is 0', () => {
      const entry = compared({
        metrics: [
          { key: 'a', unit: 'msg/s' },
          { key: 'b', unit: 'msg/s' },
        ],
      })
      const { spawn } = stub((args) => {
        const base = valueAfter(args, '--package') === 'kafkajs'
        return json({ ...identityOf(args), a: base ? 0 : 10, b: base ? 5 : 10 })
      })

      const { rows } = runEntry(entry, options(spawn, 1))

      const byKey = Object.fromEntries(rows.map((row) => [`${row.arm}/${row.metric}`, row]))
      expect(byKey['fork/a']).not.toHaveProperty('ratio')
      expect(byKey['fork/b'].ratio).toBe(2)
      expect(byKey['kafkajs@2.2.4/a']).not.toHaveProperty('ratio')
      expect(byKey['kafkajs@2.2.4/b']).not.toHaveProperty('ratio')
      expect(Object.values(byKey).every((row) => row.status === 'valid')).toBe(true)
    })

    test('rejects a compared row whose loaded package does not match its arm', () => {
      const run = (forkIdentity, baseIdentity) =>
        runEntry(
          compared(),
          options(
            (_command, args) =>
              json({
                ...(valueAfter(args, '--package') === '@countly/kafkajs'
                  ? forkIdentity
                  : baseIdentity),
                a: 1,
              }),
            1
          )
        ).rows
      const errorOf = (rows, arm) => rows.find((row) => row.arm === arm).reason

      const ok = run(FORK, BASE)
      const forkLoadedBaseline = run(BASE, BASE)
      const baselineLoadedFork = run(FORK, FORK)
      const baselineWrongVersion = run(FORK, { package: 'kafkajs', version: '2.3.0' })
      const noIdentity = run({}, BASE)

      expect(ok.map((row) => row.status)).toEqual(['valid', 'valid'])
      expect(errorOf(forkLoadedBaseline, 'fork')).toMatch(/@countly\/kafkajs/)
      expect(errorOf(forkLoadedBaseline, 'kafkajs@2.2.4')).toBeUndefined()
      expect(errorOf(baselineLoadedFork, 'kafkajs@2.2.4')).toMatch(/kafkajs/)
      expect(errorOf(baselineWrongVersion, 'kafkajs@2.2.4')).toMatch(/2\.2\.4/)
      expect(errorOf(noIdentity, 'fork')).toMatch(/@countly\/kafkajs/)
    })

    test('spawns murmur2 with the installed kafkajs 2.2.4 package directory as --baseline-tree', () => {
      const entry = manifest.entries.find((candidate) => candidate.name === 'murmur2')
      const { spawn, calls } = stub(() => json(outputFor(entry)))

      const { rows } = runEntry(entry, options(spawn, 1))

      const tree = valueAfter(calls[0].args, '--baseline-tree')
      expect(tree).toBe(path.join(baselineDir, 'node_modules', 'kafkajs'))
      expect(tree).not.toBe(baselineDir)
      expect(tree).not.toContain('@countly')
      expect(entry.arms).toEqual(['fork'])
      expect(rows.every((row) => row.status === 'valid')).toBe(true)
    })

    test('reads dotted metric paths', () => {
      const entry = single({ metrics: [{ key: 'sizes.16.nsPerOp', unit: 'ns' }] })
      const { spawn } = stub(() => json({ sizes: { 16: { nsPerOp: 21 } } }))

      expect(runEntry(entry, options(spawn, 1)).rows[0]).toMatchObject({
        metric: 'sizes.16.nsPerOp',
        median: 21,
      })
    })

    test('replaces {client} and {package} per arm in the spawned arguments', () => {
      const entry = manifest.entries.find((candidate) => candidate.arms.length === 2)
      const { spawn, calls } = stub((args) =>
        json({ ...identityOf(args), ...outputFor(entry, 30000) })
      )

      const { rows } = runEntry(entry, options(spawn, 1))

      const [fork, base] = calls.map((call) => call.args)
      expect(valueAfter(fork, '--client')).toBe(forkDir)
      expect(valueAfter(fork, '--package')).toBe('@countly/kafkajs')
      expect(valueAfter(base, '--client')).toBe(baselineDir)
      expect(valueAfter(base, '--package')).toBe('kafkajs')
      expect(rows.every((row) => row.status === 'valid')).toBe(true)
    })

    test('leaves no placeholder in the spawned arguments of any manifest entry', () => {
      for (const entry of manifest.entries) {
        const { spawn, calls } = stub((args) => json({ ...identityOf(args), ...outputFor(entry) }))

        runEntry(entry, options(spawn, 1))

        const adapter = entry.adapter && require(path.join(BENCHMARKS, 'adapters', entry.adapter))
        const profiled = entry.profile ? 1 : 0
        expect(calls).toHaveLength(
          entry.adapter ? ((adapter.runs ?? 1) + profiled) * entry.arms.length : entry.arms.length
        )
        for (const { args } of calls) expect(args.filter((arg) => /[{}]/.test(arg))).toEqual([])
      }
    })
  })

  describe('manifest', () => {
    const scriptOf = (entry) => entry.command.find((arg) => arg.endsWith('.js'))

    test('has a version, unique names, arms, a broker flag and metrics with units', () => {
      expect(manifest.version).toBe(1)
      expect(new Set(manifest.entries.map((entry) => entry.name)).size).toBe(
        manifest.entries.length
      )
      for (const entry of manifest.entries) {
        expect(typeof entry.broker).toBe('boolean')
        expect(entry.metrics.length).toBeGreaterThan(0)
        for (const metric of entry.metrics) {
          expect(metric.key).toEqual(expect.any(String))
          expect(metric.unit).toEqual(expect.any(String))
          expect(metric.unit).not.toBe('')
        }
        expect(entry.arms[0]).toBe('fork')
        expect([['fork'], ['fork', 'kafkajs@2.2.4']]).toContainEqual(entry.arms)
      }
    })

    test('every manifest command points at an existing file, and no helper module is listed', () => {
      for (const entry of manifest.entries) {
        const script = scriptOf(entry)
        expect(fs.existsSync(path.join(ROOT, script))).toBe(true)
        expect(script.startsWith('scripts/benchmarks/')).toBe(true)
        expect(HELPERS).not.toContain(path.basename(script))
      }
    })

    test('every excluded entry names a file and a reason', () => {
      const listed = manifest.entries.map((entry) => path.basename(scriptOf(entry)))
      for (const { file, reason } of manifest.excluded) {
        expect(file).toEqual(expect.any(String))
        expect(fs.existsSync(path.join(BENCHMARKS, file))).toBe(true)
        expect(reason.trim().length).toBeGreaterThan(10)
        expect(HELPERS).not.toContain(file)
        expect(listed).not.toContain(file)
      }
    })

    test('every two-arm entry contains {client} and {package}', () => {
      const twoArm = manifest.entries.filter((entry) => entry.arms.length === 2)

      expect(twoArm.length).toBeGreaterThan(0)
      for (const entry of twoArm) {
        expect(entry.command).toEqual(expect.arrayContaining(['{client}', '{package}']))
      }
    })

    test('uses only the known placeholders, and {baselinePackageDir} only in a fork-only entry', () => {
      for (const entry of manifest.entries) {
        const tokens = entry.command.flatMap((arg) => arg.match(/\{[^}]*\}/g) ?? [])
        expect(tokens.filter((token) => !KNOWN_PLACEHOLDERS.includes(token))).toEqual([])
        if (tokens.includes('{baselinePackageDir}')) expect(entry.arms).toEqual(['fork'])
      }
    })

    test('every script in scripts/benchmarks is an entry, an exclusion or a helper', () => {
      const classified = new Set([
        ...HELPERS,
        ...manifest.excluded.map(({ file }) => file),
        ...manifest.entries.map((entry) => path.basename(scriptOf(entry))),
      ])
      const scripts = fs.readdirSync(BENCHMARKS).filter((file) => file.endsWith('.js'))

      expect(scripts.filter((file) => !classified.has(file))).toEqual([])
    })
  })

  describe('renderMarkdown', () => {
    const rows = [
      {
        benchmark: 'frameDecode-64b',
        arm: 'fork',
        metric: 'nsPerFrame',
        unit: 'ns',
        runs: 3,
        status: 'valid',
        median: 1234.56789,
        min: 1200,
        max: 1300,
      },
      {
        benchmark: 'produce-latency',
        arm: 'fork',
        metric: 'messagesPerSecond',
        unit: 'msg/s',
        runs: 3,
        status: 'valid',
        median: 2000,
        min: 1900,
        max: 2100,
        ratio: 1.25,
      },
      {
        benchmark: 'produce-latency',
        arm: 'kafkajs@2.2.4',
        metric: 'messagesPerSecond',
        unit: 'msg/s',
        runs: 3,
        status: 'valid',
        median: 1600,
        min: 1500,
        max: 1700,
      },
      {
        benchmark: 'broken',
        arm: 'fork',
        metric: 'v',
        unit: 'ms',
        runs: 1,
        status: 'error',
        reason: 'run 2 of 3: exit 1: a | b\nc',
      },
    ]

    test('renders a Markdown table with benchmark, arm, metric, median, range, unit, runs and ratio', () => {
      const lines = renderMarkdown(rows, { runs: 3, kafka: '4.4.0' }).split('\n')

      expect(lines).toContain(
        '| benchmark | arm | metric | status | median | range | unit | runs | ratio |'
      )
      expect(lines).toContain('| --- | --- | --- | --- | --- | --- | --- | --- | --- |')
      expect(lines).toContain(
        '| frameDecode-64b | fork | nsPerFrame | valid | 1234.57 | 1200 to 1300 | ns | 3 | - |'
      )
      expect(lines).toContain(
        '| produce-latency | fork | messagesPerSecond | valid | 2000 | 1900 to 2100 | msg/s | 3 | 1.25 |'
      )
      expect(lines).toContain(
        '| produce-latency | kafkajs@2.2.4 | messagesPerSecond | valid | 1600 | 1500 to 1700 | msg/s | 3 | - |'
      )
    })

    test('lists the run metadata and every error row with its reason on one line', () => {
      const markdown = renderMarkdown(rows, { runs: 3, kafka: '4.4.0' })

      expect(markdown).toMatch(/^# Benchmark report/)
      expect(markdown).toContain('- runs: 3')
      expect(markdown).toContain('- kafka: 4.4.0')
      expect(markdown).toContain('| broken | fork | v | error | - | - | ms | 1 | - |')
      expect(markdown).toContain('- broken / fork / v: error: run 2 of 3: exit 1: a \\| b c')
    })
  })

  describe('main', () => {
    let out
    const log = jest.fn()
    beforeEach(() => {
      out = fs.mkdtempSync(path.join(os.tmpdir(), 'bench-report-'))
      log.mockClear()
    })
    afterEach(() => fs.rmSync(out, { recursive: true, force: true }))
    const argv = (to = out) => [
      '--runs',
      '1',
      '--fork-dir',
      forkDir,
      '--baseline-dir',
      baselineDir,
      '--out',
      to,
    ]

    test('writes both report files and exits 0 when every benchmark fails', () => {
      const failing = { version: 1, entries: [single(), compared()], excluded: [] }

      const code = main(argv(), {
        spawn: () => ({ status: 1, stdout: '', stderr: 'boom' }),
        manifest: failing,
        log,
      })

      expect(code).toBe(0)
      expect(log.mock.calls.map(([line]) => line)).toEqual(['single', 'compared'])
      const report = JSON.parse(fs.readFileSync(path.join(out, 'bench-report.json'), 'utf8'))
      expect(report.meta.runs).toBe(1)
      expect(report.rows).toHaveLength(3)
      expect(report.rows.every((row) => row.status === 'error')).toBe(true)
      expect(report.rows.every((row) => row.reason.includes('boom'))).toBe(true)
      expect(fs.readFileSync(path.join(out, 'bench-report.md'), 'utf8')).toContain(
        '# Benchmark report'
      )
    })

    test('creates the output directory and writes the rows of a passing run', () => {
      const nested = path.join(out, 'nested', 'dir')
      const passing = { version: 1, entries: [single()], excluded: [] }

      const code = main(argv(nested), { spawn: () => json({ v: 7 }), manifest: passing, log })

      expect(code).toBe(0)
      const report = JSON.parse(fs.readFileSync(path.join(nested, 'bench-report.json'), 'utf8'))
      expect(report.rows).toEqual([expect.objectContaining({ benchmark: 'single', median: 7 })])
    })

    test('rejects a bad argument with exit code 2 and writes nothing', () => {
      const deps = {
        spawn: () => json({ v: 1 }),
        manifest: { version: 1, entries: [single()], excluded: [] },
        log,
      }

      expect(
        main(['--runs', 'x', '--fork-dir', 'a', '--baseline-dir', 'b', '--out', out], deps)
      ).toBe(2)
      expect(main(['--runs', '3'], deps)).toBe(2)
      expect(main(['--bogus', '1'], deps)).toBe(2)
      expect(log).toHaveBeenCalledTimes(3)
      expect(log.mock.calls[0][0]).toMatch(/^usage: /)
      expect(fs.readdirSync(out)).toEqual([])
    })

    const three = {
      version: 1,
      entries: [single({ name: 'a' }), single({ name: 'b' }), compared()],
    }

    test('--only runs the named rows and lists every other row as not selected', () => {
      const { spawn, calls } = stub((args) =>
        args.includes('--package') ? json({ ...identityOf(args), a: 2 }) : json({ v: 5 })
      )

      const code = main([...argv(), '--only', 'compared,b'], { spawn, manifest: three, log })

      expect(code).toBe(0)
      expect(
        calls.map(({ args }) =>
          args.includes('--package') ? valueAfter(args, '--package') : 'single'
        )
      ).toEqual(['single', '@countly/kafkajs', 'kafkajs'])
      expect(log.mock.calls.map(([line]) => line)).toEqual(['b', 'compared'])
      const report = JSON.parse(fs.readFileSync(path.join(out, 'bench-report.json'), 'utf8'))
      expect(report.meta.only).toEqual(['compared', 'b'])
      expect(
        report.rows.map(({ benchmark, arm, status }) => `${benchmark} ${arm} ${status}`)
      ).toEqual([
        'a - not selected',
        'b fork valid',
        'compared fork valid',
        'compared kafkajs@2.2.4 valid',
      ])
      expect(report.rows[0]).toEqual({
        benchmark: 'a',
        arm: '-',
        metric: '-',
        unit: '-',
        runs: 0,
        status: 'not selected',
        reason: 'left out by --only',
      })
      expect(fs.readdirSync(path.join(out, 'bench-raw')).sort()).toEqual(['b', 'compared'])
      const markdown = fs.readFileSync(path.join(out, 'bench-report.md'), 'utf8')
      expect(markdown).toContain('| a | - | - | not selected | - | - | - | 0 | - |')
      expect(markdown).toContain('- only: compared,b')
      expect(markdown).not.toMatch(/\b(omitted|error)\b/)
    })

    test('--only with an unknown row is a bad argument and writes nothing', () => {
      const { spawn, calls } = stub(() => json({ v: 1 }))

      expect(main([...argv(), '--only', 'a,nope'], { spawn, manifest: three, log })).toBe(2)
      expect(main([...argv(), '--only', 'a,'], { spawn, manifest: three, log })).toBe(2)

      expect(calls).toEqual([])
      expect(log.mock.calls.map(([line]) => line)).toEqual([
        '--only names no manifest row nope',
        '--only names no manifest row ',
      ])
      expect(fs.readdirSync(out)).toEqual([])
    })

    test('an empty --only runs every row', () => {
      const { spawn, calls } = stub((args) =>
        args.includes('--package') ? json({ ...identityOf(args), a: 2 }) : json({ v: 5 })
      )

      expect(main([...argv(), '--only', ''], { spawn, manifest: three, log })).toBe(0)

      expect(calls).toHaveLength(4)
      const report = JSON.parse(fs.readFileSync(path.join(out, 'bench-report.json'), 'utf8'))
      expect(report.meta).not.toHaveProperty('only')
      expect(report.rows.every(({ status }) => status === 'valid')).toBe(true)
    })
  })
  describe('statuses, evidence and adapters', () => {
    const STATUSES = ['valid', 'inconclusive', 'invalid', 'error', 'omitted']
    const SESSION_DIR = path.join(__dirname, 'fixtures', 'stickyBalanceSession')
    /** A recorded session result: `{ status, stdout, stderr }` as spawnSync returns it. */
    const fixture = (name) =>
      JSON.parse(fs.readFileSync(path.join(SESSION_DIR, `${name}.json`), 'utf8'))
    const healthyRow = () => manifest.entries.find(({ name }) => name === 'stickyBalance-healthy')
    const slowRow = () => manifest.entries.find(({ name }) => name === 'stickyBalance-slow-broker')
    /** A plain entry whose `--tag` names it. */
    const tagged = (name) =>
      single({ name, command: ['scripts/benchmarks/single.js', '--tag', name] })
    /** A session entry like the manifest rows, with a name and a `--tag` of its own. */
    const sessionEntry = (scenario, name = `session-${scenario}`) => ({
      name,
      adapter: 'stickyBalanceSession',
      command: [
        '--expose-gc',
        'scripts/benchmarks/stickyBalance.js',
        '--session',
        '--scenario',
        scenario,
        '--raw',
        '{raw}',
        '--runs',
        scenario === 'healthy' ? '8' : '9',
        '--tag',
        name,
      ],
      broker: true,
      metrics: [
        { key: 'recordsPerRequest', unit: 'records' },
        { key: 'p99Ms', unit: 'ms' },
      ],
      arms: ['fork'],
    })
    const omitted = (fields) => ({
      ...sessionEntry('slow-broker', 'removed-option'),
      omitted: fields,
    })
    let out
    beforeEach(() => {
      out = fs.mkdtempSync(path.join(os.tmpdir(), 'bench-status-'))
    })
    afterEach(() => fs.rmSync(out, { recursive: true, force: true }))
    const argv = () => [
      '--runs',
      '1',
      '--fork-dir',
      forkDir,
      '--baseline-dir',
      baselineDir,
      '--out',
      out,
    ]
    /**
     * Runs `main` over `entries`, each answered by `respond(entryName, args)`; an entry is found
     * by its `--tag`, else by its whole command.
     */
    const report = (entries, respond) => {
      const calls = []
      const spawn = (_command, args) => {
        const name = args.includes('--tag')
          ? valueAfter(args, '--tag')
          : entries.find(
              ({ command }) =>
                command.length === args.length &&
                command.every((arg, i) => /[{}]/.test(arg) || args[i] === arg)
            ).name
        calls.push({ entry: name, args })
        return respond(name, args)
      }
      expect(
        main(argv(), { spawn, manifest: { version: 1, entries, excluded: [] }, log: () => {} })
      ).toBe(0)
      return {
        calls,
        json: JSON.parse(fs.readFileSync(path.join(out, 'bench-report.json'), 'utf8')),
        markdown: fs.readFileSync(path.join(out, 'bench-report.md'), 'utf8'),
      }
    }
    /** The cells of the report table's rows; the tables of the details come after `## `. */
    const tableLines = (markdown) =>
      markdown
        .split('\n## ')[0]
        .split('\n')
        .filter(
          (line) =>
            line.startsWith('| ') && !line.startsWith('| benchmark ') && !line.startsWith('| ---')
        )
        .map((line) => line.slice(2, -2).split(' | '))
    const raw = (...parts) => path.join(out, 'bench-raw', ...parts)
    const readJson = (file) => JSON.parse(fs.readFileSync(file, 'utf8'))

    test('a row is valid, inconclusive, invalid, error or omitted, never a bare number', () => {
      const entries = [
        tagged('plain'),
        tagged('crashing'),
        sessionEntry('healthy', 'noisy'),
        sessionEntry('slow-broker', 'control-failed'),
        omitted({
          reason: 'adaptive removed',
          decision: 'the partitioner ruling',
          evidenceRun: '42',
        }),
      ]
      const answers = {
        plain: json({ v: 2 }),
        crashing: { status: 1, stdout: '', stderr: 'down' },
        noisy: fixture('healthy-void'),
        'control-failed': fixture('slow-broker-invalid'),
      }

      const { json: result, markdown } = report(entries, (name) => answers[name])

      const statusOf = (name) => [
        ...new Set(result.rows.filter((row) => row.benchmark === name).map((row) => row.status)),
      ]
      expect(statusOf('plain')).toEqual(['valid'])
      expect(statusOf('crashing')).toEqual(['error'])
      expect(statusOf('noisy')).toEqual(['inconclusive'])
      expect(statusOf('control-failed')).toEqual(['invalid'])
      expect(statusOf('removed-option')).toEqual(['omitted'])
      for (const row of result.rows) {
        expect(STATUSES).toContain(row.status)
        const numbered = ['valid', 'inconclusive'].includes(row.status)
        expect(Number.isFinite(row.median)).toBe(numbered)
        if (!numbered) {
          expect(row).not.toHaveProperty('min')
          expect(row).not.toHaveProperty('max')
          expect(row.reason).toEqual(expect.any(String))
        }
      }
      const lines = tableLines(markdown)
      expect(lines.length).toBe(result.rows.length)
      for (const [, , , status, median, range] of lines) {
        expect(STATUSES).toContain(status)
        if (['valid', 'inconclusive'].includes(status)) {
          expect(Number.isFinite(Number(median))).toBe(true)
          expect(range).toMatch(/ to /)
        } else {
          expect([median, range]).toEqual(['-', '-'])
        }
      }
    })

    test('raw observations, controls and stdout are preserved for every arm even on failure', () => {
      const session = sessionEntry('slow-broker', 'crashed-session')
      const pair = compared({ name: 'pair' })
      const result = { records: 120000, partitionerCalls: 120000, keyedIdentical: 1, p99Ms: 905 }
      const control = { topologyValid: 1, gcAvailable: 1, valid: 1 }
      const observation = `${JSON.stringify({ round: 0, label: 'sticky', result: { ...result, ...control } })}\n`
      const { calls } = report([session, pair], (name, args) => {
        if (name === 'crashed-session') {
          // The session appends its observations before it crashes; the report keeps the file.
          const file = valueAfter(args, '--raw')
          if (!file.startsWith(raw('crashed-session'))) return { status: 2, stdout: '', stderr: '' }
          fs.writeFileSync(file, observation)
          return { ...fixture('slow-broker-crashed'), stdout: `partial\n` }
        }
        if (valueAfter(args, '--package') === '@countly/kafkajs') {
          return { status: 1, stdout: 'fork log\n', stderr: 'fork boom' }
        }
        expect(readJson(raw('pair', 'fork.json')).runs).toHaveLength(1)
        return { ...json({ ...BASE, a: 3 }), stderr: 'base warning' }
      })

      expect(calls.map(({ entry }) => entry)).toEqual(['crashed-session', 'pair', 'pair'])
      expect(readJson(raw('crashed-session', 'entry.json'))).toEqual(session)
      expect(readJson(raw('pair', 'entry.json'))).toEqual(pair)
      const crashed = readJson(raw('crashed-session', 'fork.json'))
      expect(crashed.runs).toEqual([
        expect.objectContaining({
          run: 1,
          status: 1,
          stdout: 'partial\n',
          stderr: fixture('slow-broker-crashed').stderr,
          args: calls[0].args,
        }),
      ])
      const rawFile = valueAfter(calls[0].args, '--raw')
      expect(path.dirname(rawFile)).toBe(raw('crashed-session'))
      expect(fs.readFileSync(rawFile, 'utf8')).toBe(observation)
      expect(readJson(raw('pair', 'fork.json')).runs[0]).toMatchObject({
        status: 1,
        stdout: 'fork log\n',
        stderr: 'fork boom',
      })
      expect(readJson(raw('pair', 'kafkajs@2.2.4.json')).runs[0]).toMatchObject({
        status: 0,
        stderr: 'base warning',
      })
    })

    test('an adapter is loaded from the manifest adapter field', () => {
      const entries = [
        sessionEntry('healthy', 'adapted'),
        { ...tagged('unknown-adapter'), adapter: 'noSuchAdapter' },
        { ...tagged('escaping-adapter'), adapter: '../report' },
      ]

      const { calls, json: result } = report(entries, () => fixture('healthy-passed'))

      expect(calls.map(({ entry }) => entry)).toEqual(['adapted'])
      const adapted = result.rows.filter((row) => row.benchmark === 'adapted')
      expect([...new Set(adapted.map((row) => row.arm))].sort()).toEqual([
        'adaptive',
        'round-robin',
        'round-robin~AA',
        'sticky',
      ])
      expect(result.rows.filter((row) => row.benchmark !== 'adapted')).toEqual([
        expect.objectContaining({
          benchmark: 'unknown-adapter',
          status: 'error',
          reason: expect.stringContaining('noSuchAdapter'),
        }),
        expect.objectContaining({
          benchmark: 'escaping-adapter',
          status: 'error',
          reason: expect.stringContaining('../report'),
        }),
      ])
      for (const entry of manifest.entries.filter(({ adapter }) => adapter !== undefined)) {
        expect(fs.existsSync(path.join(BENCHMARKS, 'adapters', `${entry.adapter}.js`))).toBe(true)
      }
    })

    test('the adaptive comparison rows show every arm, its A/A copy and the controls', () => {
      const healthy = healthyRow()
      const slow = slowRow()
      expect(manifest.entries.map(({ name }) => name)).not.toContain('stickyBalance')
      const shape = (scenario, runs) => [
        '--expose-gc',
        'scripts/benchmarks/stickyBalance.js',
        '--session',
        '--scenario',
        scenario,
        '--raw',
        '{raw}',
        '--runs',
        runs,
      ]
      expect(healthy).toMatchObject({
        adapter: 'stickyBalanceSession',
        broker: true,
        arms: ['fork'],
        command: shape('healthy', '24'),
      })
      expect(slow).toMatchObject({
        adapter: 'stickyBalanceSession',
        broker: true,
        arms: ['fork'],
        command: shape('slow-broker', '9'),
      })
      const base = ['recordsPerRequest', 'balance', 'p99Ms', 'heapRetainedBytes']
      const slowOnly = [
        'slowMedianMs',
        'otherMedianMs',
        'backlogAt6s',
        'backlogMax',
        'delivered',
        'rejected',
        'refused',
        'unsettled',
        'drainAtMs',
      ]
      expect(healthy.metrics.map(({ key }) => key)).toEqual(base)
      expect(slow.metrics.map(({ key }) => key)).toEqual([...base, ...slowOnly])

      const {
        calls,
        json: result,
        markdown,
      } = report([healthy, slow], (name) =>
        name === healthy.name
          ? rejudged('healthy-passed', (arms) => {
              for (const stats of Object.values(arms))
                for (const stat of Object.values(stats)) stat.n = 24
            })
          : fixture('slow-broker-failed')
      )

      expect(calls).toHaveLength(2)
      expect(valueAfter(calls[0].args, '--raw')).toBe(raw(healthy.name, 'fork-run-1.jsonl'))
      const cells = (name) =>
        result.rows
          .filter((row) => row.benchmark === name)
          .map((row) => `${row.arm} ${row.metric} ${row.status}`)
      const expected = (labels, metrics, status) =>
        labels.flatMap((label) => metrics.map((metric) => `${label} ${metric} ${status}`))
      expect(cells(healthy.name)).toEqual(
        expected(['round-robin', 'sticky', 'adaptive', 'round-robin~AA'], base, 'valid')
      )
      expect(cells(slow.name)).toEqual(
        expected(['sticky', 'adaptive', 'sticky~AA'], [...base, ...slowOnly], 'valid')
      )
      const recorded = JSON.parse(fixture('slow-broker-failed').stdout)
      expect(
        result.rows.find(
          (row) => row.benchmark === slow.name && row.arm === 'adaptive' && row.metric === 'p99Ms'
        )
      ).toMatchObject({
        median: recorded.arms.adaptive.p99Ms.median,
        min: recorded.arms.adaptive.p99Ms.min,
        max: recorded.arms.adaptive.p99Ms.max,
        runs: 9,
        unit: 'ms',
      })
      const details = Object.fromEntries(result.details.map((detail) => [detail.benchmark, detail]))
      const controlNames = [
        'partitioner calls = admitted records',
        'keyed mapping identical',
        'topology valid',
        'GC available',
        'every observation valid',
        'A/A spread',
      ]
      for (const [name, labels, reference, scenarioControls] of [
        [
          healthy.name,
          ['round-robin', 'sticky', 'adaptive', 'round-robin~AA'],
          'round-robin',
          ['round-robin records split evenly', 'round-robin~AA records split evenly'],
        ],
        [slow.name, ['sticky', 'adaptive', 'sticky~AA'], 'sticky', []],
      ]) {
        const { controls } = details[name]
        expect(
          controls
            .filter(({ name: control }) => control !== 'A/A spread')
            .map(({ arm, name: control }) => `${arm} ${control}`)
        ).toEqual([
          ...labels.flatMap((label) =>
            controlNames.slice(0, -1).map((control) => `${label} ${control}`)
          ),
          ...scenarioControls,
        ])
        expect(controls.every(({ passed }) => passed)).toBe(true)
        expect(controls.at(-1)).toMatchObject({
          arm: `${reference}~AA`,
          name: 'A/A spread',
          passed: true,
        })
        expect(controls.at(-1).detail).toMatch(
          new RegExp(`p99Ms [+-]\\d+\\.\\d\\d % from ${reference}`)
        )
      }
      expect(details[healthy.name].notes).toEqual([
        expect.stringMatching(/^session verdict passed\b.*gates nothing/),
      ])
      expect(details[slow.name]).toMatchObject({ status: 'valid' })
      expect(details[slow.name].notes).toEqual([
        expect.stringMatching(
          /^session verdict failed: adaptive p99 above half of sticky\b.*gates nothing/
        ),
      ])
      for (const name of [healthy.name, slow.name]) {
        expect(markdown).toContain(`### ${name}: valid`)
        expect(markdown).toContain(`- raw evidence: bench-raw/${name}/`)
      }
      expect(markdown).toMatch(/- control adaptive keyed mapping identical: passed/)
      expect(markdown).toMatch(
        /- control sticky~AA A\/A spread: passed \(p99Ms [+-]\d+\.\d\d % from sticky\)/
      )
      expect(markdown).toContain('- session verdict failed: adaptive p99 above half of sticky')
    })

    test('a failed control makes the row invalid', () => {
      const entry = sessionEntry('slow-broker')
      const recorded = JSON.parse(fixture('slow-broker-invalid').stdout)
      expect(recorded.arms.adaptive.keyedIdentical.min).toBe(0)
      const passingVerdict = { ...recorded, verdict: 'passed', reasons: [] }

      const { json: result, markdown } = report(
        [entry, sessionEntry('slow-broker', 'judged-pass')],
        (name) =>
          name === entry.name
            ? fixture('slow-broker-invalid')
            : { status: 0, stdout: `${JSON.stringify(passingVerdict)}\n`, stderr: '' }
      )

      for (const name of [entry.name, 'judged-pass']) {
        const rows = result.rows.filter((row) => row.benchmark === name)
        expect(rows).toEqual([
          {
            benchmark: name,
            arm: '-',
            metric: '-',
            unit: '-',
            runs: 1,
            status: 'invalid',
            reason:
              'failed controls: adaptive keyed mapping identical; adaptive every observation valid',
          },
        ])
        const detail = result.details.find((candidate) => candidate.benchmark === name)
        expect(
          detail.controls
            .filter(({ passed }) => !passed)
            .map(({ arm, name: control }) => `${arm} ${control}`)
        ).toEqual(['adaptive keyed mapping identical', 'adaptive every observation valid'])
      }
      expect(markdown).toContain(`| ${entry.name} | - | - | invalid | - | - | - | 1 | - |`)
      expect(markdown).toContain(`### ${entry.name}: invalid`)
      expect(markdown).toContain('- control adaptive keyed mapping identical: FAILED')
    })

    /**
     * A recorded session line changed by `change`, judged again by the real `judge`.
     * @returns {{ status: number, stdout: string, stderr: string }} what the session would return
     */
    const rejudged = (name, change) => {
      const { judge, sessionReport } = require('../stickyBalance')
      const line = lastJsonLine(fixture(name).stdout)
      change(line.arms, line)
      const verdict = judge(line.scenario, line)
      const status = { passed: 0, failed: 1, inconclusive: 3 }[verdict.verdict]
      return { status, stdout: `${JSON.stringify(sessionReport(line.scenario, line, verdict))}\n` }
    }

    test('an uneven round-robin split makes the row invalid; a missed performance target does not', () => {
      const uneven = (arms) => {
        for (const label of ['round-robin', 'round-robin~AA']) {
          arms[label].balance = { median: 2, min: 2, max: 2, n: 8 }
        }
      }
      const unbalancedSticky = (arms) => {
        arms.sticky.balance = { median: 1.5, min: 1.4, max: 1.6, n: 8 }
      }
      const answers = {
        uneven: rejudged('healthy-passed', uneven),
        'unbalanced-sticky': rejudged('healthy-passed', unbalancedSticky),
      }
      expect(lastJsonLine(answers.uneven.stdout)).toMatchObject({
        verdict: 'failed',
        reasons: ['round robin did not split the records evenly'],
      })
      expect(lastJsonLine(answers['unbalanced-sticky'].stdout)).toMatchObject({
        verdict: 'failed',
        reasons: ['sticky balance above 1.25'],
      })

      const { json: result } = report(
        [sessionEntry('healthy', 'uneven'), sessionEntry('healthy', 'unbalanced-sticky')],
        (name) => answers[name]
      )

      expect(result.rows.filter((row) => row.benchmark === 'uneven')).toEqual([
        {
          benchmark: 'uneven',
          arm: '-',
          metric: '-',
          unit: '-',
          runs: 1,
          status: 'invalid',
          reason:
            'failed controls: round-robin records split evenly; round-robin~AA records split evenly',
        },
      ])
      const sticky = result.rows.filter((row) => row.benchmark === 'unbalanced-sticky')
      expect(sticky).toHaveLength(8)
      expect(sticky.every((row) => row.status === 'valid' && Number.isFinite(row.median))).toBe(
        true
      )
      const detail = result.details.find(({ benchmark }) => benchmark === 'unbalanced-sticky')
      expect(detail.controls.every(({ passed }) => passed)).toBe(true)
      expect(detail.notes).toEqual([
        expect.stringMatching(/^session verdict failed: sticky balance above 1\.25\b/),
      ])
    })

    /**
     * The recorded slow-broker observations as a drain to settlement would end them, each judged
     * again by the real `validRun` and the whole summarized and judged by the real session code.
     * Test input, not run evidence: every rejection is taken as a buffer refusal, the sends still
     * open at 30 s as delivered by 52 s, and the answer counts the recorded run did not keep are
     * made up.
     * @returns {object[]} the settled observations
     */
    const settledObservations = () => {
      const { validRun, accountedRun } = require('../stickyBalance')
      return fs
        .readFileSync(path.join(SESSION_DIR, 'slow-broker-observations.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
        .map((observation) => {
          const { delivered, rejected, unsettled, drainAtMs } = observation.result
          const result = {
            ...observation.result,
            refused: rejected,
            delivered: delivered + unsettled,
            unsettled: 0,
            drainAtMs: unsettled > 0 ? 52000 : drainAtMs,
            slowAnswerCount: 40,
            otherAnswerCount: 1200,
          }
          result.valid = validRun('slow-broker', result) ? 1 : 0
          result.accounted = accountedRun(result) ? 1 : 0
          return { ...observation, result }
        })
    }
    /**
     * @param {object[]} observations
     * @returns {{ status: number, stdout: string, stderr: string }} the session line the real
     *   summary and judge give them, as `stickyBalance.js --session` prints it
     */
    const sessionOf = (observations) => {
      const { judge, sessionReport, METRICS } = require('../stickyBalance')
      const { summarize } = require('../session')
      const summary = summarize(observations, {
        metrics: METRICS,
        reference: 'sticky',
        controlOn: ['p99Ms'],
        voidOver: 10,
      })
      const verdict = judge('slow-broker', summary)
      return {
        status: { passed: 0, failed: 1, inconclusive: 3 }[verdict.verdict],
        stdout: `${JSON.stringify(sessionReport('slow-broker', summary, verdict))}\n`,
        stderr: '',
      }
    }

    test('the settled slow-broker session keeps its delivery accounting', () => {
      const observations = settledObservations()
      for (const { result } of observations) {
        expect(result.valid).toBe(1)
        expect(result.delivered + result.refused).toBe(result.offered)
        expect(result.partitionerCalls).toBe(result.offered - result.refused)
      }

      const { arms } = lastJsonLine(sessionOf(observations).stdout)

      for (const label of ['sticky', 'adaptive', 'sticky~AA']) {
        const { delivered, refused, partitionerCalls } = arms[label]
        expect([delivered.min, delivered.median, delivered.max]).toEqual([
          120000 - refused.max,
          120000 - refused.median,
          120000 - refused.min,
        ])
        expect(partitionerCalls).toEqual(delivered)
      }
      expect(arms.sticky.delivered.min).toBe(79061)
    })

    test('the slow-broker row is valid when refusals account for the missing partitioner calls', () => {
      const answer = sessionOf(settledObservations())
      const { sticky } = lastJsonLine(answer.stdout).arms
      expect(sticky.partitionerCalls.max).toBeLessThan(sticky.records.min)

      const { json: result } = report([sessionEntry('slow-broker', 'settled')], () => answer)

      const statuses = new Set(result.rows.map(({ status }) => status))
      expect(statuses).toEqual(new Set(['valid']))
      const { controls } = result.details[0]
      expect(controls.every(({ passed }) => passed)).toBe(true)
      expect(
        controls.find(
          ({ arm, name }) => arm === 'sticky' && name === 'partitioner calls = admitted records'
        ).detail
      ).toBe(
        `calls ${sticky.partitionerCalls.min} to ${sticky.partitionerCalls.max}, ` +
          `records ${sticky.records.min}, refused ${sticky.refused.min} to ${sticky.refused.max}`
      )
    })

    test('the slow-broker row is invalid when partitioner calls differ from the records less the refusals', () => {
      const line = lastJsonLine(sessionOf(settledObservations()).stdout)
      line.arms.sticky.partitionerCalls.max += 1

      const { json: result } = report([sessionEntry('slow-broker', 'off-by-one')], () => ({
        status: { passed: 0, failed: 1, inconclusive: 3 }[line.verdict],
        stdout: `${JSON.stringify(line)}\n`,
      }))

      expect(result.rows).toEqual([
        expect.objectContaining({
          status: 'invalid',
          reason: 'failed controls: sticky partitioner calls = admitted records',
        }),
      ])
    })

    test('a sticky observation that drifts past the offer window fails only every observation valid, never the accounting control', () => {
      const { validRun, accountedRun } = require('../stickyBalance')
      const observations = fs
        .readFileSync(path.join(SESSION_DIR, 'slow-broker-drift.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
        .map((observation) => {
          const result = { ...observation.result }
          result.valid = validRun('slow-broker', result) ? 1 : 0
          result.accounted = accountedRun(result) ? 1 : 0
          return { ...observation, result }
        })
      const invalid = observations.filter(({ result }) => result.valid !== 1)
      expect(invalid.map(({ round, label, warmup }) => [round, label, warmup])).toEqual([
        [1, 'sticky', false],
      ])
      const [{ result: drifted }] = invalid
      expect(drifted.maxTickDriftMs).toBeCloseTo(56.23, 2)
      expect(validRun('slow-broker', { ...drifted, maxTickDriftMs: 50 })).toBe(true)
      for (const { result } of observations) {
        expect(result.accounted).toBe(1)
        expect(result.partitionerCalls + result.refused).toBe(120000)
      }

      const { json: result, markdown } = report([sessionEntry('slow-broker', 'drift')], () =>
        sessionOf(observations)
      )

      expect(result.rows).toEqual([
        expect.objectContaining({
          status: 'invalid',
          reason: 'failed controls: sticky every observation valid',
        }),
      ])
      const accounting = result.details[0].controls.find(
        ({ arm, name }) => arm === 'sticky' && name === 'partitioner calls = admitted records'
      )
      expect(accounting).toEqual({
        arm: 'sticky',
        name: 'partitioner calls = admitted records',
        passed: true,
        detail: 'calls 75530 to 88419, records 120000, refused 31581 to 44470',
      })
      expect(markdown).toContain(
        '- control sticky partitioner calls = admitted records: passed ' +
          '(calls 75530 to 88419, records 120000, refused 31581 to 44470)'
      )
      expect(markdown).toContain('- control sticky every observation valid: FAILED')
    })

    test('the accounting control fails for one run whose admitted record was never partitioned, inside the range', () => {
      const { validRun, accountedRun } = require('../stickyBalance')
      const observations = settledObservations()
      const measured = observations.filter(({ label, warmup }) => label === 'sticky~AA' && !warmup)
      const calls = measured.map(({ result }) => result.partitionerCalls)
      const inner = measured.find(
        ({ result }) =>
          result.partitionerCalls - 1 > Math.min(...calls) &&
          result.partitionerCalls < Math.max(...calls)
      ).result
      inner.partitionerCalls -= 1
      inner.valid = validRun('slow-broker', inner) ? 1 : 0
      inner.accounted = accountedRun(inner) ? 1 : 0
      expect([inner.valid, inner.accounted]).toEqual([0, 0])

      const answer = sessionOf(observations)
      const aa = lastJsonLine(answer.stdout).arms['sticky~AA']
      expect([aa.partitionerCalls.min, aa.partitionerCalls.max]).toEqual([
        120000 - aa.refused.max,
        120000 - aa.refused.min,
      ])
      const { json: result } = report([sessionEntry('slow-broker', 'missed')], () => answer)

      expect(result.rows).toEqual([
        expect.objectContaining({
          status: 'invalid',
          reason:
            'failed controls: sticky~AA partitioner calls = admitted records; ' +
            'sticky~AA every observation valid',
        }),
      ])
    })

    test('a valid slow-broker session of 12 runs with varying refusals passes every control', () => {
      const settled = settledObservations().filter(({ warmup }) => !warmup)
      const base = (label) => settled.find((observation) => observation.label === label).result
      let call = 0
      const spawn = jest
        .spyOn(require('child_process'), 'spawnSync')
        .mockImplementation((_executable, args) => {
          const arm = args[args.indexOf('--arm') + 1]
          call += 1
          const refused = arm === 'adaptive' ? 0 : 1000 + 10 * (call % 12)
          const result = {
            ...base(arm),
            rejected: refused,
            refused,
            delivered: 120000 - refused,
            partitionerCalls: 120000 - refused,
          }
          return { status: 0, stdout: `${JSON.stringify(result)}\n`, stderr: '' }
        })
      let answer
      try {
        jest.isolateModules(() => {
          const driver = require('../stickyBalance')
          const options = driver.parseArgs([
            '--session',
            '--scenario',
            'slow-broker',
            '--raw',
            path.join(out, 'twelve.jsonl'),
            '--runs',
            '12',
          ])
          const { summary, verdict } = driver.compare(options)
          answer = {
            status: { passed: 0, failed: 1, inconclusive: 3 }[verdict.verdict],
            stdout: `${JSON.stringify(driver.sessionReport('slow-broker', summary, verdict))}\n`,
          }
        })
      } finally {
        spawn.mockRestore()
      }
      const { sticky } = lastJsonLine(answer.stdout).arms
      expect(sticky.valid.min).toBe(1)
      expect(sticky.refused.min).toBeLessThan(sticky.refused.max)
      expect(sticky.partitionerCalls.median).not.toBe(120000 - sticky.refused.median)
      const entry = sessionEntry('slow-broker', 'twelve')
      entry.command[entry.command.indexOf('--runs') + 1] = '12'

      const { json: result } = report([entry], () => answer)

      expect(new Set(result.rows.map(({ status }) => status))).toEqual(new Set(['valid']))
      expect(result.details[0].controls.filter(({ passed }) => !passed)).toEqual([])
    })

    test("every adaptive comparison row shows each arm's delivered, rejected, refused and unsettled sends", () => {
      const recorded = lastJsonLine(fixture('slow-broker-recorded').stdout)
      for (const stats of Object.values(recorded.arms)) stats.refused = { ...stats.rejected }
      const settled = sessionOf(settledObservations())
      const answers = {
        invalid: { ...fixture('slow-broker-recorded'), stdout: `${JSON.stringify(recorded)}\n` },
        valid: settled,
        inconclusive: fixture('healthy-void'),
      }

      const { json: result, markdown } = report(
        [
          sessionEntry('slow-broker', 'invalid'),
          sessionEntry('slow-broker', 'valid'),
          sessionEntry('healthy', 'inconclusive'),
        ],
        (name) => answers[name]
      )

      const range = ({ median, min, max }) => `${median} (${min} to ${max})`
      for (const name of Object.keys(answers)) {
        const line = lastJsonLine(answers[name].stdout)
        const detail = result.details.find(({ benchmark }) => benchmark === name)
        expect(detail.status).toBe(name)
        expect(detail.table.header).toEqual([
          'arm',
          'delivered',
          'rejected (all)',
          'refused by the buffer (part of rejected)',
          'unsettled',
        ])
        expect(detail.table.rows).toEqual(
          Object.keys(line.arms).map((label) => [
            label,
            ...['delivered', 'rejected', 'refused', 'unsettled'].map((key) =>
              range(line.arms[label][key])
            ),
          ])
        )
        for (const row of detail.table.rows) expect(markdown).toContain(`| ${row.join(' | ')} |`)
      }
      const { sticky } = recorded.arms
      expect(result.details.find(({ benchmark }) => benchmark === 'invalid').table.rows[0]).toEqual(
        [
          'sticky',
          range(sticky.delivered),
          range(sticky.rejected),
          range(sticky.rejected),
          range(sticky.unsettled),
        ]
      )
      expect(sticky.unsettled.min).toBeGreaterThan(0)
      expect(result.rows.filter(({ benchmark }) => benchmark === 'invalid')).toEqual([
        expect.not.objectContaining({ median: expect.anything() }),
      ])
      expect(markdown).toContain('### invalid: invalid')
    })

    test('a slow-broker A/A spread of 6 % is not void; a healthy one of 4 % is', () => {
      /** Moves the A/A copy's p99 median `spread` percent from its reference, voided as stated. */
      const widened = (spread, isVoid) => (arms, line) => {
        const { label, against } = line.control
        const copy = arms[label].p99Ms
        copy.median = arms[against].p99Ms.median * (1 + spread / 100)
        copy.min = Math.min(copy.min, copy.median)
        copy.max = Math.max(copy.max, copy.median)
        line.control.percent.p99Ms = (copy.median / arms[against].p99Ms.median - 1) * 100
        line.void = isVoid
      }
      const answers = {
        'slow-six': rejudged('slow-broker-failed', widened(6, false)),
        'healthy-four': rejudged('healthy-passed', widened(4, true)),
      }

      const { json: result } = report(
        [sessionEntry('slow-broker', 'slow-six'), sessionEntry('healthy', 'healthy-four')],
        (name) => answers[name]
      )

      const statusOf = (name) => [
        ...new Set(result.rows.filter((row) => row.benchmark === name).map((row) => row.status)),
      ]
      const spreadOf = (name) =>
        result.details
          .find(({ benchmark }) => benchmark === name)
          .controls.find(({ name: control }) => control === 'A/A spread')
      expect(statusOf('slow-six')).toEqual(['valid'])
      expect(spreadOf('slow-six')).toMatchObject({
        passed: true,
        detail: 'p99Ms +6.00 % from sticky',
      })
      expect(statusOf('healthy-four')).toEqual(['inconclusive'])
      expect(spreadOf('healthy-four')).toMatchObject({
        passed: false,
        detail: 'p99Ms +4.00 % from round-robin',
      })
    })

    test('the slow-broker row shows refused per arm', () => {
      const entry = slowRow()
      const recorded = lastJsonLine(fixture('slow-broker-recorded').stdout)

      const { json: result, markdown } = report([entry], () => sessionOf(settledObservations()))

      const cell = (arm, metric) =>
        result.rows.find((row) => row.arm === arm && row.metric === metric)
      for (const arm of ['sticky', 'adaptive', 'sticky~AA']) {
        const { rejected } = recorded.arms[arm]
        expect(cell(arm, 'refused')).toMatchObject({
          status: 'valid',
          unit: 'records',
          median: rejected.median,
          min: rejected.min,
          max: rejected.max,
        })
        expect(cell(arm, 'delivered')).toMatchObject({
          median: 120000 - rejected.median,
          min: 120000 - rejected.max,
          max: 120000 - rejected.min,
        })
        expect(cell(arm, 'unsettled')).toMatchObject({ median: 0, max: 0 })
      }
      expect(cell('sticky', 'refused').min).toBeGreaterThan(0)
      expect(cell('adaptive', 'refused').max).toBe(0)
      expect(markdown).toMatch(/\| stickyBalance-slow-broker \| sticky \| refused \| valid \| /)
    })

    test('session evidence that is incomplete or contradicts itself is an error row, never valid', () => {
      const line = (name, change) => {
        const recorded = fixture(name)
        const parsed = lastJsonLine(recorded.stdout)
        change(parsed)
        return { ...recorded, stdout: `${JSON.stringify(parsed)}\n` }
      }
      const everyStat = (session, change) => {
        for (const stats of Object.values(session.arms))
          for (const stat of Object.values(stats)) change(stat)
      }
      const widen = (session) => {
        const aa = session.arms['round-robin~AA'].p99Ms
        const reference = session.arms['round-robin'].p99Ms.median
        Object.assign(aa, { median: reference * 1.42, max: reference * 1.5 })
        session.control.percent.p99Ms = 42
        delete session.void
      }
      const cases = {
        'no-runs': [
          (s) => everyStat(s, (stat) => (stat.n = 0)),
          /round-robin recordsPerRequest has 0 runs, expected 8/,
        ],
        'missing-runs': [
          (s) => everyStat(s, (stat) => delete stat.n),
          /has undefined runs, expected 8/,
        ],
        'wrong-runs': [(s) => everyStat(s, (stat) => (stat.n = 7)), /has 7 runs, expected 8/],
        'uneven-arms': [
          (s) => (s.arms.adaptive.p99Ms.n = 7),
          /adaptive p99Ms has 7 runs, expected 8/,
        ],
        'no-void': [(s) => delete s.void, /does not say whether its A\/A control is void/],
        'text-void': [(s) => (s.void = 'false'), /does not say whether its A\/A control is void/],
        'void-dropped': [widen, /does not say whether its A\/A control is void/],
        'void-contradicted': [
          (s) => {
            widen(s)
            s.void = false
          },
          /void false disagrees with an A\/A spread of \+42\.00 %/,
        ],
        'spread-mismatch': [
          (s) => (s.control.percent.p99Ms = 0.1),
          /A\/A spread \+0\.10 % disagrees with the medians/,
        ],
        'wrong-scenario': [
          (s) => (s.scenario = 'slow-broker'),
          /reports scenario slow-broker, expected healthy/,
        ],
        'no-records': [
          (s) =>
            Object.values(s.arms).forEach((stats) => {
              for (const key of ['records', 'partitionerCalls']) {
                Object.assign(stats[key], { median: 0, min: 0, max: 0 })
              }
            }),
          /round-robin records 0 to 0, expected 100000/,
        ],
        unordered: [(s) => (s.arms.sticky.p99Ms.min = 1e6), /sticky has no ordered finite p99Ms/],
      }
      const entries = Object.keys(cases).map((name) => sessionEntry('healthy', name))
      const answers = Object.fromEntries(
        Object.entries(cases).map(([name, [change]]) => [name, line('healthy-passed', change)])
      )
      answers.recorded = fixture('healthy-passed')
      answers.void = fixture('healthy-void')

      const { json: result } = report(
        [...entries, sessionEntry('healthy', 'recorded'), sessionEntry('healthy', 'void')],
        (name) => answers[name]
      )

      const rowsOf = (name) => result.rows.filter((row) => row.benchmark === name)
      for (const [name, [, reason]] of Object.entries(cases)) {
        expect([name, rowsOf(name)]).toEqual([
          name,
          [
            expect.objectContaining({
              arm: '-',
              status: 'error',
              reason: expect.stringMatching(reason),
            }),
          ],
        ])
      }
      expect(new Set(rowsOf('recorded').map(({ status, runs }) => `${status} ${runs}`))).toEqual(
        new Set(['valid 8'])
      )
      expect(new Set(rowsOf('void').map(({ status }) => status))).toEqual(new Set(['inconclusive']))
    })

    test('a removed option reports omitted with its decision and evidence run', () => {
      const removed = omitted({
        reason: 'adaptive removed',
        decision: 'the partitioner ruling',
        evidenceRun: '43427840043',
      })
      const incomplete = {
        ...omitted({ reason: 'adaptive removed', decision: 'the partitioner ruling' }),
        name: 'no-evidence',
      }

      const {
        calls,
        json: result,
        markdown,
      } = report([removed, incomplete], () => {
        throw new Error('an omitted row never runs')
      })

      expect(calls).toEqual([])
      expect(result.rows).toEqual([
        {
          benchmark: 'removed-option',
          arm: '-',
          metric: '-',
          unit: '-',
          runs: 0,
          status: 'omitted',
          reason: 'adaptive removed under the partitioner ruling, evidence run 43427840043',
        },
        expect.objectContaining({
          benchmark: 'no-evidence',
          status: 'error',
          reason: expect.stringContaining('evidenceRun'),
        }),
      ])
      expect(markdown).toContain('| removed-option | - | - | omitted | - | - | - | 0 | - |')
      expect(markdown).toContain(
        '- removed-option: omitted: adaptive removed under the partitioner ruling, evidence run 43427840043'
      )
      expect(fs.readdirSync(raw('removed-option'))).toEqual(['entry.json'])
    })
  })
})
