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
const KNOWN_PLACEHOLDERS = ['{client}', '{package}', '{baselinePackageDir}']
const FORK = { package: '@countly/kafkajs', version: '3.0.0-test' }
const BASE = { package: 'kafkajs', version: '2.2.4' }
const forkDir = path.join(os.tmpdir(), 'bench-fork')
const baselineDir = path.join(os.tmpdir(), 'bench-base')

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
const options = (spawn, runs = 3) => ({ runs, forkDir, baselineDir, spawn })
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

      const rows = runEntry(single(), options(spawn))

      expect(calls).toHaveLength(3)
      expect(rows).toEqual([
        {
          benchmark: 'single',
          arm: 'fork',
          metric: 'v',
          unit: 'ms',
          runs: 3,
          median: 3,
          min: 1,
          max: 5,
        },
      ])
      expect(rows[0]).not.toHaveProperty('ratio')
      expect(rows[0]).not.toHaveProperty('error')
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
      const crash = runEntry(
        single(),
        options(() => ({ error: new Error('spawn ENOENT'), status: null, stdout: '', stderr: '' }))
      )
      const exit = runEntry(
        single(),
        options(() => ({ status: 1, stdout: '', stderr: 'line1\nboom\n' }))
      )
      const garbage = runEntry(
        single(),
        options(() => ({ status: 0, stdout: '{"v": NaN}\n', stderr: '' }))
      )
      const killed = runEntry(
        single(),
        options(() => ({ status: null, signal: 'SIGTERM', stdout: '', stderr: '' }))
      )
      // Only the fork arm crashes; the baseline arm and the second metric keep their rows.
      const partial = runEntry(
        compared({
          metrics: [
            { key: 'a', unit: 'msg/s' },
            { key: 'b', unit: 'ms' },
          ],
        }),
        options((args) =>
          args.includes('@countly/kafkajs')
            ? { status: 2, stdout: '', stderr: 'down' }
            : json({ ...BASE, a: 4, b: 6 })
        )
      )

      expect(crash).toEqual([
        expect.objectContaining({
          metric: 'v',
          runs: 0,
          error: expect.stringContaining('spawn ENOENT'),
        }),
      ])
      expect(exit[0].error).toBe('run 1 of 3: exit 1: line1 boom')
      expect(garbage[0].error).toMatch(/JSON object/)
      expect(killed[0].error).toMatch(/SIGTERM/)
      expect(partial.map((row) => [row.arm, row.metric, row.error === undefined])).toEqual([
        ['fork', 'a', false],
        ['fork', 'b', false],
        ['kafkajs@2.2.4', 'a', true],
        ['kafkajs@2.2.4', 'b', true],
      ])
      expect(partial[2]).toMatchObject({ median: 4, runs: 3 })
    })

    test('stops an arm at its first failed run', () => {
      const { spawn, calls } = stub((args, n) =>
        n === 2 ? { status: 1, stdout: '', stderr: 'x' } : json({ v: 1 })
      )

      const rows = runEntry(single(), options(spawn))

      expect(calls).toHaveLength(2)
      expect(rows).toEqual([
        expect.objectContaining({ runs: 1, error: expect.stringContaining('run 2 of 3') }),
      ])
    })

    test('reports a missing, null, NaN or non-numeric metric as an error row', () => {
      const keys = ['good', 'missing', 'nothing', 'text', 'huge', 'nested']
      const entry = single({ metrics: keys.map((key) => ({ key, unit: 'ms' })) })
      const stdout = '{"good":4,"nothing":null,"text":"12","huge":1e999,"nested":{"a":1}}\n'

      const rows = runEntry(
        entry,
        options(() => ({ status: 0, stdout, stderr: '' }), 1)
      )

      expect(rows.map((row) => row.metric)).toEqual(keys)
      expect(rows[0]).toMatchObject({ median: 4 })
      for (const row of rows.slice(1)) {
        expect(row.error).toContain(row.metric)
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

      const rows = runEntry(entry, options(spawn, 1))

      const byKey = Object.fromEntries(rows.map((row) => [`${row.arm}/${row.metric}`, row]))
      expect(byKey['fork/a']).not.toHaveProperty('ratio')
      expect(byKey['fork/b'].ratio).toBe(2)
      expect(byKey['kafkajs@2.2.4/a']).not.toHaveProperty('ratio')
      expect(byKey['kafkajs@2.2.4/b']).not.toHaveProperty('ratio')
      expect(Object.values(byKey).every((row) => row.error === undefined)).toBe(true)
    })

    test('rejects a compared row whose loaded package does not match its arm', () => {
      const run = (forkIdentity, baseIdentity) =>
        runEntry(
          compared(),
          options(
            (args) =>
              json({ ...(args.includes('@countly/kafkajs') ? forkIdentity : baseIdentity), a: 1 }),
            1
          )
        )
      const errorOf = (rows, arm) => rows.find((row) => row.arm === arm).error

      const ok = run(FORK, BASE)
      const forkLoadedBaseline = run(BASE, BASE)
      const baselineLoadedFork = run(FORK, FORK)
      const baselineWrongVersion = run(FORK, { package: 'kafkajs', version: '2.3.0' })
      const noIdentity = run({}, BASE)

      expect(ok.every((row) => row.error === undefined)).toBe(true)
      expect(errorOf(forkLoadedBaseline, 'fork')).toMatch(/@countly\/kafkajs/)
      expect(errorOf(forkLoadedBaseline, 'kafkajs@2.2.4')).toBeUndefined()
      expect(errorOf(baselineLoadedFork, 'kafkajs@2.2.4')).toMatch(/kafkajs/)
      expect(errorOf(baselineWrongVersion, 'kafkajs@2.2.4')).toMatch(/2\.2\.4/)
      expect(errorOf(noIdentity, 'fork')).toMatch(/@countly\/kafkajs/)
    })

    test('spawns murmur2 with the installed kafkajs 2.2.4 package directory as --baseline-tree', () => {
      const entry = manifest.entries.find((candidate) => candidate.name === 'murmur2')
      const { spawn, calls } = stub(() => json(outputFor(entry)))

      const rows = runEntry(entry, options(spawn, 1))

      const tree = valueAfter(calls[0].args, '--baseline-tree')
      expect(tree).toBe(path.join(baselineDir, 'node_modules', 'kafkajs'))
      expect(tree).not.toBe(baselineDir)
      expect(tree).not.toContain('@countly')
      expect(entry.arms).toEqual(['fork'])
      expect(rows.every((row) => row.error === undefined)).toBe(true)
    })

    test('runs stickyBalance as a fork-only single arm with garbage collection enabled', () => {
      const entry = manifest.entries.find((candidate) => candidate.name === 'stickyBalance')
      expect(entry).toBeDefined()
      const { spawn, calls } = stub(() =>
        json({ recordsPerRequest: 32, balance: 0.9, p99Ms: 12, heapRetainedBytes: 256 })
      )

      const rows = runEntry(entry, options(spawn, 1))

      expect(entry.arms).toEqual(['fork'])
      expect(entry.broker).toBe(true)
      expect(calls).toHaveLength(1)
      expect(calls[0].args).toEqual(['--expose-gc', 'scripts/benchmarks/stickyBalance.js'])
      expect(rows.map(({ metric, median, unit }) => ({ metric, median, unit }))).toEqual([
        { metric: 'recordsPerRequest', median: 32, unit: 'records' },
        { metric: 'balance', median: 0.9, unit: 'ratio' },
        { metric: 'p99Ms', median: 12, unit: 'ms' },
        { metric: 'heapRetainedBytes', median: 256, unit: 'bytes' },
      ])
      expect(rows.every((row) => row.error === undefined)).toBe(true)
      expect(manifest.excluded.map(({ file }) => file)).not.toContain('stickyBalance.js')
    })

    test('reads dotted metric paths', () => {
      const entry = single({ metrics: [{ key: 'sizes.16.nsPerOp', unit: 'ns' }] })
      const { spawn } = stub(() => json({ sizes: { 16: { nsPerOp: 21 } } }))

      expect(runEntry(entry, options(spawn, 1))[0]).toMatchObject({
        metric: 'sizes.16.nsPerOp',
        median: 21,
      })
    })

    test('replaces {client} and {package} per arm in the spawned arguments', () => {
      const entry = manifest.entries.find((candidate) => candidate.arms.length === 2)
      const { spawn, calls } = stub((args) => json({ ...identityOf(args), ...outputFor(entry) }))

      const rows = runEntry(entry, options(spawn, 1))

      const [fork, base] = calls.map((call) => call.args)
      expect(valueAfter(fork, '--client')).toBe(forkDir)
      expect(valueAfter(fork, '--package')).toBe('@countly/kafkajs')
      expect(valueAfter(base, '--client')).toBe(baselineDir)
      expect(valueAfter(base, '--package')).toBe('kafkajs')
      expect(rows.every((row) => row.error === undefined)).toBe(true)
    })

    test('leaves no placeholder in the spawned arguments of any manifest entry', () => {
      for (const entry of manifest.entries) {
        const { spawn, calls } = stub((args) => json({ ...identityOf(args), ...outputFor(entry) }))

        runEntry(entry, options(spawn, 1))

        expect(calls).toHaveLength(entry.arms.length)
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
        error: 'run 2 of 3: exit 1: a | b\nc',
      },
    ]

    test('renders a Markdown table with benchmark, arm, metric, median, range, unit, runs and ratio', () => {
      const lines = renderMarkdown(rows, { runs: 3, kafka: '4.4.0' }).split('\n')

      expect(lines).toContain('| benchmark | arm | metric | median | range | unit | runs | ratio |')
      expect(lines).toContain('| --- | --- | --- | --- | --- | --- | --- | --- |')
      expect(lines).toContain(
        '| frameDecode-64b | fork | nsPerFrame | 1234.57 | 1200 to 1300 | ns | 3 | - |'
      )
      expect(lines).toContain(
        '| produce-latency | fork | messagesPerSecond | 2000 | 1900 to 2100 | msg/s | 3 | 1.25 |'
      )
      expect(lines).toContain(
        '| produce-latency | kafkajs@2.2.4 | messagesPerSecond | 1600 | 1500 to 1700 | msg/s | 3 | - |'
      )
    })

    test('lists the run metadata and every error row with its reason on one line', () => {
      const markdown = renderMarkdown(rows, { runs: 3, kafka: '4.4.0' })

      expect(markdown).toMatch(/^# Benchmark report/)
      expect(markdown).toContain('- runs: 3')
      expect(markdown).toContain('- kafka: 4.4.0')
      expect(markdown).toContain('| broken | fork | v | error | - | ms | 1 | - |')
      expect(markdown).toContain('- broken / fork / v: run 2 of 3: exit 1: a \\| b c')
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
      expect(report.rows.every((row) => row.error.includes('boom'))).toBe(true)
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
  })
})
