const fs = require('fs')
const os = require('os')
const path = require('path')
const { once } = require('events')
const { spawn } = require('child_process')
const {
  rotate,
  median,
  runSession,
  summarize,
  format,
  exitCode,
  commandArm,
  main,
} = require('../session')

/** An observation of `label` in a measured round, or in the warm-up round when `round` is 0. */
const observed = (label, round, result, position = 0) => ({
  round,
  position,
  label,
  warmup: round === 0,
  result,
})

/**
 * The observations of a session whose labels rotate through the positions as its arms do: per
 * label a warm-up that must not count, then one measured run per entry (a number for `key`, or a
 * whole result).
 */
const sessionOf = (byLabel, key = 'v') => {
  const labels = Object.keys(byLabel)
  return labels.flatMap((label, slot) => [
    observed(label, 0, { [key]: 1e9 }, slot),
    ...byLabel[label].map((entry, i) =>
      observed(
        label,
        i + 1,
        typeof entry === 'object' ? entry : { [key]: entry },
        (slot + i + 1) % labels.length
      )
    ),
  ])
}

describe('Benchmarks > session', () => {
  describe('rotate', () => {
    test('moves every arm one place each round, so every arm takes every position', () => {
      const arms = ['a', 'b', 'c', 'd']
      const positions = new Map(arms.map((label) => [label, new Set()]))

      for (let round = 0; round < arms.length; round += 1) {
        rotate(arms, round).forEach((label, position) => positions.get(label).add(position))
      }

      expect(rotate(arms, 0)).toEqual(['a', 'b', 'c', 'd'])
      expect(rotate(arms, 1)).toEqual(['b', 'c', 'd', 'a'])
      positions.forEach((seen) => expect([...seen].sort()).toEqual([0, 1, 2, 3]))
    })
  })

  test('the median is the middle value, or the upper middle of an even count', () => {
    expect(median([3, 1, 2])).toBe(2)
    expect(median([4, 1, 3, 2])).toBe(3)
  })

  describe('runSession', () => {
    const arms = () => ['a', 'b'].map((label) => ({ label, run: jest.fn(() => ({ v: 1 })) }))

    test('runs a discarded warm-up and `runs` measured runs of every arm, and a control of the reference', () => {
      const [a, b] = arms()
      const seen = []

      const observations = runSession({
        arms: [a, b],
        runs: 3,
        reference: 'a',
        onObservation: (observation) => seen.push(observation),
      })

      expect(observations).toHaveLength(12)
      expect(seen).toEqual(observations)
      expect(observations.filter(({ warmup }) => warmup).map(({ round }) => round)).toEqual([
        0, 0, 0,
      ])
      for (const label of ['a', 'b', 'a~AA']) {
        expect(observations.filter((o) => o.label === label && !o.warmup)).toHaveLength(3)
      }
      expect(a.run).toHaveBeenCalledTimes(8)
      expect(b.run).toHaveBeenCalledTimes(4)
    })

    test('puts every arm, the control included, in every position over the measured runs', () => {
      const observations = runSession({ arms: arms(), runs: 3, reference: 'a' })

      for (const label of ['a', 'b', 'a~AA']) {
        const positions = observations
          .filter((o) => o.label === label && !o.warmup)
          .map((o) => o.position)
        expect(positions.sort()).toEqual([0, 1, 2])
      }
    })

    test('adds no control without a reference', () => {
      const observations = runSession({ arms: arms(), runs: 2 })

      expect(new Set(observations.map(({ label }) => label))).toEqual(new Set(['a', 'b']))
    })

    test.each([
      [{ runs: 2, reference: 'a' }, '2 runs cannot put each of 3 arms in every position equally'],
      [{ runs: 4, reference: 'a' }, '4 runs cannot put each of 3 arms in every position equally'],
      [{ runs: 7, reference: 'a' }, '7 runs cannot put each of 3 arms in every position equally'],
      [{ runs: 3 }, '3 runs cannot put each of 2 arms in every position equally'],
      [{ runs: 0 }, '0 runs cannot put each of 2 arms in every position equally'],
      [{ runs: 3, reference: 'c' }, 'no arm is named c'],
    ])('%j is refused before any run', (options, message) => {
      const [a, b] = arms()

      expect(() => runSession({ arms: [a, b], ...options })).toThrow(message)
      expect(a.run).not.toHaveBeenCalled()
      expect(b.run).not.toHaveBeenCalled()
    })

    test('identical arms with a cost that depends on the position show no difference over a balanced session', () => {
      let calls = 0
      const run = () => ({ v: calls++ % 3 === 0 ? 104 : 100 })
      const identical = ['a', 'b'].map((label) => ({ label, run }))

      const observations = runSession({ arms: identical, reference: 'a', runs: 6 })
      const summary = summarize(observations, {
        metrics: ['v'],
        reference: 'a',
        pairs: [['b', 'a', '3']],
      })

      expect(summary.comparisons[0].percent.v).toBe(0)
      expect(summary.control.percent.v).toBe(0)
      expect(exitCode(summary)).toBe(0)
    })
  })

  describe('summarize', () => {
    const options = { metrics: ['v'], reference: 'a', pairs: [['b', 'a']] }
    const session = (control) =>
      sessionOf({ a: [100, 100, 100], b: [103, 104, 102], 'a~AA': control })

    test('reduces the measured runs to a median, a minimum and a maximum, and drops the warm-up', () => {
      const { arms } = summarize(session([100, 100, 100]), options)

      expect(arms.b.v).toEqual({ median: 103, min: 102, max: 104, n: 3 })
    })

    test('gives the percent difference of every pair', () => {
      const { comparisons } = summarize(session([100, 100, 100]), options)

      expect(comparisons).toHaveLength(1)
      expect(comparisons[0]).toEqual(expect.objectContaining({ label: 'b', against: 'a' }))
      expect(comparisons[0].percent.v).toBeCloseTo(3, 9)
    })

    test.each([
      [102.9, '3', false],
      [103, '3', false],
      [103.04, '3', true],
      [96.96, '3', true],
      [97, '3', false],
      [110, '+10', false],
      [110.04, '+10', true],
      [60, '+10', false],
      [89.96, '10', true],
      [500, undefined, null],
    ])('a pair at %s with the limit %s is over its limit: %s', (median, limit, expected) => {
      const observations = sessionOf({
        a: [100, 100, 100, 100],
        b: [median, median, median, median],
      })

      const summary = summarize(observations, { metrics: ['v'], pairs: [['b', 'a', limit]] })

      expect(summary.comparisons[0].over).toBe(expected)
      expect(summary.over).toBe(expected === true)
    })

    test('a pair limit judges only the metrics the control rule is limited to', () => {
      const observations = sessionOf({
        a: [1, 2, 3, 4].map(() => ({ v: 100, noisy: 1 })),
        b: [1, 2, 3, 4].map(() => ({ v: 100, noisy: 50 })),
      })
      const options = { metrics: ['v', 'noisy'], pairs: [['b', 'a', '3']] }

      expect(summarize(observations, options).over).toBe(true)
      expect(summarize(observations, { ...options, controlOn: ['v'] }).over).toBe(false)
    })

    test.each([
      [[102.9, 102.9, 102.9], false],
      [[103, 103, 103], false],
      [[103.00001, 103.00001, 103.00001], true],
      [[103.1, 103.1, 103.1], true],
      [[96.9, 96.9, 96.9], true],
      [[96.99999, 96.99999, 96.99999], true],
      [[97, 97, 97], false],
      [[97.1, 97.1, 97.1], false],
    ])('a control at %j makes the session void: %s', (control, expected) => {
      expect(summarize(session(control), options).void).toBe(expected)
    })

    test('a control exactly 3 % away is not void, whatever the scale of the metric', () => {
      const voidAt = (reference, control) =>
        summarize(sessionOf({ a: [reference, reference], 'a~AA': [control, control] }), {
          metrics: ['v'],
          reference: 'a',
        }).void

      expect([voidAt(1, 1.03), voidAt(1, 0.97), voidAt(2, 2.06), voidAt(1e6, 1.03e6)]).toEqual([
        false,
        false,
        false,
        false,
      ])
      expect([voidAt(1, 1.0301), voidAt(1, 0.9699)]).toEqual([true, true])
    })

    test('judges only the metrics the control rule is limited to', () => {
      const observations = sessionOf({
        a: [1, 2, 3, 4].map(() => ({ v: 100, noisy: 1 })),
        'a~AA': [1, 2, 3, 4].map(() => ({ v: 100, noisy: 50 })),
      })

      const all = summarize(observations, { metrics: ['v', 'noisy'], reference: 'a' })
      const limited = summarize(observations, {
        metrics: ['v', 'noisy'],
        reference: 'a',
        controlOn: ['v'],
      })

      expect([all.void, limited.void]).toEqual([true, false])
    })

    test('refuses a run without the metric, arms with different run counts and an unknown arm', () => {
      expect(() => summarize([observed('a', 1, { w: 1 })], { metrics: ['v'] })).toThrow(
        'a reported no number for v'
      )
      expect(() => summarize(sessionOf({ a: [1, 2], b: [1] }), { metrics: ['v'] })).toThrow(
        'the arms have different numbers of measured runs'
      )
      expect(() =>
        summarize(sessionOf({ a: [1] }), { metrics: ['v'], pairs: [['z', 'a']] })
      ).toThrow('cannot compare z with a: no such arm')
    })

    test('refuses observations in which an arm did not take every position equally often', () => {
      const fourRounds = [1, 1, 1, 1]
      const unbalanced = sessionOf({ a: fourRounds, b: fourRounds, 'a~AA': fourRounds })
      const stuck = [1, 2].flatMap((round) =>
        ['a', 'b'].map((label) => observed(label, round, { v: 1 }))
      )

      expect(() => summarize(unbalanced, { metrics: ['v'] })).toThrow(
        /^a did not take every position equally often/
      )
      expect(() => summarize(stuck, { metrics: ['v'] })).toThrow(
        /^a did not take every position equally often/
      )
    })

    test('the warm-up round is not counted towards the positions', () => {
      expect(() => summarize(sessionOf({ a: [1, 1], b: [1, 1] }), { metrics: ['v'] })).not.toThrow()
    })

    test.each([
      ['both read 0', { a: [0, 0], 'a~AA': [0, 0] }, { reference: 'a' }],
      ['the control reads more', { a: [0, 0], 'a~AA': [5, 5] }, { reference: 'a' }],
      [
        'the median is 0 though a sample is not',
        { a: [0, 0, 0, 9], 'a~AA': [1, 1, 1, 1] },
        { reference: 'a' },
      ],
      ['a pair reads more', { a: [0, 0], b: [5, 5] }, { pairs: [['b', 'a']] }],
      ['a pair reads 0 too', { a: [0, 0], b: [0, 0] }, { pairs: [['b', 'a', '3']] }],
    ])('refuses a percent of a reference whose median is 0: %s', (_, byLabel, options) => {
      expect(() => summarize(sessionOf(byLabel), { metrics: ['v'], ...options })).toThrow(
        'a has a median of 0 for v: a percent of it is undefined'
      )
    })

    test('a pair is a percent of every metric, so a zero median is refused on a metric the limits do not judge', () => {
      const observations = sessionOf({
        a: [1, 2].map(() => ({ v: 100, errors: 0 })),
        b: [1, 2].map(() => ({ v: 100, errors: 0 })),
      })
      const options = { metrics: ['v', 'errors'], pairs: [['b', 'a', '3']], controlOn: ['v'] }

      expect(() => summarize(observations, options)).toThrow(
        'a has a median of 0 for errors: a percent of it is undefined'
      )
    })

    test('a zero is no obstacle when the reference median is not 0, even for a sample or a control that reads 0', () => {
      const upperMiddle = summarize(sessionOf({ a: [0, 0, 9, 9], b: [9, 9, 9, 9] }), {
        metrics: ['v'],
        pairs: [['b', 'a']],
      })
      const collapsed = summarize(sessionOf({ a: [100, 100], 'a~AA': [0, 0] }), {
        metrics: ['v'],
        reference: 'a',
      })
      const tiny = summarize(sessionOf({ a: [0.001, 0.001], 'a~AA': [0.001, 0.001] }), {
        metrics: ['v'],
        reference: 'a',
      })

      expect(upperMiddle.comparisons[0].percent.v).toBe(0)
      expect(collapsed.control.percent.v).toBe(-100)
      expect([collapsed.void, tiny.void]).toEqual([true, false])
    })
  })

  test('the printed summary names the verdict', () => {
    const options = { metrics: ['v'], reference: 'a' }
    const lines = (control) =>
      format(summarize(sessionOf({ a: [100, 100], 'a~AA': [control, control] }), options), ['v'])

    expect(lines(100).slice(-2)).toEqual(['a~AA vs a: v +0.00 %', 'A/A within 3 %'])
    expect(lines(110).slice(-1)[0]).toMatch(/^SESSION VOID: .* more than 3 %$/)
  })

  test('a percent prints with two decimals, so a pair just past its limit does not read as the limit', () => {
    const lines = (median) =>
      format(
        summarize(sessionOf({ a: [100, 100], b: [median, median] }), {
          metrics: ['v'],
          pairs: [['b', 'a', '3']],
        }),
        ['v']
      )

    expect(lines(103.04)).toContain('b vs a: v +3.04 %')
    expect(lines(110.04)).toContain('b vs a: v +10.04 %')
    expect(lines(96.5)).toContain('b vs a: v -3.50 %')
  })

  test('a void session is exit 3 before a pair over its limit, which is exit 1', () => {
    expect([
      exitCode({ void: false, over: false }),
      exitCode({ void: false, over: true }),
      exitCode({ void: true, over: true }),
    ]).toEqual([0, 1, 3])
  })

  describe('commandArm', () => {
    let dir
    const survivor = (name) => path.join(dir, name)
    const arm = (script, options = {}) =>
      commandArm({
        label: 'probe',
        cwd: dir,
        cpu: true,
        timeout: 5000,
        argv: [process.execPath, '-e', script],
        ...options,
      })

    beforeAll(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), 'session-arm-spec-'))
    })

    afterAll(() => {
      fs.rmSync(dir, { recursive: true, force: true })
    })

    test('a --cpu run that times out is a failure and leaves no process of its command running', async () => {
      const later = (name) =>
        `setTimeout(() => require('fs').writeFileSync(${JSON.stringify(survivor(name))}, ''), 500)`
      const script = `
        require('child_process').spawn(process.execPath, ['-e', ${JSON.stringify(later('grandchild'))}], {
          stdio: 'ignore',
        })
        ${later('child')}
      `

      expect(() => arm(script, { timeout: 200 }).run()).toThrow(/^probe failed/)
      await new Promise((resolve) => setTimeout(resolve, 800))

      expect(fs.existsSync(survivor('child'))).toBe(false)
      expect(fs.existsSync(survivor('grandchild'))).toBe(false)
    })

    test('an interrupt of the runner stops the command of a --cpu run, which has a process group of its own', async () => {
      const started = survivor('started')
      const finished = survivor('finished')
      const script = `
        const fs = require('fs')
        fs.writeFileSync(${JSON.stringify(started)}, '')
        setTimeout(() => fs.writeFileSync(${JSON.stringify(finished)}, ''), 1200)
      `
      const runner = path.join(dir, 'runner.js')
      fs.writeFileSync(
        runner,
        `require(${JSON.stringify(path.join(__dirname, '../session'))})
          .commandArm({ label: 'probe', cwd: ${JSON.stringify(dir)}, cpu: true, timeout: 30000,
            argv: [process.execPath, '-e', ${JSON.stringify(script)}] })
          .run()`
      )
      const child = spawn(process.execPath, [runner], { detached: true, stdio: 'ignore' })
      const exited = once(child, 'exit')
      while (!fs.existsSync(started)) {
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      process.kill(-child.pid, 'SIGINT')
      await exited
      await new Promise((resolve) => setTimeout(resolve, 1500))

      expect(fs.existsSync(finished)).toBe(false)
    })

    test('--cpu counts the CPU time of the processes the command starts', () => {
      const script = `
        const burn = "const t = process.cpuUsage(); while (process.cpuUsage(t).user < 250000);"
        require('child_process').spawnSync(process.execPath, ['-e', burn])
        console.log(JSON.stringify({ done: 1 }))
      `

      expect(arm(script).run().cpuMs).toBeGreaterThanOrEqual(250)
    })

    test('a command that exits non-zero is a failure with its status', () => {
      expect(() => arm('process.exit(3)', { cpu: false }).run()).toThrow(/^probe failed \(3\)/)
      expect(() => arm('process.exit(3)').run()).toThrow(/^probe failed \(3\)/)
    })
  })

  describe('the command line', () => {
    let dir
    let stub
    let log
    let raw
    const io = () => {
      const out = { lines: [], errors: [] }
      return { out, log: (line) => out.lines.push(line), error: (line) => out.errors.push(line) }
    }
    const run = (extra, arms = ['a= 100', 'b= 103'], drift = 0) => {
      const output = io()
      const code = main(
        [
          'run',
          '--command',
          `node ${stub} ${log} ${drift}`,
          '--runs',
          '3',
          '--metrics',
          'value',
          '--raw',
          raw,
          '--reference',
          'a',
          '--pair',
          'b:a',
          ...extra,
          ...arms.map((arm) => arm.replace('= ', `=${dir} `)),
        ],
        output
      )
      return { code, ...output.out }
    }

    beforeAll(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), 'session-spec-'))
      stub = path.join(dir, 'stub.js')
      log = path.join(dir, 'log.txt')
      raw = path.join(dir, 'raw.jsonl')
      fs.writeFileSync(
        stub,
        `const fs = require('fs')
const [log, drift, arm] = process.argv.slice(2)
const seen = fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\\n').length - 1 : 0
fs.appendFileSync(log, arm + '\\n')
if (arm === 'fail' || (arm === 'failThird' && seen === 2)) process.exit(3)
console.log(JSON.stringify({ value: Number(arm) + Number(drift) * seen, other: seen }))
`
      )
    })

    beforeEach(() => {
      fs.rmSync(log, { force: true })
    })

    afterAll(() => {
      fs.rmSync(dir, { recursive: true, force: true })
    })

    test('rotates the arms, keeps every observation with the warm-up first and prints the summary', () => {
      const { code, lines } = run([])

      expect(code).toBe(0)
      expect(fs.readFileSync(log, 'utf8').trim().split('\n').slice(0, 6)).toEqual([
        '100',
        '103',
        '100',
        '103',
        '100',
        '100',
      ])
      const observations = fs
        .readFileSync(raw, 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      expect(observations).toHaveLength(12)
      expect(observations.slice(0, 3).every(({ warmup }) => warmup)).toBe(true)
      expect(lines).toContain('b vs a: value +3.00 %')
      expect(lines).toContain('A/A within 3 %')
    })

    test.each([
      ['103', '3', 0, '+3.00 %'],
      ['103.04', '3', 1, '+3.04 %'],
      ['110.04', '+10', 1, '+10.04 %'],
      ['95', '+10', 0, '-5.00 %'],
    ])('a pair at %s with --pair-over %s exits %s', (value, limit, expected, shown) => {
      const { code, lines } = run(['--pair-over', limit], ['a= 100', `b= ${value}`])

      expect(code).toBe(expected)
      expect(lines).toContain(`b vs a: value ${shown}`)
      expect(lines).toContain('A/A within 3 %')
    })

    test('summarize prints the same lines from the raw file', () => {
      const first = run([])
      const output = io()

      const code = main(
        ['summarize', raw, '--metrics', 'value', '--reference', 'a', '--pair', 'b:a'],
        output
      )

      expect(code).toBe(0)
      expect(output.out.lines).toEqual(first.lines)
    })

    test('a session whose control drifts from its reference is void and exits 3, raw file intact', () => {
      const { code, lines } = run([], undefined, 10)

      expect(code).toBe(3)
      expect(lines.slice(-1)[0]).toMatch(/^SESSION VOID/)
      expect(fs.readFileSync(raw, 'utf8').trim().split('\n')).toHaveLength(12)
    })

    test('--pair-over limits the pair before it, not the first one', () => {
      const { code, lines } = run(
        ['--runs', '4', '--pair', 'c:a', '--pair-over', '3'],
        ['a= 100', 'b= 101', 'c= 103.04']
      )

      expect(code).toBe(1)
      expect(lines).toContain('c vs a: value +3.04 %')
    })

    test('a void session exits 3 even when a pair is over its limit', () => {
      const { code, lines } = run(['--pair-over', '3'], undefined, 10)

      expect(code).toBe(3)
      expect(lines.slice(-1)[0]).toMatch(/^SESSION VOID/)
    })

    test('--control-on limits the A/A rule to the keys it names', () => {
      const both = run(['--metrics', 'value,other'])
      const limited = run(['--metrics', 'value,other', '--control-on', 'value'])

      expect([both.code, limited.code]).toEqual([3, 0])
      expect(limited.lines).toContain('A/A within 3 %')
    })

    test('a run that fails ends the session with no summary and the raw file as far as it got', () => {
      const { code, lines, errors } = run([], ['a= 100', 'b= fail'])

      expect(code).toBe(1)
      expect(lines).toEqual([])
      expect(errors[0]).toMatch(/^b failed \(3\)/)
      expect(fs.readFileSync(raw, 'utf8').trim().split('\n')).toHaveLength(1)
    })

    test('a control that fails is reported under its own label, not its reference', () => {
      const { code, errors } = run([], ['a= failThird', 'b= 103'])

      expect(code).toBe(1)
      expect(errors[0]).toMatch(/^a~AA failed \(3\)/)
      expect(fs.readFileSync(raw, 'utf8').trim().split('\n')).toHaveLength(2)
    })

    test('a --runs that is not a multiple of the arms, the control included, is a usage error that runs nothing', () => {
      const { code, lines, errors } = run(['--runs', '4'])

      expect(code).toBe(2)
      expect(lines).toEqual([])
      expect(errors[0]).toMatch(/^4 runs cannot put each of 3 arms/)
      expect(fs.existsSync(log)).toBe(false)
    })

    test('summarize refuses a raw file in which an arm did not take every position equally often', () => {
      const fourRounds = [1, 1, 1, 1]
      const session = sessionOf({ a: fourRounds, b: fourRounds, 'a~AA': fourRounds }, 'value')
      fs.writeFileSync(raw, session.map((observation) => JSON.stringify(observation)).join('\n'))
      const output = io()

      const code = main(
        ['summarize', raw, '--metrics', 'value', '--reference', 'a', '--pair', 'b:a'],
        output
      )

      expect(code).toBe(1)
      expect(output.out.lines).toEqual([])
      expect(output.out.errors[0]).toMatch(/^a did not take every position equally often/)
    })

    test('--cpu adds the CPU time of the run', () => {
      const { code } = run(['--cpu'])

      expect(code).toBe(0)
      const [first] = fs.readFileSync(raw, 'utf8').trim().split('\n')
      expect(JSON.parse(first).result.cpuMs).toBeGreaterThan(0)
    })

    test.each([
      [['run', '--runs', '3']],
      [['run', '--metrics']],
      [
        [
          'run',
          '--metrics',
          'value',
          '--runs',
          '3',
          '--command',
          'node',
          '--raw',
          'x',
          'no-equals',
        ],
      ],
      [['frobnicate', '--metrics', 'value']],
      [['run', '--pair-over', '3']],
      [['run', '--metrics', 'value', '--pair', 'b:a', '--pair-over', 'three']],
    ])('%j is a usage error', (argv) => {
      const output = io()

      expect(main(argv, output)).toBe(2)
      expect(output.out.errors).toHaveLength(1)
    })
  })
})
