const fs = require('fs')
const os = require('os')
const path = require('path')
const {
  ARMS,
  MeasurementCollector,
  compare,
  main,
  overlayFiles,
  parseArgs,
  parseExceptions,
} = require('../compareDecoderBounds')

const BASELINE = 'a'.repeat(40)
const CANDIDATE = 'b'.repeat(40)
const OPTIONAL = 'record-batch:zstd'
const OPTIONAL_ARMS = ARMS.filter((arm) => !arm.required).map(({ name }) => name)

const constant = (value) => Array.from({ length: 5 }, () => value)

/** @returns {object} an arm that is 100 ns in both trees */
const steadyArm = (name) => ({ name, oldNsPerOp: constant(100), newNsPerOp: constant(100) })

/**
 * @returns {object} a complete, comparable input: every required arm is 100 ns in both trees and
 *   every optional arm is declared unavailable
 */
const completeInput = (overrides = {}) => ({
  baselineSha: BASELINE,
  candidateSha: CANDIDATE,
  node: 'v26.0.0',
  arms: ARMS.filter((arm) => arm.required).map(({ name }) => steadyArm(name)),
  unavailable: [...OPTIONAL_ARMS],
  ...overrides,
})

/** @returns {Function} runs `fn` with a temporary directory that is removed afterwards */
const inTempDir = (fn) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decoder-compare-'))
  try {
    return fn(dir)
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
}

/** @returns {{ status: number, report: string }} what `--input` exits with and prints for the input */
const runInput = (input) =>
  inTempDir((dir) => {
    const file = path.join(dir, 'in.json')
    fs.writeFileSync(file, JSON.stringify(input))
    const lines = []
    const status = main(['--input', file], (line) => lines.push(line))
    return { status, report: lines.join('\n') }
  })

/** @returns {object} the input with one arm's new-side samples replaced */
const withNew = (name, value) => {
  const input = completeInput()
  input.arms.find((arm) => arm.name === name).newNsPerOp = constant(value)
  return input
}

const verdictOf = (result, name) => result.arms.find((arm) => arm.name === name).verdict
const NO_EXCEPTIONS = new Map()
const FIRST = 'frame-decode:64b'

describe('Decoder performance comparison', () => {
  it('rejects a slowdown above five percent', () => {
    const slower = compare(withNew(FIRST, 106), NO_EXCEPTIONS)
    expect(slower.pass).toBe(false)
    expect(verdictOf(slower, FIRST)).toBe('fail')
    expect(slower.arms.find((arm) => arm.name === FIRST).slowdownPercent).toBeCloseTo(6, 6)

    const boundary = compare(withNew(FIRST, 105), NO_EXCEPTIONS)
    expect(boundary.pass).toBe(true)
    expect(verdictOf(boundary, FIRST)).toBe('pass')

    expect(compare(withNew(FIRST, 60), NO_EXCEPTIONS).pass).toBe(true)

    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decoder-compare-'))
    try {
      const file = path.join(dir, 'in.json')
      fs.writeFileSync(file, JSON.stringify(withNew(FIRST, 106)))
      const lines = []
      expect(main(['--input', file], (line) => lines.push(line))).toBe(1)
      expect(lines.join('\n')).toMatch(/FAIL\s+frame-decode:64b\s.*\+6\.00%/)
      expect(lines.join('\n')).toContain('decoder performance: FAIL')
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  it('refuses missing or incomparable measurements', () => {
    const exceptions = parseExceptions(
      ARMS.map(({ name }) => ({ arm: name, maxSlowdownPercent: 1000, ruling: 'test ruling' }))
    )
    const broken = (mutate) => {
      const input = completeInput()
      mutate(input)
      return input
    }
    const cases = {
      'required arm is missing': broken((input) => input.arms.pop()),
      'fewer than five samples': broken((input) => (input.arms[0].newNsPerOp = [100, 100, 100])),
      'a zero sample': broken((input) => (input.arms[0].oldNsPerOp = [0, 100, 100, 100, 100])),
      'a NaN sample': broken((input) => (input.arms[0].newNsPerOp = [NaN, 1, 1, 1, 1])),
      'a string sample': broken((input) => (input.arms[0].newNsPerOp = ['9', 9, 9, 9, 9])),
      'samples that are not an array': broken((input) => (input.arms[0].oldNsPerOp = 100)),
      'a short baseline sha': broken((input) => (input.baselineSha = 'abc123')),
      'identical shas': broken((input) => (input.candidateSha = BASELINE)),
      'no node version': broken((input) => delete input.node),
      'a duplicated arm': broken((input) => input.arms.push({ ...input.arms[0] })),
      'arms that are not an array': broken((input) => (input.arms = {})),
    }

    for (const [label, input] of Object.entries(cases)) {
      const result = compare(input, NO_EXCEPTIONS)
      expect({ label, pass: result.pass }).toEqual({ label, pass: false })
      expect({ label, problems: result.problems.length > 0 }).toEqual({ label, problems: true })
      expect({ label, pass: compare(input, exceptions).pass }).toEqual({ label, pass: false })
    }
    expect(compare(null, NO_EXCEPTIONS).pass).toBe(false)
    expect(compare(completeInput(), NO_EXCEPTIONS).pass).toBe(true)

    expect(main(['--input', '/nonexistent/decoder-comparison.json'], () => {})).toBe(1)
    expect(main(['--run'], () => {})).toBe(1)
  })

  const PERMISSIVE = parseExceptions(
    ARMS.map(({ name }) => ({ arm: name, maxSlowdownPercent: 1000, ruling: 'test ruling' }))
  )

  test.each(OPTIONAL_ARMS)(
    'refuses an input that leaves %s with neither measurements nor an unavailable disposition',
    (name) => {
      const input = completeInput({ unavailable: OPTIONAL_ARMS.filter((other) => other !== name) })

      const result = compare(input, NO_EXCEPTIONS)

      expect(result.pass).toBe(false)
      expect(result.problems).toEqual([
        `${name} has neither measurements nor an unavailable disposition`,
      ])
      expect(compare(input, PERMISSIVE).pass).toBe(false)
      expect(runInput(input).status).toBe(1)
    }
  )

  it('refuses an input that omits every optional arm and says so through --input', () => {
    const everyOptionalOmitted = completeInput({ unavailable: [] })
    const { unavailable, ...withoutTheField } = completeInput()

    expect(compare(everyOptionalOmitted, NO_EXCEPTIONS).pass).toBe(false)
    expect(compare(withoutTheField, NO_EXCEPTIONS).pass).toBe(false)

    const exit = runInput(everyOptionalOmitted)
    expect(exit.status).toBe(1)
    for (const name of OPTIONAL_ARMS) {
      expect(exit.report).toContain(`PROBLEM  ${name} has neither measurements nor`)
    }
    expect(exit.report).toContain('decoder performance: FAIL')
    expect(runInput(completeInput())).toMatchObject({ status: 0 })
    expect(unavailable).toEqual(OPTIONAL_ARMS)
  })

  const REQUIRED = ARMS.find((arm) => arm.required).name
  const unpermitted = {
    'an arm that is both measured and unavailable': [
      () => completeInput({ arms: [...completeInput().arms, steadyArm(OPTIONAL)] }),
      `${OPTIONAL} is both measured and unavailable`,
    ],
    'a required arm declared unavailable': [
      () =>
        completeInput({
          arms: completeInput().arms.filter((arm) => arm.name !== REQUIRED),
          unavailable: [...OPTIONAL_ARMS, REQUIRED],
        }),
      `required arm ${REQUIRED} cannot be unavailable`,
    ],
    'an unknown arm declared unavailable': [
      () => completeInput({ unavailable: [...OPTIONAL_ARMS, 'record-batch:brotli'] }),
      'record-batch:brotli is not a known arm',
    ],
    'an arm declared unavailable twice': [
      () => completeInput({ unavailable: [...OPTIONAL_ARMS, OPTIONAL] }),
      `${OPTIONAL} is declared unavailable twice`,
    ],
    'an unavailable list that is not an array': [
      () => completeInput({ unavailable: OPTIONAL }),
      'unavailable must be an array',
    ],
  }

  test.each(Object.entries(unpermitted))('refuses %s', (label, [build, problem]) => {
    const input = build()

    const result = compare(input, NO_EXCEPTIONS)

    expect(result.pass).toBe(false)
    expect(result.problems.join('\n')).toContain(problem)
    expect(compare(input, PERMISSIVE).pass).toBe(false)
    expect(runInput(input).status).toBe(1)
  })

  it('measures a real response through each array reader the decoder bounds, in both trees', () => {
    const arrayArms = ARMS.filter((arm) => arm.name.startsWith('array-decode:'))

    expect(arrayArms.map((arm) => arm.name)).toEqual([
      'array-decode:readArray',
      'array-decode:readVarIntArray',
      'array-decode:readUVarIntArray',
      'array-decode:readArrayAsync',
      'array-decode:readUVarIntArrayAsync',
    ])
    for (const arm of arrayArms) {
      const reader = arm.name.split(':')[1]
      expect(arm.required).toBe(true)
      expect(arm.metric).toBe('nsPerOp')
      expect(arm.argv.slice(0, 2)).toEqual(['node', 'scripts/benchmarks/arrayDecode.js'])
      expect(arm.argv).toContain(reader)
      expect(arm.overlay).toBe('scripts/benchmarks/arrayDecode.js')
      expect(fs.existsSync(path.join(__dirname, '..', '..', '..', arm.overlay))).toBe(true)
    }
  })

  it('overlays the candidate benchmark scripts into the baseline tree', () => {
    inTempDir((dir) => {
      const repoRoot = path.join(dir, 'candidate')
      const tree = path.join(dir, 'baseline')
      const write = (root, file, text) => {
        fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true })
        fs.writeFileSync(path.join(root, file), text)
      }
      const read = (root, file) => fs.readFileSync(path.join(root, file), 'utf8')
      write(repoRoot, 'scripts/benchmarks/a.js', 'candidate a')
      write(repoRoot, 'scripts/benchmarks/b.js', 'candidate b')
      write(tree, 'scripts/benchmarks/a.js', 'baseline a')

      const files = ['scripts/benchmarks/a.js', 'scripts/benchmarks/b.js']
      overlayFiles({ repoRoot, dir: tree, files })

      expect(read(tree, files[0])).toBe('candidate a')
      expect(read(tree, files[1])).toBe('candidate b')
      expect(() =>
        overlayFiles({ repoRoot, dir: tree, files: ['scripts/benchmarks/missing.js'] })
      ).toThrow(/missing\.js/)
    })
  })

  it('accepts a slowdown covered by a recorded exception and rejects one beyond it', () => {
    const entry = (arm, maxSlowdownPercent) => ({ arm, maxSlowdownPercent, ruling: 'panel ruling' })

    const covered = compare(withNew(FIRST, 108), parseExceptions([entry(FIRST, 10)]))
    expect(covered.pass).toBe(true)
    expect(verdictOf(covered, FIRST)).toBe('exception')

    const beyond = compare(withNew(FIRST, 108), parseExceptions([entry(FIRST, 7)]))
    expect(beyond.pass).toBe(false)
    expect(verdictOf(beyond, FIRST)).toBe('fail')

    const otherArm = compare(withNew(FIRST, 108), parseExceptions([entry('record-batch:gzip', 50)]))
    expect(otherArm.pass).toBe(false)

    expect(() => parseExceptions([{ arm: FIRST, maxSlowdownPercent: 10 }])).toThrow(/invalid/)
    expect(() => parseExceptions([entry(FIRST, 3)])).toThrow(/invalid/)
    expect(() => parseExceptions([entry(FIRST, 10), entry(FIRST, 20)])).toThrow(/duplicate/)
    expect(() => parseExceptions({})).toThrow(/array/)
    expect(JSON.parse(fs.readFileSync(require.resolve('../decoderBoundsExceptions.json')))).toEqual(
      []
    )
  })

  it('parses run and input arguments strictly', () => {
    expect(parseArgs(['--run', '--baseline', BASELINE])).toEqual({
      mode: 'run',
      baseline: BASELINE,
      out: path.join('.tmp', 'decoder-comparison.json'),
      raw: path.join('.tmp', 'decoder-comparison', 'raw'),
    })
    expect(
      parseArgs(['--run', '--baseline', BASELINE, '--out', 'o.json', '--raw', 'raw-dir'])
    ).toMatchObject({ out: 'o.json', raw: 'raw-dir' })
    expect(parseArgs(['--input', 'in.json'])).toEqual({ mode: 'input', input: 'in.json' })

    for (const argv of [
      [],
      ['--run'],
      ['--run', '--baseline', 'abc'],
      ['--run', '--baseline', BASELINE, '--input', 'in.json'],
      ['--input', 'in.json', '--baseline', BASELINE],
      ['--input'],
      ['--unknown'],
    ]) {
      expect(() => parseArgs(argv)).toThrow(/usage/)
    }
  })

  /** @returns {{ collector: MeasurementCollector, calls: string[], saved: Map<string, string> }} */
  const collect = ({ reply, pairs = 5 }) => {
    const calls = []
    const saved = new Map()
    const collector = new MeasurementCollector({
      trees: { old: 'OLD', new: 'NEW' },
      rawDir: 'raw',
      pairs,
      run: (cwd, argv) => {
        calls.push(`${cwd} ${argv.join(' ')}`)
        return reply(cwd, argv)
      },
      save: (file, text) => saved.set(file, text),
    })
    return { collector, calls, saved }
  }

  const ok = (value) => ({ status: 0, stdout: `banner\n${JSON.stringify(value)}\n`, stderr: '' })
  const valueFor = (cwd, argv) => {
    const arm = ARMS.find((candidate) => candidate.argv.join(' ') === argv.join(' '))
    return ok({ available: 1, [arm.metric]: cwd === 'OLD' ? 100 : 101 })
  }

  it('collector runs each arm in both trees, alternates their order and keeps every raw invocation', () => {
    const { collector, calls, saved } = collect({ reply: valueFor })

    const { arms, unavailable } = collector.collect()

    expect(unavailable).toEqual([])
    expect(arms.map((arm) => arm.name)).toEqual(ARMS.map((arm) => arm.name))
    for (const arm of arms) {
      expect(arm.oldNsPerOp).toEqual(constant(100))
      expect(arm.newNsPerOp).toEqual(constant(101))
    }
    expect(compare(completeInput({ arms, unavailable: [] }), NO_EXCEPTIONS).pass).toBe(true)

    const first = ARMS[0].argv.join(' ')
    const forFirst = calls.filter((call) => call.endsWith(first))
    expect(forFirst.map((call) => call.split(' ')[0])).toEqual([
      'OLD',
      'NEW',
      'NEW',
      'OLD',
      'OLD',
      'NEW',
      'NEW',
      'OLD',
      'OLD',
      'NEW',
    ])
    expect(saved.size).toBe(ARMS.length * 5 * 2)
    expect(saved.get(path.join('raw', 'frame-decode-64b', '1-new.txt'))).toContain(
      `$ ${first}\nexit 0\n--- stdout\nbanner\n{"available":1,"nsPerFrame":101}`
    )
  })

  it('collector names an arm that neither tree has and leaves an arm one tree lacks incomplete', () => {
    const lacking = (name, trees) => (cwd, argv) =>
      argv.includes(name) && trees.includes(cwd) ? ok({ available: 0 }) : valueFor(cwd, argv)

    const both = collect({ reply: lacking('zstd', ['OLD', 'NEW']) })
    const bothResult = both.collector.collect()
    expect(bothResult.unavailable).toEqual([OPTIONAL])
    expect(bothResult.arms.map((arm) => arm.name)).not.toContain(OPTIONAL)
    expect(both.calls.filter((call) => call.includes('zstd'))).toHaveLength(2)
    expect(compare(completeInput(bothResult), NO_EXCEPTIONS).pass).toBe(true)

    const one = collect({ reply: lacking('zstd', ['OLD']) })
    const oneResult = one.collector.collect()
    expect(oneResult.unavailable).toEqual([])
    const zstd = oneResult.arms.find((arm) => arm.name === OPTIONAL)
    expect(zstd.oldNsPerOp).toEqual([])
    const result = compare(completeInput(oneResult), NO_EXCEPTIONS)
    expect(result.pass).toBe(false)
    expect(result.problems.join('\n')).toContain(`${OPTIONAL}: oldNsPerOp needs at least 5`)
  })

  it('collector fails on a benchmark that exits non-zero or prints no usable number', () => {
    const failing = (reply) => collect({ reply: () => reply })

    expect(() => failing({ status: 1, stdout: '', stderr: 'boom' }).collector.collect()).toThrow(
      /frame-decode:64b failed in the old tree \(exit 1\)/
    )
    expect(() => failing({ status: 0, stdout: 'no json', stderr: '' }).collector.collect()).toThrow(
      /printed no JSON line/
    )
    expect(() => failing(ok({ available: 1, nsPerFrame: 0 })).collector.collect()).toThrow(
      /no usable nsPerFrame/
    )
    expect(() => failing(ok({ available: 1 })).collector.collect()).toThrow(/no usable/)
  })
})
