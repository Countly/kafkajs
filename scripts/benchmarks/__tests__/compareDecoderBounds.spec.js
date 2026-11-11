const fs = require('fs')
const os = require('os')
const path = require('path')
const { EventEmitter } = require('events')
const {
  ARMS,
  MeasurementCollector,
  compare,
  main,
  medianInterval,
  createSchedule,
  overlayFiles,
  parseArgs,
  parseExceptions,
  runCommand,
} = require('../compareDecoderBounds')
const replay = require('./fixtures/decoderReplay.json')

const FIRST = ARMS[0].name
const BASELINE = 'a'.repeat(40)
const CANDIDATE = 'b'.repeat(40)
const OPTIONAL = ARMS.filter(({ required }) => !required).map(({ name }) => name)
const constant = (value) => Array(30).fill(value)
const sortedMedian = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/** @returns {object} complete paired observations with explicit invocation identity */
const row = (name, A = constant(100), B = A, C = A) => {
  const arm = ARMS.find((arm) => arm.name === name)
  const schedule = createSchedule(42)
  return {
    name,
    oldNsPerOp: A,
    newNsPerOp: B,
    baselineControlNsPerOp: C,
    invocations: schedule.flatMap((order, block) =>
      order.map((side) => ({
        block,
        side,
        [arm.metric]: { A, B, C }[side][block],
        roundNsPerOp: Array(arm.name.startsWith('frame-') ? 7 : 5).fill({ A, B, C }[side][block]),
      }))
    ),
  }
}
const completeInput = (overrides = {}) => ({
  baselineSha: BASELINE,
  candidateSha: CANDIDATE,
  node: 'v26.0.0',
  registeredRows: ARMS.map(({ name }) => name),
  seed: 42,
  schedule: createSchedule(42),
  arms: ARMS.filter(({ required }) => required).map(({ name }) => row(name)),
  unavailable: [...OPTIONAL],
  ...overrides,
})
const withRow = (arm) =>
  completeInput({ arms: completeInput().arms.map((a) => (a.name === arm.name ? arm : a)) })
const resultOf = (arm) => compare(withRow(arm)).arms.find(({ name }) => name === arm.name)
const runInput = async (input) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decoder-compare-'))
  try {
    const file = path.join(dir, 'input.json')
    fs.writeFileSync(file, JSON.stringify(input))
    const lines = []
    return {
      status: await main(['--input', file], (line) => lines.push(line)),
      report: lines.join('\n'),
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
}

describe('Decoder performance comparison', () => {
  it('uses exact Bonferroni order statistics including optional registered rows and ties', () => {
    const samples = Array.from({ length: 30 }, (_, i) => i + 1)
    expect(ARMS).toHaveLength(15)
    expect(medianInterval(samples, 15)).toEqual({ alpha: 0.05 / 30, k: 7, lower: 7, upper: 24 })
    expect(medianInterval(samples, 1)).toEqual({ alpha: 0.025, k: 9, lower: 9, upper: 22 })
    expect(medianInterval(constant(1), 15)).toMatchObject({ lower: 1, upper: 1 })
    expect(resultOf(row(FIRST)).ratioInterval.k).toBe(7)
  })

  it('passes equal speed and inclusive five percent bounds but rejects stable regressions', async () => {
    for (const value of [60, 100, 105]) {
      const input = withRow(row(FIRST, constant(100), constant(value)))
      expect(compare(input).pass).toBe(true)
      expect((await runInput(input)).status).toBe(0)
    }
    for (const value of [105.001, 112, 1800]) {
      const input = withRow(row(FIRST, constant(100), constant(value)))
      expect(compare(input).pass).toBe(false)
      expect(resultOf(input.arms[0]).verdict).toBe('regression')
      expect((await runInput(input)).status).toBe(1)
    }
  })

  it('a paired ratio pass cannot override a failed unpaired median check', () => {
    const A = [...Array(15).fill(100), ...Array(15).fill(114)]
    const B = [...Array(15).fill(105), ...Array(15).fill(119.7)]
    B[0] = 114
    const result = resultOf(row(FIRST, A, B, A))
    expect(result.ratioInterval.upper).toBeLessThanOrEqual(1.05)
    expect(result.newMedian / result.oldMedian).toBeGreaterThan(1.05)
    expect(result.verdict).toBe('fail')
  })

  it('an interval straddling five percent is inconclusive and no exception rescues it', async () => {
    const arm = row(FIRST, constant(100), [...Array(23).fill(100), ...Array(7).fill(106)])
    expect(resultOf(arm).verdict).toBe('inconclusive')
    expect(compare(withRow(arm), new Map([[FIRST, { maxSlowdownPercent: 1000 }]])).pass).toBe(false)
    expect((await runInput(withRow(arm))).status).toBe(1)
    expect(() =>
      parseExceptions([{ arm: FIRST, maxSlowdownPercent: 1000, ruling: 'test' }])
    ).toThrow(/empty/)
    expect(parseExceptions([]).size).toBe(0)
    expect(JSON.parse(fs.readFileSync(require.resolve('../decoderBoundsExceptions.json')))).toEqual(
      []
    )
  })

  it('calibrates both the paired A/A interval and the unpaired control median inclusively', () => {
    for (const C of [95, 100, 105])
      expect(resultOf(row(FIRST, constant(100), constant(100), constant(C))).verdict).toBe('pass')
    for (const C of [94.999, 105.001])
      expect(resultOf(row(FIRST, constant(100), constant(100), constant(C))).verdict).toBe(
        'invalid'
      )
    const A = [...Array(15).fill(100), ...Array(15).fill(114)]
    const C = [...Array(15).fill(105), ...Array(15).fill(119.7)]
    C[0] = 114
    const result = resultOf(row(FIRST, A, A, C))
    expect(result.controlInterval.upper).toBeLessThanOrEqual(1.05)
    expect(result.verdict).toBe('invalid')
    const intervalDrift = [...Array(23).fill(100), ...Array(7).fill(106)]
    expect(resultOf(row(FIRST, constant(100), constant(100), intervalDrift)).verdict).toBe(
      'invalid'
    )
  })

  it('enforces the inclusive fifteen percent spread on every side and rejects bimodal samples', () => {
    for (const side of ['A', 'B', 'C']) {
      const make = (high) => {
        const samples = constant(100)
        samples[28] = samples[29] = high
        return row(
          FIRST,
          side === 'A' ? samples : constant(100),
          side === 'B' ? samples : constant(100),
          side === 'C' ? samples : constant(100)
        )
      }
      expect(resultOf(make(115)).verdict).toBe('pass')
      expect(resultOf(make(115.001)).verdict).toBe('invalid')
    }
    const bimodal = [...Array(15).fill(100), ...Array(15).fill(200)]
    expect(resultOf(row(FIRST, bimodal, bimodal, bimodal)).verdict).toBe('invalid')
  })

  it('rejects missing or duplicate blocks, malformed data, errors and changed registrations', async () => {
    const mutations = [
      (i) => i.arms[0].invocations.pop(),
      (i) => i.arms[0].invocations.push(i.arms[0].invocations[0]),
      (i) => (i.arms[0].invocations[0].block = 1),
      (i) => (i.arms[0].invocations[0].side = 'wrong'),
      (i) => (i.arms[0].invocations[0].error = 'control failed'),
      (i) => (i.arms[0].invocations[0].roundNsPerOp = [NaN]),
      (i) => (i.arms[0].oldNsPerOp[0] = 0),
      (i) => (i.arms[0].newNsPerOp[0] = Infinity),
      (i) => (i.arms[0].baselineControlNsPerOp = []),
      (i) => (i.arms[0].newNsPerOp = '100'),
      (i) => i.arms[0].newNsPerOp.pop(),
      (i) => i.registeredRows.pop(),
      (i) => i.schedule[0].reverse(),
      (i) => (i.seed = '42'),
      (i) => (i.baselineSha = 'abc'),
      (i) => (i.candidateSha = BASELINE),
      (i) => delete i.node,
      (i) => i.arms.push(i.arms[0]),
      (i) => (i.arms[0].name = 'unknown'),
      (i) => (i.timedOut = true),
      (i) => (i.arms = {}),
    ]
    for (const mutate of mutations) {
      const input = completeInput()
      mutate(input)
      expect(compare(input).pass).toBe(false)
      expect((await runInput(input)).status).toBe(1)
    }
    expect(compare(null).pass).toBe(false)
  })

  test.each(OPTIONAL)('requires a measured or unavailable disposition for %s', async (name) => {
    const input = completeInput({ unavailable: OPTIONAL.filter((other) => other !== name) })
    expect(compare(input).problems).toContain(
      `${name} has neither measurements nor an unavailable disposition`
    )
    expect((await runInput(input)).status).toBe(1)
    input.arms.push(row(name))
    expect(compare(input).pass).toBe(true)
  })

  it('rejects unavailable required, unknown, duplicated or simultaneously measured codecs', () => {
    for (const unavailable of [
      [FIRST, ...OPTIONAL],
      [...OPTIONAL, 'unknown'],
      [...OPTIONAL, OPTIONAL[0]],
      'wrong',
    ]) {
      expect(compare(completeInput({ unavailable })).pass).toBe(false)
    }
    expect(compare(completeInput({ arms: [...completeInput().arms, row(OPTIONAL[0])] })).pass).toBe(
      false
    )
    const input = completeInput()
    input.arms.pop()
    expect(compare(input).pass).toBe(false)
  })

  it('reports the old unpaired median delta as information alongside paired bounds', async () => {
    const output = await runInput(withRow(row(FIRST, constant(100), constant(112))))
    expect(output.report).toContain('REGRESSION')
    expect(output.report).toContain('unpaired +12.00% (information only)')
    expect(output.report).toContain('R [1.1200, 1.1200]')
    expect(output.report).toContain('Q [1.0000, 1.0000]')
  })

  it('replays honestly derived median projections and keeps noisy Exit frames non-regressions', () => {
    expect(replay.derivation).toContain('synthetic')
    for (const [index, expectedDelta, verdict] of [
      [0, -4.07, 'pass'],
      [1, 6.59, 'regression'],
      [2, 19.71, 'regression'],
    ]) {
      const source = replay.sources[index].arm
      const derived = row(
        source.name,
        constant(sortedMedian(source.oldNsPerOp)),
        constant(sortedMedian(source.newNsPerOp)),
        constant(sortedMedian(source.baselineControlNsPerOp))
      )
      const result = resultOf(derived)
      expect(result.slowdownPercent).toBeCloseTo(expectedDelta, 2)
      expect(result.verdict).toBe(verdict)
      expect(source.oldNsPerOp).toHaveLength(7)
    }
    const source = replay.sources[3].arm
    const repeat = (samples) => Array.from({ length: 30 }, (_, n) => samples[n % samples.length])
    const derived = row(source.name, repeat(source.oldNsPerOp), repeat(source.newNsPerOp))
    derived.baselineControlNsPerOp = []
    derived.invocations = derived.invocations.filter(({ side }) => side !== 'C')
    const result = resultOf(derived)
    expect(
      (sortedMedian(source.newNsPerOp) / sortedMedian(source.oldNsPerOp) - 1) * 100
    ).toBeCloseTo(109.66, 2)
    expect(result.verdict).toBe('invalid')
    expect(compare(withRow(derived)).pass).toBe(false)
  })

  it('records a seeded balanced schedule before measurement and keeps every round without extension', async () => {
    const schedule = createSchedule(42)
    expect(createSchedule(42)).toEqual(schedule)
    expect(createSchedule(43)).not.toEqual(schedule)
    const counts = {}
    for (const order of schedule) counts[order.join('')] = (counts[order.join('')] || 0) + 1
    expect(counts).toEqual({ ACB: 5, ABC: 5, CAB: 5, CBA: 5, BAC: 5, BCA: 5 })
    const { collector, calls, saved } = collectorFor()
    const collected = await collector.collect()
    expect(calls).toHaveLength(15 * 30 * 3)
    expect(JSON.parse(saved.get(path.join('raw', 'schedule.json')))).toEqual({
      seed: 42,
      registeredRows: ARMS.map(({ name }) => name),
      schedule,
    })
    for (const arm of collected.arms) {
      expect(arm.invocations).toHaveLength(90)
      expect(arm.invocations.map(({ side }) => side)).toEqual(schedule.flat())
      expect(arm.invocations.every(({ roundNsPerOp }) => roundNsPerOp.length >= 5)).toBe(true)
    }
    expect(compare(completeInput(collected)).pass).toBe(true)
    expect(saved.size).toBe(1 + calls.length)
  })

  it('a control error invalidates its row without retry or extension', async () => {
    const setup = collectorFor()
    const measure = setup.collector.measure.bind(setup.collector)
    setup.collector.measure = (arm, side, block, timeout) =>
      side === 'C' && arm.name === FIRST && block === 0
        ? Promise.reject(new Error('control failed'))
        : measure(arm, side, block, timeout)
    const collected = await setup.collector.collect()
    expect(collected.arms[0].invocations).toHaveLength(90)
    expect(resultOf(collected.arms[0]).verdict).toBe('invalid')
    expect(setup.calls).toHaveLength(15 * 30 * 3 - 1)
  })

  it('allows only codecs absent in both trees and preserves their availability evidence', async () => {
    for (const missing of [['OLD', 'NEW'], ['OLD']]) {
      const setup = collectorFor({ missing })
      const collected = await setup.collector.collect()
      expect(collected.unavailable).toEqual(missing.length === 2 ? [OPTIONAL[1]] : [])
      expect(compare(completeInput(collected)).pass).toBe(missing.length === 2)
      expect(setup.saved.size).toBeGreaterThan(1)
    }
  })

  it('the fixed deadline stops collection with partial evidence and a non-green offline verdict', async () => {
    let clock = 0
    const setup = collectorFor({ now: () => clock, advance: () => (clock += 1) })
    setup.collector.deadlineMs = 4
    const collected = await setup.collector.collect()
    expect(collected.timedOut).toBe(true)
    expect(setup.calls).toHaveLength(4)
    expect(setup.calls.map(({ timeoutMs }) => timeoutMs)).toEqual([4, 3, 2, 1])
    expect(compare(completeInput(collected)).pass).toBe(false)
    expect((await runInput(completeInput(collected))).status).toBe(1)
  })

  it('kills the subprocess group at the deadline under a fake clock', async () => {
    jest.useFakeTimers()
    try {
      const child = new EventEmitter()
      child.stdout = new EventEmitter()
      child.stderr = new EventEmitter()
      child.pid = 1234
      const kill = jest.fn(() => child.emit('close', null, 'SIGKILL'))
      const pending = runCommand('tree', ['node', 'bench.js'], {
        timeoutMs: 50,
        spawn: () => child,
        kill,
      })
      jest.advanceTimersByTime(49)
      expect(kill).not.toHaveBeenCalled()
      jest.advanceTimersByTime(1)
      expect(await pending).toMatchObject({ status: 1, timedOut: true })
      expect(kill).toHaveBeenCalledWith(-1234, 'SIGKILL')
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      jest.useRealTimers()
    }
  })

  it('overlays identical scripts for all frame, array and record-batch rows', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decoder-overlay-'))
    try {
      const files = [...new Set(ARMS.map(({ overlay }) => overlay))]
      overlayFiles({ repoRoot: path.resolve(__dirname, '../../..'), dir, files })
      for (const file of files)
        expect(fs.readFileSync(path.join(dir, file))).toEqual(
          fs.readFileSync(path.resolve(__dirname, '../../..', file))
        )
      expect(ARMS.filter(({ name }) => name.startsWith('array-decode:'))).toHaveLength(5)
      expect(
        ARMS.slice(0, 5).every(({ argv }) => argv[0] === 'node' && argv[1] === '--expose-gc')
      ).toBe(true)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  it('parses run and offline modes strictly including reproducible seeds', async () => {
    expect(parseArgs(['--run', '--baseline', BASELINE, '--seed', '42'])).toMatchObject({
      mode: 'run',
      seed: 42,
    })
    expect(parseArgs(['--input', 'in.json'])).toEqual({ mode: 'input', input: 'in.json' })
    for (const argv of [
      [],
      ['--run'],
      ['--run', '--baseline', 'abc'],
      ['--input', 'i', '--seed', '42'],
      ['--run', '--baseline', BASELINE, '--seed', '-1'],
    ])
      expect(() => parseArgs(argv)).toThrow(/usage/)
    expect(await main(['--run'], () => {})).toBe(1)
    expect(await main(['--input', '/nonexistent/input.json'], () => {})).toBe(1)
  })
})

/** @returns {object} collector driven by deterministic fake subprocess responses */
const collectorFor = ({ missing = [], now = () => 0, advance = () => {} } = {}) => {
  const calls = []
  const saved = new Map()
  const collector = new MeasurementCollector({
    trees: { old: 'OLD', new: 'NEW' },
    rawDir: 'raw',
    seed: 42,
    now,
    save: (file, text) => {
      if (file.endsWith('schedule.json')) expect(calls).toHaveLength(0)
      saved.set(file, text)
    },
    run: async (cwd, argv, { timeoutMs }) => {
      calls.push({ cwd, argv, timeoutMs })
      advance()
      const arm = ARMS.find((arm) => arm.argv.join(' ') === argv.join(' '))
      const measurement =
        argv.includes('zstd') && missing.includes(cwd)
          ? { available: 0 }
          : {
              available: 1,
              [arm.metric]: cwd === 'OLD' ? 100 : 101,
              roundNsPerOp: Array(arm.name.startsWith('frame-') ? 7 : 5).fill(
                cwd === 'OLD' ? 100 : 101
              ),
            }
      return { status: 0, stdout: `${JSON.stringify(measurement)}\n`, stderr: '' }
    },
  })
  return { collector, calls, saved }
}
