const fs = require('fs')
const os = require('os')
const path = require('path')
const {
  ARMS,
  PARTITIONS,
  WORK_MULTIPLIERS,
  MeasurementCollector,
  combine,
  compare,
  createSchedule,
  main,
  parseArgs,
} = require('../compareDecoderBounds')

const EXPECTED = {
  'frame-1m': ['frame-decode:1m'],
  'frame-other': ['64b', '64k', 'coalesced', 'byte-chunks'].map((s) => `frame-decode:${s}`),
  'batch-array': [
    ...['uncompressed', 'gzip', 'zstd', 'lz4', 'snappy'].map((s) => `record-batch:${s}`),
    ...[
      'readArray',
      'record-headers',
      'readUVarIntArray',
      'readArrayAsync',
      'readUVarIntArrayAsync',
    ].map((s) => `array-decode:${s}`),
  ],
}

/** @returns {object[]} deterministic complete partition evidence, with different seeds */
const evidence = () =>
  Object.entries(EXPECTED).map(([partition, names], index) => {
    const seed = 42 + index
    const schedule = createSchedule(seed)
    return {
      partition,
      baselineSha: 'a'.repeat(40),
      candidateSha: 'b'.repeat(40),
      node: 'v26.0.0',
      seed,
      schedule,
      registeredRows: ARMS.map(({ name }) => name),
      workMultipliers: WORK_MULTIPLIERS,
      timedOut: false,
      unavailable: [],
      arms: names.map((name) => {
        const arm = ARMS.find((arm) => arm.name === name)
        return {
          name,
          oldNsPerOp: Array(30).fill(100),
          newNsPerOp: Array(30).fill(100),
          baselineControlNsPerOp: Array(30).fill(100),
          invocations: schedule.flatMap((order, block) =>
            order.map((side) => ({
              side,
              block,
              workMultiplier: WORK_MULTIPLIERS[name],
              [arm.metric]: 100,
              roundNsPerOp: Array(arm.metric === 'nsPerFrame' ? 7 : 5).fill(100),
              ...(arm.metric === 'nsPerFrame' && { warmupRoundNsPerOp: [100, 100, 100] }),
            }))
          ),
        }
      }),
    }
  })

/** @param {string} partition @param {Function} now @param {Function} advance @returns {object} fake collector */
const collectorFor = (partition, now = () => 0, advance = () => {}) => {
  const calls = []
  const saved = new Map()
  const collector = new MeasurementCollector({
    partition,
    trees: { old: 'OLD', new: 'NEW' },
    rawDir: 'raw',
    seed: 42,
    now,
    save: (file, text) => saved.set(file, text),
    run: async (_cwd, argv, { timeoutMs }) => {
      calls.push({ argv, timeoutMs })
      advance()
      const arm = ARMS.find((arm) => arm.argv.join(' ') === argv.join(' '))
      const sample = {
        available: 1,
        workMultiplier: WORK_MULTIPLIERS[arm.name],
        [arm.metric]: 100,
        roundNsPerOp: Array(arm.metric === 'nsPerFrame' ? 7 : 5).fill(100),
        ...(arm.metric === 'nsPerFrame' && { warmupRoundNsPerOp: [100, 100, 100] }),
      }
      return { status: 0, stdout: JSON.stringify(sample), stderr: '' }
    },
  })
  return { collector, calls, saved }
}

describe('Decoder measurement partitions', () => {
  it('fixes all fifteen work multipliers and passes and records them without changing units', async () => {
    expect(WORK_MULTIPLIERS).toEqual(
      Object.fromEntries(
        ARMS.map(({ name }) => [
          name,
          name === 'frame-decode:1m'
            ? 64
            : name.startsWith('frame-decode:') || name === 'record-batch:uncompressed'
              ? 16
              : ['record-batch:gzip', 'record-batch:zstd'].includes(name)
                ? 2
                : 1,
        ])
      )
    )
    for (const partition of Object.keys(EXPECTED)) {
      const { collector, calls, saved } = collectorFor(partition)
      const input = await collector.collect()
      expect(input.workMultipliers).toEqual(WORK_MULTIPLIERS)
      expect(JSON.parse(saved.get(path.join('raw', 'schedule.json'))).workMultipliers).toEqual(
        WORK_MULTIPLIERS
      )
      for (const { argv } of calls) {
        const arm = ARMS.find((arm) => arm.argv.join(' ') === argv.join(' '))
        expect(argv[argv.indexOf('--work-multiplier') + 1]).toBe(String(WORK_MULTIPLIERS[arm.name]))
      }
      expect(input.arms.every((row) => row.oldNsPerOp.every((n) => n === 100))).toBe(true)
    }
    const inputs = evidence()
    delete inputs[0].workMultipliers
    expect(combine(inputs).pass).toBe(false)
    const changed = evidence()
    changed[0].arms[0].invocations[0].workMultiplier = 1
    expect(combine(changed).pass).toBe(false)
  })

  it('collects exactly each partition rows with its own balanced thirty-block schedule and m fifteen', async () => {
    expect(PARTITIONS).toEqual(EXPECTED)
    for (const partition of Object.keys(EXPECTED)) {
      const { collector, calls } = collectorFor(partition)
      const input = await collector.collect()
      expect(input.partition).toBe(partition)
      expect(input.arms.map(({ name }) => name)).toEqual(EXPECTED[partition])
      expect(calls).toHaveLength(EXPECTED[partition].length * 30 * 3)
      const result = compare({ ...evidence().find((p) => p.partition === partition), ...input })
      expect(result.pass).toBe(true)
      for (const arm of result.arms) {
        expect(arm.ratioInterval).toMatchObject({ alpha: 0.05 / 30, k: 7 })
        expect(arm.controlInterval).toMatchObject({ alpha: 0.05 / 30, k: 7 })
      }
    }
  })

  it('combines only complete disjoint partitions from the same trees and requires fifteen passes', () => {
    const valid = combine(evidence())
    expect(valid.pass).toBe(true)
    expect(valid.arms).toHaveLength(15)
    const mutations = [
      (p) => p.pop(),
      (p) => p.push(p[0]),
      (p) => p[0].arms.push(p[0].arms[0]),
      (p) => p[1].arms.push(p[0].arms[0]),
      (p) => p[2].arms.pop(),
      (p) => (p[0].candidateSha = 'c'.repeat(40)),
      (p) => (p[0].node = 'v24.0.0'),
      (p) => (p[0].timedOut = true),
      (p) => p[0].schedule[0].reverse(),
    ]
    for (const mutate of mutations) {
      const inputs = evidence()
      mutate(inputs)
      expect(combine(inputs).pass).toBe(false)
    }
    const absent = evidence()
    const optional = absent[2].arms.find((r) => r.name === 'record-batch:gzip')
    absent[2].arms = absent[2].arms.filter((r) => r !== optional)
    absent[2].unavailable = [optional.name]
    absent[2].unavailableInvocations = [
      {
        name: optional.name,
        invocations: optional.invocations.map(({ side, block }) => ({ side, block, available: 0 })),
      },
    ]
    expect(combine(absent).pass).toBe(false)
  })

  it('starts a separate twenty-five minute clock per partition and propagates one timeout globally', async () => {
    let clock = 0
    const first = collectorFor(
      'frame-1m',
      () => clock,
      () => {
        clock += 25 * 60 * 1000
      }
    )
    const timedOut = await first.collector.collect()
    expect(timedOut.timedOut).toBe(true)
    expect(first.calls).toHaveLength(1)
    const second = collectorFor(
      'frame-other',
      () => clock,
      () => {
        clock++
      }
    )
    const collected = await second.collector.collect()
    expect(second.calls[0].timeoutMs).toBe(25 * 60 * 1000)
    expect(collected.timedOut).toBe(false)
    const inputs = evidence()
    inputs[0] = { ...inputs[0], ...timedOut }
    expect(combine(inputs).pass).toBe(false)
  })

  it('requires a run partition and combines offline files without collecting again', async () => {
    expect(() => parseArgs(['--run', '--baseline', 'a'.repeat(40)])).toThrow(/usage/)
    expect(
      parseArgs(['--run', '--baseline', 'a'.repeat(40), '--partition', 'frame-1m'])
    ).toMatchObject({ partition: 'frame-1m' })
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decoder-partitions-'))
    try {
      const files = evidence().map((input) => {
        const file = path.join(dir, `${input.partition}.json`)
        fs.writeFileSync(file, JSON.stringify(input))
        return file
      })
      const argv = files.flatMap((file) => ['--input', file])
      expect(await main(argv, () => {})).toBe(0)
      expect(await main(argv.slice(2), () => {})).toBe(1)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })
})
