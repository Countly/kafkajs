const path = require('path')
const { Partitioners } = require('../../../index')
const {
  parseArgs,
  quantile,
  balanceOf,
  keyedIdentical,
  judge,
  METRICS,
  validRun,
  OfferWindow,
  HEAP_LIMIT,
} = require('../stickyBalance')

const ROOT = path.join(__dirname, '../../..')

/** A session summary whose arms have the given medians, every other metric 0. */
const summaryOf = (medians, isVoid = false) => ({
  void: isVoid,
  arms: Object.fromEntries(
    Object.entries(medians).map(([label, values]) => [
      label,
      Object.fromEntries(
        METRICS.map((metric) => [
          metric,
          { median: values[metric] ?? 0, min: values[metric] ?? 0, max: values[metric] ?? 0 },
        ])
      ),
    ])
  ),
})

const HEALTHY_ARM = {
  balance: 1.1,
  p99Ms: 10,
  keyedIdentical: 1,
  partitionerCalls: 100,
  records: 100,
  valid: 1,
  heapRetainedBytes: 1048576,
}
const healthy = (changes = {}) =>
  summaryOf({
    'round-robin': { ...HEALTHY_ARM, recordsPerRequest: 6, balance: 1.01 },
    'round-robin~AA': { ...HEALTHY_ARM, recordsPerRequest: 6, balance: 1.01 },
    sticky: { ...HEALTHY_ARM, recordsPerRequest: 30, ...changes.sticky },
    adaptive: { ...HEALTHY_ARM, recordsPerRequest: 30, ...changes.adaptive },
  })
const SLOW_ARM = {
  delivered: 120000,
  valid: 1,
  heapRetainedBytes: 1048576,
  rejected: 0,
  unsettled: 0,
  slowMedianMs: 300,
  otherMedianMs: 3,
}
const slow = (changes = {}) =>
  summaryOf({
    sticky: { ...SLOW_ARM, p99Ms: 900, backlogAt6s: 400, ...changes.sticky },
    'sticky~AA': { ...SLOW_ARM, p99Ms: 900, backlogAt6s: 400, ...changes.control },
    adaptive: { ...SLOW_ARM, p99Ms: 300, backlogAt6s: 40, ...changes.adaptive },
  })

describe('Benchmarks > stickyBalance', () => {
  test('flags default to one healthy sticky run on the three brokers of the test cluster', () => {
    expect(parseArgs([])).toEqual({
      scenario: 'healthy',
      arm: 'sticky',
      records: 100000,
      concurrency: 200,
      brokers: 'localhost:9092,localhost:9095,localhost:9098',
      tree: ROOT,
      session: false,
      raw: null,
      runs: 8,
    })
    expect(
      parseArgs(['--session', '--scenario', 'slow-broker', '--raw', 'x', '--runs', '9'])
    ).toMatchObject({
      session: true,
      scenario: 'slow-broker',
      raw: path.resolve('x'),
      runs: 9,
    })
  })

  test.each([
    [['--scenario', 'busy'], '--scenario is healthy or slow-broker'],
    [['--arm', 'random'], '--arm is one of round-robin, sticky, adaptive'],
    [['--records', '0'], '--records needs a positive integer'],
    [['--session'], '--raw is needed with --session'],
    [['--session', '--raw', 'x', '--runs', '7'], '--runs needs a multiple of 4 with --session'],
    [
      ['--session', '--scenario', 'slow-broker', '--raw', 'x', '--runs', '6'],
      '--runs needs at least seven measured runs per arm',
    ],
    [['--pace', '1'], 'unknown or incomplete flag --pace'],
  ])('%j is refused', (argv, message) => {
    expect(() => parseArgs(argv)).toThrow(message)
  })

  test('the p99 is the nearest-rank quantile, and the balance the most over the fewest', () => {
    const values = Array.from({ length: 100 }, (_, i) => i + 1)

    expect([quantile(values, 0.99), quantile(values, 0.5), quantile([], 0.99)]).toEqual([99, 50, 0])
    expect([balanceOf([10, 12, 11]), balanceOf([5, 0])]).toEqual([1.2, Infinity])
  })

  test('the sticky arms map keyed records as DefaultPartitioner does, and the legacy hash does not', () => {
    expect(keyedIdentical(Partitioners, Partitioners.UniformStickyPartitioner())).toBe(1)
    expect(
      keyedIdentical(Partitioners, Partitioners.UniformStickyPartitioner({ adaptive: true }))
    ).toBe(1)
    expect(keyedIdentical(Partitioners, Partitioners.LegacyPartitioner)).toBe(0)
  })

  test('a healthy session passes when sticky and adaptive batch 2x round robin, stay within 1.25 and 3 %', () => {
    expect(judge('healthy', healthy())).toEqual({ verdict: 'passed', reasons: [] })
  })

  test.each([
    ['batching', { sticky: { recordsPerRequest: 11 } }, 'sticky batches less than 2x round robin'],
    ['balance', { adaptive: { balance: 1.3 } }, 'adaptive balance above 1.25'],
    ['p99', { sticky: { p99Ms: 10.31 } }, 'sticky p99 more than 3 % above round robin'],
    [
      'keyed placement',
      { adaptive: { keyedIdentical: 0 } },
      'adaptive maps keyed records differently',
    ],
    [
      'the call count control',
      { sticky: { partitionerCalls: 99 } },
      'sticky partitioner calls differ from the records',
    ],
  ])('a healthy session fails on %s', (_, changes, reason) => {
    expect(judge('healthy', healthy(changes))).toEqual({ verdict: 'failed', reasons: [reason] })
  })

  test('a void A/A control is inconclusive, never a pass', () => {
    expect(judge('healthy', { ...healthy(), void: true }).verdict).toBe('inconclusive')
  })

  test('a slow-broker session passes on the declared rules, with no balance rule', () => {
    expect(judge('slow-broker', slow({ adaptive: { balance: 9 } }))).toEqual({
      verdict: 'passed',
      reasons: [],
    })
  })

  test.each([
    ['p99', { adaptive: { p99Ms: 451 } }, 'adaptive p99 above half of sticky'],
    [
      'the backlog',
      { adaptive: { backlogAt6s: 101 } },
      'adaptive backlog at 6 s above a quarter of sticky',
    ],
    ['delivery', { adaptive: { delivered: 119999 } }, 'adaptive delivered less than sticky'],
    ['rejection', { adaptive: { rejected: 1 } }, 'adaptive rejected more than sticky'],
    ['unsettled work', { adaptive: { unsettled: 5 } }, 'adaptive left work unsettled'],
  ])('a slow-broker session fails on %s', (_, changes, reason) => {
    expect(judge('slow-broker', slow(changes))).toEqual({ verdict: 'failed', reasons: [reason] })
  })

  test('unsettled work in the A/A control as well is inconclusive', () => {
    expect(
      judge('slow-broker', slow({ adaptive: { unsettled: 5 }, control: { unsettled: 3 } })).verdict
    ).toBe('inconclusive')
  })

  test('a slow broker that was not 5x slower fails the positive control', () => {
    const summary = slow()
    summary.arms.sticky.slowMedianMs.median = 10
    summary.arms.sticky.valid.min = 0
    expect(judge('slow-broker', summary).reasons).toContain(
      'sticky has invalid workload, accounting, window or controls'
    )
  })
  test.each(['round-robin', 'round-robin~AA', 'sticky', 'adaptive'])(
    'healthy fails closed on retained heap in %s, including an outlier hidden by the median',
    (arm) => {
      const summary = healthy()
      summary.arms[arm].heapRetainedBytes.max = 100 * 1024 * 1024
      expect(judge('healthy', summary).verdict).toBe('failed')
    }
  )

  test.each(['sticky', 'sticky~AA', 'adaptive'])(
    'slow fails closed on missing or invalid controls in %s',
    (arm) => {
      const summary = slow()
      summary.arms[arm].valid.min = 0
      expect(judge('slow-broker', summary).verdict).toBe('failed')
      delete summary.arms[arm].valid
      expect(judge('slow-broker', summary).verdict).toBe('failed')
    }
  )

  test('a reference with unsettled work cannot truncate its latency sample into a pass', () => {
    const summary = slow({ sticky: { unsettled: 123 } })
    expect(judge('slow-broker', summary).verdict).toBe('failed')
    summary.arms['sticky~AA'].unsettled.max = 123
    expect(judge('slow-broker', summary).verdict).toBe('inconclusive')
  })

  const observation = (scenario = 'slow-broker') => ({
    records: scenario === 'healthy' ? 100000 : 120000,
    offered: scenario === 'healthy' ? 100000 : 120000,
    delivered: scenario === 'healthy' ? 100000 : 120000,
    rejected: 0,
    unsettled: 0,
    partitionerCalls: scenario === 'healthy' ? 100000 : 120000,
    keyedIdentical: 1,
    gcAvailable: 1,
    topologyValid: 1,
    heapRetainedBytes: 1048576,
    p99Ms: 10,
    recordsPerRequest: 30,
    ticks: 600,
    sampleAtMs: 6000,
    lastOfferAtMs: 5990,
    maxTickDriftMs: 0,
    backlogAt6s: 40,
    backlogMax: 80,
    drainAtMs: 12000,
    slowMedianMs: 300,
    otherMedianMs: 3,
  })

  test.each([
    ['heap', { heapRetainedBytes: 100 * 1024 * 1024 }],
    ['accounting', { delivered: 119999 }],
    ['offered count', { offered: 119999 }],
    ['partitioner calls', { partitionerCalls: 0 }],
    ['keyed placement', { keyedIdentical: 0 }],
    ['slow delay', { slowMedianMs: 3 }],
    ['missing delay', { otherMedianMs: 0 }],
    ['sample instant', { sampleAtMs: 6100 }],
    ['last offer outside window', { lastOfferAtMs: 6001 }],
    ['tick drift', { maxTickDriftMs: 100 }],
    ['submission window', { ticks: 599 }],
    ['collection', { gcAvailable: 0 }],
    ['topology', { topologyValid: 0 }],
    ['drain bound', { drainAtMs: 30100 }],
  ])('a raw observation fails validity on %s before medians are computed', (_, changes) => {
    expect(validRun('slow-broker', observation())).toBe(true)
    expect(validRun('slow-broker', { ...observation(), ...changes })).toBe(false)
  })

  test('healthy raw validity checks the workload, accounting and collected memory too', () => {
    expect(validRun('healthy', observation('healthy'))).toBe(true)
    expect(validRun('healthy', { ...observation('healthy'), rejected: 1 })).toBe(false)
    expect(
      validRun('healthy', { ...observation('healthy'), heapRetainedBytes: HEAP_LIMIT + 1 })
    ).toBe(false)
    expect(validRun('healthy', {})).toBe(false)
  })

  test('an independent timer samples at six seconds even when submission cannot complete its ticks', async () => {
    jest.useFakeTimers({ now: 0 })
    try {
      let offered = 0
      const window = new OfferWindow(
        () => Date.now(),
        (ms) => new Promise((resolve) => setTimeout(resolve, ms + 100))
      )
      const result = window.run(
        () => {
          offered += 1
        },
        () => offered
      )
      await jest.advanceTimersByTimeAsync(6200)
      const measured = await result
      expect(measured.sampleAtMs).toBe(6000)
      expect(measured.ticks).toBeLessThan(600)
      expect(measured.offered).toBe(measured.ticks * 200)
      expect(measured.backlogAt6s).toBeLessThanOrEqual(measured.offered)
      expect(validRun('slow-broker', { ...observation(), ...measured })).toBe(false)
    } finally {
      jest.useRealTimers()
    }
  })
  test('a slow session launches nine collected runs per arm plus warm-up and validates every child result', () => {
    const fs = require('fs')
    const write = jest.spyOn(fs, 'writeFileSync').mockImplementation(() => {})
    const append = jest.spyOn(fs, 'appendFileSync').mockImplementation(() => {})
    const spawn = jest
      .spyOn(require('child_process'), 'spawnSync')
      .mockImplementation((executable, args) => {
        expect(executable).toBe(process.execPath)
        expect(args[0]).toBe('--expose-gc')
        const arm = args[args.indexOf('--arm') + 1]
        return {
          status: 0,
          stdout: JSON.stringify({
            ...observation(),
            balance: 1.1,
            p99Ms: arm === 'adaptive' ? 300 : 900,
            backlogAt6s: arm === 'adaptive' ? 40 : 400,
            backlogMax: 800,
          }),
        }
      })
    try {
      jest.isolateModules(() => {
        const driver = require('../stickyBalance')
        const options = driver.parseArgs([
          '--session',
          '--scenario',
          'slow-broker',
          '--raw',
          'evidence',
          '--runs',
          '9',
        ])
        const result = driver.compare(options)
        expect(spawn).toHaveBeenCalledTimes(30)
        expect(result.verdict.verdict).toBe('passed')
        expect(result.summary.arms.adaptive.valid.min).toBe(1)
      })
    } finally {
      spawn.mockRestore()
      write.mockRestore()
      append.mockRestore()
    }
  })
})
