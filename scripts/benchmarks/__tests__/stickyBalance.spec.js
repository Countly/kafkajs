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
  accountedRun,
  OfferWindow,
  HEAP_LIMIT,
  tallyRejection,
} = require('../stickyBalance')
const {
  KafkaJSBufferExhaustedError,
  KafkaJSDeliveryTimeout,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../../src/errors')

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
    refused: 0,
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
    slowAnswerCount: 40,
    otherAnswerCount: 1200,
  })

  test.each([
    ['heap', { heapRetainedBytes: 100 * 1024 * 1024 }],
    ['accounting', { delivered: 119999 }],
    ['offered count', { offered: 119999 }],
    ['partitioner calls', { partitionerCalls: 0 }],
    ['keyed placement', { keyedIdentical: 0 }],
    ['slow delay', { slowMedianMs: 3 }],
    ['missing delay', { slowMedianMs: 4, otherMedianMs: 0 }],
    ['sample instant', { sampleAtMs: 6100 }],
    ['last offer outside window', { lastOfferAtMs: 6001 }],
    ['tick drift', { maxTickDriftMs: 100 }],
    ['submission window', { ticks: 599 }],
    ['collection', { gcAvailable: 0 }],
    ['topology', { topologyValid: 0 }],
    ['drain bound', { drainAtMs: 130100 }],
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

  test('a buffer refusal is counted as refused and never as a partitioned send', () => {
    const counts = { refused: 0, byClass: {} }
    const queueFull = () => new KafkaJSBufferExhaustedError('no room', { reason: 'queue-full' })

    tallyRejection(queueFull(), counts)
    tallyRejection(queueFull(), counts)
    tallyRejection(new KafkaJSNumberOfRetriesExceeded(new Error('down'), {}), counts)
    tallyRejection(new KafkaJSDeliveryTimeout('late', { outcome: 'unknown' }), counts)

    expect(counts).toEqual({
      refused: 2,
      byClass: {
        'KafkaJSBufferExhaustedError:queue-full': 2,
        KafkaJSNumberOfRetriesExceeded: 1,
        KafkaJSDeliveryTimeout: 1,
      },
    })
  })

  /**
   * A real measured slow-broker sticky observation with a fast-broker median of 0 ms, completed as
   * a drain to settlement would end it. Test input, not run evidence: its rejections are taken as
   * buffer refusals, the sends still open at 30 s as delivered by 52 s, and the answer counts the
   * recorded run did not keep are made up.
   */
  const settledSticky = () => {
    const recorded = require('fs')
      .readFileSync(
        path.join(__dirname, 'fixtures/stickyBalanceSession/slow-broker-observations.jsonl'),
        'utf8'
      )
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))
      .find(
        ({ label, warmup, result }) => label === 'sticky' && !warmup && !result.otherMedianMs
      ).result
    return {
      ...recorded,
      refused: recorded.rejected,
      delivered: recorded.delivered + recorded.unsettled,
      unsettled: 0,
      drainAtMs: 52000,
      slowAnswerCount: 40,
      otherAnswerCount: 1200,
    }
  }

  test('validRun accepts a slow-broker run whose missing partitioner calls are exactly its buffer refusals', () => {
    const run = settledSticky()
    expect(run.partitionerCalls).toBeLessThan(run.offered)
    expect(run.otherMedianMs).toBe(0)

    expect(validRun('slow-broker', run)).toBe(true)
  })

  test('validRun refuses partitioner calls other than the offered records minus the buffer refusals', () => {
    const run = settledSticky()

    expect(validRun('slow-broker', { ...run, partitionerCalls: run.partitionerCalls + 1 })).toBe(
      false
    )
    expect(validRun('slow-broker', { ...run, refused: 0 })).toBe(false)
    expect(
      validRun('slow-broker', {
        ...run,
        refused: run.rejected + 1,
        partitionerCalls: run.partitionerCalls - 1,
      })
    ).toBe(false)
    expect(validRun('slow-broker', { ...run, refused: undefined })).toBe(false)
  })

  test('accountedRun keeps a run whose window drifted, and refuses a send neither partitioned nor refused', () => {
    const drifted = require('fs')
      .readFileSync(
        path.join(__dirname, 'fixtures/stickyBalanceSession/slow-broker-drift.jsonl'),
        'utf8'
      )
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line).result)
      .find(({ maxTickDriftMs }) => maxTickDriftMs > 50)
    expect(drifted).toMatchObject({ partitionerCalls: 83502, refused: 36498, unsettled: 0 })

    expect(validRun('slow-broker', drifted)).toBe(false)
    expect(accountedRun(drifted)).toBe(true)
    expect(accountedRun({ ...drifted, partitionerCalls: 83501 })).toBe(false)
    expect(accountedRun({ ...drifted, partitionerCalls: 83503 })).toBe(false)
    expect(accountedRun({ ...drifted, delivered: 83501 })).toBe(false)
    expect(accountedRun({ ...drifted, refused: 36499 })).toBe(false)
    expect(accountedRun({ ...drifted, refused: undefined })).toBe(false)
  })

  test('validRun reads a fast-broker median of 0 ms as under 1 ms', () => {
    const at = (slowMedianMs) => ({ ...observation(), slowMedianMs, otherMedianMs: 0 })

    expect([301, 5, 4].map((ms) => validRun('slow-broker', at(ms)))).toEqual([true, true, false])
  })

  test('validRun needs Produce answers from the slow broker and from the others, a 0 ms median included', () => {
    const run = settledSticky()

    expect(validRun('slow-broker', run)).toBe(true)
    expect(validRun('slow-broker', { ...run, otherAnswerCount: 0 })).toBe(false)
    expect(validRun('slow-broker', { ...run, slowAnswerCount: 0 })).toBe(false)
    expect(validRun('slow-broker', { ...run, otherAnswerCount: undefined })).toBe(false)
    expect(validRun('slow-broker', { ...run, slowAnswerCount: undefined })).toBe(false)
  })

  test('validRun refuses unsettled work and a drain past 130 s', () => {
    const open = { ...observation(), delivered: 119999, unsettled: 1 }

    expect(validRun('slow-broker', { ...open, drainAtMs: 30000 })).toBe(false)
    expect(validRun('slow-broker', { ...open, drainAtMs: 130000 })).toBe(false)
    expect(validRun('slow-broker', { ...observation(), drainAtMs: 129000 })).toBe(true)
    expect(validRun('slow-broker', { ...observation(), drainAtMs: 130051 })).toBe(false)
  })

  test('a slow-broker session uses a 10 % A/A limit and a healthy one 3 %', () => {
    const fs = require('fs')
    const write = jest.spyOn(fs, 'writeFileSync').mockImplementation(() => {})
    const append = jest.spyOn(fs, 'appendFileSync').mockImplementation(() => {})
    const spawn = jest
      .spyOn(require('child_process'), 'spawnSync')
      .mockImplementation((_executable, args) => ({
        status: 0,
        stdout: JSON.stringify({
          ...observation(args[args.indexOf('--scenario') + 1]),
          balance: 1.1,
        }),
      }))
    /** @returns {object} the options `compare` gave `summarize` for a session of `scenario` */
    const limitOf = (scenario, runs) => {
      let options
      jest.isolateModules(() => {
        const session = jest.requireActual('../session')
        const summarize = jest.fn(session.summarize)
        jest.doMock('../session', () => ({ ...session, summarize }))
        const driver = require('../stickyBalance')
        driver.compare(
          driver.parseArgs(['--session', '--scenario', scenario, '--raw', 'x', '--runs', runs])
        )
        expect(summarize).toHaveBeenCalledTimes(1)
        options = summarize.mock.calls[0][1]
      })
      return options
    }
    try {
      expect(limitOf('slow-broker', '9')).toMatchObject({ voidOver: 10, controlOn: ['p99Ms'] })
      expect(limitOf('healthy', '8')).toMatchObject({ voidOver: 3, controlOn: ['p99Ms'] })
    } finally {
      jest.dontMock('../session')
      spawn.mockRestore()
      write.mockRestore()
      append.mockRestore()
    }
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

  /**
   * Runs a mocked session: every child prints a valid observation of its arm.
   * @returns {{ spawned: string[], labels: string[], report: object }} the arm of every child in
   *   launch order, the label of every kept observation and the printed session report
   */
  const mockedSession = (argv) => {
    const fs = require('fs')
    const labels = []
    const write = jest.spyOn(fs, 'writeFileSync').mockImplementation(() => {})
    const append = jest
      .spyOn(fs, 'appendFileSync')
      .mockImplementation((_file, line) => labels.push(JSON.parse(line).label))
    const spawned = []
    const spawn = jest
      .spyOn(require('child_process'), 'spawnSync')
      .mockImplementation((_executable, args) => {
        const arm = args[args.indexOf('--arm') + 1]
        const scenario = args[args.indexOf('--scenario') + 1]
        spawned.push(arm)
        return {
          status: 0,
          stdout: JSON.stringify({ ...observation(scenario), balance: 1.1, backlogMax: 80 }),
        }
      })
    try {
      let report
      jest.isolateModules(() => {
        const driver = require('../stickyBalance')
        const options = driver.parseArgs(argv)
        const { summary, verdict } = driver.compare(options)
        report = driver.sessionReport(options.scenario, summary, verdict)
      })
      return { spawned, labels, report }
    } finally {
      spawn.mockRestore()
      write.mockRestore()
      append.mockRestore()
    }
  }
  const count = (values) =>
    values.reduce((counts, value) => ({ ...counts, [value]: (counts[value] ?? 0) + 1 }), {})

  test('healthy session runs round-robin, sticky, adaptive and an A/A round-robin arm', () => {
    const argv = ['--session', '--scenario', 'healthy', '--raw', 'evidence', '--runs', '8']

    const { spawned, labels, report } = mockedSession(argv)

    expect(count(spawned)).toEqual({ 'round-robin': 18, sticky: 9, adaptive: 9 })
    expect(count(labels)).toEqual({
      'round-robin': 9,
      sticky: 9,
      adaptive: 9,
      'round-robin~AA': 9,
    })
    expect(report.scenario).toBe('healthy')
    expect(Object.keys(report.arms).sort()).toEqual(
      ['adaptive', 'round-robin', 'round-robin~AA', 'sticky'].sort()
    )
    expect(report.control).toEqual({
      label: 'round-robin~AA',
      against: 'round-robin',
      percent: { p99Ms: 0 },
    })
    expect(report.void).toBe(false)
  })

  test('slow-broker session runs sticky, adaptive and an A/A sticky arm', () => {
    const argv = (runs) => ['--session', '--scenario', 'slow-broker', '--raw', 'x', '--runs', runs]
    expect(() => parseArgs(argv('8'))).toThrow('--runs needs a multiple of 3 with --session')

    const { spawned, labels, report } = mockedSession(argv('9'))

    expect(count(spawned)).toEqual({ sticky: 20, adaptive: 10 })
    expect(count(labels)).toEqual({ sticky: 10, adaptive: 10, 'sticky~AA': 10 })
    expect(report.scenario).toBe('slow-broker')
    expect(Object.keys(report.arms).sort()).toEqual(['adaptive', 'sticky', 'sticky~AA'])
    expect(report.control).toEqual({ label: 'sticky~AA', against: 'sticky', percent: { p99Ms: 0 } })
    expect(report.arms.adaptive.slowMedianMs.median).toBe(300)
  })

  test.each([
    [
      'a failed child',
      {
        status: 1,
        signal: null,
        stdout: 'child stdout evidence\n',
        stderr: 'child stderr evidence\n',
      },
      /the round-robin run failed: child stderr evidence/,
    ],
    [
      'a child whose output is not JSON',
      { status: 0, signal: null, stdout: 'not json\n', stderr: 'parse evidence\n' },
      /JSON/,
    ],
    [
      'a child that never ran',
      { status: null, signal: 'SIGTERM', error: new Error('spawnSync node ETIMEDOUT') },
      /the round-robin run failed/,
    ],
  ])(
    'a session keeps the arguments, exit and output of every child attempt before it reads %s',
    (_case, failure, thrown) => {
      const fs = require('fs')
      const os = require('os')
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sticky-attempts-'))
      const raw = path.join(dir, 'fork-run-1.jsonl')
      const calls = []
      const spawn = jest
        .spyOn(require('child_process'), 'spawnSync')
        .mockImplementation((_executable, args) => {
          calls.push(args)
          if (calls.length === 4) return failure
          return {
            status: 0,
            signal: null,
            stdout: `${JSON.stringify({ ...observation('healthy'), balance: 1.1 })}\n`,
            stderr: `warning ${calls.length}\n`,
          }
        })
      try {
        jest.isolateModules(() => {
          const driver = require('../stickyBalance')
          const options = driver.parseArgs(['--session', '--raw', raw, '--runs', '8'])
          expect(() => driver.compare(options)).toThrow(thrown)
        })
        const files = fs.readdirSync(dir).sort()
        expect(files).toEqual([
          'fork-run-1.jsonl',
          ...[1, 2, 3, 4].map((n) => `fork-run-1.jsonl.attempt-${n}.json`),
        ])
        const receipt = (n) =>
          JSON.parse(fs.readFileSync(path.join(dir, `fork-run-1.jsonl.attempt-${n}.json`), 'utf8'))
        const order = [
          ['round-robin', 'round-robin'],
          ['sticky', 'sticky'],
          ['adaptive', 'adaptive'],
          ['round-robin~AA', 'round-robin'],
        ]
        order.slice(0, 3).forEach(([label, arm], i) =>
          expect(receipt(i + 1)).toEqual({
            attempt: i + 1,
            label,
            arm,
            args: calls[i],
            status: 0,
            signal: null,
            error: null,
            stdout: expect.stringContaining('"partitionerCalls":100000'),
            stderr: `warning ${i + 1}\n`,
          })
        )
        expect(calls[3]).toEqual(expect.arrayContaining(['--arm', 'round-robin', '--scenario']))
        expect(receipt(4)).toEqual({
          attempt: 4,
          label: 'round-robin~AA',
          arm: 'round-robin',
          args: calls[3],
          status: failure.status,
          signal: failure.signal,
          error: failure.error?.message ?? null,
          stdout: failure.stdout ?? '',
          stderr: failure.stderr ?? '',
        })
        const kept = fs.readFileSync(raw, 'utf8').trim().split('\n').map(JSON.parse)
        expect(kept.map(({ label }) => label)).toEqual(['round-robin', 'sticky', 'adaptive'])
      } finally {
        spawn.mockRestore()
        fs.rmSync(dir, { recursive: true, force: true })
      }
    }
  )
})
