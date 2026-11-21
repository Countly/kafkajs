const {
  EXIT,
  FAULT_SCHEDULES,
  LOAD_DEFAULTS,
  LatencyHistogram,
  OfferSchedule,
  parseLoadArgs,
  rateReport,
  verdictOf,
} = require('../loadProfile')

const clean = () => ({ complete: true, findings: { loss: { count: 0 }, duplicate: { count: 0 } } })
const reached = { reached: true }
const notReached = { reached: false }

describe('load profile arguments', () => {
  it('applies the defaults and the given flags', () => {
    expect(parseLoadArgs(['--out', 'o'])).toEqual({ ...LOAD_DEFAULTS, out: 'o' })
    expect(
      parseLoadArgs([
        '--out',
        'o',
        '--rate',
        '50000',
        '--record-bytes',
        '200',
        '--compression',
        'lz4',
        '--faults',
        'rolling-restarts',
        '--producer-kinds',
        'idempotent,transactional',
        '--replication-factor',
        '1',
      ])
    ).toMatchObject({
      rate: 50000,
      recordBytes: 200,
      compression: 'lz4',
      faults: 'rolling-restarts',
      producerKinds: ['idempotent', 'transactional'],
      replicationFactor: 1,
      minInsyncReplicas: 1,
    })
  })

  it('refuses the extension-point fault schedules, unknown flags and bad values', () => {
    expect(() => parseLoadArgs(['--out', 'o', '--faults', 'slow-broker'])).toThrow(
      'fault schedule slow-broker is not implemented yet'
    )
    expect(() => parseLoadArgs(['--out', 'o', '--faults', 'many-topics'])).toThrow(
      'fault schedule many-topics is not implemented yet'
    )
    expect(() => parseLoadArgs(['--out', 'o', '--rate', '0'])).toThrow(
      '--rate must be a positive integer'
    )
    expect(() => parseLoadArgs(['--out', 'o', '--compression', 'brotli'])).toThrow(
      'unknown codec brotli'
    )
    expect(() => parseLoadArgs(['--rate', '5'])).toThrow('--out is required')
    expect(() => parseLoadArgs(['--out', 'o', '--bogus', '1'])).toThrow('unknown flag --bogus')
  })
})

describe('load profile offer schedule and latency', () => {
  it('makes records due on a deadline schedule, whether or not they were taken', () => {
    const schedule = new OfferSchedule({ rate: 1000, start: 100 })
    expect(schedule.due(100)).toBe(0)
    expect(schedule.due(110)).toBe(10)
    const { first, dueAt } = schedule.take(4)
    expect(first).toBe(0)
    expect([dueAt(0), dueAt(3)]).toEqual([101, 104])
    expect(schedule.due(110)).toBe(6)
    expect(schedule.due(1100)).toBe(996)
  })

  it('reports percentiles within the 2 % bucket width, from a fixed-size histogram', () => {
    const histogram = new LatencyHistogram()
    const size = histogram.counts.length
    for (let i = 1; i <= 100000; i++) histogram.record(i / 100)
    expect(histogram.counts.length).toBe(size)
    const { samples, p50, p99, max } = histogram.toJSON()
    expect(samples).toBe(100000)
    expect(p50).toBeGreaterThanOrEqual(500)
    expect(p50).toBeLessThanOrEqual(500 * 1.02)
    expect(p99).toBeGreaterThanOrEqual(990)
    expect(p99).toBeLessThanOrEqual(1000)
    expect(max).toBe(1000)
    expect(new LatencyHistogram().percentile(50)).toBeNull()
  })
})

describe('load profile rate report and verdict', () => {
  it('reports the rate reached only when sent and acknowledged both meet the fraction', () => {
    const base = { rate: 1000, windowMs: 10000, offered: 10000, targetFraction: 0.95 }
    expect(rateReport({ ...base, sent: 9600, acked: 9500 })).toEqual({
      reached: true,
      target: 1000,
      offeredRate: 1000,
      sentRate: 960,
      ackedRate: 950,
      shortfall: null,
    })
    expect(rateReport({ ...base, sent: 9000, acked: 9000 })).toMatchObject({
      reached: false,
      shortfall: 'sent 9000 of 10000 offered; acknowledged 9000 of 10000 offered',
    })
    expect(rateReport({ ...base, sent: 10000, acked: 8000, failed: 2000 })).toMatchObject({
      reached: false,
      shortfall: 'acknowledged 8000 of 10000 offered (2000 rejected)',
    })
    expect(rateReport({ ...base, offered: 0, sent: 0, acked: 0 })).toMatchObject({
      reached: false,
      shortfall: 'nothing was offered',
    })
  })

  it('reports a rate not reached as a failure of the profile, not of the product', () => {
    expect(verdictOf({ oracle: clean(), rate: notReached })).toEqual({
      ok: false,
      product: 'PASS',
      profile: 'RATE_NOT_REACHED',
      exitCode: EXIT.rateNotReached,
    })
    expect(verdictOf({ oracle: clean(), rate: reached })).toEqual({
      ok: true,
      product: 'PASS',
      profile: 'REACHED',
      exitCode: EXIT.pass,
    })
  })

  it('fails the product on a finding, and is inconclusive on a read problem alone', () => {
    const lossy = clean()
    lossy.findings.loss.count = 1
    expect(verdictOf({ oracle: lossy, rate: notReached })).toMatchObject({
      product: 'FAIL',
      exitCode: EXIT.product,
    })
    expect(verdictOf({ oracle: { ...lossy, complete: false }, rate: reached })).toMatchObject({
      product: 'INCONCLUSIVE',
      exitCode: EXIT.inconclusive,
    })
    const duplicated = {
      complete: false,
      findings: { ...clean().findings, duplicate: { count: 2 } },
    }
    expect(verdictOf({ oracle: duplicated, rate: reached }).product).toBe('FAIL')
    const unparseable = {
      complete: true,
      findings: { ...clean().findings, unparseable: { count: 1 } },
    }
    expect(verdictOf({ oracle: unparseable, rate: reached }).product).toBe('INCONCLUSIVE')
    expect(verdictOf({ oracle: null, rate: reached }).product).toBe('INCONCLUSIVE')
    expect(verdictOf({ oracle: clean(), rate: reached, error: 'driver: boom' }).product).toBe(
      'INCONCLUSIVE'
    )
  })
})

describe('load profile fault schedules', () => {
  /** A bouncer that records its calls and the fake time of each. */
  const scripted = () => {
    const time = { t: 0 }
    const calls = []
    return {
      time,
      calls,
      sleep: async (ms, signal) => {
        if (!signal?.aborted) time.t += ms
      },
      bouncer: {
        rollingBounce: async ({ hard = false, between, topics }) => {
          calls.push({ at: time.t, hard, between, topics })
          return [{ service: 'kafka1', kind: hard ? 'kill' : 'stop', outageMs: 1 }]
        },
      },
    }
  }

  it('rolls clean restarts from a third of the window, then hard restarts', async () => {
    const { time, calls, sleep, bouncer } = scripted()
    const outages = await FAULT_SCHEDULES['rolling-restarts'].start({
      bouncer,
      admin: {},
      topics: ['t'],
      windowMs: 600000,
      signal: new AbortController().signal,
      sleep,
    })
    expect(calls).toEqual([
      { at: 200000, hard: false, between: 20000, topics: ['t'] },
      { at: 220000, hard: true, between: 20000, topics: ['t'] },
    ])
    expect(outages.map((o) => o.kind)).toEqual(['stop', 'kill'])
    expect(time.t).toBe(220000)
  })

  it('starts no restart once the window has ended', async () => {
    const { calls, sleep, bouncer } = scripted()
    const window = new AbortController()
    window.abort()
    const outages = await FAULT_SCHEDULES['rolling-restarts'].start({
      bouncer,
      admin: {},
      topics: ['t'],
      windowMs: 600000,
      signal: window.signal,
      sleep,
    })
    expect(outages).toEqual([])
    expect(calls).toEqual([])
  })

  it('names the slow-broker and many-topics schedules as extension points with no start', () => {
    expect(FAULT_SCHEDULES['slow-broker']).toEqual({ extensionPoint: true })
    expect(FAULT_SCHEDULES['many-topics']).toEqual({ extensionPoint: true })
  })
})
