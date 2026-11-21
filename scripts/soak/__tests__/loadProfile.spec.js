const {
  EXIT,
  FAULT_SCHEDULES,
  LOAD_DEFAULTS,
  LatencyHistogram,
  OfferSchedule,
  parseLoadArgs,
  flowVerdict,
  phaseList,
  phaseMemory,
  rateReport,
  verdictOf,
} = require('../loadProfile')
const { FlowMeter } = require('../loadMeters')

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

  it('refuses unknown flags, bad values and a fault schedule the profile cannot hold', () => {
    expect(() =>
      parseLoadArgs(['--out', 'o', '--faults', 'slow-broker', '--duration', '120'])
    ).toThrow('fault schedule slow-broker needs --duration of at least 195')
    expect(() => parseLoadArgs(['--out', 'o', '--faults', 'many-topics'])).toThrow(
      'fault schedule many-topics needs at least 1000 partitions (--topics × --partitions)'
    )
    expect(() => parseLoadArgs(['--out', 'o', '--faults', 'slow'])).toThrow(
      'unknown fault schedule slow'
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

  it('applies the overrides of the slow-broker and many-topics schedules', () => {
    expect(parseLoadArgs(['--out', 'o', '--faults', 'slow-broker'])).toMatchObject({
      faults: 'slow-broker',
      ticksPerSecond: 10,
      leaderLanes: true,
      topicHeader: false,
    })
    expect(
      parseLoadArgs([
        '--out',
        'o',
        '--faults',
        'many-topics',
        '--topics',
        '2000',
        '--partitions',
        '1',
      ])
    ).toMatchObject({
      faults: 'many-topics',
      topics: 2000,
      partitions: 1,
      ticksPerSecond: 100,
      leaderLanes: false,
      topicHeader: true,
    })
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
})

describe('load profile slow-broker and many-topics schedules', () => {
  /** A fake clock and the scripted fault tools, each logging what it was asked and when. */
  const scripted = () => {
    const clock = {
      t: 1000,
      now() {
        return this.t
      },
    }
    const log = []
    const at = () => clock.t - 1000
    return {
      clock,
      log,
      sleep: async (ms, signal) => {
        if (!signal?.aborted) clock.t += ms
      },
      netem: {
        apply: async () => log.push(['delay', at()]),
        clear: async () => log.push(['clear', at()]),
      },
      bouncer: {
        step: async ({ service, kind, holdMs }) => {
          log.push([kind, at(), service])
          clock.t += holdMs + 700
          log.push(['un' + kind, at(), service])
          return { service, kind, outageMs: holdMs + 700 }
        },
        rollingBounce: async ({ between, topics }) => {
          log.push(['rolling', at(), between, topics.length])
          clock.t += 90000
          return ['kafka1', 'kafka2', 'kafka3'].map((service) => ({ service, kind: 'stop' }))
        },
      },
    }
  }

  it('slow-broker delays one broker, then freezes it past the request timeout, then lets it recover, inside the middle third', async () => {
    const { clock, log, sleep, netem, bouncer } = scripted()
    const records = await FAULT_SCHEDULES['slow-broker'].start({
      netem,
      bouncer,
      admin: {},
      topics: ['t'],
      windowMs: 600000,
      windowStart: 1000,
      clock,
      signal: new AbortController().signal,
      sleep,
    })
    expect(log).toEqual([
      ['delay', 200000],
      ['clear', 277500],
      ['pause', 277500, 'kafka2'],
      ['unpause', 323200, 'kafka2'],
    ])
    expect(records).toEqual([
      {
        phase: 'slow',
        service: 'kafka2',
        kind: 'delay',
        delayMs: 300,
        startMs: 200000,
        endMs: 277500,
      },
      {
        phase: 'frozen',
        service: 'kafka2',
        kind: 'pause',
        requestedMs: 277500,
        startMs: 277500,
        endMs: 323200,
        outageMs: 45700,
      },
      { phase: 'recovery', service: 'kafka2', kind: 'none', startMs: 323200, endMs: 400000 },
    ])
    expect(FAULT_SCHEDULES['slow-broker'].freezeMs).toBeGreaterThan(30000)
    expect(clock.t - 1000).toBe(400000)
  })

  it('slow-broker clears the delay and freezes nothing when the window ends during the slow phase', async () => {
    const { clock, log, netem, bouncer } = scripted()
    const window = new AbortController()
    let slept = 0
    const sleep = async (ms, signal) => {
      if (signal?.aborted) return
      if (++slept === 2) {
        window.abort()
        return
      }
      clock.t += ms
    }
    const records = await FAULT_SCHEDULES['slow-broker'].start({
      netem,
      bouncer,
      admin: {},
      topics: ['t'],
      windowMs: 600000,
      windowStart: 1000,
      clock,
      signal: window.signal,
      sleep,
    })
    expect(log).toEqual([
      ['delay', 200000],
      ['clear', 200000],
    ])
    expect(records.map((r) => r.phase)).toEqual(['slow'])
  })

  it('many-topics rolls one clean restart from a third of the window and records its phase', async () => {
    const { clock, log, sleep, bouncer } = scripted()
    const records = await FAULT_SCHEDULES['many-topics'].start({
      bouncer,
      admin: {},
      topics: Array.from({ length: 2000 }, (_, i) => `t${i}`),
      windowMs: 300000,
      windowStart: 1000,
      clock,
      signal: new AbortController().signal,
      sleep,
    })
    expect(log).toEqual([['rolling', 100000, 20000, 2000]])
    expect(records[0]).toEqual({
      phase: 'rolling-restart',
      kind: 'stop',
      startMs: 100000,
      endMs: 190000,
    })
    expect(records.slice(1).map((r) => r.service)).toEqual(['kafka1', 'kafka2', 'kafka3'])
  })

  it('checks the slow-broker delay tool before the run, and refuses a run without one', async () => {
    const check = jest.fn(async () => {
      throw new Error('sudo: a password is required')
    })
    await expect(FAULT_SCHEDULES['slow-broker'].check({ netem: { check } })).rejects.toThrow(
      'sudo: a password is required'
    )
    expect(check).toHaveBeenCalledTimes(1)
    await expect(FAULT_SCHEDULES['slow-broker'].check({ netem: null })).rejects.toThrow(
      'slow-broker needs a network delay tool'
    )
  })
})

describe('load profile per-leader flow verdict', () => {
  const PHASES = [
    { phase: 'slow', startMs: 200000, endMs: 280000 },
    { phase: 'frozen', startMs: 280000, endMs: 325000 },
    { phase: 'recovery', startMs: 325000, endMs: 400000 },
  ]
  const RATES = { 1: 100, 2: 100, 3: 100 }

  /** Acknowledges every leader's offered rate each second, except where `gap` says not to. */
  const meter = (gap = () => false) => {
    const flow = new FlowMeter()
    flow.begin(0)
    for (let s = 0; s < 600; s++)
      for (const leader of ['1', '2', '3'])
        if (!gap(leader, s)) flow.add('acked', leader, RATES[leader], s * 1000 + 500)
    return flow
  }
  const judge = (flow) =>
    flowVerdict({ phases: phaseList(PHASES, 600000), flow, rates: RATES, slowNode: '2' })

  it("keeps flowing when only the slow broker's partitions stall", () => {
    const result = judge(meter((leader, s) => leader === '2' && s >= 200 && s < 325))
    expect(result.reasons).toEqual([])
    expect(result.verdict).toBe('KEPT_FLOWING')
    const frozen = result.table.find((r) => r.phase === 'frozen' && r.leader === '2')
    expect(frozen).toMatchObject({ ackedRate: 0, longestStallSeconds: 45 })
    expect(result.table.map((r) => r.phase)).toEqual(
      ['baseline', 'slow', 'frozen', 'recovery', 'after'].flatMap((p) => [p, p, p])
    )
  })

  it("stalls when another broker's partitions stop for longer than the frozen-phase bound", () => {
    const result = judge(meter((leader, s) => leader !== '2' && s >= 280 && s < 301))
    expect(result.verdict).toBe('STALLED')
    expect(result.reasons).toEqual([
      'frozen: leader 1 stalled 21 s (most 20 s)',
      'frozen: leader 3 stalled 21 s (most 20 s)',
    ])
    expect(judge(meter((leader, s) => leader !== '2' && s >= 280 && s < 300)).verdict).toBe(
      'KEPT_FLOWING'
    )
  })

  it("stalls when another broker's partitions fall under 90 % of their offered rate while one broker is slow", () => {
    const result = judge(meter((leader, s) => leader === '3' && s >= 200 && s < 280 && s % 5 === 0))
    expect(result.verdict).toBe('STALLED')
    expect(result.reasons).toEqual([
      'slow: leader 3 acknowledged 80 % of its offered rate (least 90 %)',
    ])
  })

  it("fails recovery when the slow broker's partitions do not flow again after the freeze", () => {
    const result = judge(meter((leader, s) => leader === '2' && s >= 200))
    expect(result.verdict).toBe('STALLED')
    expect(result.reasons).toEqual([
      'recovery: leader 2 acknowledged 0 % of its offered rate (least 90 %)',
    ])
  })

  it('is inconclusive when a judged phase is too short to measure or the slow broker led nothing', () => {
    const short = flowVerdict({
      phases: phaseList([{ phase: 'recovery', startMs: 300000, endMs: 303000 }], 600000),
      flow: meter(),
      rates: RATES,
      slowNode: '2',
    })
    expect(short).toMatchObject({
      verdict: 'INCONCLUSIVE',
      reasons: ['recovery: 3 whole seconds, too short to judge (least 5)'],
    })
    const none = flowVerdict({
      phases: phaseList(PHASES, 600000),
      flow: meter(),
      rates: { 1: 100, 3: 100 },
      slowNode: '2',
    })
    expect(none).toMatchObject({
      verdict: 'INCONCLUSIVE',
      reasons: ['leader 2 led no partition: nothing was slowed'],
    })
  })

  it('exits 4 on a stalled flow with the product passing, and lets a product failure take precedence', () => {
    expect(verdictOf({ oracle: clean(), rate: reached, flow: 'STALLED' })).toEqual({
      ok: false,
      product: 'PASS',
      profile: 'REACHED',
      flow: 'STALLED',
      exitCode: EXIT.flowStalled,
    })
    expect(EXIT.flowStalled).toBe(4)
    const lossy = clean()
    lossy.findings.loss.count = 1
    expect(verdictOf({ oracle: lossy, rate: reached, flow: 'STALLED' }).exitCode).toBe(EXIT.product)
    expect(verdictOf({ oracle: clean(), rate: reached, flow: 'INCONCLUSIVE' }).exitCode).toBe(
      EXIT.inconclusive
    )
    expect(verdictOf({ oracle: clean(), rate: reached, flow: 'KEPT_FLOWING' })).toEqual({
      ok: true,
      product: 'PASS',
      profile: 'REACHED',
      flow: 'KEPT_FLOWING',
      exitCode: EXIT.pass,
    })
  })

  it('reports the memory peak of each phase and the level at its end', () => {
    const samples = [10, 20, 30, 40, 50, 60].map((s, i) => ({
      atMs: s * 1000,
      heapUsed: [5, 9, 7, 30, 12, 11][i],
      rss: [50, 90, 70, 300, 120, 110][i],
    }))
    expect(
      phaseMemory(samples, phaseList([{ phase: 'frozen', startMs: 25000, endMs: 45000 }], 60000))
    ).toEqual([
      {
        phase: 'baseline',
        startMs: 0,
        endMs: 25000,
        peakHeapUsed: 9,
        peakRss: 90,
        endHeapUsed: 9,
        endRss: 90,
      },
      {
        phase: 'frozen',
        startMs: 25000,
        endMs: 45000,
        peakHeapUsed: 30,
        peakRss: 300,
        endHeapUsed: 30,
        endRss: 300,
      },
      {
        phase: 'after',
        startMs: 45000,
        endMs: 60000,
        peakHeapUsed: 12,
        peakRss: 120,
        endHeapUsed: 11,
        endRss: 110,
      },
    ])
  })
})
