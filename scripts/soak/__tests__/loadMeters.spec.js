const {
  BrokerCpuProbe,
  FlowMeter,
  HostMeter,
  LagProbe,
  ThroughputSeries,
} = require('../loadMeters')

describe('load meters', () => {
  it('counts a series per key in whole seconds from its origin and finds the longest silence', () => {
    const flow = new FlowMeter()
    flow.add('acked', '1', 5, 100)
    flow.begin(1000)
    for (const at of [1000, 1500, 2100, 6900, 7000]) flow.add('acked', '1', 10, at)
    expect(flow.sum('acked', '1', 0, 8000)).toBe(50)
    expect(flow.sum('acked', '1', 500, 6500)).toBe(20)
    expect(flow.longestStall('acked', '1', 0, 8000)).toBe(3)
    expect(flow.longestStall('acked', '2', 0, 3000)).toBe(3)
    expect(flow.wholeSeconds(500, 6500)).toBe(5)
  })

  it('records the worst event-loop lag of each second and answers for a span', () => {
    let tick = null
    let cleared = false
    const clock = { t: 0, now: () => clock.t }
    const probe = new LagProbe({
      clock,
      intervalMs: 20,
      timers: {
        setInterval: (fn) => {
          tick = fn
          return { unref() {} }
        },
        clearInterval: () => {
          cleared = true
        },
      },
    })
    probe.start(0)
    for (const t of [20, 40, 560, 580, 1600, 1620]) {
      clock.t = t
      tick()
    }
    probe.stop()
    expect(cleared).toBe(true)
    expect(probe.maxBetween(0, 999)).toBe(500)
    expect(probe.maxBetween(1000, 1999)).toBe(1000)
    expect(probe.maxBetween(0, 2000)).toBe(1000)
    expect(probe.maxBetween(3000, 4000)).toBeNull()
  })
})

describe('load CPU meters', () => {
  it("reads this process's CPU as a share of one core and the machine's busy share of all its cores", () => {
    let usage = { user: 0, system: 0 }
    let cores = [
      { user: 0, nice: 0, sys: 0, irq: 0, idle: 0 },
      { user: 0, nice: 0, sys: 0, irq: 0, idle: 0 },
    ]
    const host = new HostMeter({
      cpuUsage: () => usage,
      cpuTimes: () => cores,
      loadavg: () => [3.5, 2, 1],
      cpus: 2,
    })
    const from = host.read()
    usage = { user: 1500000, system: 300000 }
    cores = [
      { user: 1500, nice: 0, sys: 300, irq: 0, idle: 200 },
      { user: 400, nice: 100, sys: 0, irq: 0, idle: 1500 },
    ]
    const to = host.read()
    expect(to).toEqual({
      processUs: 1800000,
      machineBusyMs: 2300,
      machineTotalMs: 4000,
      load1: 3.5,
    })
    expect(HostMeter.between(from, to, 2000)).toEqual({ coreShare: 0.9, machineBusyShare: 0.575 })
    expect(HostMeter.between(to, to, 0)).toEqual({ coreShare: null, machineBusyShare: null })
  })

  it('probes the broker containers until the window ends, keeping a mean and a maximum per container and counting failed calls', async () => {
    const window = new AbortController()
    const outputs = [
      'kafka1\t120.50%\nkafka2\t80.00%\n',
      new Error('docker: daemon not answering'),
      'kafka1\t60.50%\nkafka2\t100.00%\nnot a row\n',
    ]
    const sleeps = []
    const probe = new BrokerCpuProbe({
      stats: async () => {
        const next = outputs.shift()
        if (next instanceof Error) throw next
        if (outputs.length === 0) window.abort()
        return next
      },
      sleep: async (ms) => sleeps.push(ms),
      intervalMs: 5000,
    })
    probe.start(window.signal)
    await probe.stop()
    expect(sleeps).toEqual([5000, 5000, 5000])
    expect(probe.toJSON()).toEqual({
      containers: {
        kafka1: { samples: 2, meanPercent: 91, maxPercent: 120.5 },
        kafka2: { samples: 2, meanPercent: 90, maxPercent: 100 },
      },
      failures: 1,
      lastFailure: 'docker: daemon not answering',
    })
  })
})

describe('load throughput series', () => {
  it('counts each interval per group and keeps the longest stretch inside it with no acknowledgement', () => {
    const series = new ThroughputSeries({ intervalMs: 5000, spanMs: 20000 })
    series.count('sent', 5, 100)
    series.begin(1000)
    series.count('sent', 10, 1000, '1')
    series.count('acked', 4, 2000, '1')
    series.count('acked', 6, 3000, '2')
    series.count('failed', 1, 4000)
    series.count('consumed', 9, 6500, '1')
    series.count('acked', 3, 17000, '1')
    const { intervalMs, intervals, beyond, groups } = series.snapshot()
    expect({ intervalMs, intervals, beyond }).toEqual({ intervalMs: 5000, intervals: 4, beyond: 0 })
    expect(groups.all).toEqual({
      sent: [10, 0, 0, 0],
      acked: [10, 0, 0, 3],
      failed: [1, 0, 0, 0],
      consumed: [0, 9, 0, 0],
      firstAckMs: [1000, null, null, 16000],
      lastAckMs: [2000, null, null, 16000],
      maxGapMs: [3000, 5000, 5000, 4000],
    })
    expect(groups['1'].acked).toEqual([4, 0, 0, 3])
    expect(groups['1'].maxGapMs).toEqual([4000, 5000, 5000, 4000])
    expect(groups['2']).toMatchObject({ acked: [6, 0, 0, 0], sent: [0, 0, 0, 0] })
  })

  it('is sized for its span when created and only counts what falls past it', () => {
    const series = new ThroughputSeries({ intervalMs: 5000, spanMs: 12000 })
    series.begin(0)
    series.count('acked', 1, 1000)
    const cells = series.snapshot().groups.all.acked
    for (let at = 0; at < 1e6; at += 997) series.count('acked', 1, at)
    expect(series.snapshot().groups.all.acked).toHaveLength(cells.length)
    expect(cells).toHaveLength(3)
    expect(series.beyond).toBe(Math.ceil((1e6 - 15000) / 997))
  })

  it("merges another process's series: counts add, acknowledgement times widen, an interval's gap is the shortest any process saw", () => {
    const parent = new ThroughputSeries({ intervalMs: 5000, spanMs: 10000 })
    parent.begin(0)
    parent.count('consumed', 7, 6000)
    const child = (acks) => {
      const s = new ThroughputSeries({ intervalMs: 5000, spanMs: 10000 })
      s.begin(50000)
      for (const at of acks) s.count('acked', 1, 50000 + at)
      s.count('sent', acks.length, 50000)
      return s.snapshot()
    }
    parent.merge(child([500, 4500]))
    parent.merge(child([2500, 7000]))
    expect(parent.snapshot().groups.all).toEqual({
      sent: [4, 0],
      acked: [3, 1],
      failed: [0, 0],
      consumed: [0, 7],
      firstAckMs: [500, 7000],
      lastAckMs: [4500, 7000],
      maxGapMs: [2500, 3000],
    })
    expect(() =>
      parent.merge(new ThroughputSeries({ intervalMs: 1000, spanMs: 10000 }).snapshot())
    ).toThrow('a series of 10 intervals of 1000 ms cannot merge into 2 of 5000 ms')
  })
})
