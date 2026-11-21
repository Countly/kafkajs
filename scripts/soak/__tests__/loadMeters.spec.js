const { BrokerCpuProbe, FlowMeter, HostMeter, LagProbe } = require('../loadMeters')

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
