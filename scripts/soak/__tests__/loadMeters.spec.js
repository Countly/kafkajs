const { FlowMeter, LagProbe } = require('../loadMeters')

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
