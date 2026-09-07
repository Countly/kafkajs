const PollTimer = require('../pollTimer')

describe('Consumer > PollTimer', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  it('expires once the deadline has passed, and not before', () => {
    const timer = new PollTimer({ pollDeadline: 1000 })

    jest.advanceTimersByTime(999)
    expect(timer.expired()).toBe(false)

    jest.advanceTimersByTime(2)
    expect(timer.expired()).toBe(true)
    expect(timer.elapsed()).toBeGreaterThan(1000)
  })

  it('is monotonic: a wall-clock jump does not expire a healthy consumer', () => {
    const timer = new PollTimer({ pollDeadline: 1000 })

    // Modern fake timers move performance.now() with advanceTimersByTime but NOT with
    // setSystemTime - which is exactly the difference this timer relies on.
    jest.setSystemTime(new Date(Date.now() + 3600000))

    expect(timer.expired()).toBe(false)
  })

  it('poll() and reset() both restart the deadline', () => {
    const timer = new PollTimer({ pollDeadline: 1000 })

    jest.advanceTimersByTime(900)
    timer.poll()
    jest.advanceTimersByTime(900)
    expect(timer.expired()).toBe(false)

    jest.advanceTimersByTime(200)
    expect(timer.expired()).toBe(true)

    timer.reset()
    expect(timer.expired()).toBe(false)
  })

  it('stops reporting after stop() and reports again after reset()', () => {
    const timer = new PollTimer({ pollDeadline: 10 })

    jest.advanceTimersByTime(100)
    expect(timer.expired()).toBe(true)

    timer.stop()
    expect(timer.expired()).toBe(false)

    timer.reset()
    jest.advanceTimersByTime(100)
    expect(timer.expired()).toBe(true)
  })

  it('reports the configured deadline', () => {
    expect(new PollTimer({ pollDeadline: 300000 }).deadline()).toEqual(300000)
  })
})
