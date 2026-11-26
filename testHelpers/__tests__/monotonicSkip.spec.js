const { skipMonotonicTo } = require('../monotonicSkip')

describe('testHelpers > skipMonotonicTo', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0 })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  test('shows the given monotonic time without moving the wall clock or running a timer', () => {
    const fired = jest.fn()
    setTimeout(fired, 10)

    skipMonotonicTo(1000)

    expect(performance.now()).toBe(1000)
    expect(Date.now()).toBe(0)
    expect(jest.getTimerCount()).toBe(1)
    expect(fired).not.toHaveBeenCalled()
  })

  test('keeps advancing with the fake timers afterwards', () => {
    skipMonotonicTo(1000)

    jest.advanceTimersByTime(5)

    expect(performance.now()).toBe(1005)
    expect(Date.now()).toBe(5)
  })

  test('a later skip moves it again, to a fractional time as well', () => {
    skipMonotonicTo(1000)
    jest.advanceTimersByTime(5)

    skipMonotonicTo(2000.3)

    expect(performance.now()).toBeCloseTo(2000.3, 6)
  })

  test('a new fake clock starts again from zero', () => {
    skipMonotonicTo(1000)

    jest.useRealTimers()
    jest.useFakeTimers({ now: 0 })

    expect(performance.now()).toBe(0)
  })
})
