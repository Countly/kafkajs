const { Deadline, ExpiryTimer } = require('../deadline')
const { KafkaJSDeliveryTimeout } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

describe('delivery deadlines and their owner timer', () => {
  beforeEach(() => jest.useFakeTimers({ now: 100 }))
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  it('has an absolute deadline and never returns negative remaining time', () => {
    const deadline = new Deadline(100, 1000)
    expect(deadline.dueAt).toBe(1100)
    expect(deadline.remaining(100)).toBe(1000)
    expect(deadline.remaining(1100)).toBe(0)
    expect(deadline.remaining(2000)).toBe(0)
    expect(new Deadline(100, null).remaining()).toBe(Infinity)
    expect(new Deadline(100, undefined).dueAt).toBe(Infinity)
  })

  it('keeps one handle, moves earlier, and expires every due delivery before rearming', () => {
    const pending = [1100, 1500, 1500]
    const expired = []
    const timer = new ExpiryTimer((now) => {
      for (const due of pending.splice(0)) {
        if (due <= now) expired.push(due)
        else pending.push(due)
      }
      return Math.min(...pending)
    })
    timer.arm(1500)
    timer.arm(1800)
    timer.arm(1100)
    expect(jest.getTimerCount()).toBe(1)
    jest.advanceTimersByTime(999)
    expect(expired).toEqual([])
    jest.advanceTimersByTime(1)
    expect(expired).toEqual([1100])
    expect(jest.getTimerCount()).toBe(1)
    jest.advanceTimersByTime(400)
    expect(expired).toEqual([1100, 1500, 1500])
    expect(jest.getTimerCount()).toBe(0)
  })

  it('steps a delay longer than one timer interval without expiring early', () => {
    const deadline = new Deadline(Date.now(), 2147483647 + 1000)
    const expired = jest.fn()
    const timer = new ExpiryTimer((now) => {
      if (deadline.remaining(now) > 0) return deadline.dueAt
      expired()
      return Infinity
    })
    const schedule = jest.spyOn(global, 'setTimeout')
    timer.arm(deadline.dueAt)
    expect(schedule).toHaveBeenLastCalledWith(expect.any(Function), 2147483647)
    jest.advanceTimersByTime(2147483647)
    expect(expired).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(1)
    jest.advanceTimersByTime(999)
    expect(expired).not.toHaveBeenCalled()
    jest.advanceTimersByTime(1)
    expect(expired).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('rechecks the wall clock at the scheduled wake after backward and forward steps', () => {
    const dueAt = 1100
    const settled = jest.fn()
    const timer = new ExpiryTimer((now) => {
      if (now < dueAt) return dueAt
      settled()
      return Infinity
    })
    timer.arm(dueAt)
    jest.setSystemTime(-900)
    jest.advanceTimersByTime(1000)
    expect(settled).not.toHaveBeenCalled()
    jest.setSystemTime(2100)
    expect(settled).not.toHaveBeenCalled()
    jest.advanceTimersByTime(1000)
    expect(settled).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('clear is idempotent and an infinite deadline owns no handle', () => {
    const callback = jest.fn()
    const timer = new ExpiryTimer(callback)
    timer.arm(Infinity)
    expect(jest.getTimerCount()).toBe(0)
    timer.arm(100)
    timer.clear()
    timer.clear()
    jest.advanceTimersByTime(1000)
    expect(callback).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  it.each(['unsent', 'unknown'])(
    'copies protocol cause fields and keeps a fresh %s outcome',
    (outcome) => {
      const cause = createErrorFromCode(6)
      const error = new KafkaJSDeliveryTimeout('delivery expired', { cause, outcome })
      expect(error).toMatchObject({ outcome, type: 'NOT_LEADER_FOR_PARTITION', code: 6 })
      expect(error.cause).toBe(cause)
      expect(cause).not.toHaveProperty('outcome')
      expect(new KafkaJSDeliveryTimeout('other')).not.toHaveProperty('outcome')
    }
  )
})
