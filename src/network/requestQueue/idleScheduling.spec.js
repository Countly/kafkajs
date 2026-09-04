const RequestQueue = require('./index')
const { newLogger } = require('testHelpers')

const createRequestQueue = (args = {}) =>
  new RequestQueue({
    maxInFlightRequests: null,
    requestTimeout: 30000,
    enforceRequestTimeout: false,
    clientId: 'KafkaJS',
    broker: 'localhost:9092',
    logger: newLogger(),
    ...args,
  })

describe('Network > RequestQueue > scheduleCheckPendingRequests', () => {
  let requestQueue

  beforeEach(() => {
    jest.useFakeTimers()
    requestQueue = createRequestQueue()
  })

  afterEach(() => {
    requestQueue.destroy()
    jest.clearAllTimers()
    jest.useRealTimers()
  })

  it('schedules nothing when the queue is idle and not throttled', () => {
    requestQueue.scheduleCheckPendingRequests()

    expect(requestQueue.throttleCheckTimeoutId).toBeNull()
    expect(jest.getTimerCount()).toBe(0)
  })

  it('does not re-arm itself after every fulfilled request (#1704, #1556)', () => {
    // fulfillRequest -> checkPendingRequests -> scheduleCheckPendingRequests
    requestQueue.fulfillRequest({ correlationId: 1, payload: null, size: 0 })
    requestQueue.fulfillRequest({ correlationId: 2, payload: null, size: 0 })

    expect(jest.getTimerCount()).toBe(0)
  })

  it('never passes a negative delay to setTimeout (#1751, TimeoutNegativeWarning on Node >= 24)', () => {
    const setTimeoutSpy = jest.spyOn(global, 'setTimeout')

    requestQueue.pending.push({ correlationId: 3 })
    requestQueue.throttledUntil = Date.now() - 5000 // throttle already expired
    requestQueue.scheduleCheckPendingRequests()

    expect(setTimeoutSpy).toHaveBeenCalledTimes(1)
    expect(setTimeoutSpy.mock.calls[0][1]).toBeGreaterThanOrEqual(0)
    setTimeoutSpy.mockRestore()
  })

  it('schedules exactly once, at the remaining throttle time, while throttled', () => {
    const setTimeoutSpy = jest.spyOn(global, 'setTimeout')

    requestQueue.maybeThrottle(500)
    requestQueue.scheduleCheckPendingRequests()
    requestQueue.scheduleCheckPendingRequests()

    expect(setTimeoutSpy).toHaveBeenCalledTimes(1)
    expect(setTimeoutSpy.mock.calls[0][1]).toBeGreaterThan(0)
    expect(setTimeoutSpy.mock.calls[0][1]).toBeLessThanOrEqual(500)
    setTimeoutSpy.mockRestore()
  })

  it('keeps re-checking a pending queue that is blocked by the in-flight limit', () => {
    const queue = createRequestQueue({ maxInFlightRequests: 1 })
    queue.inflight.set(1, { correlationId: 1 })
    queue.pending.push({ correlationId: 2 })

    queue.scheduleCheckPendingRequests()

    expect(queue.throttleCheckTimeoutId).not.toBeNull()
    queue.destroy()
  })
})
