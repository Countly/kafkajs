const path = require('node:path')
const { spawnSync } = require('node:child_process')
const RequestQueue = require('./index')
const { newLogger } = require('testHelpers')

describe('Network > RequestQueue > timer release', () => {
  it('keeps its field layout when timeout enforcement is disabled', () => {
    const queue = new RequestQueue({
      logger: newLogger(),
      enforceRequestTimeout: false,
      requestTimeout: 100,
    })
    const fields = Reflect.ownKeys(queue)

    queue.scheduleRequestTimeoutCheck()
    queue.destroy()
    queue.destroy()

    expect(Reflect.ownKeys(queue)).toEqual(fields)
    expect(queue.requestTimeoutIntervalId).toBeNull()
  })

  it('releases the cleared interval and its scope while the queue stays alive', () => {
    const child = spawnSync(
      process.execPath,
      [
        '--expose-gc',
        path.join(__dirname, '../__tests__/fixtures/requestScopeChild.js'),
        'timer',
        'consumer',
      ],
      { encoding: 'utf8', timeout: 15000 }
    )
    expect(child.error).toBeUndefined()
    expect(child.stderr).toBe('')
    expect(child.status).toBe(0)
    expect(JSON.parse(child.stdout)).toEqual({
      markerCollected: true,
      intervalCleared: true,
      queueAlive: true,
      positiveAlive: true,
    })
  })

  it('destroys idempotently and rearms one interval for a reconnected queue', () => {
    jest.useFakeTimers()
    const queue = new RequestQueue({
      logger: newLogger(),
      enforceRequestTimeout: true,
      requestTimeout: 100,
    })
    try {
      queue.scheduleRequestTimeoutCheck()
      expect(queue.requestTimeoutIntervalId).not.toBeNull()
      expect(jest.getTimerCount()).toBe(1)
      queue.destroy()
      queue.destroy()
      expect(queue.requestTimeoutIntervalId).toBeNull()
      expect(jest.getTimerCount()).toBe(0)
      queue.scheduleRequestTimeoutCheck()
      queue.scheduleRequestTimeoutCheck()
      expect(queue.requestTimeoutIntervalId).not.toBeNull()
      expect(jest.getTimerCount()).toBe(1)
      const timeout = jest.fn()
      queue.inflight.set(1, { sentAt: Date.now(), requestTimeout: 100, timeoutRequest: timeout })
      jest.advanceTimersByTime(200)
      expect(timeout).toHaveBeenCalledTimes(1)
      queue.destroy()
      jest.advanceTimersByTime(1000)
      expect(timeout).toHaveBeenCalledTimes(1)
      expect(queue.requestTimeoutIntervalId).toBeNull()
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      queue.destroy()
      jest.useRealTimers()
    }
  })
})
