const { performance: importedClock } = require('node:perf_hooks')
const RebalanceHooks = require('../rebalanceHooks')
const { useSharedFakeClock } = require('./fixtures/sharedFakeClock')

const WORKER_AGE_MS = 600001

/** @returns {Promise<void>} invokes a hook whose deadline is one minute on the faked global clock */
const invokeOneMinuteHook = async () => {
  const hook = jest.fn(async () => {})
  const hooks = new RebalanceHooks(
    { onPartitionsAssigned: hook },
    { isCurrent: () => true, onExpire: jest.fn() }
  )
  await hooks.invoke(
    'onPartitionsAssigned',
    {},
    { deadlineAt: performance.now() + 60000, identity: { acquisitions: [] } }
  )
  expect(hook).toHaveBeenCalledTimes(1)
}

describe('Shared fake clock for a long-lived worker', () => {
  beforeEach(() => {
    const real = importedClock.now.bind(importedClock)
    jest.spyOn(importedClock, 'now').mockImplementation(() => real() + WORKER_AGE_MS)
  })
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  test('plain fake timers leave the imported clock at worker age, so a live hook deadline looks expired', async () => {
    jest.useFakeTimers({ doNotFake: ['setImmediate'] })
    expect(performance.now()).toBeLessThan(WORKER_AGE_MS)
    await expect(invokeOneMinuteHook()).rejects.toMatchObject({ reason: 'timeout' })
  })

  test('the helper makes the imported and global clocks one fake clock', async () => {
    useSharedFakeClock()
    expect(importedClock.now()).toBe(performance.now())
    await jest.advanceTimersByTimeAsync(250)
    expect(importedClock.now()).toBe(performance.now())
    await invokeOneMinuteHook()
    expect(jest.getTimerCount()).toBe(0)
  })
})
