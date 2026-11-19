const { createLogger, LEVELS } = require('../../loggers')
const { advanceUntil, deferred, settled, start, teardown } = require('./fixtures/scriptedConsumer')

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

/** @returns {number} how many entries of `kind` the run's trace holds */
const count = (run, kind) => run.log.filter((entry) => entry.kind === kind).length

/** Records DISCONNECT in the run's trace, beside the fixture's STOP. */
const traceDisconnect = (run) =>
  run.consumer.on(run.consumer.events.DISCONNECT, () =>
    run.log.push({ kind: 'DISCONNECT', at: Date.now() })
  )

/**
 * A debug logger whose sink runs `onLine(message)` synchronously for every line, as a user's
 * log creator would.
 * @param {(message: string) => void} onLine
 */
const reentrantLogger = (onLine) =>
  createLogger({
    level: LEVELS.DEBUG,
    logCreator:
      () =>
      ({ log }) =>
        onLine(log.message),
  })

/** Starts a run whose eachMessage parks on `held` and waits until it has entered. */
const startHeld = async (options = {}) => {
  const entered = jest.fn()
  const run = await start({
    ...options,
    eachMessage: async (_payload, held) => {
      entered()
      await held.promise
    },
  })
  await advanceUntil(() => entered.mock.calls.length === 1)
  traceDisconnect(run)
  return run
}

describe('Consumer > a stop joined by a timed close', () => {
  it('a natural stop and a timed disconnect both made from the first stop log line share one stop, which the deadline ends with one STOP and one cluster disconnect', async () => {
    let reentered = false
    let inner = null
    const consumerRef = {}
    const logger = reentrantLogger((message) => {
      if (message !== 'stop consumer group' || reentered) return
      reentered = true
      inner = {
        stop: settled(consumerRef.consumer.stop()),
        close: settled(consumerRef.consumer.disconnect({ timeout: 5 })),
      }
    })
    const run = await startHeld({ logger })
    consumerRef.consumer = run.consumer
    const outer = settled(run.consumer.stop())
    await jest.advanceTimersByTimeAsync(4)

    expect(reentered).toBe(true)
    expect([outer.done, inner.stop.done, inner.close.done]).toEqual([false, false, false])

    await advanceUntil(() => inner.close.done, 100)

    expect(inner.close).toEqual({ done: true, value: undefined })
    expect(outer).toEqual({ done: true, value: undefined })
    expect(inner.stop).toEqual({ done: true, value: undefined })
    expect(count(run, 'STOP')).toBe(1)
    expect(count(run, 'DISCONNECT')).toBe(1)
    expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)
  }, 60000)

  it('a zero-timeout disconnect made from the stop log of a natural disconnect joins that close: one STOP, one cluster disconnect and the first close options', async () => {
    let reentered = false
    let inner = null
    const consumerRef = {}
    const logger = reentrantLogger((message) => {
      if (message !== 'stop consumer group' || reentered) return
      reentered = true
      inner = settled(consumerRef.consumer.disconnect({ timeout: 0 }))
    })
    const run = await start({ logger })
    consumerRef.consumer = run.consumer
    traceDisconnect(run)
    const outer = settled(run.consumer.disconnect({ leaveGroup: false }))
    await advanceUntil(() => outer.done && inner?.done, 5000)

    expect(reentered).toBe(true)
    expect(outer).toEqual({ done: true, value: undefined })
    expect(inner).toEqual({ done: true, value: undefined })
    expect(count(run, 'STOP')).toBe(1)
    expect(count(run, 'DISCONNECT')).toBe(1)
    expect(run.cluster.disconnect.mock.calls).toEqual([[]])
    expect(run.log.filter((e) => e.kind === 'heartbeat' && e.epoch < 0)).toEqual([])
  }, 60000)

  it('a timed disconnect joining a natural stop held in its close callback ends that callback wait at the deadline and sends no leave afterwards', async () => {
    const entered = jest.fn()
    const hookHeld = deferred()
    const run = await start({
      hook: {
        onPartitionsRevoked: async () => {
          entered()
          await hookHeld.promise
        },
      },
    })
    traceDisconnect(run)
    const stop = settled(run.consumer.stop())
    await advanceUntil(() => entered.mock.calls.length === 1, 1000)
    const close = settled(run.consumer.disconnect({ timeout: 5 }))
    await advanceUntil(() => close.done, 100)

    expect(close).toEqual({ done: true, value: undefined })
    expect(stop).toEqual({ done: true, value: undefined })
    expect(count(run, 'STOP')).toBe(1)

    const sentAtClose = run.log.filter((e) => e.kind === 'heartbeat').length
    await jest.advanceTimersByTimeAsync(5000)
    hookHeld.resolve()
    await jest.advanceTimersByTimeAsync(1000)

    expect(run.log.filter((e) => e.kind === 'heartbeat').length).toBe(sentAtClose)
    expect(jest.getTimerCount()).toBe(0)
    expect(count(run, 'STOP')).toBe(1)
    expect(count(run, 'DISCONNECT')).toBe(1)
  }, 60000)

  it('a disconnect from a handler whose options getter retires that handler run is refused with CONSUMER_STOPPING after the options are read', async () => {
    let outcome = null
    const consumerRef = {}
    const go = deferred()
    const run = await start({
      eachMessage: async () => {
        await go.promise
        outcome = settled(
          consumerRef.consumer.disconnect({
            get timeout() {
              consumerRef.consumer.disconnect({ timeout: 0 }).catch(() => {})
              return 5
            },
          })
        )
      },
    })
    consumerRef.consumer = run.consumer
    go.resolve()
    await advanceUntil(() => outcome?.done, 5000)

    expect(outcome.done).toBe(true)
    expect(outcome.error).toMatchObject({ type: 'CONSUMER_STOPPING' })
  }, 60000)
})
