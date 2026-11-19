const {
  fenced,
  advanceUntil,
  deferred,
  settled,
  start,
  teardown,
  beats,
} = require('./fixtures/scriptedConsumer')

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

/** @returns {number} how many entries of `kind` the run's trace holds */
const count = (run, kind) => run.log.filter((entry) => entry.kind === kind).length

/**
 * What the consumer does today in three shutdown schedules. Each case records the current
 * behaviour exactly, so a change to it is a deliberate decision that turns the case around.
 */
describe('Consumer > shutdown characterization', () => {
  it.each([
    ['without', false],
    ['with', true],
  ])(
    'a fatal heartbeat answer while eachMessage is held, %s onPartitionsLost, reports no CRASH, no restartOnFailure call and no disconnect until the handler returns, then one CRASH with restart false',
    async (_, withHook) => {
      const policy = jest.fn(async () => true)
      const lost = jest.fn(async () => {})
      const entered = jest.fn()
      const run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        ...(withHook && { hook: { onPartitionsLost: lost } }),
        eachMessage: async (_payload, held) => {
          entered()
          await held.promise
        },
      })
      await advanceUntil(() => entered.mock.calls.length === 1)
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      const fetchesAtFault = count(run, 'fetch')
      await jest.advanceTimersByTimeAsync(12000)

      expect(run.crashes).toEqual([])
      expect(policy).not.toHaveBeenCalled()
      expect(run.cluster.disconnect).not.toHaveBeenCalled()
      expect(lost).not.toHaveBeenCalled()
      expect(count(run, 'fetch')).toBe(fetchesAtFault)

      run.held.resolve()
      await advanceUntil(() => run.crashes.length > 0, 10000)

      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
      expect(policy).toHaveBeenCalledTimes(1)
      expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)
      expect(lost).toHaveBeenCalledTimes(withHook ? 1 : 0)
      expect(entered).toHaveBeenCalledTimes(1)
    },
    60000
  )

  it('a zero-timeout disconnect joining the transport drain of crash cleanup asks for a zero drain but stays pending until that drain ends, then one CRASH with restart false', async () => {
    const transport = deferred()
    const policy = jest.fn(async () => true)
    const run = await start({
      fault: fenced,
      hook: {
        onPartitionsLost: () => {
          throw new Error('hook failed')
        },
      },
      retry: { initialRetryTime: 300, restartOnFailure: policy },
    })
    run.state.disconnecting = transport.promise
    run.arm()
    await advanceUntil(() => run.cluster.disconnect.mock.calls.length > 0)
    const close = settled(run.consumer.disconnect({ timeout: 0 }))
    await jest.advanceTimersByTimeAsync(100)

    expect(close.done).toBe(false)
    expect(run.cluster.disconnect.mock.calls).toEqual([
      [{ preserveSetupState: true }],
      [{ drainTimeout: 0 }],
    ])
    expect(run.crashes).toEqual([])
    expect(policy).not.toHaveBeenCalled()

    transport.resolve()
    await advanceUntil(() => close.done && run.crashes.length > 0, 10000)
    await jest.advanceTimersByTimeAsync(5000)

    expect(close).toEqual({ done: true, value: undefined })
    expect(run.crashes.map(({ error, restart }) => [error.name, restart])).toEqual([
      ['KafkaJSRebalanceListenerError', false],
    ])
    expect(policy).toHaveBeenCalledTimes(1)
    expect(run.cluster.connect).toHaveBeenCalledTimes(1)
  }, 60000)

  it('a timed disconnect forcing a close while a natural stop drains a held handler settles first; a run started before that stop ends resolves without connecting or joining, and one started after it connects and joins', async () => {
    const entered = jest.fn()
    const run = await start({
      eachMessage: async (_payload, held) => {
        entered()
        await held.promise
      },
    })
    await advanceUntil(() => entered.mock.calls.length === 1)
    const stop = settled(run.consumer.stop())
    await jest.advanceTimersByTimeAsync(10)
    const close = settled(run.consumer.disconnect({ timeout: 5 }))
    await advanceUntil(() => close.done, 1000)

    expect(close).toEqual({ done: true, value: undefined })
    expect(stop.done).toBe(false)
    expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)

    const joinsBefore = beats(run, false)
    const early = settled(run.consumer.run({ eachMessage: async () => {} }))
    await advanceUntil(() => early.done, 1000)
    await jest.advanceTimersByTimeAsync(1000)

    expect(early).toEqual({ done: true, value: undefined })
    expect(run.cluster.connect).toHaveBeenCalledTimes(1)
    expect(beats(run, false)).toBe(joinsBefore)
    expect(count(run, 'STOP')).toBe(0)

    run.held.resolve()
    await advanceUntil(() => stop.done, 5000)

    expect(stop).toEqual({ done: true, value: undefined })
    expect(count(run, 'STOP')).toBe(1)

    const late = settled(run.consumer.run({ eachMessage: async () => {} }))
    await advanceUntil(() => late.done, 5000)
    await jest.advanceTimersByTimeAsync(100)

    expect(late).toEqual({ done: true, value: undefined })
    expect(run.cluster.connect).toHaveBeenCalledTimes(2)
    expect(beats(run, false)).toBe(joinsBefore + 1)
    expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)
  }, 60000)
})
