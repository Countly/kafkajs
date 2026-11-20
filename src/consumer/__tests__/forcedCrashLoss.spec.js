const { createLogger, LEVELS } = require('../../loggers')
const {
  advanceUntil,
  deferred,
  recordingLogger,
  settled,
  start,
  teardown,
} = require('./fixtures/scriptedConsumer')

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

/** @returns {number} how many entries of `kind` the run's trace holds */
const count = (run, kind) => run.log.filter((entry) => entry.kind === kind).length

/** @returns {Array<[number, boolean]>} each CRASH as its error code and restart flag */
const crashes = (run) => run.crashes.map(({ error, restart }) => [error.code, restart])

/** A debug logger whose sink runs `onLine(message)` synchronously for every line. */
const reentrantLogger = (onLine) =>
  createLogger({
    level: LEVELS.DEBUG,
    logCreator:
      () =>
      ({ log }) =>
        onLine(log.message),
  })

/**
 * A close callback that records its entry, parks on its own gate and then runs `after`. Its
 * `trace` holds `loss:start`, then `loss:end` once the gate is released and `after` has returned.
 * @param {(consumer: object) => Promise<unknown>} [after] what the callback does once released
 */
const heldLoss = (after = async () => {}) => {
  const loss = { gate: deferred(), trace: [], calls: 0, enteredAt: null, consumer: null }
  loss.callback = async () => {
    loss.calls++
    loss.enteredAt = Date.now()
    loss.trace.push('loss:start')
    await loss.gate.promise
    loss.after = await after(loss.consumer)
    loss.trace.push('loss:end')
  }
  return loss
}

/**
 * Starts a consumer-protocol run with `hook` whose eachMessage parks on the fixture's `held`, and
 * waits until that handler has entered. `run.handlerEnded()` tells whether it has returned.
 */
const startHeld = async (hook, options = {}) => {
  let entered = false
  let ended = false
  const run = await start({
    ...options,
    hook,
    eachMessage: async (_payload, held) => {
      entered = true
      await held.promise
      ended = true
    },
  })
  await advanceUntil(() => entered)
  run.consumer.on(run.consumer.events.DISCONNECT, () =>
    run.log.push({ kind: 'DISCONNECT', at: Date.now() })
  )
  run.handlerEnded = () => ended
  return run
}

/** Sends the fatal answer and crosses its deadline, which forces the crash. */
const forceCrash = async (run) => {
  run.arm()
  await advanceUntil(() => run.faultAt() !== null)
  await jest.advanceTimersByTimeAsync(run.faultAt() + 4000 - Date.now())
}

/**
 * Starts a new run of the same consumer that records its assignment and deliveries in `order`.
 * @returns {{ done: boolean }}
 */
const runAgain = (run, order) =>
  settled(
    run.consumer.run({
      eachMessage: async ({ partition, message }) =>
        order.push(`handler2:${partition}:${message.offset}`),
      rebalanceListener: { onPartitionsAssigned: async () => order.push('assigned') },
    })
  )

/** @returns {boolean} whether the second run has delivered its first record */
const delivered = (order) => order.includes('handler2:0:40')

/**
 * The calls a released old callback makes against the consumer, each recorded as `accepted` or
 * the refusal's type.
 * @returns {Promise<{ pause: string, stop: string, disconnect: string }>}
 */
const lateCalls = async (consumer) => {
  const outcome = async (call) => {
    try {
      await call()
      return 'accepted'
    } catch (error) {
      return error.type ?? error.name
    }
  }
  return {
    pause: await outcome(() => consumer.pause([{ topic: 'topic-a' }])),
    stop: await outcome(() => consumer.stop()),
    disconnect: await outcome(() => consumer.disconnect()),
  }
}

/**
 * Starts a second run right after the close, waits for it to deliver, then releases the old
 * callback and checks that its late calls were refused without touching the second run.
 */
const expectLateCallsRefused = async (run, loss) => {
  const order = []
  const again = runAgain(run, order)
  await advanceUntil(() => again.done && delivered(order), 2000)

  expect(again).toEqual({ done: true, value: undefined })
  expect(order).toEqual(['assigned', 'handler2:0:40'])

  const stops = count(run, 'STOP')
  const disconnects = run.cluster.disconnect.mock.calls.length
  loss.gate.resolve()
  await advanceUntil(() => loss.after !== undefined, 500)

  expect(loss.after).toEqual({
    pause: expect.not.stringMatching(/^accepted$/),
    stop: expect.not.stringMatching(/^accepted$/),
    disconnect: expect.not.stringMatching(/^accepted$/),
  })
  expect(run.consumer.paused()).toEqual([])
  expect([count(run, 'STOP'), run.cluster.disconnect.mock.calls.length]).toEqual([
    stops,
    disconnects,
  ])
  expect(run.log.filter((entry) => entry.kind === 'heartbeat' && entry.epoch < 0)).toEqual([])
}

describe('Consumer > the loss callback of a crash the fatal deadline forces', () => {
  it.each([['onPartitionsLost'], ['onPartitionsRevoked']])(
    'a plain disconnect made while the loss callback %s runs waits for it: CRASH and the close come after it ends, and a run made after the close is assigned and delivers topic-a/0 only then, with the old handler still held',
    async (callback) => {
      const order = []
      const loss = heldLoss()
      const run = await startHeld({ [callback]: loss.callback })
      run.consumer.on(run.consumer.events.CRASH, () => order.push('CRASH'))
      await forceCrash(run)
      await advanceUntil(() => loss.calls === 1, 100)
      const close = settled(run.consumer.disconnect())
      await jest.advanceTimersByTimeAsync(1000)

      expect([loss.trace, order, close.done, run.crashes]).toEqual([['loss:start'], [], false, []])

      loss.gate.resolve()
      await advanceUntil(() => close.done && run.crashes.length > 0, 500)
      order.push('closed')
      const again = runAgain(run, order)
      await advanceUntil(() => again.done && delivered(order), 2000)

      expect(close).toEqual({ done: true, value: undefined })
      expect([...loss.trace, ...order]).toEqual([
        'loss:start',
        'loss:end',
        'CRASH',
        'closed',
        'assigned',
        'handler2:0:40',
      ])
      expect(loss.calls).toBe(1)
      expect(crashes(run)).toEqual([[30, false]])
      expect(run.handlerEnded()).toBe(false)
    },
    60000
  )

  it.each([
    ['a plain disconnect made after the fatal answer', undefined, false],
    ['a longer timed disconnect made after the fatal answer', 10000, false],
    ['a plain disconnect made from the fatal membership log line', undefined, true],
    ['a longer timed disconnect made from the fatal membership log line', 10000, true],
  ])(
    '%s, waiting in its stop when the deadline forces the crash, waits for the loss callback the crash delivers once: one STOP, cluster disconnect, DISCONNECT and CRASH, and a run made after the close delivers',
    async (_label, timeout, fromLog) => {
      const ref = {}
      let close = null
      const disconnect = () =>
        settled(ref.consumer.disconnect(timeout === undefined ? undefined : { timeout }))
      const logger = reentrantLogger((message) => {
        if (!fromLog || close || !ref.consumer) return
        if (message === 'The consumer group membership failed fatally') close = disconnect()
      })
      const loss = heldLoss()
      const run = await startHeld({ onPartitionsLost: loss.callback }, { logger })
      ref.consumer = run.consumer
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      if (!fromLog) close = disconnect()

      expect(close?.done).toBe(false)

      await jest.advanceTimersByTimeAsync(run.faultAt() + 4000 - Date.now())
      await advanceUntil(() => loss.calls === 1, 100)
      await jest.advanceTimersByTimeAsync(500)

      expect([loss.trace, close.done, run.crashes]).toEqual([['loss:start'], false, []])

      loss.gate.resolve()
      await advanceUntil(() => close.done && run.crashes.length > 0, 500)

      expect(close).toEqual({ done: true, value: undefined })
      expect(loss.trace).toEqual(['loss:start', 'loss:end'])
      expect(crashes(run)).toEqual([[30, false]])
      expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
      expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)

      const order = []
      const again = runAgain(run, order)
      await advanceUntil(() => again.done && delivered(order), 2000)

      expect(order).toEqual(['assigned', 'handler2:0:40'])
      expect(loss.calls).toBe(1)
    },
    60000
  )

  it('a timed disconnect waiting in its stop when the deadline forces the crash ends the wait for the held loss callback at its own deadline, though the forced retirement cancelled that close timer', async () => {
    const loss = heldLoss()
    const run = await startHeld({ onPartitionsLost: loss.callback })
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    const closeAt = Date.now()
    const close = settled(run.consumer.disconnect({ timeout: 5000 }))
    await jest.advanceTimersByTimeAsync(run.faultAt() + 4000 - Date.now())
    await advanceUntil(() => loss.calls === 1, 100)
    await jest.advanceTimersByTimeAsync(closeAt + 4990 - Date.now())

    expect([loss.trace, close.done, run.crashes]).toEqual([['loss:start'], false, []])

    await advanceUntil(() => close.done && run.crashes.length > 0, 100)

    expect(close).toEqual({ done: true, value: undefined })
    expect(Date.now() - closeAt).toBeLessThan(5100)
    expect(loss.trace).toEqual(['loss:start'])
    expect(crashes(run)).toEqual([[30, false]])
    expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)

    await expectLateCallsRefused(run, loss)
  }, 60000)

  it('a loss callback whose promise never settles holds a plain disconnect and CRASH until the rebalance timeout and no longer, leaves no timer armed, and its late calls after a run made after the close are refused without touching that run', async () => {
    const loss = heldLoss(lateCalls)
    const run = await startHeld({ onPartitionsLost: loss.callback })
    loss.consumer = run.consumer
    await forceCrash(run)
    await advanceUntil(() => loss.calls === 1, 100)
    const close = settled(run.consumer.disconnect())
    // The group's rebalance timeout is its maxPollInterval, 4000 ms here.
    await jest.advanceTimersByTimeAsync(loss.enteredAt + 3990 - Date.now())

    expect([close.done, run.crashes]).toEqual([false, []])

    await advanceUntil(() => close.done && run.crashes.length > 0, 200)

    expect(close).toEqual({ done: true, value: undefined })
    expect(Date.now() - loss.enteredAt).toBeLessThan(4100)
    expect(loss.trace).toEqual(['loss:start'])
    expect(crashes(run)).toEqual([[30, false]])
    expect(jest.getTimerCount()).toBe(0)

    await expectLateCallsRefused(run, loss)
  }, 60000)

  it.each([
    [
      'throws synchronously',
      (order) => {
        order.push('loss:throw')
        throw new Error('loss failed')
      },
    ],
    [
      'rejects after an await',
      async (order) => {
        await new Promise((resolve) => setTimeout(resolve, 200))
        order.push('loss:throw')
        throw new Error('loss failed')
      },
    ],
  ])(
    'a loss callback that %s is logged and not retried; the close and one CRASH for the original fatal answer with restart false follow it, and the old handler failing later adds nothing',
    async (_label, fail) => {
      const order = []
      const { logger, lines } = recordingLogger()
      const lost = jest.fn(() => fail(order))
      const run = await start({
        logger,
        retry: { retries: 0, initialRetryTime: 300, restartOnFailure: async () => true },
        hook: { onPartitionsLost: lost },
        eachMessage: async (_payload, held) => {
          order.push('handler')
          await held.promise
          throw new Error('late handler failure')
        },
      })
      run.consumer.on(run.consumer.events.CRASH, () => order.push('CRASH'))
      await forceCrash(run)
      await advanceUntil(() => lost.mock.calls.length === 1, 100)
      const close = settled(run.consumer.disconnect())
      await advanceUntil(() => close.done && run.crashes.length > 0, 1000)

      expect(close).toEqual({ done: true, value: undefined })
      expect(order).toEqual(['handler', 'loss:throw', 'CRASH'])
      expect(lines).toContain('Rebalance callback failed while closing')
      expect(crashes(run)).toEqual([[30, false]])
      expect(run.crashes[0].error.cause).toMatchObject({ type: 'GROUP_AUTHORIZATION_FAILED' })

      run.held.resolve()
      await jest.advanceTimersByTimeAsync(5000)

      expect(lost).toHaveBeenCalledTimes(1)
      expect(run.crashes).toHaveLength(1)
    },
    60000
  )

  it.each([
    ['disconnect', false],
    ['disconnect', true],
    ['stop', false],
    ['stop', true],
  ])(
    'a %s() awaited inside the loss callback (an outside disconnect already waiting: %s) returns at once, and CRASH and the outside disconnect follow the callback',
    async (api, outside) => {
      const out = {}
      const loss = heldLoss(async (consumer) => {
        const at = Date.now()
        await consumer[api]()
        out.ackIn = Date.now() - at
        await new Promise((resolve) => setTimeout(resolve, 50))
        out.crashesAtEnd = out.run.crashes.length
        out.outsideAtEnd = out.outer?.done
      })
      const run = await startHeld({ onPartitionsLost: loss.callback })
      out.run = run
      loss.consumer = run.consumer
      await forceCrash(run)
      await advanceUntil(() => loss.calls === 1, 100)
      if (outside) out.outer = settled(run.consumer.disconnect())
      await jest.advanceTimersByTimeAsync(10)
      loss.gate.resolve()
      await advanceUntil(
        () => loss.trace.length === 2 && run.crashes.length > 0 && (!outside || out.outer.done),
        1000
      )

      expect([out.ackIn, out.crashesAtEnd]).toEqual([0, 0])
      expect(crashes(run)).toEqual([[30, false]])
      if (outside) {
        expect(out.outsideAtEnd).toBe(false)
        expect(out.outer).toEqual({ done: true, value: undefined })
      }
    },
    60000
  )

  describe('a close with a deadline first asked while the loss callback runs', () => {
    it('waits until its own deadline and no longer, the callback promise still pending; the callback resumed after a run made after the close is refused pause, stop and disconnect without touching that run', async () => {
      const loss = heldLoss(lateCalls)
      const run = await startHeld({ onPartitionsLost: loss.callback })
      loss.consumer = run.consumer
      await forceCrash(run)
      await advanceUntil(() => loss.calls === 1, 100)
      const close = settled(run.consumer.disconnect({ timeout: 500 }))
      await jest.advanceTimersByTimeAsync(499)

      expect([close.done, run.crashes]).toEqual([false, []])

      await advanceUntil(() => close.done && run.crashes.length > 0, 100)

      expect(close).toEqual({ done: true, value: undefined })
      expect(loss.trace).toEqual(['loss:start'])
      expect(crashes(run)).toEqual([[30, false]])

      await expectLateCallsRefused(run, loss)
    }, 60000)

    it('retires the callback at its deadline but returns only once the held crash transport has closed', async () => {
      const transport = deferred()
      const loss = heldLoss(lateCalls)
      const run = await startHeld({ onPartitionsLost: loss.callback })
      loss.consumer = run.consumer
      run.state.disconnecting = transport.promise
      await forceCrash(run)
      await advanceUntil(() => loss.calls === 1, 100)
      const close = settled(run.consumer.disconnect({ timeout: 500 }))
      await jest.advanceTimersByTimeAsync(600)
      loss.gate.resolve()
      await advanceUntil(() => loss.after !== undefined, 100)

      expect(loss.after.pause).toBe('CONSUMER_STOPPING')
      expect([close.done, run.crashes]).toEqual([false, []])

      transport.resolve()
      await advanceUntil(() => close.done && run.crashes.length > 0, 500)

      expect(close).toEqual({ done: true, value: undefined })
      expect(crashes(run)).toEqual([[30, false]])
    }, 60000)

    it('returns at its deadline while the crash policy still decides, and CRASH follows the policy', async () => {
      const policy = deferred()
      const loss = heldLoss()
      const run = await startHeld(
        { onPartitionsLost: loss.callback },
        { retry: { retries: 0, initialRetryTime: 300, restartOnFailure: () => policy.promise } }
      )
      await forceCrash(run)
      await advanceUntil(() => loss.calls === 1, 100)
      const close = settled(run.consumer.disconnect({ timeout: 500 }))
      await jest.advanceTimersByTimeAsync(499)

      expect(close.done).toBe(false)

      await advanceUntil(() => close.done, 100)

      expect(close).toEqual({ done: true, value: undefined })
      expect([loss.trace, run.crashes]).toEqual([['loss:start'], []])

      policy.resolve(true)
      await advanceUntil(() => run.crashes.length > 0, 100)

      expect(crashes(run)).toEqual([[30, false]])
      loss.gate.resolve()
    }, 60000)

    it('a zero-timeout disconnect made from the Crash log line still delivers the loss once and then retires it: one STOP, one undrained cluster disconnect, one DISCONNECT, and the resumed callback is refused against a run made after the close', async () => {
      const ref = {}
      let close = null
      const logger = reentrantLogger((message) => {
        if (!message.startsWith('Crash:') || close || !ref.consumer) return
        close = settled(ref.consumer.disconnect({ timeout: 0 }))
      })
      const loss = heldLoss(lateCalls)
      const run = await startHeld({ onPartitionsLost: loss.callback }, { logger })
      ref.consumer = run.consumer
      loss.consumer = run.consumer
      await forceCrash(run)
      await advanceUntil(() => close?.done && run.crashes.length > 0, 500)

      expect(close).toEqual({ done: true, value: undefined })
      expect([loss.calls, loss.trace]).toEqual([1, ['loss:start']])
      expect(crashes(run)).toEqual([[30, false]])
      expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
      expect(run.cluster.disconnect.mock.calls).toEqual([[{ drainTimeout: 0 }]])

      await expectLateCallsRefused(run, loss)
    }, 60000)

    it('a disconnect shorter than the poll interval that claims the fatal itself delivers the loss once and retires it at once: one STOP, cluster disconnect and DISCONNECT, and the resumed callback is refused against a run made after the close', async () => {
      const loss = heldLoss(lateCalls)
      const run = await startHeld({ onPartitionsLost: loss.callback })
      loss.consumer = run.consumer
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      const close = settled(run.consumer.disconnect({ timeout: 100 }))
      await jest.advanceTimersByTimeAsync(99)

      expect([close.done, loss.calls]).toEqual([false, 0])

      await advanceUntil(() => close.done && run.crashes.length > 0, 100)

      expect(close).toEqual({ done: true, value: undefined })
      expect([loss.calls, loss.trace]).toEqual([1, ['loss:start']])
      expect(crashes(run)).toEqual([[30, false]])
      expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
      expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)

      await expectLateCallsRefused(run, loss)
    }, 60000)
  })
})
