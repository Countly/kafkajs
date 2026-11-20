const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSNonRetriableError } = require('../../errors')
const {
  advanceUntil,
  deferred,
  fenced,
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

describe('Consumer > a timed close joining crash cleanup', () => {
  it('a positive-timeout disconnect joining the transport drain of crash cleanup asks for its remaining drain and, at its deadline, for a zero drain', async () => {
    const transport = deferred()
    const run = await start({
      fault: fenced,
      hook: {
        onPartitionsLost: () => {
          throw new Error('hook failed')
        },
      },
      retry: { initialRetryTime: 300, restartOnFailure: async () => false },
    })
    run.state.disconnecting = transport.promise
    run.arm()
    await advanceUntil(() => run.cluster.disconnect.mock.calls.length > 0)
    const close = settled(run.consumer.disconnect({ timeout: 5 }))
    await jest.advanceTimersByTimeAsync(4)

    expect(run.cluster.disconnect.mock.calls).toEqual([
      [{ preserveSetupState: true }],
      [{ drainTimeout: 5 }],
    ])

    await jest.advanceTimersByTimeAsync(1)

    expect(run.cluster.disconnect.mock.calls).toEqual([
      [{ preserveSetupState: true }],
      [{ drainTimeout: 5 }],
      [{ drainTimeout: 0 }],
    ])
    expect(close.done).toBe(false)

    transport.resolve()
    await advanceUntil(() => close.done && run.crashes.length > 0, 1000)

    expect(close).toEqual({ done: true, value: undefined })
    expect(run.crashes).toHaveLength(1)
  }, 60000)
})

describe('Consumer > a close joining crash cleanup that is past its stop', () => {
  /**
   * Crashes a run on a handler failure whose stop is held in the revoked close callback, with the
   * crash's own cluster disconnect held on `crashTransport`.
   */
  const startCrashing = async ({ onRevoked } = {}) => {
    const hookHeld = deferred()
    const revoked = jest.fn()
    const crashTransport = deferred()
    let fail = false
    const run = await start({
      retry: { retries: 0, initialRetryTime: 300, restartOnFailure: async () => false },
      hook: {
        onPartitionsRevoked: async () => {
          revoked()
          await onRevoked?.(run)
          await hookHeld.promise
        },
      },
      eachMessage: async () => {
        if (fail) throw new KafkaJSNonRetriableError('handler failed')
      },
    })
    traceDisconnect(run)
    run.cluster.disconnect.mockImplementation(async (options) => {
      if (options?.preserveSetupState) await crashTransport.promise
      run.log.push({ kind: 'transport', options })
    })
    run.state.queue['topic-a|0'] = [40, 41, 42]
    fail = true
    await advanceUntil(() => revoked.mock.calls.length === 1, 3000)
    return { run, hookHeld, crashTransport }
  }

  it('a timed disconnect resolves only after the crash cleanup it overlaps has finished its own cluster disconnect', async () => {
    const { run, hookHeld, crashTransport } = await startCrashing()
    const close = settled(run.consumer.disconnect({ timeout: 5 }))
    await jest.advanceTimersByTimeAsync(200)

    expect(close.done).toBe(false)
    expect(
      run.log.filter(({ kind }) => kind === 'transport').map(({ options }) => options)
    ).toEqual([{ drainTimeout: 0 }])

    crashTransport.resolve()
    await advanceUntil(() => close.done && run.crashes.length > 0, 1000)

    expect(close).toEqual({ done: true, value: undefined })
    expect(
      run.log.filter(({ kind }) => kind === 'transport').map(({ options }) => options)
    ).toEqual([{ drainTimeout: 0 }, { preserveSetupState: true }])
    hookHeld.resolve()
  }, 60000)

  it('a plain disconnect resolves only after the crash cleanup it overlaps has finished its own cluster disconnect and DISCONNECT', async () => {
    const { run, hookHeld, crashTransport } = await startCrashing()
    hookHeld.resolve()
    await advanceUntil(() => run.cluster.disconnect.mock.calls.length > 0, 200)
    const close = settled(run.consumer.disconnect())
    await jest.advanceTimersByTimeAsync(200)

    expect([close.done, count(run, 'DISCONNECT')]).toEqual([false, 0])

    crashTransport.resolve()
    await advanceUntil(() => close.done && run.crashes.length > 0, 1000)

    expect(close).toEqual({ done: true, value: undefined })
    expect(count(run, 'DISCONNECT')).toBe(2)
  }, 60000)

  it('a timed disconnect is still bounded by its timeout when the crash cleanup it overlaps is held in a close callback', async () => {
    const { run, hookHeld, crashTransport } = await startCrashing()
    crashTransport.resolve()
    const close = settled(run.consumer.disconnect({ timeout: 50 }))
    await jest.advanceTimersByTimeAsync(49)

    expect(close.done).toBe(false)

    await advanceUntil(() => close.done, 20)

    expect(close).toEqual({ done: true, value: undefined })
    hookHeld.resolve()
    await advanceUntil(() => run.crashes.length > 0, 1000)
    expect(run.crashes).toHaveLength(1)
  }, 60000)

  it('a disconnect awaited from the close callback of the crash settles without waiting on itself, and the crash completes', async () => {
    let awaited = null
    const { run, hookHeld, crashTransport } = await startCrashing({
      onRevoked: async ({ consumer }) => {
        awaited = settled(consumer.disconnect())
        await new Promise((resolve) => setTimeout(resolve, 0))
      },
    })
    await jest.advanceTimersByTimeAsync(10)

    expect(awaited).toEqual({ done: true, value: undefined })

    hookHeld.resolve()
    crashTransport.resolve()
    await advanceUntil(() => run.crashes.length > 0, 1000)

    expect(run.crashes).toHaveLength(1)
  }, 60000)
})

describe('Consumer > a fatal heartbeat answer while a handler runs', () => {
  /** Starts a run whose eachMessage runs `body(payload, held)` and waits until it has entered. */
  const startWith = async (body, options = {}) => {
    const entered = jest.fn()
    const ready = deferred()
    const ref = {}
    const run = await start({
      ...options,
      eachMessage: async (payload, held) => {
        entered()
        await ready.promise
        return body(payload, held, ref)
      },
    })
    ref.run = run
    traceDisconnect(run)
    ready.resolve()
    await advanceUntil(() => entered.mock.calls.length === 1)
    return run
  }

  it('a handler released before the deadline is claimed by the fetch loop: one CRASH and nothing armed after it (control)', async () => {
    const run = await startWith((_payload, held) => held.promise)
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    await jest.advanceTimersByTimeAsync(2900)
    run.held.resolve()
    await advanceUntil(() => run.crashes.length > 0, 1000)
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
    expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)
    expect(count(run, 'DISCONNECT')).toBe(1)
    expect(jest.getTimerCount()).toBe(0)
  }, 60000)

  it("a handler whose own heartbeat gets the fatal answer and then awaits a plain disconnect is not waited for past the deadline: one STOP, DISCONNECT and CRASH, the handler's disconnect settles and a CRASH listener's connect is an outside call", async () => {
    const outcome = {}
    const connects = []
    const run = await startWith(async ({ heartbeat }, _held, ref) => {
      // Each answer arrives 300 ms after its request, so the handler's heartbeat joins one in flight.
      const send = ref.run.coordinator.consumerGroupHeartbeat.getMockImplementation()
      ref.run.coordinator.consumerGroupHeartbeat.mockImplementation(async (request) => {
        const answer = send(request).then(
          (value) => () => value,
          (error) => () => {
            throw error
          }
        )
        await new Promise((resolve) => setTimeout(resolve, 300))
        return (await answer)()
      })
      ref.run.arm()
      for (;;) {
        try {
          await heartbeat()
        } catch (error) {
          outcome.heartbeat = error
          outcome.at = Date.now()
          outcome.disconnect = settled(ref.run.consumer.disconnect())
          await new Promise(() => {})
        }
        await new Promise((resolve) => setTimeout(resolve, 100))
      }
    })
    run.consumer.on(run.consumer.events.CRASH, () => connects.push(settled(run.consumer.connect())))
    await advanceUntil(() => outcome.disconnect !== undefined, 2000)

    expect(outcome.heartbeat).toMatchObject({ code: 30 })

    await jest.advanceTimersByTimeAsync(outcome.at + 3999 - Date.now())

    expect(outcome.disconnect.done).toBe(false)
    expect(run.crashes).toEqual([])

    await advanceUntil(() => outcome.disconnect.done && run.crashes.length > 0, 100)

    expect(outcome.disconnect).toEqual({ done: true, value: undefined })
    expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
    expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)

    await advanceUntil(() => connects[0]?.done, 1000)

    expect(connects).toHaveLength(1)
    expect(connects[0]).toEqual({ done: true, value: undefined })
    await run.consumer.disconnect()
  }, 60000)

  it.each([
    ['a plain', undefined],
    ['a longer timed', 10000],
  ])(
    '%s disconnect made from the fatal membership log line is bounded by the fatal deadline: it settles one poll interval after the answer with one STOP, DISCONNECT and CRASH',
    async (_label, timeout) => {
      const ref = {}
      let close = null
      const logger = reentrantLogger((message) => {
        if (message !== 'The consumer group membership failed fatally' || close || !ref.consumer)
          return
        close = settled(ref.consumer.disconnect(timeout === undefined ? undefined : { timeout }))
      })
      const run = await startWith((_payload, held) => held.promise, { logger })
      ref.consumer = run.consumer
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      await jest.advanceTimersByTimeAsync(run.faultAt() + 3999 - Date.now())

      expect(close?.done).toBe(false)
      expect(run.crashes).toEqual([])

      await advanceUntil(() => close.done && run.crashes.length > 0, 100)

      expect(close).toEqual({ done: true, value: undefined })
      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
      expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    },
    60000
  )

  it.each([['onPartitionsLost'], ['onPartitionsRevoked']])(
    'a crash the fatal deadline forces while the handler is held delivers the loss to %s once, before CRASH and without waiting for the handler',
    async (callback) => {
      const listener = jest.fn()
      const run = await startWith((_payload, held) => held.promise, {
        hook: { [callback]: listener },
      })
      const atCrash = []
      run.consumer.on(run.consumer.events.CRASH, () => atCrash.push(listener.mock.calls.length))
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      await jest.advanceTimersByTimeAsync(run.faultAt() + 3999 - Date.now())

      expect(listener).not.toHaveBeenCalled()

      await advanceUntil(() => run.crashes.length > 0, 100)

      expect(atCrash).toEqual([1])
      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])

      run.held.resolve()
      await jest.advanceTimersByTimeAsync(3000)

      expect(listener.mock.calls.map(([event]) => event.partitions)).toEqual([
        [{ topic: 'topic-a', partitions: [0] }],
      ])
    },
    60000
  )

  it('a natural stop made after the fatal answer while the handler is held leaves the forced crash one lost call, made at the deadline', async () => {
    const lost = jest.fn()
    const run = await startWith((_payload, held) => held.promise, {
      hook: { onPartitionsLost: lost },
    })
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    const stop = settled(run.consumer.stop())
    await jest.advanceTimersByTimeAsync(run.faultAt() + 3999 - Date.now())

    expect([stop.done, lost.mock.calls.length]).toEqual([false, 0])

    await advanceUntil(() => stop.done && run.crashes.length > 0, 100)
    run.held.resolve()
    await jest.advanceTimersByTimeAsync(3000)

    expect(stop).toEqual({ done: true, value: undefined })
    expect(lost).toHaveBeenCalledTimes(1)
    expect(run.crashes).toHaveLength(1)
  }, 60000)

  it('a zero-timeout disconnect made from the Crash log line of the crash the fatal deadline forces joins that crash: one stop, one cluster disconnect and one DISCONNECT', async () => {
    const ref = {}
    let close = null
    const logger = reentrantLogger((message) => {
      if (!message.startsWith('Crash:') || close || !ref.consumer) return
      close = settled(ref.consumer.disconnect({ timeout: 0 }))
    })
    const run = await startWith((_payload, held) => held.promise, { logger })
    ref.consumer = run.consumer
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    await jest.advanceTimersByTimeAsync(run.faultAt() + 4000 - Date.now())
    await advanceUntil(() => close?.done && run.crashes.length > 0, 500)
    await jest.advanceTimersByTimeAsync(1000)

    expect(close).toEqual({ done: true, value: undefined })
    expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
    expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    expect(run.cluster.disconnect.mock.calls).toEqual([[{ drainTimeout: 0 }]])
  }, 60000)

  it.each([
    ['released before the deadline', true],
    ['still held at the deadline', false],
  ])(
    'the CRASH error for a fatal answer while the handler is %s is the fetch retrier non-retriable error caused by the answer',
    async (_label, release) => {
      const run = await startWith((_payload, held) => held.promise)
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      if (release) {
        await jest.advanceTimersByTimeAsync(500)
        run.held.resolve()
      }
      await advanceUntil(() => run.crashes.length > 0, 5000)

      expect(
        run.crashes.map(({ error }) => [error.name, error.code, error.retriable, error.cause.name])
      ).toEqual([['KafkaJSNonRetriableError', 30, false, 'KafkaJSProtocolError']])
    },
    60000
  )

  it('a disconnect awaited by the old handler while the forced crash cleanup drains settles without waiting on that cleanup', async () => {
    const transport = deferred()
    let outcome = null
    const run = await startWith(async (_payload, held, ref) => {
      await held.promise
      outcome = settled(ref.run.consumer.disconnect())
      await outcome.promise
    })
    run.state.disconnecting = transport.promise
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    await jest.advanceTimersByTimeAsync(run.faultAt() + 4000 - Date.now())
    await advanceUntil(() => run.cluster.disconnect.mock.calls.length > 0, 100)
    run.held.resolve()
    await jest.advanceTimersByTimeAsync(10)

    expect(outcome).toMatchObject({ done: true, error: { type: 'CONSUMER_STOPPING' } })

    transport.resolve()
    await advanceUntil(() => run.crashes.length > 0, 1000)
    expect(run.crashes).toHaveLength(1)
  }, 60000)

  it('a disconnect with a deadline shorter than the poll interval ends the held handler wait at its own deadline and still reports one CRASH with restart false', async () => {
    const run = await startWith((_payload, held) => held.promise)
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    const close = settled(run.consumer.disconnect({ timeout: 100 }))
    await jest.advanceTimersByTimeAsync(99)

    expect(close.done).toBe(false)

    await advanceUntil(() => close.done && run.crashes.length > 0, 100)

    expect(close).toEqual({ done: true, value: undefined })
    expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
    expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)
  }, 60000)

  describe('the loss callback of the crash the deadline forces', () => {
    /**
     * Starts a held run with `hook`, gives it to `prepare` and runs it into the crash the fatal
     * deadline forces.
     */
    const forceCrash = async (hook, prepare = () => {}) => {
      const run = await startWith((_payload, held) => held.promise, { hook })
      prepare(run)
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      await jest.advanceTimersByTimeAsync(run.faultAt() + 4000 - Date.now())
      return run
    }

    it('CRASH and a plain disconnect wait for an async onPartitionsLost, and a run made after that disconnect is assigned only after the callback has ended', async () => {
      const order = []
      const lostHeld = deferred()
      const run = await forceCrash(
        {
          onPartitionsLost: async () => {
            order.push('lost:start')
            await lostHeld.promise
            order.push('lost:end')
          },
        },
        (run) => run.consumer.on(run.consumer.events.CRASH, () => order.push('CRASH'))
      )
      await advanceUntil(() => order.includes('lost:start'), 100)
      const close = settled(run.consumer.disconnect())
      await jest.advanceTimersByTimeAsync(1000)

      expect([order, close.done, run.crashes]).toEqual([['lost:start'], false, []])

      lostHeld.resolve()
      await advanceUntil(() => close.done, 500)
      order.push('disconnect')
      const again = settled(
        run.consumer.run({
          eachMessage: async () => {},
          rebalanceListener: { onPartitionsAssigned: async () => order.push('assigned') },
        })
      )
      await advanceUntil(() => again.done && order.includes('assigned'), 2000)

      expect(close).toEqual({ done: true, value: undefined })
      expect(order).toEqual(['lost:start', 'lost:end', 'CRASH', 'disconnect', 'assigned'])
      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
    }, 60000)

    it('a loss callback that never settles holds CRASH and a plain disconnect for the rebalance timeout and no longer', async () => {
      const lost = jest.fn(() => new Promise(() => {}))
      const run = await forceCrash({ onPartitionsLost: lost })
      await advanceUntil(() => lost.mock.calls.length === 1, 100)
      const startedAt = Date.now()
      const close = settled(run.consumer.disconnect())
      await jest.advanceTimersByTimeAsync(2900)

      expect([close.done, run.crashes]).toEqual([false, []])

      await advanceUntil(() => close.done && run.crashes.length > 0, 200)

      expect(close).toEqual({ done: true, value: undefined })
      expect(Date.now() - startedAt).toBeLessThan(3200)
      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
      expect(lost).toHaveBeenCalledTimes(1)
    }, 60000)

    it('a timed disconnect shorter than the rebalance timeout returns at its own deadline while the loss callback is held, and a later call from that callback is refused', async () => {
      const lostHeld = deferred()
      const out = {}
      const run = await forceCrash({
        onPartitionsLost: async () => {
          out.started = true
          await lostHeld.promise
          try {
            run.consumer.pause([{ topic: 'topic-a' }])
            out.pause = 'accepted'
          } catch (error) {
            out.pause = error.type
          }
        },
      })
      await advanceUntil(() => out.started, 100)
      const close = settled(run.consumer.disconnect({ timeout: 500 }))
      await jest.advanceTimersByTimeAsync(490)

      expect(close.done).toBe(false)

      await advanceUntil(() => close.done && run.crashes.length > 0, 100)

      expect(close).toEqual({ done: true, value: undefined })
      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])

      lostHeld.resolve()
      await advanceUntil(() => out.pause !== undefined, 100)

      expect(out.pause).toBe('CONSUMER_STOPPING')
    }, 60000)

    it('a disconnect and a stop made from inside the loss callback return at once, and CRASH follows the callback', async () => {
      const out = {}
      const run = await forceCrash(
        {
          onPartitionsLost: async () => {
            const close = settled(out.run.consumer.disconnect())
            const stop = settled(out.run.consumer.stop())
            await Promise.resolve()
            await Promise.resolve()
            out.atOnce = [close.done, stop.done]
            await new Promise((resolve) => setTimeout(resolve, 50))
            out.crashesAtEnd = out.run.crashes.length
          },
        },
        (run) => (out.run = run)
      )
      await advanceUntil(() => out.crashesAtEnd !== undefined && run.crashes.length > 0, 1000)

      expect([out.atOnce, out.crashesAtEnd]).toEqual([[true, true], 0])
      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
    }, 60000)

    it('a loss callback that throws after an await does not hide the forced crash: one CRASH with restart false, after the callback', async () => {
      const order = []
      const run = await forceCrash(
        {
          onPartitionsLost: async () => {
            await new Promise((resolve) => setTimeout(resolve, 200))
            order.push('lost:throw')
            throw new Error('lost callback failed')
          },
        },
        (run) => run.consumer.on(run.consumer.events.CRASH, () => order.push('CRASH'))
      )
      await advanceUntil(() => run.crashes.length > 0, 1000)

      expect(order).toEqual(['lost:throw', 'CRASH'])
      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
    }, 60000)
  })
})
