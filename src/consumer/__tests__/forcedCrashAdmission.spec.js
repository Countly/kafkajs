const Runner = require('../runner')
const ConsumerGroup = require('../consumerGroup')
const { createLogger, LEVELS } = require('../../loggers')
const {
  advanceUntil,
  buildConsumer,
  deferred,
  groupAuthorization,
  settled,
  start,
  target,
  teardown,
} = require('./fixtures/scriptedConsumer')

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

/**
 * Starts a consumer-protocol run whose eachMessage parks on the fixture's `held` and whose
 * `onPartitionsLost` pushes `loss:start`, waits on `gate`, runs `inside` and pushes `loss:end`
 * into `order`.
 * @param {string[]} order the shared trace
 * @param {{ inside?: (consumer: object) => Promise<unknown>, retry?: object }} [options]
 */
const startHeld = async (order, { inside = async () => {}, retry } = {}) => {
  const ref = {}
  const gate = deferred()
  let entered = false
  const run = await start({
    retry,
    hook: {
      onPartitionsLost: async () => {
        order.push('loss:start')
        await gate.promise
        ref.inside = await inside(ref.consumer)
        order.push('loss:end')
      },
    },
    eachMessage: async (_payload, held) => {
      entered = true
      await held.promise
    },
  })
  ref.consumer = run.consumer
  await advanceUntil(() => entered)
  run.consumer.on(run.consumer.events.CRASH, () => order.push('CRASH'))
  return Object.assign(run, { gate, ref })
}

/** Sends the fatal answer and crosses its deadline, then waits for the loss callback to enter. */
const forceCrash = async (run, order) => {
  run.arm()
  await advanceUntil(() => run.faultAt() !== null)
  await jest.advanceTimersByTimeAsync(run.faultAt() + 4000 - Date.now())
  await advanceUntil(() => order.includes('loss:start'), 100)
}

/** The options of a second run that records its assignment and deliveries in `order`. */
const secondRun = (order) => ({
  eachMessage: async ({ partition, message }) =>
    order.push(`handler2:${partition}:${message.offset}`),
  rebalanceListener: { onPartitionsAssigned: async () => order.push('assigned') },
})

describe('Consumer > starting again while a forced crash still waits for its loss callback, with no disconnect', () => {
  it('a run() made while the loss callback is held connects, joins and delivers only after the callback ends', async () => {
    const order = []
    const run = await startHeld(order)
    await forceCrash(run, order)
    const joins = run.log.filter((entry) => entry.kind === 'heartbeat').length
    const again = settled(run.consumer.run(secondRun(order)))
    await jest.advanceTimersByTimeAsync(2000)

    expect([order, again.done]).toEqual([['loss:start'], false])
    expect(run.log.filter((entry) => entry.kind === 'heartbeat').length).toBe(joins)

    run.gate.resolve()
    await advanceUntil(() => again.done && order.includes('handler2:0:40'), 2000)

    expect(again).toEqual({ done: true, value: undefined })
    expect(order).toEqual(['loss:start', 'loss:end', 'CRASH', 'assigned', 'handler2:0:40'])
  })

  it('a run() made while the loss callback never settles starts at the rebalance timeout that retires it, not before', async () => {
    const order = []
    const run = await startHeld(order)
    await forceCrash(run, order)
    const enteredAt = Date.now()
    const again = settled(run.consumer.run(secondRun(order)))
    await jest.advanceTimersByTimeAsync(3900)

    expect([order, again.done]).toEqual([['loss:start'], false])

    await advanceUntil(() => again.done && order.includes('handler2:0:40'), 2000)

    expect(order).toEqual(['loss:start', 'CRASH', 'assigned', 'handler2:0:40'])
    expect(Date.now() - enteredAt).toBeGreaterThanOrEqual(4000)
  })

  it('a connect() made while the loss callback is held resolves and connects only after the callback ends', async () => {
    const order = []
    const run = await startHeld(order)
    await forceCrash(run, order)
    const connects = run.cluster.connect.mock.calls.length
    const connecting = settled(run.consumer.connect())
    await jest.advanceTimersByTimeAsync(2000)

    expect([connecting.done, run.cluster.connect.mock.calls.length]).toEqual([false, connects])

    run.gate.resolve()
    await advanceUntil(() => connecting.done, 500)

    expect(connecting).toEqual({ done: true, value: undefined })
    expect(order).toEqual(['loss:start', 'loss:end', 'CRASH'])
    expect(run.cluster.connect.mock.calls.length).toBe(connects + 1)
  })
})

describe('Consumer > starting again from inside a forced crash does not wait for itself', () => {
  it('connect() awaited inside the loss callback returns at once, and run() inside it is refused as from any rebalance callback', async () => {
    const order = []
    const run = await startHeld(order, {
      inside: async (consumer) => {
        await consumer.connect()
        order.push('connected')
        return consumer.run(secondRun(order)).catch((error) => error.name)
      },
    })
    await forceCrash(run, order)
    run.gate.resolve()
    await advanceUntil(() => order.includes('CRASH'), 500)

    expect(order).toEqual(['loss:start', 'connected', 'loss:end', 'CRASH'])
    expect(run.ref.inside).toBe('KafkaJSNonRetriableError')
  })

  it('a run() from a CRASH listener starts and delivers without waiting for the crash that emitted it', async () => {
    const order = []
    const run = await startHeld(order)
    let again = null
    run.consumer.on(run.consumer.events.CRASH, () => {
      again = settled(run.consumer.run(secondRun(order)))
    })
    await forceCrash(run, order)
    run.gate.resolve()
    await advanceUntil(() => again?.done && order.includes('handler2:0:40'), 2000)

    expect(again).toEqual({ done: true, value: undefined })
    expect(order).toEqual(['loss:start', 'loss:end', 'CRASH', 'assigned', 'handler2:0:40'])
  })

  it('a connect() awaited inside restartOnFailure returns at once, so the crash policy decides and CRASH follows', async () => {
    const order = []
    const run = await startHeld(order, {
      retry: {
        retries: 0,
        initialRetryTime: 300,
        restartOnFailure: async () => {
          await run.consumer.connect()
          order.push('policy')
          return true
        },
      },
    })
    await forceCrash(run, order)
    run.gate.resolve()
    await advanceUntil(() => order.includes('CRASH'), 500)

    expect(order).toEqual(['loss:start', 'loss:end', 'policy', 'CRASH'])
    expect(run.crashes.map(({ restart }) => restart)).toEqual([false])
  })
})

describe('Consumer > a run that ends before its fatal deadline leaves that deadline disarmed', () => {
  it('a first round that fails after a background heartbeat answered fatally crashes with its own error, and the fatal deadline later neither delivers the loss callback nor reports a second CRASH', async () => {
    const order = []
    const failure = new Error('first round failed')
    const fatal = deferred()
    const runInitialRound = Runner.prototype.runInitialRound
    jest.spyOn(Runner.prototype, 'runInitialRound').mockImplementationOnce(async function () {
      await runInitialRound.call(this)
      await fatal.promise
      throw failure
    })
    const { consumer, state, crashes } = buildConsumer({ maxPollInterval: 4000 })
    let armed = false
    state.answer = (req) => {
      if (req.memberEpoch === 0) return target(1, { 'topic-a': [0] })
      if (!armed) return target(req.memberEpoch, null)
      armed = false
      fatal.resolve()
      throw groupAuthorization()
    }
    await consumer.subscribe({ topic: 'topic-a' })
    const started = settled(
      consumer.run({
        eachMessage: async () => {},
        rebalanceListener: {
          onPartitionsAssigned: async () => {
            armed = true
          },
          onPartitionsLost: async () => order.push('loss'),
        },
      })
    )
    await advanceUntil(() => started.done && crashes.length > 0, 2000)
    await jest.advanceTimersByTimeAsync(6000)
    const end = settled(consumer.disconnect())
    await advanceUntil(() => end.done, 500)

    expect(crashes.map(({ error }) => error)).toEqual([failure])
    expect(order).toEqual([])
    expect(end).toEqual({ done: true, value: undefined })
  })

  it('a stop made from the fatal membership log line whose stage fails ends the fatal deadline: no CRASH and no loss callback follow it', async () => {
    const order = []
    const ref = {}
    let stopped = null
    const logger = createLogger({
      level: LEVELS.ERROR,
      logCreator:
        () =>
        ({ log }) => {
          if (stopped || !log.message.includes('failed fatally')) return
          stopped = settled(ref.consumer.stop())
        },
    })
    jest
      .spyOn(ConsumerGroup.prototype, 'closeFetchSessions')
      .mockImplementationOnce(async () => Promise.reject(new Error('stage failed')))
    const run = await start({
      logger,
      hook: { onPartitionsLost: async () => order.push('loss') },
      eachMessage: async () => {},
    })
    ref.consumer = run.consumer
    run.arm()
    await advanceUntil(() => stopped?.done, 2000)
    await jest.advanceTimersByTimeAsync(run.faultAt() + 6000 - Date.now())

    expect(stopped).toEqual({ done: true, value: undefined })
    expect([order, run.crashes]).toEqual([[], []])
  })
})
