const { AsyncResource } = require('node:async_hooks')
const Cluster = require('../../cluster')
const Runner = require('../runner')
const createConsumer = require('../index')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createLogger, LEVELS } = require('../../loggers')
const { consumerStoppingError } = require('../rebalanceErrorContract')
const { KafkaJSNoBrokerAvailableError } = require('../../errors')
const { newLogger } = require('testHelpers')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC, prepareRecordBatches } = require('./fixtures/fetchHarness')
const { pump } = require('./fixtures/lifecycleCase')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const RETRY = { retries: 3, initialRetryTime: 10, maxRetryTime: 20 }
const GROUP = 'crash-report-group'
const ALREADY_RUNNING = 'consumer#run was called, but the consumer is already running'
const RESTARTING = 'Restarting the consumer in'
const CRASH_LINE = 'Crash:'
/** A restart delay no other timer of the consumer or the scripted cluster has. */
const LONG = 1234567

/**
 * A consumer over a scripted cluster that crashes once, with its log sink and its CRASH listener
 * calling the consumer while the crash is reported. All waiting is fake time: `pump` and `settle`
 * advance it one millisecond per step, and a restart delay of `LONG` is observed, never waited for.
 */
describe('Consumer > calls made while a crash is reported', () => {
  const handlers = { eachMessage: async () => {} }
  let scripted
  let cluster
  let consumer
  let runners
  let messages
  let calls
  let crashes
  let crashLogged
  let held
  let mark
  let timers
  let clearSpy

  beforeAll(prepareRecordBatches)

  /**
   * Builds the consumer and its cluster; the cluster's disconnect also waits for `held`.
   * @param {object} [options]
   * @param {(message: string) => void} [options.onLog] called by the sink with each logged message,
   *   from inside that log call
   * @param {object} [options.retry] consumer retry options added to `RETRY`
   * @param {number} [options.batches=0] fetches that carry a record before the empty ones
   */
  const build = ({ onLog = () => {}, retry = {}, batches = 0 } = {}) => {
    scripted = groupScript(createScriptedCluster({ nodes: NODES }), { batches })
    const instrumentationEmitter = new InstrumentationEventEmitter()
    cluster = new Cluster({
      ...scripted.kafkaOptions(),
      logger: newLogger(),
      retry: RETRY,
      instrumentationEmitter,
    })
    const realDisconnect = cluster.disconnect.bind(cluster)
    cluster.disconnect = async (...args) => {
      await realDisconnect(...args)
      await held?.promise
    }
    const sink = ({ log }) => {
      messages.push(log.message)
      if (log.message.startsWith(CRASH_LINE)) crashLogged = true
      onLog(log.message)
    }
    consumer = createConsumer({
      cluster,
      groupId: GROUP,
      maxWaitTimeInMs: 20,
      heartbeatInterval: 10000,
      logger: createLogger({ level: LEVELS.INFO, logCreator: () => sink }),
      retry: { ...RETRY, ...retry },
      instrumentationEmitter,
    })
  }

  /**
   * Installs the CRASH listener: it records the event, takes the timer mark and sets the restart
   * delay.
   * @param {object} [options]
   * @param {number|null} [options.retryTime=LONG] the restart delay, or null to leave the default
   * @param {() => void} [options.onCrash] called from inside the event
   */
  const listenToCrash = ({ retryTime = LONG, onCrash = () => {} } = {}) =>
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      if (retryTime !== null) payload.error.retryTime = retryTime
      mark = timers.mock.calls.length
      crashes.push(payload)
      onCrash()
    })

  /**
   * @param {() => void} action
   * @returns {(message: string) => void} a log hook that runs `action` once, on the first
   *   `Restarting the consumer in` line
   */
  const onRestartLog = (action) => {
    let done = false
    return (message) => {
      if (done || !message.startsWith(RESTARTING)) return
      done = true
      action()
    }
  }

  /**
   * Awaits a promise while advancing fake time.
   * @param {Promise<any>} promise
   * @param {string} label named in the failure when it never settles
   * @returns {Promise<any>} what it resolved to
   * @throws {Error} what it rejected with
   */
  const finish = async (promise, label) => {
    let done = false
    let failure
    let value
    const job = promise.then(
      (result) => {
        done = true
        value = result
      },
      (error) => {
        done = true
        failure = error
      }
    )
    await pump(() => done, label)
    await job
    if (failure) throw failure
    return value
  }

  /** Advances fake time `steps` milliseconds, letting socket deliveries run between steps. */
  const settle = async (steps) => {
    for (let step = 0; step < steps; step++) {
      await new Promise(setImmediate)
      await jest.advanceTimersByTimeAsync(1)
    }
  }

  /**
   * Saves a call made from the sink or the listener for the test to await; a rejection is reported
   * by that await, not as an unhandled one in between.
   * @param {Promise<any>} promise
   */
  const keep = (promise) => {
    promise.catch(() => {})
    calls.push(promise)
  }

  const frames = (apis) => scripted.requests().filter(({ api }) => apis.includes(api)).length
  const joins = () => frames(['JoinGroup'])
  const restartLines = () => messages.filter((message) => message.startsWith(RESTARTING)).length

  /** @returns the timers armed with the `LONG` delay since the CRASH listener took its mark */
  const restartTimers = () =>
    timers.mock.results
      .slice(mark)
      .filter((_, index) => timers.mock.calls[mark + index][1] === LONG)
      .map(({ value }) => value)

  /** Connects, subscribes and runs, then waits until the run is fetching. */
  const runUntilFetching = async (runOptions = handlers) => {
    await finish(consumer.connect(), 'connect')
    await finish(consumer.subscribe({ topic: TOPIC, fromBeginning: true }), 'subscribe')
    await finish(consumer.run(runOptions), 'run')
    await pump(() => frames(['Fetch']) > 0, 'a fetch from the first run')
    expect(joins()).toBe(1)
  }

  /** Makes the first run's next fetch fail once with a retriable error, which crashes the run. */
  const failNextFetch = () => {
    const failure = new KafkaJSNoBrokerAvailableError()
    jest.spyOn(runners[0].consumerGroup, 'fetch').mockRejectedValueOnce(failure)
  }

  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['setImmediate'] })
    timers = jest.spyOn(global, 'setTimeout')
    clearSpy = jest.spyOn(global, 'clearTimeout')
    runners = []
    messages = []
    calls = []
    crashes = []
    crashLogged = false
    held = null
    mark = 0
    const start = Runner.prototype.start
    jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
      runners.push(this)
      return start.apply(this, args)
    })
  })

  afterEach(async () => {
    try {
      await finish(consumer.disconnect(), 'final disconnect')
    } finally {
      for (const runner of runners) runner.retire(consumerStoppingError())
      jest.restoreAllMocks()
      jest.useRealTimers()
    }
  })

  it('a stop from the restart log cancels the restart and arms no timer', async () => {
    build({ onLog: onRestartLog(() => keep(consumer.stop())) })
    listenToCrash()
    await runUntilFetching()

    failNextFetch()
    await pump(() => calls.length === 1, 'the stop from the restart log')
    await finish(calls[0], 'stop from the restart log')
    await settle(50)

    expect(crashes).toHaveLength(1)
    expect(restartLines()).toBe(1)
    expect(restartTimers()).toEqual([])
    expect(joins()).toBe(1)
  })

  it('an unsubscribe from the restart log cancels the restart and arms no timer', async () => {
    build({ onLog: onRestartLog(() => keep(consumer.unsubscribe())) })
    listenToCrash()
    await runUntilFetching()

    failNextFetch()
    await pump(() => calls.length === 1, 'the unsubscribe from the restart log')
    await finish(calls[0], 'unsubscribe from the restart log')
    await settle(50)

    expect(crashes).toHaveLength(1)
    expect(restartLines()).toBe(1)
    expect(restartTimers()).toEqual([])
    expect(joins()).toBe(1)
  })

  it('a disconnect from the restart log cancels the restart and arms no timer', async () => {
    build({ onLog: onRestartLog(() => keep(consumer.disconnect())) })
    listenToCrash()
    await runUntilFetching()

    failNextFetch()
    await pump(() => calls.length === 1, 'the disconnect from the restart log')
    await finish(calls[0], 'disconnect from the restart log')
    await settle(50)

    expect(crashes).toHaveLength(1)
    expect(restartLines()).toBe(1)
    expect(restartTimers()).toEqual([])
    expect(joins()).toBe(1)
  })

  it('a run from the restart log supersedes the restart and arms no timer', async () => {
    build({ onLog: onRestartLog(() => keep(consumer.run(handlers))) })
    listenToCrash()
    await runUntilFetching()

    failNextFetch()
    await pump(() => joins() >= 2, 'the join of the run from the restart log')
    await finish(calls[0], 'run from the restart log')
    await settle(50)

    expect(crashes).toHaveLength(1)
    expect(restartLines()).toBe(1)
    expect(restartTimers()).toEqual([])
    expect(joins()).toBe(2)
    expect(runners).toHaveLength(2)
  })

  it('a run from the CRASH listener supersedes the restart and arms no timer', async () => {
    build()
    listenToCrash({ onCrash: () => keep(consumer.run(handlers)) })
    await runUntilFetching()

    failNextFetch()
    await pump(() => joins() >= 2, 'the join of the run from the CRASH listener')
    await finish(calls[0], 'run from the CRASH listener')
    await settle(50)

    expect(crashes).toHaveLength(1)
    expect(restartLines()).toBe(0)
    expect(restartTimers()).toEqual([])
    expect(joins()).toBe(2)
    expect(runners).toHaveLength(2)
  })

  it('a run made while restartOnFailure decides supersedes the restart and the event reports no restart', async () => {
    let asked = false
    let decide
    const verdict = new Promise((resolve) => (decide = resolve))
    build({
      retry: {
        restartOnFailure: () => {
          asked = true
          return verdict
        },
      },
    })
    listenToCrash()
    await runUntilFetching()

    failNextFetch()
    await pump(() => asked, 'the restartOnFailure call')
    keep(consumer.run(handlers))
    await pump(() => joins() >= 2, 'the join of the explicit run')
    await finish(calls[0], 'explicit run')
    decide(true)
    await pump(() => crashes.length === 1, 'the CRASH event')
    await settle(50)

    expect(crashes[0].restart).toBe(false)
    expect(restartLines()).toBe(0)
    expect(restartTimers()).toEqual([])
    expect(joins()).toBe(2)
    expect(runners).toHaveLength(2)
  })

  it('a run that waited on the crash cleanup cancels the restart armed meanwhile', async () => {
    let release
    build()
    listenToCrash()
    await runUntilFetching()
    held = { promise: new Promise((resolve) => (release = resolve)) }

    failNextFetch()
    await pump(() => crashLogged, 'the Crash line')
    keep(consumer.run(handlers))
    await settle(20)
    expect(joins()).toBe(1)

    release()
    await finish(calls[0], 'run that waited on the crash cleanup')
    await settle(50)

    const clearedTimers = clearSpy.mock.calls.map(([timer]) => timer)
    expect(crashes).toHaveLength(1)
    expect(restartTimers().every((timer) => clearedTimers.includes(timer))).toBe(true)
    expect(joins()).toBe(2)
    expect(runners).toHaveLength(2)
  })

  it('a run refused as already running during the crash report leaves the restart in place (control)', async () => {
    let bound = null
    let called = false
    build({
      batches: 1,
      onLog: (message) => {
        if (called || !bound || !message.startsWith(CRASH_LINE)) return
        called = true
        keep(bound())
      },
    })
    listenToCrash({ retryTime: null })
    await finish(consumer.connect(), 'connect')
    await finish(consumer.subscribe({ topic: TOPIC, fromBeginning: true }), 'subscribe')
    await finish(
      consumer.run({
        eachMessage: async () => {
          bound ??= AsyncResource.bind(() => consumer.run(handlers))
        },
      }),
      'run'
    )
    await pump(() => bound !== null, 'a message handler call')

    failNextFetch()
    await pump(() => crashes.length === 1, 'the CRASH event')
    await pump(() => joins() >= 2, 'the restart join')
    await settle(100)

    expect(called).toBe(true)
    await expect(finish(calls[0], 'run from the Crash line')).resolves.toBeUndefined()
    expect(messages).toContain(ALREADY_RUNNING)
    expect(crashes[0].restart).toBe(true)
    expect(restartLines()).toBe(1)
    expect(joins()).toBe(2)
    expect(runners).toHaveLength(2)
  })

  it('a crash report that no call interrupts restarts the consumer once (control)', async () => {
    build()
    listenToCrash({ retryTime: null })
    await runUntilFetching()

    failNextFetch()
    await pump(() => joins() >= 2, 'the restart join')
    await settle(100)

    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(true)
    expect(restartLines()).toBe(1)
    expect(joins()).toBe(2)
    expect(runners).toHaveLength(2)
  })
})
