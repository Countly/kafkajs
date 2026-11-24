const createConsumer = require('../index')
const Runner = require('../runner')
const { newLogger, waitFor } = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const OffsetOperations = require('../offsetManager/offsetOperations')
const {
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSNonRetriableError,
  KafkaJSTopicRecreatedError,
} = require('../../errors')

const createFakeCluster = ({ connect = async () => {} } = {}) => ({
  connect,
  disconnect: jest.fn(async () => {}),
  refreshMetadataIfNecessary: jest.fn(async () => {}),
  removeBroker: jest.fn(),
  isConnected: () => true,
  addMultipleTargetTopics: jest.fn(async () => {}),
  onTopicRecreated: () => () => {},
  offsetOperations: () => new OffsetOperations(),
  retry: {},
})

const createFakeLogger = () => {
  const logger = {
    error: jest.fn(),
    warn: jest.fn(),
    info: jest.fn(),
    debug: jest.fn(),
    namespace: () => logger,
  }
  return logger
}

/** Runs `fn` while recording any `unhandledRejection`, returning the recorded reasons. */
const withUnhandledRejectionTracking = async (fn) => {
  const unhandledRejections = []
  const onUnhandledRejection = (reason) => unhandledRejections.push(reason)
  process.on('unhandledRejection', onUnhandledRejection)

  try {
    await fn()
    // Give any same-tick unhandled rejection a chance to surface before we stop listening.
    await new Promise((resolve) => setImmediate(resolve))
  } finally {
    process.off('unhandledRejection', onUnhandledRejection)
  }

  return unhandledRejections
}

describe('Consumer > CRASH contract', () => {
  it('emits CRASH once even when disconnect() throws while handling the crash', async () => {
    const cluster = createFakeCluster()
    cluster.disconnect = jest.fn(async () => {
      throw new Error('socket already gone')
    })
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const crashes = []

    const consumer = createConsumer({
      cluster,
      groupId: 'crash-test',
      logger: newLogger(),
      instrumentationEmitter,
      retry: { retries: 0, restartOnFailure: async () => false },
    })
    consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))

    // Force the runner to die on start
    jest.spyOn(cluster, 'connect').mockImplementation(async () => {
      throw new KafkaJSNumberOfRetriesExceeded(new Error('boom'), { retryCount: 5 })
    })

    await consumer.subscribe({ topic: 'crash-topic' })
    await consumer.run({ eachMessage: async () => {} })

    expect(crashes).toHaveLength(1)
    expect(crashes[0].groupId).toEqual('crash-test')
    expect(crashes[0].restart).toBe(false)
  })

  it('honors restartOnFailure for non-retriable errors', async () => {
    const restartOnFailure = jest.fn(async () => false)
    const cluster = createFakeCluster()
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const crashes = []

    const consumer = createConsumer({
      cluster,
      groupId: 'crash-test-2',
      logger: newLogger(),
      instrumentationEmitter,
      retry: { retries: 0, restartOnFailure },
    })
    consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))

    jest.spyOn(cluster, 'connect').mockImplementation(async () => {
      throw new KafkaJSNonRetriableError('fatal misconfiguration')
    })

    await consumer.subscribe({ topic: 'crash-topic' })
    await consumer.run({ eachMessage: async () => {} })

    expect(restartOnFailure).toHaveBeenCalledTimes(1)
    expect(crashes[0].restart).toBe(false)
  })

  it('schedules a restart when restartOnFailure returns true for a non-retriable error', async () => {
    const cluster = createFakeCluster()
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const crashes = []

    const consumer = createConsumer({
      cluster,
      groupId: 'crash-test-restart-true',
      logger: newLogger(),
      instrumentationEmitter,
      retry: { retries: 0, initialRetryTime: 20, restartOnFailure: async () => true },
    })
    consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))

    jest.spyOn(cluster, 'connect').mockImplementation(async () => {
      throw new KafkaJSNonRetriableError('fatal misconfiguration')
    })

    await consumer.subscribe({ topic: 'crash-topic' })
    await consumer.run({ eachMessage: async () => {} })

    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(true)
    expect(cluster.connect).toHaveBeenCalledTimes(1)

    // A restart was genuinely scheduled and attempted, not just recorded in the payload.
    await waitFor(() => cluster.connect.mock.calls.length > 1, { delay: 10, maxWait: 2000 })
    expect(cluster.connect.mock.calls.length).toBeGreaterThan(1)

    await consumer.disconnect()
  })

  it('defaults to restart when restartOnFailure itself throws, for a non-retriable error', async () => {
    const cluster = createFakeCluster()
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const crashes = []

    const consumer = createConsumer({
      cluster,
      groupId: 'crash-test-callback-throws',
      logger: newLogger(),
      instrumentationEmitter,
      retry: {
        retries: 0,
        initialRetryTime: 20,
        restartOnFailure: async () => {
          throw new Error('restartOnFailure itself is broken')
        },
      },
    })
    consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))

    jest.spyOn(cluster, 'connect').mockImplementation(async () => {
      throw new KafkaJSNonRetriableError('fatal misconfiguration')
    })

    await consumer.subscribe({ topic: 'crash-topic' })
    await consumer.run({ eachMessage: async () => {} })

    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(true)

    await consumer.disconnect()
  })

  describe('a crash caused by a recreated topic', () => {
    const recreated = () =>
      new KafkaJSTopicRecreatedError('topic crash-topic was recreated', {
        topic: 'crash-topic',
        previousTopicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        groupId: 'crash-recreated',
        cause: new Error('seen in a metadata answer'),
      })

    // Nothing else in these runs arms a timer with this delay, so it identifies the restart timer.
    const RESTART_DELAY = 4321

    /**
     * Starts a consumer whose run dies at once with `error`, and returns what its crash did.
     * @param {Error} error the run's failure
     * @param {Function} [restartOnFailure] the user's restart callback, none by default
     * @returns {Promise<{crashes: object[], restartsArmed: number, cleanups: object[],
     *   connects: number}>} the CRASH payloads, the restart timers armed, the cluster disconnects
     *   of the crash cleanup, and the connect attempts, all read before the final disconnect
     */
    const crashWith = async (error, restartOnFailure) => {
      const cluster = createFakeCluster()
      const crashes = []
      const consumer = createConsumer({
        cluster,
        groupId: 'crash-recreated',
        logger: createFakeLogger(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        retry: { retries: 0, initialRetryTime: RESTART_DELAY, restartOnFailure },
      })
      consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))
      jest.spyOn(cluster, 'connect').mockImplementation(async () => {
        throw error
      })

      const timers = jest.spyOn(global, 'setTimeout')
      let restartsArmed
      try {
        await consumer.subscribe({ topic: 'crash-topic' })
        await consumer.run({ eachMessage: async () => {} })
        restartsArmed = timers.mock.calls.filter(([, delay]) => delay === RESTART_DELAY).length
      } finally {
        timers.mockRestore()
      }
      const cleanups = cluster.disconnect.mock.calls.map(([options]) => options)
      const connects = cluster.connect.mock.calls.length
      await consumer.disconnect()
      return { crashes, restartsArmed, cleanups, connects }
    }

    /**
     * One CRASH for `error`, after a finished cleanup, with no restart reported or armed.
     * @param {object} outcome what `crashWith` returned
     * @param {Error} error the run's failure
     */
    const expectCrashedForGood = ({ crashes, restartsArmed, cleanups, connects }, error) => {
      expect(crashes).toHaveLength(1)
      expect(crashes[0].error).toBe(error)
      expect(crashes[0].restart).toBe(false)
      expect(cleanups).toEqual([{ preserveSetupState: true }])
      expect(restartsArmed).toBe(0)
      expect(connects).toBe(1)
    }

    it.each([
      ['returns true', () => jest.fn(async () => true)],
      [
        'throws',
        () =>
          jest.fn(async () => {
            throw new Error('restartOnFailure itself is broken')
          }),
      ],
    ])('is never restarted when restartOnFailure %s', async (_, callback) => {
      const error = recreated()
      const restartOnFailure = callback()

      expectCrashedForGood(await crashWith(error, restartOnFailure), error)
      // The callback is still asked; its answer is overruled.
      expect(restartOnFailure).toHaveBeenCalledTimes(1)
      expect(restartOnFailure).toHaveBeenCalledWith(error)
    })

    it('is never restarted when the error is inside the cause chain of the crash', async () => {
      // In the middle of the chain: neither the reported error nor its deepest cause.
      const error = new KafkaJSError('the fetch round failed', {
        retriable: true,
        cause: recreated(),
      })
      const restartOnFailure = jest.fn(async () => true)

      expectCrashedForGood(await crashWith(error, restartOnFailure), error)
      expect(restartOnFailure).toHaveBeenCalledWith(error)
    })

    it('is never restarted by default when the retries were exhausted on it', async () => {
      // Without restartOnFailure an exhausted retry restarts, whatever it wraps.
      const error = new KafkaJSNumberOfRetriesExceeded(recreated(), { retryCount: 3 })

      expectCrashedForGood(await crashWith(error), error)
    })

    it('leaves an unrelated error to restartOnFailure', async () => {
      const error = new KafkaJSNonRetriableError('fatal misconfiguration')
      const restartOnFailure = jest.fn(async () => true)

      const { crashes, restartsArmed, cleanups, connects } = await crashWith(
        error,
        restartOnFailure
      )

      expect(restartOnFailure).toHaveBeenCalledWith(error)
      expect(crashes).toHaveLength(1)
      expect(crashes[0].error).toBe(error)
      expect(crashes[0].restart).toBe(true)
      expect(cleanups).toEqual([{ preserveSetupState: true }])
      expect(restartsArmed).toBe(1)
      expect(connects).toBe(1)
    })

    it('is never restarted when the error sits in a cause chain that loops', async () => {
      const error = new KafkaJSError('the fetch round failed', {
        retriable: true,
        cause: recreated(),
      })
      error.cause.cause = error
      const restartOnFailure = jest.fn(async () => true)

      expectCrashedForGood(await crashWith(error, restartOnFailure), error)
      expect(restartOnFailure).toHaveBeenCalledWith(error)
    })

    it('is never restarted when a plain object links the error into the chain', async () => {
      const error = new KafkaJSError('the fetch round failed', {
        retriable: true,
        cause: { message: 'reported by another library', cause: recreated() },
      })
      const restartOnFailure = jest.fn(async () => true)

      expectCrashedForGood(await crashWith(error, restartOnFailure), error)
    })

    it('leaves an unrelated crash whose cause chain loops to its own restart policy', async () => {
      const inner = new KafkaJSError('the broker went away', { retriable: true })
      const error = new KafkaJSNonRetriableError('the fetch round failed', { cause: inner })
      inner.cause = error

      const { crashes, restartsArmed, cleanups, connects } = await crashWith(error)

      expect(crashes).toHaveLength(1)
      expect(crashes[0].error).toBe(error)
      // The deepest distinct cause is retriable, so the default policy restarts.
      expect(crashes[0].restart).toBe(true)
      expect(cleanups).toEqual([{ preserveSetupState: true }])
      expect(restartsArmed).toBe(1)
      expect(connects).toBe(1)
    })
  })

  it('crashes and reports when the retries run out on a handler error whose cause chain loops', async () => {
    const failure = new KafkaJSError('the handler failed', { retriable: true })
    const inner = new Error('the store refused it')
    failure.cause = inner
    // Every bounded walk reads this a few times; only an unbounded one gets past the limit, and
    // throwing there ends it instead of hanging the run.
    const LIMIT = 10000
    let reads = 0
    Object.defineProperty(inner, 'cause', {
      get() {
        reads += 1
        if (reads > LIMIT) throw new Error('the cause chain was walked round its loop')
        return failure
      },
    })

    const cluster = createFakeCluster()
    const crashes = []
    const consumer = createConsumer({
      cluster,
      groupId: 'crash-looping-cause',
      logger: createFakeLogger(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      retry: { retries: 0, restartOnFailure: async () => false },
    })
    consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))
    // The run goes straight to its fetch loop, which fails as a handler's error makes it fail.
    const start = jest.spyOn(Runner.prototype, 'start').mockImplementationOnce(async function () {
      this.running = true
      this.fetchManager = {
        start: async () => {
          throw failure
        },
        stop: async () => {},
      }
      this.scheduleFetchManager()
    })

    jest.useFakeTimers()
    try {
      const advanceUntil = async (done) => {
        for (let spent = 0; !done() && spent < 5000; spent += 10) {
          await jest.advanceTimersByTimeAsync(10)
        }
      }
      await consumer.subscribe({ topic: 'crash-topic' })
      let ran = false
      const run = consumer.run({ eachMessage: async () => {} })
      run.then(
        () => (ran = true),
        () => {}
      )
      await advanceUntil(() => ran && crashes.length > 0)
      const cleanups = cluster.disconnect.mock.calls.map(([options]) => options)

      expect(reads).toBeLessThanOrEqual(LIMIT)
      expect(ran).toBe(true)
      expect(crashes).toHaveLength(1)
      expect(crashes[0].error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(crashes[0].error.cause).toBe(failure)
      expect(crashes[0].error.retryCount).toBe(0)
      expect(crashes[0].error).not.toHaveProperty('type')
      expect(crashes[0].restart).toBe(false)
      expect(cleanups).toContainEqual({ preserveSetupState: true })

      let ended = false
      const end = consumer.disconnect()
      end.then(
        () => (ended = true),
        () => {}
      )
      await advanceUntil(() => ended)
      expect(ended).toBe(true)
      await end
    } finally {
      jest.useRealTimers()
      start.mockRestore()
    }
  })

  it('does not resurrect a consumer that was disconnected while restartOnFailure was still deciding', async () => {
    let resolveDecision
    const restartOnFailure = () => new Promise((resolve) => (resolveDecision = resolve))

    const cluster = createFakeCluster()
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const crashes = []

    const consumer = createConsumer({
      cluster,
      groupId: 'crash-test-zombie-restart',
      logger: newLogger(),
      instrumentationEmitter,
      retry: { retries: 0, restartOnFailure },
    })
    consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))

    jest.spyOn(cluster, 'connect').mockImplementation(async () => {
      throw new KafkaJSNonRetriableError('fatal misconfiguration')
    })

    await consumer.subscribe({ topic: 'crash-topic' })

    const runPromise = consumer.run({ eachMessage: async () => {} })

    // Wait until onCrash is blocked on the (slow) restartOnFailure decision.
    await waitFor(() => resolveDecision != null, { delay: 5, maxWait: 2000 })

    // The user gives up on the consumer while restartOnFailure is still deciding...
    await consumer.disconnect()

    // ...and only now does the slow callback resolve, wanting to restart.
    resolveDecision(true)
    await runPromise

    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(false)
    expect(cluster.connect).toHaveBeenCalledTimes(1)
  })
})

describe('Runner > crash handling from the fetch-loop path', () => {
  const createStubConsumerGroup = (overrides = {}) => ({
    groupId: 'runner-crash-test',
    memberId: 'member-1',
    attach: jest.fn(),
    // `stop()` clears the assigner state before LeaveGroup, inside the same try: a missing stub
    // would swallow the very leave() the assertions below depend on.
    clearAssignerState: jest.fn(),
    // `stop()` tells the membership before it cancels the heartbeat chain; the real ConsumerGroup
    // always answers this.
    onStopRequested: jest.fn(),
    // The Fetch-session seams of stop and forced retirement; the real group always has both.
    closeFetchSessions: jest.fn(async () => {}),
    retireFetchSessions: jest.fn(),
    retireCoordinatorLookup: jest.fn(),
    leave: jest.fn(async () => {}),
    getNodeIds: () => [],
    ...overrides,
  })

  it('stops the fetch manager and leaves the group on crash, and a restart triggered from onCrash sees a stopped runner', async () => {
    const consumerGroup = createStubConsumerGroup()
    const fetchManagerStop = jest.fn(async () => {})
    const err = new Error('fetch exploded')

    let restartTimeRunnerState = null
    let done
    const finished = new Promise((resolve) => (done = resolve))

    const runner = new Runner({
      logger: newLogger(),
      consumerGroup,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      retry: { retries: 0 },
      onCrash: async () => {}, // replaced below, once `runner` exists to close over
      eachMessage: async () => {},
    })

    const onCrash = jest.fn(async () => {
      // Mirrors the real onCrash: cleanup (disconnect -> stop) runs before a restart would be
      // scheduled from inside onCrash (`setTimeout(() => start(onCrash), retryTime)`).
      await runner.stop()
      restartTimeRunnerState = { running: runner.running, consuming: runner.consuming }
      done()
    })
    runner.onCrash = onCrash

    runner.fetchManager = {
      start: async () => {
        throw err
      },
      stop: fetchManagerStop,
    }

    runner.running = true

    runner.scheduleFetchManager()
    await finished

    expect(onCrash).toHaveBeenCalledTimes(1)
    expect(fetchManagerStop).toHaveBeenCalledTimes(1)
    expect(consumerGroup.leave).toHaveBeenCalledTimes(1)
    expect(restartTimeRunnerState).toEqual({ running: false, consuming: false })
    expect(runner.running).toBe(false)
    expect(runner.consuming).toBe(false)
  })

  it('awaits the crash handler without leaking an unhandled rejection when disconnect() fails inside it', async () => {
    const consumerGroup = createStubConsumerGroup()
    const err = new Error('fetch exploded')
    const crashes = []

    let done
    const finished = new Promise((resolve) => (done = resolve))

    const runner = new Runner({
      logger: newLogger(),
      consumerGroup,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      retry: { retries: 0 },
      onCrash: async (e) => {
        crashes.push(e)
        // Mirrors the real onCrash swallowing a disconnect() failure internally.
        try {
          await Promise.reject(new Error('disconnect failed'))
        } catch {}
        done()
      },
      eachMessage: async () => {},
    })

    runner.fetchManager = {
      start: async () => {
        throw err
      },
      stop: async () => {},
    }

    runner.running = true

    const unhandledRejections = await withUnhandledRejectionTracking(async () => {
      runner.scheduleFetchManager()
      await finished
    })

    expect(crashes).toHaveLength(1)
    expect(unhandledRejections).toHaveLength(0)
    expect(runner.running).toBe(false)
    expect(runner.consuming).toBe(false)
  })

  it('logs and swallows a rejection from onCrash itself without leaking an unhandled rejection', async () => {
    const logger = createFakeLogger()
    const consumerGroup = createStubConsumerGroup()
    const err = new Error('fetch exploded')
    const crashHandlerError = new Error('onCrash itself blew up')

    const runner = new Runner({
      logger,
      consumerGroup,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      retry: { retries: 0 },
      onCrash: async () => {
        throw crashHandlerError
      },
      eachMessage: async () => {},
    })

    runner.fetchManager = {
      start: async () => {
        throw err
      },
      stop: async () => {},
    }

    runner.running = true

    const unhandledRejections = await withUnhandledRejectionTracking(async () => {
      runner.scheduleFetchManager()
      await waitFor(() => logger.error.mock.calls.some(([msg]) => msg === 'Crash handler failed'), {
        delay: 10,
        maxWait: 2000,
      })
    })

    expect(unhandledRejections).toHaveLength(0)
    expect(runner.running).toBe(false)
    expect(runner.consuming).toBe(false)

    const [, meta] = logger.error.mock.calls.find(([msg]) => msg === 'Crash handler failed')
    expect(meta.error).toEqual(crashHandlerError.message)
    expect(meta.cause).toEqual(err.message)
  })
})
