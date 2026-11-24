const Cluster = require('../../cluster')
const Runner = require('../runner')
const createConsumer = require('../index')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createLogger, LEVELS } = require('../../loggers')
const { consumerStoppingError } = require('../rebalanceErrorContract')
const { newLogger } = require('testHelpers')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC, prepareRecordBatches } = require('./fixtures/fetchHarness')
const { pump } = require('./fixtures/lifecycleCase')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const RETRY = { retries: 3, initialRetryTime: 10, maxRetryTime: 20 }
const GROUP = 'starting-log-group'
const ALREADY_RUNNING = 'consumer#run was called, but the consumer is already running'
const MEMBER_FRAMES = ['Fetch', 'Heartbeat', 'JoinGroup']

/**
 * A consumer over a scripted cluster whose log sink calls the consumer from its `Starting` line.
 * All waiting is fake time: `pump` and `settle` advance it one millisecond per step.
 */
describe('Consumer > public calls made from the Starting log', () => {
  const handlers = { eachMessage: async () => {} }
  let scripted
  let cluster
  let consumer
  let runners
  let messages
  let calls
  let stops
  let startings

  beforeAll(prepareRecordBatches)

  /**
   * Builds the consumer and its cluster.
   * @param {(count: number) => void} [onStarting] called by the sink with the number of `Starting`
   *   lines logged so far, from inside that log call
   */
  const build = (onStarting = () => {}) => {
    startings = 0
    scripted = groupScript(createScriptedCluster({ nodes: NODES }), { batches: 0 })
    const instrumentationEmitter = new InstrumentationEventEmitter()
    cluster = new Cluster({
      ...scripted.kafkaOptions(),
      logger: newLogger(),
      retry: RETRY,
      instrumentationEmitter,
    })
    const sink = ({ log }) => {
      messages.push(log.message)
      if (log.message === 'Starting') onStarting(++startings)
    }
    consumer = createConsumer({
      cluster,
      groupId: GROUP,
      maxWaitTimeInMs: 20,
      heartbeatInterval: 10000,
      logger: createLogger({ level: LEVELS.INFO, logCreator: () => sink }),
      retry: RETRY,
      instrumentationEmitter,
    })
    consumer.on(consumer.events.STOP, () => stops++)
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

  const frames = (apis) => scripted.requests().filter(({ api }) => apis.includes(api)).length
  const joins = () => frames(['JoinGroup'])

  /** Connects and subscribes, so only `run()` is left to the test. */
  const prepare = async () => {
    await finish(consumer.connect(), 'connect')
    await finish(consumer.subscribe({ topic: TOPIC, fromBeginning: true }), 'subscribe')
  }

  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['setImmediate'] })
    runners = []
    messages = []
    calls = []
    stops = 0
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

  it('a run from the Starting log adds no second member, and after an awaited stop nothing is sent', async () => {
    build(() => calls.push(consumer.run(handlers)))
    await prepare()

    await finish(consumer.run(handlers), 'run')
    await settle(50)

    await expect(finish(calls[0], 'nested run')).resolves.toBeUndefined()
    expect(messages).toContain(ALREADY_RUNNING)
    expect(joins()).toBe(1)
    expect(runners).toHaveLength(1)

    await finish(consumer.stop(), 'stop')
    const sent = frames(MEMBER_FRAMES)
    await settle(100)

    expect(frames(MEMBER_FRAMES)).toBe(sent)
  })

  it('a stop from the Starting log ends the start before any JoinGroup', async () => {
    build(() => calls.push(consumer.stop()))
    await prepare()

    await finish(consumer.run(handlers), 'run')
    await finish(calls[0], 'stop from the log')
    await settle(50)

    expect(joins()).toBe(0)
    expect(stops).toBe(1)
  })

  it('a disconnect from the Starting log ends the start and nothing reconnects', async () => {
    build(() => calls.push(consumer.disconnect()))
    await prepare()

    await finish(consumer.run(handlers), 'run')
    await finish(calls[0], 'disconnect from the log')
    await settle(100)

    expect(joins()).toBe(0)
    expect(cluster.isConnected()).toBe(false)
  })

  it('a run from the Starting log of a crash restart adds no second member', async () => {
    build((count) => {
      if (count === 2) calls.push(consumer.run(handlers))
    })
    const crashed = new Promise((resolve) => consumer.on(consumer.events.CRASH, resolve))
    await prepare()
    await finish(consumer.run(handlers), 'run')
    await settle(20)
    expect(joins()).toBe(1)

    cluster.disconnect()
    const { payload } = await finish(crashed, 'crash')
    await pump(() => joins() >= 2, 'restart join')
    await settle(100)

    expect(payload.restart).toBe(true)
    expect(startings).toBe(2)
    expect(messages).toContain(ALREADY_RUNNING)
    expect(joins()).toBe(2)
  })

  it('three start and stop rounds with a run from each Starting log send one JoinGroup per round', async () => {
    build(() => calls.push(consumer.run(handlers)))
    await prepare()

    for (let round = 1; round <= 3; round++) {
      await finish(consumer.run(handlers), `run ${round}`)
      await settle(20)
      expect(joins()).toBe(round)

      await finish(consumer.stop(), `stop ${round}`)
      const sent = frames(MEMBER_FRAMES)
      await settle(50)
      expect(frames(MEMBER_FRAMES)).toBe(sent)
    }

    expect(runners).toHaveLength(3)
  })

  it('a Starting log sink that throws does not change the start', async () => {
    build(() => {
      throw new Error('sink failed')
    })
    await prepare()

    await finish(consumer.run(handlers), 'run')
    await settle(50)

    expect(startings).toBe(1)
    expect(joins()).toBe(1)
    expect(runners).toHaveLength(1)
  })
})
