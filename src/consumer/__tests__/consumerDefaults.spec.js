const { inspect } = require('util')
const { Kafka } = require('../../../index')
const Cluster = require('../../cluster')
const resolveGroupTimeouts = require('../groupTimeouts')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { KafkaJSNonRetriableError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('./fixtures/fetchHarness')
const createRetry = require('../../retry')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]

/**
 * Runs bounded event-loop turns until the real socket records a request.
 * @param {() => boolean} done the observable condition
 * @returns {Promise<void>}
 * @throws {Error} when the observation is absent within 5000 ms
 */
const turnsUntil = async (done) => {
  for (let index = 0; index < 1000 && !done(); index++)
    await new Promise((resolve) => setTimeout(resolve, 5))
  if (!done()) throw new Error('expected socket observation was absent')
}

/**
 * Captures actual factory-to-group options using subclasses in a private module registry.
 * The runner starts no background work and production exports stay unchanged.
 * @param {object} [config] consumer options
 * @returns {Promise<object>} the options the real factory built
 */
const capturedOptions = async (config = {}) => {
  let captured
  const logger = newLogger()
  const cluster = {
    onTopicRecreated: () => () => {},
    offsetOperations: () => new (require('../offsetManager/offsetOperations'))(),
    addMultipleTargetTopics: async () => {},
    disconnect: async () => {},
    isConnected: () => false,
    committedOffsets: () => ({}),
    retry: {},
  }
  try {
    await jest.isolateModulesAsync(async () => {
      const RealGroup = require('../consumerGroup')
      const RealRunner = require('../runner')
      jest.doMock(
        '../consumerGroup',
        () =>
          class extends RealGroup {
            constructor(options) {
              super(options)
              captured = options
            }
          }
      )
      jest.doMock(
        '../runner',
        () =>
          class extends RealRunner {
            async start() {}
          }
      )
      const consumer = require('../index')({
        cluster,
        logger,
        groupId: 'captured-defaults',
        metadataMaxAge: 300000,
        ...config,
      })
      await consumer.subscribe({ topics: [TOPIC] })
      await consumer.run({ eachMessage: async () => {} })
      await consumer.disconnect()
    })
  } finally {
    jest.dontMock('../consumerGroup')
    jest.dontMock('../runner')
  }
  return captured
}

describe('consumer configuration defaults', () => {
  beforeAll(prepareRecordBatches)

  test('the factory keeps classic round-robin, fetch targets and committed isolation', async () => {
    const options = await capturedOptions()
    expect({
      sessionTimeout: options.sessionTimeout,
      groupProtocol: options.groupProtocol,
      assigners: options.assigners.map(({ name }) => name),
      maxWaitTimeInMs: options.maxWaitTimeInMs,
      maxBytes: options.maxBytes,
      isolationLevel: options.isolationLevel,
    }).toEqual({
      sessionTimeout: 45000,
      groupProtocol: 'classic',
      assigners: ['RoundRobinAssigner'],
      maxWaitTimeInMs: 5000,
      maxBytes: 10485760,
      isolationLevel: 1,
    })
  })

  test('the resolver default is independently forty-five seconds', () => {
    expect(resolveGroupTimeouts({ logger: newLogger() })).toEqual({
      rebalanceTimeout: 300000,
      pollDeadline: 300000,
      sessionTimeout: 45000,
      heartbeatInterval: 3000,
      syncGroupTimeout: 65000,
    })
  })

  test.each([
    ['implicit', {}, 45000, 1],
    ['explicit session', { sessionTimeout: 30000 }, 30000, 1],
    ['uncommitted escape', { readUncommitted: true }, 45000, 0],
  ])(
    '%s options reach the first real requests',
    async (_label, config, sessionTimeout, isolationLevel) => {
      const seam = groupScript(createScriptedCluster({ nodes: NODES }), { batches: 0 })
      seam.answer('Fetch', () => ({ partitions: [{ partition: 0, errorCode: 0, records: false }] }))
      const consumer = new Kafka(seam.kafkaOptions()).consumer({
        groupId: 'wire-defaults',
        ...config,
      })
      const crashes = []
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload.error))
      try {
        await consumer.connect()
        await consumer.subscribe({ topics: [TOPIC], fromBeginning: true })
        await consumer.run({ autoCommit: false, eachMessage: async () => {} })
        await turnsUntil(
          () => seam.requests().some(({ api }) => api === 'Fetch') || crashes.length > 0
        )
        expect(crashes).toEqual([])
        seam.assertClean()
        const join = seam.requests().find(({ api }) => api === 'JoinGroup')
        expect(join).toMatchObject({
          version: 8,
          fields: {
            sessionTimeout,
            rebalanceTimeout: 300000,
            groupProtocols: [{ name: 'RoundRobinAssigner' }],
          },
        })
        expect(seam.requests().find(({ api }) => api === 'Fetch')).toMatchObject({
          version: 18,
          fields: { maxWaitMs: 5000, maxBytes: 10485760, isolationLevel },
        })
      } finally {
        await consumer.disconnect()
        seam.assertClean()
      }
    }
  )

  test.each([0, -1, 1.5, NaN, Infinity, '30000', 2147483648, null, Symbol('timeout'), 3n])(
    'refuses an invalid session timeout %p with its value',
    (value) => {
      const make = () =>
        new Kafka({ brokers: ['localhost:9092'], logCreator: newLogger.logCreator }).consumer({
          groupId: 'invalid-session',
          sessionTimeout: value,
        })
      expect(make).toThrow(KafkaJSNonRetriableError)
      expect(make).toThrow(
        `sessionTimeout must be an integer from 1 to 2147483647, got ${inspect(value)}`
      )
    }
  )

  test.each([0, -1, 2.5, NaN, Infinity, '3000', 2147483648, null, Symbol('heartbeat'), 3n])(
    'refuses an invalid heartbeat interval %p with its value',
    (value) => {
      const make = () =>
        new Kafka({ brokers: ['localhost:9092'], logCreator: newLogger.logCreator }).consumer({
          groupId: 'invalid-heartbeat',
          heartbeatInterval: value,
        })
      expect(make).toThrow(KafkaJSNonRetriableError)
      if (Number.isInteger(value) && value >= 1) {
        expect(make).toThrow(
          `heartbeatInterval (${inspect(value)}) must be lower than sessionTimeout (45000)`
        )
      } else {
        expect(make).toThrow(
          `heartbeatInterval must be an integer of at least 1, got ${inspect(value)}`
        )
      }
    }
  )

  test.each([
    [2, 1],
    [2147483647, 2147483646],
  ])('accepts valid boundary pair %p / %p', (sessionTimeout, heartbeatInterval) => {
    expect(
      resolveGroupTimeouts({ logger: newLogger(), sessionTimeout, heartbeatInterval })
    ).toMatchObject({ sessionTimeout, heartbeatInterval })
  })

  test.each([45000, 45001])(
    'keeps the lower-than-session rule for heartbeat %p',
    (heartbeatInterval) => {
      expect(() => resolveGroupTimeouts({ logger: newLogger(), heartbeatInterval })).toThrow(
        `Consumer heartbeatInterval (${heartbeatInterval}) must be lower than sessionTimeout (45000). It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.`
      )
    }
  )

  test('keeps the third-of-session warning and its strict boundary', () => {
    const logger = newLogger()
    const warn = jest.spyOn(logger, 'warn')
    resolveGroupTimeouts({ logger, heartbeatInterval: 15000 })
    expect(warn).not.toHaveBeenCalled()
    resolveGroupTimeouts({ logger, heartbeatInterval: 15001 })
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('more than a third'), {
      sessionTimeout: 45000,
      heartbeatInterval: 15001,
    })
  })

  test('a defaulted classic session is not an explicit consumer-protocol conflict', () => {
    const kafka = new Kafka({ brokers: ['localhost:9092'], logCreator: newLogger.logCreator })
    expect(() =>
      kafka.consumer({ groupId: 'server-session', groupProtocol: 'consumer' })
    ).not.toThrow()
    expect(() =>
      kafka.consumer({
        groupId: 'server-session',
        groupProtocol: 'consumer',
        sessionTimeout: undefined,
      })
    ).not.toThrow()
    expect(() =>
      kafka.consumer({
        groupId: 'server-session',
        groupProtocol: 'consumer',
        sessionTimeout: 45000,
      })
    ).toThrow('sessionTimeout cannot be set when groupProtocol=consumer')
    expect(() =>
      kafka.consumer({
        groupId: 'server-session',
        groupProtocol: 'consumer',
        heartbeatInterval: 3000,
      })
    ).toThrow('heartbeatInterval cannot be set when groupProtocol=consumer')
  })

  test('a broker session refusal names its settings and is never retried', async () => {
    const seam = groupScript(createScriptedCluster({ nodes: NODES }))
    seam.answer('JoinGroup', () => ({ errorCode: 26 }))
    const cluster = new Cluster({
      ...seam.kafkaOptions(),
      logger: newLogger(),
      retry: { retries: 0 },
      metadataMaxAge: 300000,
      allowAutoTopicCreation: true,
    })
    const protocol = new ClassicGroupProtocol({
      cluster,
      logger: newLogger(),
      groupId: 'refused-session',
      sessionTimeout: 45000,
      rebalanceTimeout: 300000,
      assigners: [],
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })
    const heartbeatManager = { disable: jest.fn(), enable: jest.fn() }
    protocol.attach({ heartbeatManager, rebalanceCoordinator: null })
    try {
      await cluster.connect()
      let caught
      try {
        await createRetry({ retries: 2, initialRetryTime: 1, factor: 0 })(() =>
          protocol.join({ topicsSubscribed: [TOPIC] })
        )
      } catch (error) {
        caught = error
      }
      expect(caught).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(caught).toMatchObject({
        retriable: false,
        type: 'INVALID_SESSION_TIMEOUT',
        code: 26,
        cause: { type: 'INVALID_SESSION_TIMEOUT', code: 26 },
      })
      expect(caught.message).toContain('sessionTimeout (45000)')
      expect(caught.message).toContain('group.min.session.timeout.ms')
      expect(caught.message).toContain('group.max.session.timeout.ms')
      expect(caught.message).not.toMatch(/6000|1800000/)
      expect(seam.requests().filter(({ api }) => api === 'JoinGroup')).toHaveLength(1)
      expect(heartbeatManager.disable).toHaveBeenCalledTimes(1)
      expect(heartbeatManager.enable).toHaveBeenCalledTimes(1)
      seam.assertClean()
    } finally {
      await cluster.disconnect()
      seam.assertClean()
    }
  })

  test('other join failures keep identity and re-enable heartbeats', async () => {
    const original = createErrorFromCode(30)
    const coordinator = {
      joinGroup: jest.fn(async () => {
        throw original
      }),
    }
    const heartbeatManager = { disable: jest.fn(), enable: jest.fn() }
    const protocol = new ClassicGroupProtocol({
      cluster: { findGroupCoordinator: async () => coordinator },
      logger: newLogger(),
      groupId: 'unchanged-failure',
      sessionTimeout: 45000,
      rebalanceTimeout: 300000,
      assigners: [],
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })
    protocol.attach({ heartbeatManager, rebalanceCoordinator: null })
    await expect(protocol.join({ topicsSubscribed: [TOPIC] })).rejects.toBe(original)
    expect(heartbeatManager.enable).toHaveBeenCalledTimes(1)
  })
})
