const createConsumer = require('../index')
const { disposeConsumer } = require('../disposal')
const Runner = require('../runner')
const OffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')
const { KafkaJSNonRetriableError } = require('../../errors')
const { createLogger, LEVELS } = require('../../loggers')

const TOPIC = 'test-topic'

/** A stub cluster with a real instrumentation emitter, so nothing here needs a broker. */
const createCluster = () => {
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const operations = new OffsetOperations()
  return {
    instrumentationEmitter,
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    metadata: async () => ({ topicMetadata: [] }),
    addMultipleTargetTopics: async () => {},
    refreshMetadata: async () => {},
    refreshMetadataIfNecessary: async () => {},
    committedOffsets: () => ({}),
    offsetOperations: () => operations,
    defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
    topicId: () => undefined,
    onTopicRecreated: (handler) =>
      instrumentationEmitter.addListener(CLUSTER_TOPIC_RECREATED, handler),
  }
}

/**
 * A consumer over the stub cluster whose logger keeps every entry from errors up.
 * @returns {{ consumer: object, entries: object[] }}
 */
const build = () => {
  const cluster = createCluster()
  const entries = []
  const consumer = createConsumer({
    cluster,
    groupId: 'test-group',
    logger: createLogger({
      level: LEVELS.ERROR,
      logCreator: () => (entry) => entries.push(entry),
    }),
    instrumentationEmitter: cluster.instrumentationEmitter,
    metadataMaxAge: 300000,
    retry: { retries: 0 },
  })
  return { consumer, entries }
}

/** A consumer that has subscribed and run, with the runner stubbed out. */
const buildRunning = async () => {
  jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
  jest.spyOn(Runner.prototype, 'stop').mockResolvedValue()
  const built = build()
  await built.consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  await built.consumer.run({ eachMessage: async () => {} })
  return built
}

describe('Consumer > what a call is refused for before it reaches a broker', () => {
  afterEach(() => {
    jest.restoreAllMocks()
  })

  test('subscribe is refused while the consumer is running', async () => {
    const { consumer } = await buildRunning()

    const error = await consumer.subscribe({ topic: 'other-topic' }).catch((thrown) => thrown)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe('Cannot subscribe to topic while consumer is running')
    await disposeConsumer(consumer)
  })

  test('subscribe is refused for topics that is not an array', async () => {
    const { consumer } = build()

    const error = await consumer.subscribe({ topics: TOPIC }).catch((thrown) => thrown)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe('Argument "topics" must be an array')
    await disposeConsumer(consumer)
  })

  test.each([
    ['a number', 5],
    ['an object', {}],
  ])('commitOffsets is refused for metadata that is %s', async (_, metadata) => {
    const { consumer } = build()

    const error = await consumer
      .commitOffsets([{ topic: TOPIC, partition: 0, offset: '1', metadata }])
      .catch((thrown) => thrown)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe(
      `Invalid offset metadata, expected string or null, received ${metadata}`
    )
    await disposeConsumer(consumer)
  })

  describe.each(['pause', 'resume'])('%s', (verb) => {
    test.each([
      ['an entry with no topic', [{ partitions: [0] }], 'Invalid topic [object Object]'],
      ['an entry that is not an object', [null], 'Invalid topic null'],
      [
        'partitions that are not an array',
        [{ topic: TOPIC, partitions: 'zero' }],
        `Array of valid partitions required to ${verb} specific partitions instead of zero`,
      ],
      [
        'a partition that is not a number',
        [{ topic: TOPIC, partitions: [0, 'one'] }],
        `Array of valid partitions required to ${verb} specific partitions instead of 0,one`,
      ],
    ])('is refused for %s', async (_, topicPartitions, message) => {
      const { consumer } = build()

      expect(() => consumer[verb](topicPartitions)).toThrow(new KafkaJSNonRetriableError(message))
      await disposeConsumer(consumer)
    })

    test('is refused before the consumer has run', async () => {
      const { consumer } = build()

      expect(() => consumer[verb]([{ topic: TOPIC }])).toThrow(
        new KafkaJSNonRetriableError(
          'Consumer group was not initialized, consumer#run must be called first'
        )
      )
      await disposeConsumer(consumer)
    })
  })

  test('a listener that rejects is logged as an error with its event name and stack, and the others still run', async () => {
    const { consumer, entries } = build()
    const failure = new Error('listener failed')
    const other = jest.fn()
    consumer.on(consumer.events.CONNECT, async () => {
      throw failure
    })
    consumer.on(consumer.events.CONNECT, other)

    await consumer.connect()
    await new Promise((resolve) => setImmediate(resolve))

    expect(other).toHaveBeenCalledTimes(1)
    expect(entries.map(({ level, log }) => ({ level, ...log }))).toEqual([
      expect.objectContaining({
        level: LEVELS.ERROR,
        message: 'Failed to execute listener: listener failed',
        eventName: consumer.events.CONNECT,
        stack: failure.stack,
      }),
    ])
    await disposeConsumer(consumer)
  })
})
