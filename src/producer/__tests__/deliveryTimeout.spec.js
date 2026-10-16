jest.mock('../../cluster', () => jest.fn())

const Kafka = require('../../index')
const Cluster = require('../../cluster')
const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createErrorFromCode } = require('../../protocol/error')
const { createLogger, LEVELS } = require('../../loggers')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')

const TOPIC = 'topic-a'
const MESSAGES = [{ key: 'key-0', value: 'value-0' }]
const REQUEST_TIMED_OUT = 7

const silentLogger = () => createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })

describe('Producer > deliveryTimeout', () => {
  describe('validation at kafka.producer()', () => {
    beforeEach(() => {
      Cluster.mockImplementation((options) => createFakeCluster({ options }))
    })

    const createKafka = (options = {}) =>
      new Kafka({ brokers: ['localhost:9092'], logLevel: LEVELS.NOTHING, ...options })

    test.each([
      ['the default requestTimeout and linger', {}, {}, 30005],
      ['requestTimeout: 1000', { requestTimeout: 1000 }, {}, 1005],
      ['linger: 20', { requestTimeout: 1000 }, { linger: 20 }, 1020],
      ['linger: 0', { requestTimeout: 1000 }, { linger: 0 }, 1000],
      ['batchSize alone', { requestTimeout: 1000 }, { batchSize: 1 }, 1005],
    ])('with %s it must be at least linger + requestTimeout', (_, client, producer, floor) => {
      const kafka = createKafka(client)
      const create = (deliveryTimeout) => kafka.producer({ ...producer, deliveryTimeout })

      expect(() => create(floor - 1)).toThrow(KafkaJSInvalidConfigurationError)
      expect(() => create(floor - 1)).toThrow(
        `deliveryTimeout must be at least linger + requestTimeout (${floor} ms)`
      )
      expect(() => create(floor)).not.toThrow()
    })

    test.each([1500.5, 0, -1, '2000'])('%p is not a number of milliseconds', (deliveryTimeout) => {
      expect(() => createKafka({ requestTimeout: 1 }).producer({ deliveryTimeout })).toThrow(
        /deliveryTimeout must be an integer >= 1/
      )
    })

    test.each([
      ['linger', { linger: -1 }, /linger must be an integer >= 0/],
      ['linger', { linger: 2.5 }, /linger must be an integer >= 0/],
      ['batchSize', { batchSize: 0 }, /batchSize must be an integer >= 1/],
    ])('an invalid %s is refused', (_, options, message) => {
      expect(() => createKafka().producer(options)).toThrow(message)
    })

    test.each(['linger', 'batchSize', 'deliveryTimeout'])(
      '%s: a Symbol is refused as a configuration error, and a string value is quoted',
      (option) => {
        const create = (value) => createKafka().producer({ [option]: value })

        expect(() => create(Symbol('x'))).toThrow(KafkaJSInvalidConfigurationError)
        expect(() => create(Symbol('x'))).toThrow(/got Symbol\(x\)/)
        expect(() => create('x')).toThrow(/got 'x'/)
      }
    )

    test.each([
      ['linger', /linger must be an integer >= 0 and <= 2147483647, got 2147483648/],
      [
        'deliveryTimeout',
        /deliveryTimeout must be an integer >= 1 and <= 2147483647, got 2147483648/,
      ],
    ])(
      '%s is accepted up to 2147483647 ms, the longest timer delay, and refused above it',
      (option, message) => {
        const kafka = createKafka()

        expect(() => kafka.producer({ [option]: 2147483647 })).not.toThrow()
        expect(() => kafka.producer({ [option]: 2147483648 })).toThrow(
          KafkaJSInvalidConfigurationError
        )
        expect(() => kafka.producer({ [option]: 2147483648 })).toThrow(message)
      }
    )

    test('unset, nothing is checked and no deadline applies', () => {
      expect(() => createKafka({ requestTimeout: 1000 }).producer()).not.toThrow()
    })
  })

  describe.each([
    ['an idempotent producer', true],
    ['a non-idempotent producer', false],
  ])('on %s', (_, idempotent) => {
    /**
     * A producer whose every Produce fails with the retriable REQUEST_TIMED_OUT, so a send ends
     * only when its deadline or its retries run out.
     */
    const failingProducer = ({ deliveryTimeout, retry }) => {
      const lastErrors = []
      const cluster = createFakeCluster({
        produce: async () => {
          const error = createErrorFromCode(REQUEST_TIMED_OUT)
          lastErrors.push(error)
          throw error
        },
      })
      const producer = createProducer({
        cluster,
        logger: silentLogger(),
        idempotent,
        retry,
        deliveryTimeout,
      })
      return { producer, lastErrors }
    }

    test('a send that keeps failing rejects with KafkaJSDeliveryTimeout once the deadline passes', async () => {
      const { producer, lastErrors } = failingProducer({
        deliveryTimeout: 100,
        retry: { retries: 1000, initialRetryTime: 10, maxRetryTime: 20 },
      })
      await producer.connect()
      const started = Date.now()

      const error = await producer.send({ topic: TOPIC, messages: MESSAGES }).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.category).toEqual('abortable')
      expect(error.cause).toBe(lastErrors[lastErrors.length - 1])
      expect(error.cause).toMatchObject({ code: REQUEST_TIMED_OUT })
      expect(Date.now() - started).toBeLessThan(1000)
    })

    test('without deliveryTimeout, a deadline set as retry.maxTotalTime still bounds the send', async () => {
      const { producer } = failingProducer({
        retry: { retries: 1000, initialRetryTime: 10, maxRetryTime: 20, maxTotalTime: 100 },
      })
      await producer.connect()

      await expect(producer.send({ topic: TOPIC, messages: MESSAGES })).rejects.toThrow(
        KafkaJSDeliveryTimeout
      )
    })

    test('retries exhausted before the deadline still reject with KafkaJSNumberOfRetriesExceeded', async () => {
      const { producer } = failingProducer({
        deliveryTimeout: 60000,
        retry: { retries: 1, initialRetryTime: 1 },
      })
      await producer.connect()

      await expect(producer.send({ topic: TOPIC, messages: MESSAGES })).rejects.toThrow(
        KafkaJSNumberOfRetriesExceeded
      )
    })
  })

  test("kafka.producer({ deliveryTimeout }) bounds a send, ahead of the client's longer retry.maxTotalTime", async () => {
    Cluster.mockImplementation((options) =>
      createFakeCluster({
        options,
        produce: async () => {
          throw createErrorFromCode(REQUEST_TIMED_OUT)
        },
      })
    )
    const producer = new Kafka({
      brokers: ['localhost:9092'],
      logLevel: LEVELS.NOTHING,
      requestTimeout: 1,
      retry: { retries: 100, initialRetryTime: 10, maxRetryTime: 20, maxTotalTime: 60000 },
    }).producer({ deliveryTimeout: 100 })
    await producer.connect()
    const started = Date.now()

    const error = await producer.send({ topic: TOPIC, messages: MESSAGES }).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(Date.now() - started).toBeLessThan(1000)
  })
})
