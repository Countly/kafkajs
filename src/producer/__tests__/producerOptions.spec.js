jest.mock('../../cluster', () => jest.fn())
jest.mock('../../retry', () => {
  const actual = jest.requireActual('../../retry')
  return Object.assign(jest.fn(actual), actual)
})

const fs = require('fs')
const Kafka = require('../../index')
const Cluster = require('../../cluster')
const createRetry = require('../../retry')
const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { DefaultPartitioner } = require('../partitioners')
const { PRODUCTION_RETRY_DEFAULTS } = require('../../../testHelpers/compatBaseline/capture')
const { createLogger, LEVELS } = require('../../loggers')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
} = require('../../errors')

const TOPIC = 'topic-a'
const MESSAGES = [{ key: 'key-0', value: 'value-0' }]
const DOWNGRADE = /^Idempotence disabled/

describe('Producer > options: the idempotent default and the settings that conflict with it', () => {
  let clusters, entries

  /** The warnings the client logged that turned idempotence off. */
  const downgrades = () =>
    entries.filter(({ level, log }) => level === LEVELS.WARN && DOWNGRADE.test(log.message))

  /** A client whose clusters are fakes recording the options they were built with. */
  const createKafka = (options = {}) =>
    new Kafka({
      clientId: 'producer-options',
      brokers: ['localhost:9092'],
      logLevel: LEVELS.WARN,
      logCreator: () => (entry) => entries.push(entry),
      ...options,
    })

  /** The fake cluster behind the producer created last. */
  const lastCluster = () => clusters[clusters.length - 1]

  beforeEach(() => {
    clusters = []
    entries = []
    Cluster.mockImplementation((options) => {
      const cluster = createFakeCluster({ options })
      clusters.push(cluster)
      return cluster
    })
    createRetry.mockClear()
  })

  test('kafka.producer() is idempotent and allocates a producer id on connect()', async () => {
    const producer = createKafka().producer()

    expect(producer.isIdempotent()).toBe(true)
    await producer.connect()

    expect(lastCluster().broker.initProducerId).toHaveBeenCalledTimes(1)
    expect(downgrades()).toEqual([])
  })

  describe('acks other than -1 on a send', () => {
    test('a producer that did not ask for idempotence warns once and sends as idempotent: false does', async () => {
      const producer = createKafka().producer()
      await producer.connect()

      await producer.send({ acks: 1, topic: TOPIC, messages: MESSAGES })

      expect(downgrades()).toHaveLength(1)
      expect(downgrades()[0].log.message).toMatch(/acks 1.*acks -1.*idempotent: false/)
      expect(producer.isIdempotent()).toBe(false)

      const plain = createKafka().producer({ idempotent: false })
      await plain.connect()
      await plain.send({ acks: 1, topic: TOPIC, messages: MESSAGES })
      expect(lastCluster().broker.initProducerId).not.toHaveBeenCalled()
      const [[downgraded]] = clusters[0].broker.produce.mock.calls
      const [[nonIdempotent]] = lastCluster().broker.produce.mock.calls
      expect(downgraded).toEqual(nonIdempotent)
      expect(downgraded).toMatchObject({ producerId: -1, producerEpoch: 0 })
    })

    test('a second conflicting send does not warn again', async () => {
      const producer = createKafka().producer()
      await producer.connect()

      await producer.send({ acks: 1, topic: TOPIC, messages: MESSAGES })
      await producer.send({ acks: 0, topic: TOPIC, messages: MESSAGES })

      expect(downgrades()).toHaveLength(1)
    })

    test("idempotent: true rejects the send with a configuration error, today's message kept", async () => {
      const producer = createKafka().producer({ idempotent: true })
      await producer.connect()

      const sent = producer.send({ acks: 0, topic: TOPIC, messages: MESSAGES })

      await expect(sent).rejects.toThrow(KafkaJSInvalidConfigurationError)
      await expect(sent).rejects.toThrow(KafkaJSNonRetriableError)
      await expect(sent).rejects.toThrow(
        "Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees"
      )
      expect(lastCluster().broker.produce).not.toHaveBeenCalled()
      expect(producer.isIdempotent()).toBe(true)
    })

    test("a transactional producer rejects it, on its own sends and its transactions', and never downgrades", async () => {
      const producer = createKafka().producer({ transactionalId: 'test-transactional-id' })
      await producer.connect()

      await expect(producer.send({ acks: 1, topic: TOPIC, messages: MESSAGES })).rejects.toThrow(
        KafkaJSInvalidConfigurationError
      )
      const transaction = await producer.transaction()
      await expect(transaction.send({ acks: 1, topic: TOPIC, messages: MESSAGES })).rejects.toThrow(
        KafkaJSInvalidConfigurationError
      )

      expect(downgrades()).toEqual([])
      expect(producer.isIdempotent()).toBe(true)
      expect(lastCluster().broker.produce).not.toHaveBeenCalled()
    })
  })

  describe('maxInFlightRequests above 5, or null', () => {
    test.each([6, null])(
      '%s with idempotent: true throws at kafka.producer(), naming the key',
      (value) => {
        const create = () =>
          createKafka().producer({ idempotent: true, maxInFlightRequests: value })

        expect(create).toThrow(KafkaJSInvalidConfigurationError)
        expect(create).toThrow(/maxInFlightRequests must be 5 or lower .* drop idempotent: true/)
      }
    )

    test('with idempotent: false the producer is built and sends non-idempotently, with no warning', async () => {
      const producer = createKafka().producer({ idempotent: false, maxInFlightRequests: 6 })
      await producer.connect()

      await producer.send({ topic: TOPIC, messages: MESSAGES })

      expect(producer.isIdempotent()).toBe(false)
      expect(lastCluster().broker.initProducerId).not.toHaveBeenCalled()
      expect(lastCluster().broker.produce.mock.calls[0][0]).toMatchObject({ producerId: -1 })
      expect(downgrades()).toEqual([])
    })

    test.each([
      [6, '6'],
      [null, 'null (unbounded)'],
    ])('%s without idempotent warns once and allocates no producer id', async (value, shown) => {
      const producer = createKafka().producer({ maxInFlightRequests: value })
      await producer.connect()

      expect(producer.isIdempotent()).toBe(false)
      expect(lastCluster().broker.initProducerId).not.toHaveBeenCalled()
      expect(downgrades()).toHaveLength(1)
      expect(downgrades()[0].log.message).toContain(`maxInFlightRequests is ${shown}`)
      expect(lastCluster().options.maxInFlightRequests).toEqual(value)
    })

    test('unset, it is 5 - within the limit - so the producer stays idempotent without a warning', () => {
      const producer = createKafka().producer()

      expect(producer.isIdempotent()).toBe(true)
      expect(downgrades()).toEqual([])
    })

    test('a producer that also sends with acks 1 is downgraded once, with one warning', async () => {
      const producer = createKafka().producer({ maxInFlightRequests: 6 })
      await producer.connect()

      await producer.send({ acks: 1, topic: TOPIC, messages: MESSAGES })
      await producer.send({ acks: 1, topic: TOPIC, messages: MESSAGES })

      expect(downgrades()).toHaveLength(1)
      expect(producer.isIdempotent()).toBe(false)
      expect(
        lastCluster().broker.produce.mock.calls.map(([request]) => request.producerId)
      ).toEqual([-1, -1])
    })

    test.each([
      ['without idempotent', {}],
      ['with idempotent: true', { idempotent: true }],
    ])(
      'an invalid value such as 7.5 %s throws the validation error, with no idempotence warning',
      (_, options) => {
        const create = () => createKafka().producer({ ...options, maxInFlightRequests: 7.5 })

        expect(create).toThrow(
          'maxInFlightRequests must be null (unbounded) or an integer >= 1, got 7.5'
        )
        expect(downgrades()).toEqual([])
      }
    )
  })

  describe('retry.retries of 0', () => {
    test('without idempotent the producer is built, warns once and allocates no producer id', async () => {
      const producer = createKafka().producer({
        createPartitioner: DefaultPartitioner,
        retry: { retries: 0 },
      })
      await producer.connect()

      expect(producer.isIdempotent()).toBe(false)
      expect(lastCluster().broker.initProducerId).not.toHaveBeenCalled()
      const warnings = entries.filter(({ level }) => level === LEVELS.WARN)
      expect(warnings).toHaveLength(1)
      expect(warnings[0].log.message).toMatch(/^Idempotence disabled: retry\.retries is 0/)
    })

    test('with idempotent: true it throws, naming retries', () => {
      const create = () => createKafka().producer({ idempotent: true, retry: { retries: 0 } })

      expect(create).toThrow(KafkaJSInvalidConfigurationError)
      expect(create).toThrow('Idempotent producer must allow retries')
    })

    test('idempotent: false with retries between 1 and 4 logs no EoS warning', () => {
      createKafka().producer({
        createPartitioner: DefaultPartitioner,
        idempotent: false,
        retry: { retries: 3 },
      })

      expect(entries.filter(({ level }) => level === LEVELS.WARN)).toEqual([])
    })

    test('retries between 1 and 4 keep the "limiting retries" warning on an idempotent producer', () => {
      const producer = createKafka().producer({ idempotent: true, retry: { retries: 3 } })

      expect(producer.isIdempotent()).toBe(true)
      expect(entries.map(({ log }) => log.message)).toContain(
        'Limiting retries for the idempotent producer may invalidate EoS guarantees'
      )
    })
  })

  describe('a transactionalId counts as asking for idempotence', () => {
    test.each([
      ['maxInFlightRequests: null', { maxInFlightRequests: null }, /transactional producer/],
      ['retry.retries: 0', { retry: { retries: 0 } }, /must allow retries/],
      [
        'idempotent: false with maxInFlightRequests: 6',
        { idempotent: false, maxInFlightRequests: 6 },
        /transactional producer/,
      ],
    ])('%s throws, with no warning and no downgrade', (_, options, message) => {
      const create = () =>
        createKafka().producer({ transactionalId: 'test-transactional-id', ...options })

      expect(create).toThrow(KafkaJSInvalidConfigurationError)
      expect(create).toThrow(message)
      expect(downgrades()).toEqual([])
    })

    test('idempotent: false is overridden: the producer is idempotent', () => {
      const producer = createKafka().producer({
        transactionalId: 'test-transactional-id',
        idempotent: false,
      })

      expect(producer.isIdempotent()).toBe(true)
      expect(downgrades()).toEqual([])
    })
  })

  describe('falsy idempotent and transactionalId, read as in 2.2.4', () => {
    test('idempotent: 0 is an explicit off: acks 1 sends non-idempotently and null builds', async () => {
      const producer = createKafka().producer({ idempotent: 0, maxInFlightRequests: null })
      await producer.connect()

      await producer.send({ acks: 1, topic: TOPIC, messages: MESSAGES })

      expect(producer.isIdempotent()).toBe(false)
      expect(lastCluster().broker.initProducerId).not.toHaveBeenCalled()
      expect(lastCluster().broker.produce.mock.calls[0][0]).toMatchObject({ producerId: -1 })
      expect(downgrades()).toEqual([])
    })

    test("transactionalId: '' is not transactional: a conflict downgrades instead of throwing", () => {
      const producer = createKafka().producer({ transactionalId: '', maxInFlightRequests: null })

      expect(producer.isIdempotent()).toBe(false)
      expect(downgrades()).toHaveLength(1)
    })
  })

  describe('the retry budget', () => {
    /** The options the producer built its own retrier from: the only call carrying a deadline. */
    const producerRetryOptions = () =>
      createRetry.mock.calls
        .map(([options]) => options)
        .find((options) => options != null && 'maxTotalTime' in options)

    test.each([
      ['by default', {}],
      ['with idempotent: true', { idempotent: true }],
    ])('is 5 retries %s, as in 2.2.4', (_, options) => {
      createKafka().producer(options)

      const resolved = Object.assign({}, PRODUCTION_RETRY_DEFAULTS, producerRetryOptions())
      expect(resolved.retries).toEqual(5)
    })

    test('no Number.MAX_SAFE_INTEGER fallback is left in the producer', () => {
      const source = fs.readFileSync(require.resolve('../index'), 'utf8')

      expect(source).not.toMatch(/MAX_SAFE_INTEGER/)
    })
  })

  describe('isIdempotent()', () => {
    test('reports true before and false after a send downgrades it', async () => {
      const producer = createKafka().producer()
      await producer.connect()
      expect(producer.isIdempotent()).toBe(true)

      await producer.send({ acks: 1, topic: TOPIC, messages: MESSAGES })

      expect(producer.isIdempotent()).toBe(false)
    })

    test.each([
      ['maxInFlightRequests', { maxInFlightRequests: 6 }],
      ['retry.retries', { retry: { retries: 0 } }],
    ])(
      'reports false for a producer whose %s downgraded it, true for one without',
      (_, options) => {
        expect(createKafka().producer(options).isIdempotent()).toBe(false)
        expect(createKafka().producer().isIdempotent()).toBe(true)
      }
    )
  })

  describe('the idempotence switch', () => {
    /** Required here, not at the top, so the rest of the file runs before the module exists. */
    const createIdempotence = (explicit) => {
      const Idempotence = require('../idempotence')
      const logger = createLogger({
        level: LEVELS.WARN,
        logCreator: () => (entry) => entries.push(entry),
      })
      return new Idempotence({ enabled: true, explicit, logger })
    }

    test('turns off once, warning once with the rule that turned it off', () => {
      const idempotence = createIdempotence(false)

      idempotence.disable('acks', 'Idempotence disabled: first')
      idempotence.disable('retries', 'Idempotence disabled: second')

      expect(idempotence.isEnabled()).toBe(false)
      expect(downgrades().map(({ log }) => [log.message, log.rule])).toEqual([
        ['Idempotence disabled: first', 'acks'],
      ])
    })

    test('refuses to turn off idempotence its creator asked for', () => {
      const idempotence = createIdempotence(true)

      expect(() => idempotence.disable('acks', 'Idempotence disabled: never')).toThrow(
        KafkaJSInvariantViolation
      )
      expect(idempotence.isEnabled()).toBe(true)
    })
  })

  describe('created directly', () => {
    const logger = () => createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })

    test('createProducer never applies the maxInFlightRequests rule: it never sees the option', () => {
      const producer = createProducer({ cluster: createFakeCluster(), logger: logger() })

      expect(producer.isIdempotent()).toBe(true)
    })

    test('an idempotent: null producer is idempotent and did not ask for it', async () => {
      const cluster = createFakeCluster()
      const producer = createProducer({ cluster, logger: logger(), idempotent: null })
      await producer.connect()

      await producer.send({ acks: 1, topic: TOPIC, messages: MESSAGES })

      expect(producer.isIdempotent()).toBe(false)
    })
  })
})
