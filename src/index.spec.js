jest.mock('./producer')
jest.mock('./consumer')
jest.mock('./admin')
jest.mock('./cluster')

const { Kafka: Client } = require('../index')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')
const Cluster = require('./cluster')
const { TelemetryFactory } = require('./telemetry/factory')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')
const { KafkaJSInvalidConfigurationError, KafkaJSNonRetriableError } = require('./errors')

describe('Client', () => {
  // The factories are automocks shared by every case: calls must not leak from one to the next.
  beforeEach(() => jest.clearAllMocks())

  it('gives access to its logger', () => {
    expect(new Client({ brokers: [] }).logger()).toMatchSnapshot()
  })

  it('shares a commit mapping between the consumer and the producer', () => {
    const client = new Client({ brokers: [] })

    expect(Cluster).toHaveBeenCalledTimes(0)

    client.producer({})
    client.consumer({})

    expect(Cluster).toHaveBeenCalledTimes(2)
    expect(Cluster.mock.calls[0][0].offsets).toBeInstanceOf(Map)
    expect(Cluster.mock.calls[0][0].offsets).toBe(Cluster.mock.calls[1][0].offsets)

    expect(createProducer.mock.calls[0][0].cluster).toBe(Cluster.mock.instances[0])
    expect(createConsumer.mock.calls[0][0].cluster).toBe(Cluster.mock.instances[1])
  })

  it('shares one store of offset operations between the clusters of one client, and none between clients', () => {
    const client = new Client({ brokers: [] })
    client.producer({})
    client.consumer({})
    client.admin({})
    new Client({ brokers: [] }).consumer({})

    const [producer, consumer, admin, other] = Cluster.mock.calls.map(
      ([options]) => options.operations
    )
    expect(producer).toBeInstanceOf(Map)
    expect(consumer).toBe(producer)
    expect(admin).toBe(producer)
    expect(other).toBeInstanceOf(Map)
    expect(other).not.toBe(producer)
  })

  it('producer rack options reach the producer factory', () => {
    const client = new Client({ brokers: [] })
    const options = [
      undefined,
      {},
      { rackId: undefined, rackAware: undefined },
      { rackId: 'rack-b' },
      { rackId: 'rack-b', rackAware: false },
      { rackId: 'rack-b', rackAware: true },
    ]

    options.forEach((producerOptions) => client.producer(producerOptions))

    expect(
      createProducer.mock.calls.map(([{ rackId, rackAware }]) => ({ rackId, rackAware }))
    ).toEqual([
      { rackId: '', rackAware: false },
      { rackId: '', rackAware: false },
      { rackId: '', rackAware: false },
      { rackId: 'rack-b', rackAware: false },
      { rackId: 'rack-b', rackAware: false },
      { rackId: 'rack-b', rackAware: true },
    ])
  })

  it('invalid producer rack options reject before cluster construction', () => {
    const client = new Client({ brokers: [] })
    const invalid = [
      [{ rackId: null }, /rackId must be a string, got null/],
      [{ rackId: 7 }, /rackId must be a string, got 7/],
      [{ rackAware: null }, /rackAware must be a boolean, got null/],
      [{ rackAware: 'true' }, /rackAware must be a boolean, got 'true'/],
      [{ rackAware: 1 }, /rackAware must be a boolean, got 1/],
      [{ rackAware: true }, /rackAware requires a non-empty rackId/],
      [{ rackAware: true, rackId: '' }, /rackAware requires a non-empty rackId/],
    ]

    for (const [options, message] of invalid) {
      expect(() => client.producer(options)).toThrow(KafkaJSInvalidConfigurationError)
      expect(() => client.producer(options)).toThrow(message)
    }

    expect(Cluster).not.toHaveBeenCalled()
    expect(createProducer).not.toHaveBeenCalled()
  })

  describe('maxInFlightRequests', () => {
    it.each([
      ['producer', (client, maxInFlightRequests) => client.producer({ maxInFlightRequests })],
      ['consumer', (client, maxInFlightRequests) => client.consumer({ maxInFlightRequests })],
    ])('%s: rejects a Symbol with KafkaJSNonRetriableError, not a TypeError', (_name, create) => {
      const client = new Client({ brokers: [] })

      expect(() => create(client, Symbol('in-flight'))).toThrow(KafkaJSNonRetriableError)
      expect(() => create(client, Symbol('in-flight'))).toThrow(/maxInFlightRequests.*Symbol/)
    })

    it('quotes a string value, so it is not mistaken for a number', () => {
      expect(() => new Client({ brokers: [] }).consumer({ maxInFlightRequests: '5' })).toThrow(
        /got '5'/
      )
    })
  })

  describe('maxResponseSize', () => {
    it('defaults to 100 MiB and reaches every cluster', () => {
      const client = new Client({ brokers: [] })

      client.producer({})
      client.consumer({})

      expect(Cluster.mock.calls.map(([options]) => options.maxResponseSize)).toEqual([
        104857600, 104857600,
      ])
    })

    it('passes the configured size to the cluster', () => {
      new Client({ brokers: [], maxResponseSize: 4096 }).consumer({})

      expect(Cluster).toHaveBeenCalledWith(expect.objectContaining({ maxResponseSize: 4096 }))
    })

    it.each([4, 1048576, 2147483647])('accepts %d', (maxResponseSize) => {
      expect(() => new Client({ brokers: [], maxResponseSize })).not.toThrow()
    })

    it.each([
      3,
      0,
      -1,
      2147483648,
      1.5,
      1048576.5,
      '1048576',
      null,
      NaN,
      Infinity,
      4n,
      Symbol('size'),
      Object.create(null),
    ])('rejects %p, naming the option', (maxResponseSize) => {
      expect(() => new Client({ brokers: [], maxResponseSize })).toThrow(
        KafkaJSInvalidConfigurationError
      )
      expect(() => new Client({ brokers: [], maxResponseSize })).toThrow(/maxResponseSize/)
    })

    it('quotes a string value, so it is not mistaken for a number', () => {
      expect(() => new Client({ brokers: [], maxResponseSize: '104857600' })).toThrow(
        /got '104857600'/
      )
    })
  })

  describe('maxDecompressedBatchBytes', () => {
    it('defaults to 1 GiB and reaches the consumer', () => {
      new Client({ brokers: [] }).consumer({ groupId: 'g' })

      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({ maxDecompressedBatchBytes: 1073741824 })
      )
    })

    it('passes the configured size to the consumer', () => {
      new Client({ brokers: [] }).consumer({ groupId: 'g', maxDecompressedBatchBytes: 4194304 })

      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({ maxDecompressedBatchBytes: 4194304 })
      )
    })

    it.each([1, 511, 1073741824, 2147483647])('accepts %d', (maxDecompressedBatchBytes) => {
      expect(() =>
        new Client({ brokers: [] }).consumer({ groupId: 'g', maxDecompressedBatchBytes })
      ).not.toThrow()
    })

    it.each([0, -1, 2147483648, 1.5, '4194304', null, NaN, Infinity, 4n, Object.create(null)])(
      'rejects %p, naming the option, before a consumer is built',
      (maxDecompressedBatchBytes) => {
        const consumer = () =>
          new Client({ brokers: [] }).consumer({ groupId: 'g', maxDecompressedBatchBytes })

        expect(consumer).toThrow(KafkaJSInvalidConfigurationError)
        expect(consumer).toThrow(/maxDecompressedBatchBytes/)
        expect(createConsumer).not.toHaveBeenCalled()
        expect(Cluster).not.toHaveBeenCalled()
      }
    )
  })

  it('passes options to the producer', () => {
    const client = new Client({ brokers: [] })
    const options = {
      cluster: expect.any(Object),
      logger: expect.any(Object),
      createPartitioner: () => 0,
      retry: { retries: 10 },
      idempotent: true,
      transactionalId: 'transactional-id',
      transactionTimeout: 1,
      instrumentationEmitter: expect.any(Object),
    }
    client.producer(options)

    expect(createProducer).toHaveBeenCalledWith({
      ...options,
      cluster: Cluster.mock.instances[0],
      telemetry: expect.any(TelemetryFactory),
      linger: 5,
      batchSize: 16384,
      deliveryTimeout: 120000,
      rackId: '',
      rackAware: false,
    })
  })

  describe('consumer', () => {
    test('creates a consumer with the correct isolation level', () => {
      const client = new Client({ brokers: [] })

      const readCommittedConsumerOptions = {
        readUncommitted: false,
      }
      const readUncommittedConsumerOptions = {
        readUncommitted: true,
      }
      client.consumer(readCommittedConsumerOptions)

      expect(Cluster).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
      )
      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
      )

      createConsumer.mockClear()
      Cluster.mockClear()
      client.consumer(readUncommittedConsumerOptions)

      expect(Cluster).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        })
      )
      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        })
      )
    })
  })

  describe('retry configurations', () => {
    it('merges local producer options with the client options', () => {
      const client = new Client({ retry: { initialRetryTime: 100 } })
      client.producer({ retry: { multiplier: 3 } })

      expect(createProducer).toHaveBeenCalledWith(
        expect.objectContaining({
          retry: { initialRetryTime: 100, multiplier: 3 },
        })
      )
    })

    it('merges local consumer options with the client options', () => {
      const client = new Client({ retry: { initialRetryTime: 100 } })
      client.consumer({ retry: { multiplier: 3 } })

      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          retry: { initialRetryTime: 100, multiplier: 3 },
        })
      )
    })

    it('creates consumer with the default options', () => {
      const client = new Client({})
      client.consumer()

      expect(createConsumer).toHaveBeenCalledWith(
        expect.objectContaining({
          retry: { retries: 5 },
        })
      )
    })

    it('merges local admin options with the client options', () => {
      const client = new Client({ retry: { initialRetryTime: 100 } })
      client.admin({ retry: { multiplier: 3 } })

      expect(createAdmin).toHaveBeenCalledWith(
        expect.objectContaining({
          retry: { initialRetryTime: 100, multiplier: 3 },
        })
      )
    })
  })
})
