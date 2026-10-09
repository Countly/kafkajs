const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSGroupCoordinatorNotFound, KafkaJSNonRetriableError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const silentLogger = () => createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })

describe('Producer > transaction() awaits the transaction it begins', () => {
  let cluster, producer, unhandled

  const recordUnhandled = (reason) => unhandled.push(reason)

  beforeEach(async () => {
    unhandled = []
    process.on('unhandledRejection', recordUnhandled)
    cluster = createFakeCluster()
    producer = createProducer({
      cluster,
      logger: silentLogger(),
      transactionalId: 'test-transactional-id',
    })
    await producer.connect()
  })

  afterEach(() => {
    process.removeListener('unhandledRejection', recordUnhandled)
  })

  test('a coordinator lookup that fails while beginning rejects transaction() itself', async () => {
    const lookupFailed = new KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator')
    // InitProducerId finds the coordinator first; beginning the transaction looks it up again.
    cluster.findGroupCoordinator
      .mockImplementationOnce(async () => cluster.broker)
      .mockImplementationOnce(async () => {
        throw lookupFailed
      })

    await expect(producer.transaction()).rejects.toBe(lookupFailed)
    await new Promise((resolve) => setImmediate(resolve))

    expect(unhandled).toEqual([])
  })

  test('after such a failure the producer is still ready to begin a transaction', async () => {
    cluster.findGroupCoordinator
      .mockImplementationOnce(async () => cluster.broker)
      .mockImplementationOnce(async () => {
        throw new KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator')
      })
    await expect(producer.transaction()).rejects.toThrow(KafkaJSGroupCoordinatorNotFound)

    const transaction = await producer.transaction()

    expect(transaction.isActive()).toBe(true)
    expect(cluster.broker.initProducerId).toHaveBeenCalledTimes(2)
  })

  test('a transaction that began is active once transaction() resolves', async () => {
    const transaction = await producer.transaction()

    expect(transaction.isActive()).toBe(true)
    await transaction.abort()
    expect(transaction.isActive()).toBe(false)
  })

  test('a second transaction() begun while the first finds its coordinator rejects as 2.2.4 did', async () => {
    await (await producer.transaction()).abort()

    const [first, second] = await Promise.allSettled([
      producer.transaction(),
      producer.transaction(),
    ])

    expect(first.status).toEqual('fulfilled')
    expect(second.reason).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(second.reason.message).toEqual(
      'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.'
    )
  })
})

describe('Producer > a transaction adds its partitions', () => {
  test("retrying AddPartitionsToTxn with the producer's retry options, not the cluster's", async () => {
    const cluster = createFakeCluster({ options: { retry: { retries: 0 } } })
    cluster.broker.addPartitionsToTxn.mockRejectedValue(createErrorFromCode(51))
    const producer = createProducer({
      cluster,
      logger: silentLogger(),
      transactionalId: 'test-transactional-id',
      retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 1 },
    })
    await producer.connect()
    const transaction = await producer.transaction()

    await expect(transaction.send({ topic: 't', messages: [{ value: 'v' }] })).rejects.toThrow()

    expect(cluster.broker.addPartitionsToTxn).toHaveBeenCalledTimes(3)
  })
})
