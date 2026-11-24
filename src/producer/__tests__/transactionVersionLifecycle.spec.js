const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSNonRetriableError,
  KafkaJSProducerClosedError,
  KafkaJSProtocolError,
} = require('../../errors')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const TRANSACTIONAL_ID = 'transactional-a'
const REQUEST_TIMED_OUT = 7
const ONGOING = /already an ongoing transaction/

/** A promise and the functions that settle it. */
const deferred = () => {
  let resolve
  let reject
  const promise = new Promise((done, fail) => {
    resolve = done
    reject = fail
  })
  return { promise, resolve, reject }
}

/** Lets every pending promise callback run. */
const settle = () => new Promise((done) => setImmediate(done))

/**
 * The finalized features a coordinator reports for `transaction.version` at `level`.
 * @param {number} level
 * @param {number} [epoch=1] the finalized features epoch
 */
const features = (level, epoch = 1) => ({
  finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: level } },
  finalizedFeaturesEpoch: epoch,
})

/**
 * A transactional producer over the fake cluster, whose one broker is also the transaction
 * coordinator and reports `transaction.version` at `level`. InitProducerId answers producer id
 * 1000 one epoch above the one it was sent; EndTxn answers with `endTxn`.
 * @param {{ level?: number, endTxn?: (request: object) => object }} [options]
 */
const createTransactionalProducer = ({ level = 2, endTxn } = {}) => {
  const cluster = createFakeCluster({ partitions: 2 })
  const { broker } = cluster
  Object.assign(broker, features(level), { negotiatedVersion: () => 5 })
  broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  broker.endTxn.mockImplementation(
    endTxn ||
      (async ({ producerEpoch }) => ({ producerId: '1000', producerEpoch: producerEpoch + 1 }))
  )
  const producer = createProducer({
    cluster,
    logger: newLogger(),
    transactionalId: TRANSACTIONAL_ID,
  })

  /** The InitProducerId requests the transactional id made. */
  const transactionalInits = () =>
    broker.initProducerId.mock.calls
      .map(([request]) => request)
      .filter(({ transactionalId }) => transactionalId === TRANSACTIONAL_ID)

  /** The producer epoch and first sequence of every transactional Produce, in order. */
  const produced = () =>
    broker.produce.mock.calls
      .map(([request]) => request)
      .filter(({ transactionalId }) => transactionalId === TRANSACTIONAL_ID)
      .map(({ producerEpoch, transactionVersion, topicData }) => ({
        producerEpoch,
        transactionVersion,
        firstSequence: topicData[0].partitions[0].firstSequence,
      }))

  return { cluster, broker, producer, transactionalInits, produced }
}

/**
 * A connected transactional producer that holds a producer id: one empty transaction began and
 * ended with no EndTxn, so the next `transaction()` has only its begin left to do.
 * @param {{ level?: number, endTxn?: (request: object) => object }} [options]
 */
const connectedWithProducerId = async (options) => {
  const setup = createTransactionalProducer(options)
  await setup.producer.connect()
  await (await setup.producer.transaction()).commit()
  return setup
}

const message = { key: 'key', value: 'value', partition: 0 }

describe('Producer > transaction version lifecycle', () => {
  test.each([
    ['transaction version 1', 1],
    ['transaction version 2', 2],
  ])(
    'under %s a transaction that produced nothing ends with no EndTxn, and the next one begins',
    async (_, level) => {
      const { broker, producer } = createTransactionalProducer({ level })
      await producer.connect()

      const committed = await producer.transaction()
      await committed.commit()
      const aborted = await producer.transaction()
      await aborted.abort()

      expect(broker.endTxn).not.toHaveBeenCalled()
      expect(committed.isActive()).toBe(false)
      expect(aborted.isActive()).toBe(false)
      await expect(producer.transaction()).resolves.toMatchObject({
        isActive: expect.any(Function),
      })
    }
  )

  test('under transaction version 2 the EndTxn of a transaction that produced is v5, and its pair is adopted before the next request', async () => {
    const { broker, producer, produced } = createTransactionalProducer({ level: 2 })
    await producer.connect()

    const first = await producer.transaction()
    await first.send({ topic: TOPIC, messages: [message, message] })
    await first.commit()
    const second = await producer.transaction()
    await second.send({ topic: TOPIC, messages: [message] })
    await second.abort()

    expect(broker.addPartitionsToTxn).not.toHaveBeenCalled()
    expect(broker.endTxn.mock.calls.map(([request]) => request)).toEqual([
      expect.objectContaining({ producerEpoch: 0, transactionResult: true, transactionVersion: 2 }),
      expect.objectContaining({
        producerEpoch: 1,
        transactionResult: false,
        transactionVersion: 2,
      }),
    ])
    expect(produced()).toEqual([
      { producerEpoch: 0, transactionVersion: 2, firstSequence: 0 },
      { producerEpoch: 1, transactionVersion: 2, firstSequence: 0 },
    ])
  })

  test('a v5 EndTxn answering -1/-1 leaves the producer id and epoch as they were', async () => {
    const { producer, produced } = createTransactionalProducer({
      level: 2,
      endTxn: async () => ({ producerId: '-1', producerEpoch: -1 }),
    })
    await producer.connect()

    const first = await producer.transaction()
    await first.send({ topic: TOPIC, messages: [message, message] })
    await first.commit()
    const second = await producer.transaction()
    await second.send({ topic: TOPIC, messages: [message] })
    await second.commit()

    expect(produced()).toEqual([
      { producerEpoch: 0, transactionVersion: 2, firstSequence: 0 },
      { producerEpoch: 0, transactionVersion: 2, firstSequence: 2 },
    ])
  })

  test('a version 2 EndTxn answer that carries no producer id leaves the producer id and epoch as they were', async () => {
    const { producer, produced, transactionalInits } = createTransactionalProducer({
      level: 2,
      endTxn: async () => ({}),
    })
    await producer.connect()

    const first = await producer.transaction()
    await first.send({ topic: TOPIC, messages: [message, message] })
    await first.commit()
    const second = await producer.transaction()
    await second.send({ topic: TOPIC, messages: [message] })
    await second.commit()

    expect(transactionalInits()).toHaveLength(1)
    expect(produced()).toEqual([
      { producerEpoch: 0, transactionVersion: 2, firstSequence: 0 },
      { producerEpoch: 0, transactionVersion: 2, firstSequence: 2 },
    ])
  })

  test('under transaction version 1 partitions are added with AddPartitionsToTxn, EndTxn stays at v4 and nothing is adopted', async () => {
    const { broker, producer, produced } = createTransactionalProducer({
      level: 1,
      endTxn: async () => ({ producerId: '2000', producerEpoch: 7 }),
    })
    await producer.connect()

    const first = await producer.transaction()
    await first.send({ topic: TOPIC, messages: [message] })
    await first.commit()
    const second = await producer.transaction()
    await second.send({ topic: TOPIC, messages: [message] })
    await second.commit()

    expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(2)
    expect(broker.endTxn.mock.calls.map(([{ transactionVersion }]) => transactionVersion)).toEqual([
      1, 1,
    ])
    expect(produced()).toEqual([
      { producerEpoch: 0, transactionVersion: 1, firstSequence: 0 },
      { producerEpoch: 0, transactionVersion: 1, firstSequence: 1 },
    ])
  })

  test('a move from version 1 to 2 re-initializes once, before the first version 2 transaction sends anything', async () => {
    const { broker, producer, transactionalInits, produced } = createTransactionalProducer({
      level: 1,
    })
    await producer.connect()
    const first = await producer.transaction()
    await first.send({ topic: TOPIC, messages: [message] })
    await first.commit()

    Object.assign(broker, features(2, 2))
    const second = await producer.transaction()
    await second.send({ topic: TOPIC, messages: [message] })
    await second.commit()
    const third = await producer.transaction()
    await third.send({ topic: TOPIC, messages: [message] })
    await third.commit()

    expect(transactionalInits()).toEqual([
      expect.objectContaining({ producerId: -1, producerEpoch: -1 }),
      expect.objectContaining({ producerId: '1000', producerEpoch: 0 }),
    ])
    expect(produced()).toEqual([
      { producerEpoch: 0, transactionVersion: 1, firstSequence: 0 },
      { producerEpoch: 1, transactionVersion: 2, firstSequence: 0 },
      { producerEpoch: 2, transactionVersion: 2, firstSequence: 0 },
    ])
  })

  test('a move from version 2 to 1 re-initializes nothing', async () => {
    const { broker, producer, transactionalInits, produced } = createTransactionalProducer({
      level: 2,
    })
    await producer.connect()
    const first = await producer.transaction()
    await first.send({ topic: TOPIC, messages: [message] })
    await first.commit()

    Object.assign(broker, features(1, 2))
    const second = await producer.transaction()
    await second.send({ topic: TOPIC, messages: [message] })
    await second.commit()

    expect(transactionalInits()).toHaveLength(1)
    expect(produced()).toEqual([
      { producerEpoch: 0, transactionVersion: 2, firstSequence: 0 },
      { producerEpoch: 1, transactionVersion: 1, firstSequence: 0 },
    ])
  })

  test('a re-initialization that fails ends that transaction() call, and the next call bumps again', async () => {
    const { broker, producer, transactionalInits, produced } = createTransactionalProducer({
      level: 1,
    })
    await producer.connect()
    const first = await producer.transaction()
    await first.send({ topic: TOPIC, messages: [message] })
    await first.commit()
    Object.assign(broker, features(2, 2))
    const timedOut = new KafkaJSProtocolError(createErrorFromCode(REQUEST_TIMED_OUT))
    broker.initProducerId.mockRejectedValueOnce(timedOut)

    await expect(producer.transaction()).rejects.toBe(timedOut)
    const second = await producer.transaction()
    await second.send({ topic: TOPIC, messages: [message] })
    await second.commit()

    expect(transactionalInits()).toEqual([
      expect.objectContaining({ producerEpoch: -1 }),
      expect.objectContaining({ producerEpoch: 0 }),
      expect.objectContaining({ producerEpoch: 0 }),
    ])
    expect(produced()[1]).toEqual({ producerEpoch: 1, transactionVersion: 2, firstSequence: 0 })
  })

  describe('one transaction() at a time', () => {
    test('a second transaction() while the first holds its lookup: the first begins, sends, is acknowledged and commits; the second is refused with the ongoing-transaction error and sends no FindCoordinator, no InitProducerId; producer id, epoch and transaction version unchanged; one EndTxn', async () => {
      const { cluster, broker, producer, transactionalInits, produced } =
        await connectedWithProducerId({ level: 1 })
      // Every lookup after the held one answers as a coordinator that has moved to version 2.
      const moved = Object.assign(Object.create(broker), features(2, 2))
      const held = deferred()
      let lookups = 0
      cluster.findGroupCoordinator.mockImplementation(() =>
        ++lookups === 1 ? held.promise : Promise.resolve(moved)
      )

      const calls = Promise.allSettled([producer.transaction(), producer.transaction()])
      await settle()
      held.resolve(broker)
      const [first, second] = await calls

      expect([first.status, second.status]).toEqual(['fulfilled', 'rejected'])
      expect(second.reason).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(second.reason.message).toMatch(ONGOING)
      const records = await first.value.send({ topic: TOPIC, messages: [message] })
      await first.value.commit()

      expect(Array.isArray(records)).toBe(true)
      // The first caller's begin, its AddPartitionsToTxn and its EndTxn: nothing for the second.
      expect(lookups).toBe(3)
      expect(transactionalInits()).toHaveLength(1)
      expect(produced()).toEqual([{ producerEpoch: 0, transactionVersion: 1, firstSequence: 0 }])
      expect(broker.endTxn.mock.calls.map(([request]) => request)).toEqual([
        expect.objectContaining({
          producerEpoch: 0,
          transactionResult: true,
          transactionVersion: 1,
        }),
      ])
    })

    test('three concurrent transaction() calls whose first owner fails: exactly one of the others begins, the third is refused without remote work', async () => {
      const { cluster, broker, producer, transactionalInits } = await connectedWithProducerId({
        level: 1,
      })
      const failure = new KafkaJSNonRetriableError('the coordinator lookup failed')
      const held = deferred()
      let lookups = 0
      cluster.findGroupCoordinator.mockImplementation(() =>
        ++lookups === 1 ? held.promise : Promise.resolve(broker)
      )

      const calls = Promise.allSettled([
        producer.transaction(),
        producer.transaction(),
        producer.transaction(),
      ])
      await settle()
      held.reject(failure)
      const [first, ...others] = await calls

      expect(first.status).toBe('rejected')
      expect(first.reason).toBe(failure)
      expect(others.map(({ status }) => status).sort()).toEqual(['fulfilled', 'rejected'])
      const refused = others.find(({ status }) => status === 'rejected')
      expect(refused.reason.message).toMatch(ONGOING)
      // The failed owner's lookup and the one next owner's: the refused caller looked up nothing.
      expect(lookups).toBe(2)
      expect(transactionalInits()).toHaveLength(1)
      await others.find(({ status }) => status === 'fulfilled').value.abort()
    })

    test('a send and commit issued back to back after the winning handle resolves: one EndTxn, the send acknowledged', async () => {
      const { broker, producer, produced } = await connectedWithProducerId({ level: 1 })

      const [first, second] = await Promise.allSettled([
        producer.transaction(),
        producer.transaction(),
      ])
      expect([first.status, second.status]).toEqual(['fulfilled', 'rejected'])
      expect(second.reason.message).toMatch(ONGOING)
      const sent = first.value.send({ topic: TOPIC, messages: [message] })
      const committed = first.value.commit()
      const [records] = await Promise.all([sent, committed])

      expect(Array.isArray(records)).toBe(true)
      expect(produced()).toEqual([{ producerEpoch: 0, transactionVersion: 1, firstSequence: 0 }])
      expect(broker.endTxn).toHaveBeenCalledTimes(1)
    })

    test('a transaction() after a failed one begins normally', async () => {
      const { cluster, broker, producer, transactionalInits, produced } =
        await connectedWithProducerId({ level: 1 })
      const failure = new KafkaJSNonRetriableError('the coordinator lookup failed')
      cluster.findGroupCoordinator.mockImplementationOnce(async () => {
        throw failure
      })

      await expect(producer.transaction()).rejects.toBe(failure)
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: [message] })
      await transaction.commit()

      expect(transactionalInits()).toHaveLength(1)
      expect(produced()).toEqual([{ producerEpoch: 0, transactionVersion: 1, firstSequence: 0 }])
      expect(broker.endTxn).toHaveBeenCalledTimes(1)
    })

    test('a transaction() after one that committed, and another after one that aborted, begins each time', async () => {
      const { broker, producer } = createTransactionalProducer({ level: 1 })
      await producer.connect()

      const first = await producer.transaction()
      await first.send({ topic: TOPIC, messages: [message] })
      await first.commit()
      const second = await producer.transaction()
      await second.send({ topic: TOPIC, messages: [message] })
      await second.abort()
      const third = await producer.transaction()

      expect(third.isActive()).toBe(true)
      expect(broker.endTxn.mock.calls.map(([{ transactionResult }]) => transactionResult)).toEqual([
        true,
        false,
      ])
      await third.abort()
    })

    test('two transaction() calls on a producer that is still initializing: one InitProducerId, one handle, one ongoing-transaction refusal', async () => {
      const { producer, transactionalInits } = createTransactionalProducer({ level: 1 })
      await producer.connect()

      const [first, second] = await Promise.allSettled([
        producer.transaction(),
        producer.transaction(),
      ])

      expect([first.status, second.status]).toEqual(['fulfilled', 'rejected'])
      expect(second.reason.message).toMatch(ONGOING)
      expect(transactionalInits()).toHaveLength(1)
      await first.value.abort()
    })

    test('the upgrade bump happens once, at the next legitimate begin', async () => {
      const { broker, producer, transactionalInits, produced } = createTransactionalProducer({
        level: 1,
      })
      await producer.connect()
      const first = await producer.transaction()
      await first.send({ topic: TOPIC, messages: [message] })
      await first.commit()
      Object.assign(broker, features(2, 2))

      const [second, refused] = await Promise.allSettled([
        producer.transaction(),
        producer.transaction(),
      ])
      expect([second.status, refused.status]).toEqual(['fulfilled', 'rejected'])
      expect(refused.reason.message).toMatch(ONGOING)
      await second.value.send({ topic: TOPIC, messages: [message] })
      await second.value.commit()
      const third = await producer.transaction()
      await third.send({ topic: TOPIC, messages: [message] })
      await third.commit()

      expect(transactionalInits()).toEqual([
        expect.objectContaining({ producerId: -1, producerEpoch: -1 }),
        expect.objectContaining({ producerId: '1000', producerEpoch: 0 }),
      ])
      expect(produced()).toEqual([
        { producerEpoch: 0, transactionVersion: 1, firstSequence: 0 },
        { producerEpoch: 1, transactionVersion: 2, firstSequence: 0 },
        { producerEpoch: 2, transactionVersion: 2, firstSequence: 0 },
      ])
    })

    test('a transaction() whose producer was closed while it waited is refused with the close error', async () => {
      const { cluster, broker, producer, transactionalInits } = await connectedWithProducerId({
        level: 1,
      })
      const held = deferred()
      let lookups = 0
      cluster.findGroupCoordinator.mockImplementation(() =>
        ++lookups === 1 ? held.promise : Promise.resolve(broker)
      )

      const calls = Promise.allSettled([producer.transaction(), producer.transaction()])
      await settle()
      await producer.disconnect()
      held.resolve(broker)
      const [first, second] = await calls

      expect([first.status, second.status]).toEqual(['rejected', 'rejected'])
      expect(first.reason).toBeInstanceOf(KafkaJSProducerClosedError)
      expect(second.reason).toBeInstanceOf(KafkaJSProducerClosedError)
      // Only the first caller looked anything up; the waiting one did no work after the close.
      expect(lookups).toBe(1)
      expect(transactionalInits()).toHaveLength(1)
      expect(broker.endTxn).not.toHaveBeenCalled()
    })
  })
})
