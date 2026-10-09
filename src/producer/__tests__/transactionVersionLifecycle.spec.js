const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSProtocolError } = require('../../errors')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const TRANSACTIONAL_ID = 'transactional-a'
const REQUEST_TIMED_OUT = 7

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
})
