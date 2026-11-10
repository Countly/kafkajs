const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createLogger, LEVELS } = require('../../loggers')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSProtocolError,
  KafkaJSRequestTimeoutError,
} = require('../../errors')

const TOPIC = 'topic-a'
const INVALID_TXN_STATE = 48
const PRODUCER_FENCED = 90

/**
 * A transactional producer over the fake cluster, with one transaction that sent a record.
 * @returns {Promise<{ broker: object, producer: object, transaction: object }>}
 */
const createCommittingProducer = async () => {
  const cluster = createFakeCluster({})
  const { broker } = cluster
  Object.assign(broker, {
    finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 } },
    finalizedFeaturesEpoch: 1,
    negotiatedVersion: () => 5,
  })
  broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  broker.endTxn.mockImplementation(async ({ producerEpoch }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  const producer = createProducer({
    cluster,
    logger: createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} }),
    transactionalId: 'transactional-a',
    retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 2 },
    deliveryTimeout: 30000,
  })
  await producer.connect()
  const transaction = await producer.transaction()
  await transaction.send({ topic: TOPIC, messages: [{ key: 'key', value: 'value' }] })
  return { broker, producer, transaction }
}

/*
 * A scripted coordinator: it does not show that a broker is fixed, only that the client keeps the
 * fencing answer the coordinator sent after the transaction timeout aborted the transaction.
 */
describe('Kafka 4.4 client fix audit', () => {
  test('KAFKA-20785 EndTxn timeout race retains the fencing error', async () => {
    for (const code of [INVALID_TXN_STATE, PRODUCER_FENCED]) {
      const { broker, producer, transaction } = await createCommittingProducer()
      broker.endTxn
        .mockRejectedValueOnce(new KafkaJSRequestTimeoutError('Request EndTxn timed out'))
        .mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(code)))

      const error = await transaction.commit().then(
        () => null,
        (failure) => failure
      )

      expect(error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(error).not.toBeInstanceOf(KafkaJSRequestTimeoutError)
      expect(error.cause).toMatchObject({ code, type: createErrorFromCode(code).type })
      expect(broker.endTxn).toHaveBeenCalledTimes(2)

      // The transaction is finished: nothing can commit, abort or begin again.
      await expect(transaction.commit()).rejects.toBe(error)
      await expect(transaction.abort()).rejects.toBe(error)
      await expect(producer.transaction()).rejects.toBe(error)
      expect(broker.endTxn).toHaveBeenCalledTimes(2)
    }
  })
})
