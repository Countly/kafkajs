const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const PartitionSequencer = require('../state/partitionSequencer')
const encodeProduce = require('../../protocol/requests/produce/v12/request')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const { READY, ABORTABLE_ERROR, FATAL_ERROR } = require('../eosManager/transactionStates')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const TRANSACTIONAL_ID = 'transactional-a'
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 2 }

const UNKNOWN_TOPIC_OR_PARTITION = 3
const NOT_LEADER_OR_FOLLOWER = 6
const REQUEST_TIMED_OUT = 7
const MESSAGE_TOO_LARGE = 10
const INVALID_TIMESTAMP = 32
const CONCURRENT_TRANSACTIONS = 51
const CLUSTER_AUTHORIZATION_FAILED = 31
const OUT_OF_ORDER_SEQUENCE_NUMBER = 45
const INVALID_RECORD = 87
const PRODUCER_FENCED = 90
const TRANSACTION_ABORTABLE = 120

const ONE = [{ key: 'key', value: 'value' }]

/** A promise and the function that resolves it. */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/** Lets every pending promise callback run. */
const settle = () => new Promise((done) => setImmediate(done))

/**
 * The per-partition answer to a Produce, failing every partition with `errorCode` when given.
 * @param {{ topicData: object[] }} request
 * @param {number} [errorCode=0]
 */
const answer = ({ topicData }, errorCode = 0) => ({
  topics: topicData.map(({ topic, partitions }) => ({
    topicName: topic,
    partitions: partitions.map(({ partition }) => ({
      partition,
      errorCode,
      baseOffset: errorCode === 0 ? '0' : '-1',
      logAppendTime: '-1',
      logStartOffset: '0',
    })),
  })),
})

/**
 * A transactional producer over the fake cluster, its coordinator at transaction version
 * `level`; every Produce goes through `produce`.
 * @param {{ level?: number, produce?: (request: object) => Promise<object>, retry?: object,
 *   partitions?: number, linger?: number }} [options]
 */
const createTransactionalProducer = ({
  level = 2,
  produce,
  retry = RETRY,
  partitions = 1,
  linger,
} = {}) => {
  const cluster = createFakeCluster({
    partitions,
    produce: produce || (async (request) => answer(request)),
  })
  const { broker } = cluster
  Object.assign(broker, {
    finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: level } },
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
    logger: newLogger(),
    transactionalId: TRANSACTIONAL_ID,
    retry,
    linger,
  })
  return { cluster, broker, producer }
}

/** A transaction that began and was aborted: every method of it says it has ended. */
const endedTransaction = async (producer) => {
  const transaction = await producer.transaction()
  await transaction.abort()
  return transaction
}

describe('Producer > the transaction wrapper', () => {
  describe('a send that fails inside a transaction', () => {
    test('arms ABORTABLE_ERROR: commit() rejects with the armed error and sends no EndTxn', async () => {
      const { broker, producer } = createTransactionalProducer({
        produce: async (request) => answer(request, INVALID_RECORD),
      })
      await producer.connect()
      const transaction = await producer.transaction()

      const sendError = await transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      const commitError = await transaction.commit().catch((e) => e)

      expect(sendError).toBeInstanceOf(KafkaJSProtocolError)
      expect(sendError).toMatchObject({ code: INVALID_RECORD, category: 'invalidConfiguration' })
      expect(commitError).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(commitError.cause).toBe(sendError)
      expect(broker.endTxn).not.toHaveBeenCalled()
      expect(transaction.isActive()).toBe(false)
      await expect(transaction.send({ topic: TOPIC, messages: ONE })).rejects.toBe(commitError)
    })

    test.each([
      ['PRODUCER_FENCED', FATAL_ERROR, PRODUCER_FENCED],
      ['CLUSTER_AUTHORIZATION_FAILED', FATAL_ERROR, CLUSTER_AUTHORIZATION_FAILED],
      ['INVALID_RECORD', ABORTABLE_ERROR, INVALID_RECORD],
      ['TRANSACTION_ABORTABLE', ABORTABLE_ERROR, TRANSACTION_ABORTABLE],
      ['a retriable error that ran out of retries', ABORTABLE_ERROR, REQUEST_TIMED_OUT],
    ])('%s leaves the transaction in %s, which refuses commit()', async (_, state, code) => {
      const { broker, producer } = createTransactionalProducer({
        produce: async (request) => answer(request, code),
      })
      await producer.connect()
      const transaction = await producer.transaction()

      await expect(transaction.send({ topic: TOPIC, messages: ONE })).rejects.toThrow()
      const commitError = await transaction.commit().catch((e) => e)
      expect(broker.endTxn).not.toHaveBeenCalled()
      const abortResult = await transaction.abort().then(
        () => READY,
        (e) => e
      )

      if (state === FATAL_ERROR) {
        expect(commitError).toBeInstanceOf(KafkaJSApplicationRecoverableError)
        expect(abortResult).toBe(commitError)
        await expect(producer.transaction()).rejects.toBe(commitError)
      } else {
        expect(commitError).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(abortResult).toEqual(READY)
        await expect(producer.transaction()).resolves.toBeDefined()
      }
    })

    test.each([
      ['MESSAGE_TOO_LARGE', MESSAGE_TOO_LARGE],
      ['INVALID_TIMESTAMP', INVALID_TIMESTAMP],
    ])(
      '%s fails the send, abort() ends the transaction, and a corrected send in the next one commits',
      async (type, code) => {
        let rejected = false
        const { broker, producer } = createTransactionalProducer({
          produce: async (request) => {
            const errorCode = rejected ? 0 : code
            rejected = true
            return answer(request, errorCode)
          },
        })
        await producer.connect()
        const first = await producer.transaction()

        const sendError = await first.send({ topic: TOPIC, messages: ONE }).catch((e) => e)

        await expect(first.abort()).resolves.toBeUndefined()
        expect(sendError).toBeInstanceOf(KafkaJSProtocolError)
        expect(sendError).toMatchObject({
          type,
          code,
          retriable: false,
          category: 'invalidConfiguration',
        })

        const second = await producer.transaction()
        await second.send({ topic: TOPIC, messages: ONE })
        await expect(second.commit()).resolves.toBeUndefined()
        expect(
          broker.endTxn.mock.calls.map(([{ transactionResult }]) => transactionResult)
        ).toEqual([false, true])
      }
    )

    test('a retriable error that ran out of retries surfaces as KafkaJSTransactionAbortableError, and outside a transaction as it is', async () => {
      const { producer } = createTransactionalProducer({
        produce: async (request) => answer(request, REQUEST_TIMED_OUT),
      })
      await producer.connect()
      const transaction = await producer.transaction()

      const inside = await transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      const outside = await producer.send({ topic: TOPIC, messages: ONE }).catch((e) => e)

      expect(inside).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(inside.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(inside.cause.cause).toMatchObject({ code: REQUEST_TIMED_OUT })
      await expect(transaction.commit()).rejects.toBe(inside)
      expect(outside).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(outside).not.toBeInstanceOf(KafkaJSTransactionAbortableError)
    })

    test('a send whose topic cannot be found arms ABORTABLE_ERROR as a failed batch does', async () => {
      const { cluster, producer } = createTransactionalProducer()
      await producer.connect()
      const transaction = await producer.transaction()
      cluster.addMultipleTargetTopics.mockRejectedValue(
        new KafkaJSProtocolError(createErrorFromCode(UNKNOWN_TOPIC_OR_PARTITION))
      )

      const error = await transaction
        .send({ topic: 'no-such-topic', messages: ONE })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(error.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      await expect(transaction.commit()).rejects.toBe(error)
    })

    test('an argument error raised before anything is queued leaves the transaction committable', async () => {
      const { broker, producer } = createTransactionalProducer()
      await producer.connect()
      const transaction = await producer.transaction()

      await expect(transaction.send({ topic: '', messages: ONE })).rejects.toThrow(
        new KafkaJSNonRetriableError('Invalid topic')
      )
      await expect(
        transaction.send({ topic: TOPIC, messages: ONE, acks: 1 })
      ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      await transaction.send({ topic: TOPIC, messages: ONE })
      await transaction.commit()

      expect(broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ transactionResult: true })
      )
    })

    test('a leader that offers no Produce v12 under transaction version 2 makes the producer unusable', async () => {
      const unsupported = new KafkaJSServerDoesNotSupportApiKey(
        'Broker 127.0.0.1:9092 offers Produce v3-v11, but a transaction under transaction version 2 needs v12',
        { apiKey: 0, apiName: 'Produce' }
      )
      const { broker, producer } = createTransactionalProducer({
        produce: async () => {
          throw unsupported
        },
      })
      await producer.connect()
      const transaction = await producer.transaction()

      await expect(transaction.send({ topic: TOPIC, messages: ONE })).rejects.toBe(unsupported)
      const commitError = await transaction.commit().catch((e) => e)

      expect(commitError).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(commitError.cause).toBe(unsupported)
      expect(broker.produce).toHaveBeenCalledTimes(1)
      await expect(producer.transaction()).rejects.toBe(commitError)
    })

    test('a partitioner that throws leaves the transaction committable: only a client error arms it', async () => {
      const cluster = createFakeCluster()
      Object.assign(cluster.broker, {
        finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 } },
        finalizedFeaturesEpoch: 1,
      })
      const broken = new TypeError('the partitioner failed')
      const producer = createProducer({
        cluster,
        logger: newLogger(),
        transactionalId: TRANSACTIONAL_ID,
        retry: RETRY,
        createPartitioner:
          () =>
          ({ topic }) => {
            if (topic === 'broken-topic') {
              throw broken
            }
            return 0
          },
      })
      await producer.connect()
      const transaction = await producer.transaction()

      await expect(transaction.send({ topic: 'broken-topic', messages: ONE })).rejects.toBe(broken)
      await transaction.send({ topic: TOPIC, messages: ONE })
      await transaction.commit()

      expect(cluster.broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ transactionResult: true })
      )
    })
  })

  describe('a batch that fails for good on a route other than a broker answer', () => {
    /** The transactional InitProducerId requests of `broker`. */
    const transactionalInits = (broker) =>
      broker.initProducerId.mock.calls.filter(
        ([{ transactionalId }]) => transactionalId === TRANSACTIONAL_ID
      )

    /**
     * A transaction whose one batch was answered REQUEST_TIMED_OUT and was waiting out its
     * backoff when `disconnect()` began, which waited for it to run out of retries; the producer
     * is connected again.
     * @param {number} level the transaction version
     */
    const reSendFailingThroughDisconnect = async (level) => {
      let fail = true
      const { broker, producer } = createTransactionalProducer({
        level,
        retry: { retries: 5, initialRetryTime: 200, maxRetryTime: 200 },
        produce: async (request) => answer(request, fail ? REQUEST_TIMED_OUT : 0),
      })
      await producer.connect()
      const transaction = await producer.transaction()
      const sent = transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      await new Promise((done) => setTimeout(done, 50))
      await producer.disconnect()
      const sendError = await sent
      fail = false
      await producer.connect()
      return { broker, producer, transaction, sendError }
    }

    test('connect() waits for an earlier disconnect() to end, which never touches the new lifecycle', async () => {
      const { cluster, broker, producer } = createTransactionalProducer({ level: 1 })
      await producer.connect()
      const torn = deferred()
      cluster.disconnect.mockImplementationOnce(() => torn.promise)
      const disconnected = producer.disconnect()
      await settle()
      let reconnected = false
      const connected = producer.connect().then(() => {
        reconnected = true
      })
      await settle()

      expect(reconnected).toBe(false)
      expect(cluster.connect).toHaveBeenCalledTimes(1)
      torn.resolve()
      await disconnected
      await connected
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: ONE })
      await transaction.commit()

      expect(cluster.connect).toHaveBeenCalledTimes(2)
      expect(cluster.disconnect).toHaveBeenCalledTimes(1)
      expect(broker.produce).toHaveBeenCalledTimes(1)
      expect(broker.endTxn).toHaveBeenCalledTimes(1)
    })

    test('disconnect() waits for a failing re-send to run out of retries, which makes the transaction abort-only: it aborts it, and commit() after reconnecting is refused', async () => {
      const { broker, transaction, sendError } = await reSendFailingThroughDisconnect(2)

      const commitError = await transaction.commit().catch((e) => e)

      expect(sendError).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(sendError.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(broker.produce).toHaveBeenCalledTimes(6)
      expect(commitError).toEqual(
        new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
      )
      expect(broker.endTxn.mock.calls.map(([{ transactionResult }]) => transactionResult)).toEqual([
        false,
      ])
    })

    test.each([1, 2])(
      'under transaction version %p, the abort disconnect() made after a re-send ran out of retries leaves the next transaction clean',
      async (level) => {
        const { broker, producer, transaction } = await reSendFailingThroughDisconnect(level)

        await expect(transaction.abort()).rejects.toEqual(
          new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
        )
        const next = await producer.transaction()
        await next.send({ topic: TOPIC, messages: ONE })
        await next.commit()

        expect(transactionalInits(broker)).toHaveLength(level === 1 ? 2 : 1)
        expect(broker.produce.mock.calls.at(-1)[0]).toMatchObject({ producerEpoch: 1 })
        expect(broker.produce.mock.calls.at(-1)[0].topicData[0].partitions[0].firstSequence).toBe(0)
        expect(
          broker.endTxn.mock.calls.map(([{ transactionResult }]) => transactionResult)
        ).toEqual([false, true])
      }
    )

    test('a fault in the send loop arms the state its error calls for: an invariant violation makes the producer unusable', async () => {
      const { broker, producer } = createTransactionalProducer()
      await producer.connect()
      const transaction = await producer.transaction()
      const fault = new KafkaJSInvariantViolation('a fault in the send loop')
      const complete = jest
        .spyOn(PartitionSequencer.prototype, 'complete')
        .mockImplementationOnce(() => {
          throw fault
        })

      const sendError = await transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      const commitError = await transaction.commit().catch((e) => e)
      complete.mockRestore()

      expect(sendError).toBe(fault)
      expect(broker.produce).toHaveBeenCalledTimes(1)
      expect(commitError).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(commitError.cause).toBe(fault)
      expect(broker.endTxn).not.toHaveBeenCalled()
    })

    test.each([1, 2])(
      'under transaction version %p, a fault in the send loop while another request is on the wire makes the producer unusable for commit() and abort() alike',
      async (level) => {
        const first = deferred()
        const second = deferred()
        let calls = 0
        const { broker, producer } = createTransactionalProducer({
          level,
          partitions: 2,
          linger: 0,
          produce: async (request) => {
            calls += 1
            await (calls === 1 ? first : second).promise
            return answer(request)
          },
        })
        await producer.connect()
        const transaction = await producer.transaction()
        const a = transaction
          .send({ topic: TOPIC, messages: [{ value: 'a', partition: 0 }] })
          .catch((e) => e)
        await settle()
        const b = transaction
          .send({ topic: TOPIC, messages: [{ value: 'b', partition: 1 }] })
          .catch((e) => e)
        await settle()
        const bug = new TypeError('a bug in the send loop')
        const complete = jest
          .spyOn(PartitionSequencer.prototype, 'complete')
          .mockImplementationOnce(() => {
            throw bug
          })

        first.resolve()
        expect(await a).toBe(bug)
        expect(await b).toBe(bug)
        complete.mockRestore()
        const commitError = await transaction.commit().catch((e) => e)
        const abortError = await transaction.abort().catch((e) => e)
        second.resolve()
        await settle()

        expect(calls).toEqual(2)
        expect(commitError).toBeInstanceOf(KafkaJSApplicationRecoverableError)
        expect(commitError.cause).toBeInstanceOf(KafkaJSInvariantViolation)
        expect(abortError).toBe(commitError)
        expect(broker.endTxn).not.toHaveBeenCalled()
      }
    )

    test('a partition with no leader until the retries run out makes the transaction abort-only', async () => {
      const { cluster, broker, producer } = createTransactionalProducer()
      cluster.findLeaderForPartitions = () => ({})
      await producer.connect()
      const transaction = await producer.transaction()

      const sendError = await transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)

      expect(sendError).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(sendError.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      await expect(transaction.commit()).rejects.toBe(sendError)
      expect(broker.produce).not.toHaveBeenCalled()
      expect(broker.endTxn).not.toHaveBeenCalled()
    })

    test('an error the client raises while it sends a batch makes the transaction abort-only, not the producer unusable', async () => {
      const { broker, producer } = createTransactionalProducer({
        produce: async (request) => {
          await encodeProduce(request).encode()
          return answer(request)
        },
      })
      await producer.connect()
      const transaction = await producer.transaction()
      broker.produce.mockImplementationOnce(async () => {
        Buffer.byteLength(5)
      })

      const sendError = await transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      const commitError = await transaction.commit().catch((e) => e)

      expect(sendError).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(sendError.cause).toMatchObject({ name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' })
      expect(commitError).toBe(sendError)
      expect(broker.endTxn).not.toHaveBeenCalled()
      await transaction.abort()
      const next = await producer.transaction()
      await next.send({ topic: TOPIC, messages: ONE })
      await next.commit()
      expect(broker.endTxn.mock.calls.at(-1)[0]).toMatchObject({ transactionResult: true })
    })

    test('an invariant violation raised by the send stays fatal', async () => {
      const violation = new KafkaJSInvariantViolation('a broken send')
      const { producer } = createTransactionalProducer({
        produce: async () => {
          throw violation
        },
      })
      await producer.connect()
      const transaction = await producer.transaction()

      await expect(transaction.send({ topic: TOPIC, messages: ONE })).rejects.toBe(violation)
      const commitError = await transaction.commit().catch((e) => e)

      expect(commitError).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(commitError.cause).toBe(violation)
      await expect(producer.transaction()).rejects.toBe(commitError)
    })
  })

  describe('the documented recovery', () => {
    test('a fenced send rejects with the broker error, abort() then says to replace the producer, and a new one carries on', async () => {
      let fenced = true
      const { cluster, broker, producer } = createTransactionalProducer({
        produce: async (request) => answer(request, fenced ? PRODUCER_FENCED : 0),
      })
      await producer.connect()
      const transaction = await producer.transaction()

      const sendError = await transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      const abortError = await transaction.abort().catch((e) => e)
      fenced = false
      const replacement = createProducer({
        cluster,
        logger: newLogger(),
        transactionalId: TRANSACTIONAL_ID,
        retry: RETRY,
      })
      await replacement.connect()
      const next = await replacement.transaction()
      await next.send({ topic: TOPIC, messages: ONE })
      await next.commit()

      expect(sendError).toMatchObject({ name: 'KafkaJSProtocolError', code: PRODUCER_FENCED })
      expect(sendError).not.toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(abortError).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(broker.endTxn.mock.calls.map(([{ transactionResult }]) => transactionResult)).toEqual([
        true,
      ])
    })
  })

  describe('producer.transaction()', () => {
    test('from ABORTABLE_ERROR rejects with the armed error, and from FATAL_ERROR with KafkaJSApplicationRecoverableError', async () => {
      let code = INVALID_RECORD
      const { producer } = createTransactionalProducer({
        produce: async (request) => answer(request, code),
      })
      await producer.connect()
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: ONE }).catch(() => {})

      const abortable = await producer.transaction().catch((e) => e)
      expect(abortable).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(abortable.message).toMatch(/call abort\(\)/)

      await transaction.abort()
      code = PRODUCER_FENCED
      const next = await producer.transaction()
      await next.send({ topic: TOPIC, messages: ONE }).catch(() => {})

      const fatal = await producer.transaction().catch((e) => e)
      expect(fatal).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(fatal.message).not.toMatch(/once ended/)
    })

    test('a second call while a transaction is open still says so', async () => {
      const { producer } = createTransactionalProducer()
      await producer.connect()
      await producer.transaction()

      await expect(producer.transaction()).rejects.toThrow(/already an ongoing transaction/)
    })

    test('of two first calls, the one whose InitProducerId retry ends after the other began also says so', async () => {
      const cluster = createFakeCluster({
        options: {
          retry: { retries: 2, initialRetryTime: 5, factor: 0, multiplier: 1, maxRetryTime: 5 },
        },
      })
      let transactionalInits = 0
      cluster.broker.initProducerId.mockImplementation(async ({ transactionalId }) => {
        if (transactionalId === TRANSACTIONAL_ID && ++transactionalInits === 1) {
          throw new KafkaJSProtocolError(createErrorFromCode(CONCURRENT_TRANSACTIONS))
        }
        return { producerId: '1000', producerEpoch: 0 }
      })
      const producer = createProducer({
        cluster,
        logger: newLogger(),
        transactionalId: TRANSACTIONAL_ID,
      })
      await producer.connect()

      const [first, second] = await Promise.allSettled([
        producer.transaction(),
        producer.transaction(),
      ])

      expect(first.status).toEqual('fulfilled')
      expect(second.reason).toEqual(
        new KafkaJSNonRetriableError(
          'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.'
        )
      )
      expect(transactionalInits).toEqual(2)
    })
  })

  describe('commit() and abort()', () => {
    test('commit() waits for the sends not awaited, then commits; a send failing meanwhile makes it reject', async () => {
      const held = []
      let failNext = false
      const { broker, producer } = createTransactionalProducer({
        produce: (request) => {
          const release = deferred()
          held.push(() => release.resolve(answer(request, failNext ? INVALID_RECORD : 0)))
          return release.promise
        },
      })
      await producer.connect()

      const first = await producer.transaction()
      const sending = first.send({ topic: TOPIC, messages: ONE })
      const committing = first.commit()
      await settle()
      expect(broker.endTxn).not.toHaveBeenCalled()
      await expect(first.send({ topic: TOPIC, messages: ONE })).rejects.toThrow(/once ended/)
      held.shift()()
      await committing
      await sending
      expect(broker.endTxn).toHaveBeenCalledTimes(1)

      const second = await producer.transaction()
      const failing = second.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      const refused = second.commit().catch((e) => e)
      await settle()
      failNext = true
      held.shift()()

      expect(await failing).toMatchObject({ code: INVALID_RECORD })
      expect(await refused).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(broker.endTxn).toHaveBeenCalledTimes(1)
      await second.abort()
    })

    test('a new transaction cannot begin while a send of the previous one is still in flight', async () => {
      const held = []
      const { broker, producer } = createTransactionalProducer({
        linger: 0,
        produce: (request) => {
          const release = deferred()
          held.push(() => release.resolve(answer(request)))
          return release.promise
        },
      })
      await producer.connect()
      const first = await producer.transaction()
      const sending = first.send({ topic: TOPIC, messages: ONE })
      const committing = first.commit()
      await settle()

      await expect(producer.transaction()).rejects.toThrow(/already an ongoing transaction/)
      held.shift()()
      await Promise.all([sending, committing])
      const second = await producer.transaction()
      const next = second.send({ topic: TOPIC, messages: ONE })
      await settle()
      held.shift()()
      await next
      await second.commit()

      expect(broker.endTxn.mock.calls.map(([{ transactionResult }]) => transactionResult)).toEqual([
        true,
        true,
      ])
    })

    test('abort() called while commit() waits for its sends waits for the commit, which keeps every record, and is then refused', async () => {
      const held = []
      const { broker, producer } = createTransactionalProducer({
        linger: 0,
        produce: (request) => {
          const release = deferred()
          held.push(() => release.resolve(answer(request)))
          return release.promise
        },
      })
      await producer.connect()
      const transaction = await producer.transaction()
      const sent = transaction.send({ topic: TOPIC, messages: ONE })
      await settle()
      const queued = transaction.send({ topic: TOPIC, messages: ONE })
      await settle()

      const committing = transaction.commit()
      const aborting = transaction.abort().catch((e) => e)
      held.shift()()
      await settle()
      held.shift()()

      await expect(Promise.all([sent, queued, committing])).resolves.toBeDefined()
      expect(await aborting).toEqual(
        new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
      )
      expect(broker.produce).toHaveBeenCalledTimes(2)
      expect(broker.endTxn).toHaveBeenCalledTimes(1)
      expect(broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ transactionResult: true })
      )
    })

    test('under transaction version 1, an AddPartitionsToTxn that runs out of retries leaves the transaction abort-only; abort() ends it and the next transaction commits', async () => {
      const { broker, producer } = createTransactionalProducer({ level: 1 })
      await producer.connect()
      broker.addPartitionsToTxn.mockRejectedValue(
        new KafkaJSProtocolError(createErrorFromCode(CONCURRENT_TRANSACTIONS))
      )
      const first = await producer.transaction()

      const sendError = await first.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      expect(sendError).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(sendError.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(RETRY.retries + 1)
      await expect(first.commit()).rejects.toBe(sendError)
      await expect(first.abort()).resolves.toBeUndefined()

      broker.addPartitionsToTxn.mockResolvedValue({})
      const second = await producer.transaction()
      await second.send({ topic: TOPIC, messages: ONE })
      await second.commit()

      expect(broker.produce).toHaveBeenCalledTimes(1)
      expect(broker.endTxn.mock.calls.map(([request]) => request)).toEqual([
        expect.objectContaining({ transactionResult: true, transactionVersion: 1 }),
      ])
    })

    test('abort() rejects the batches still queued, waits for those in flight, then aborts', async () => {
      const { broker, producer } = createTransactionalProducer({ level: 1 })
      await producer.connect()
      const added = deferred()
      broker.addPartitionsToTxn.mockImplementationOnce(() => added.promise)
      const transaction = await producer.transaction()

      const queued = transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      await settle()
      const aborting = transaction.abort()
      await settle()
      expect(broker.endTxn).not.toHaveBeenCalled()
      added.resolve({})
      await aborting

      const error = await queued
      expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(error.message).toEqual(
        'The transaction was aborted while this batch was waiting to be sent'
      )
      expect(broker.produce).not.toHaveBeenCalled()
      expect(broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ transactionResult: false, transactionVersion: 1 })
      )
    })

    test.each([
      ['transaction version 1', 1, false],
      ['transaction version 2, with an EndTxn that hands back no new epoch', 2, true],
    ])(
      'under %s, aborting while a sent batch waits to be re-sent moves the next transaction to a new epoch',
      async (_, level, noNewEpoch) => {
        const partitions = new Map()
        let refuse = false
        const { broker, producer } = createTransactionalProducer({
          level,
          linger: 0,
          retry: { retries: 2, initialRetryTime: 5000, maxRetryTime: 5000 },
          produce: async ({ topicData, producerEpoch }) => ({
            topics: topicData.map(({ topic, partitions: sent }) => ({
              topicName: topic,
              partitions: sent.map(({ partition, firstSequence, messages }) => {
                const key = `${topic}/${partition}`
                const last = partitions.get(key)
                let errorCode = 0
                if (refuse) {
                  errorCode = NOT_LEADER_OR_FOLLOWER
                } else if (
                  last !== undefined &&
                  (producerEpoch !== last.epoch ? firstSequence !== 0 : firstSequence !== last.next)
                ) {
                  errorCode = OUT_OF_ORDER_SEQUENCE_NUMBER
                } else {
                  partitions.set(key, {
                    epoch: producerEpoch,
                    next: firstSequence + messages.length,
                  })
                }
                return {
                  partition,
                  errorCode,
                  baseOffset: errorCode === 0 ? '0' : '-1',
                  logAppendTime: '-1',
                  logStartOffset: '0',
                }
              }),
            })),
          }),
        })
        if (noNewEpoch) {
          broker.endTxn.mockImplementation(async () => ({}))
        }
        await producer.connect()

        const first = await producer.transaction()
        await first.send({ topic: TOPIC, messages: ONE })
        refuse = true
        const resend = first.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
        await settle()
        refuse = false
        await first.abort()
        expect(await resend).toBeInstanceOf(KafkaJSTransactionAbortableError)

        const second = await producer.transaction()
        await second.send({ topic: TOPIC, messages: ONE })
        await second.commit()

        expect(broker.produce.mock.calls.map(([{ producerEpoch }]) => producerEpoch)).toEqual([
          0, 0, 1,
        ])
        expect(broker.produce.mock.calls.at(-1)[0].topicData[0].partitions[0].firstSequence).toBe(0)
      }
    )

    test('abort() rejects with the armed error when a fatal answer arrives while it waits', async () => {
      const { broker, producer } = createTransactionalProducer({ level: 1 })
      await producer.connect()
      const added = deferred()
      broker.addPartitionsToTxn.mockImplementationOnce(() => added.promise)
      const transaction = await producer.transaction()
      const queued = transaction.send({ topic: TOPIC, messages: ONE }).catch((e) => e)
      await settle()

      const aborting = transaction.abort().catch((e) => e)
      await settle()
      added.resolve(Promise.reject(new KafkaJSProtocolError(createErrorFromCode(PRODUCER_FENCED))))
      const error = await aborting

      expect(error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(error.cause).toMatchObject({ code: PRODUCER_FENCED })
      expect(broker.endTxn).not.toHaveBeenCalled()
      await expect(producer.transaction()).rejects.toBe(error)
      await queued
    })

    test('abort() from ABORTABLE_ERROR swallows an abortable failure of its EndTxn and ends the transaction', async () => {
      const { broker, producer } = createTransactionalProducer({
        produce: async (request) => answer(request, INVALID_RECORD),
      })
      await producer.connect()
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: ONE }).catch(() => {})
      broker.endTxn.mockRejectedValueOnce(
        new KafkaJSProtocolError(createErrorFromCode(TRANSACTION_ABORTABLE))
      )

      await expect(transaction.abort()).resolves.toBeUndefined()
      expect(transaction.isActive()).toBe(false)
      await expect(transaction.abort()).rejects.toThrow(/once ended/)
      await expect(producer.transaction()).resolves.toBeDefined()
    })

    test.each([1, 2])(
      'under transaction version %p, an abort whose EndTxn failed moves the next transaction to a new epoch',
      async (level) => {
        const { broker, producer } = createTransactionalProducer({ level })
        await producer.connect()
        const first = await producer.transaction()
        await first.send({ topic: TOPIC, messages: ONE })
        broker.endTxn.mockRejectedValueOnce(
          new KafkaJSProtocolError(createErrorFromCode(TRANSACTION_ABORTABLE))
        )

        await first.abort()
        const second = await producer.transaction()
        await second.send({ topic: TOPIC, messages: ONE })

        const reinitialized = broker.initProducerId.mock.calls.at(-1)[0]
        expect(reinitialized).toMatchObject({ producerId: '1000', producerEpoch: 0 })
        expect(broker.produce.mock.calls.map(([{ producerEpoch }]) => producerEpoch)).toEqual([
          0, 1,
        ])
        await second.commit()
      }
    )

    test('an EndTxn the coordinator is not ready for is sent again', async () => {
      const { broker, producer } = createTransactionalProducer()
      await producer.connect()
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: ONE })
      broker.endTxn.mockRejectedValueOnce(
        new KafkaJSProtocolError(createErrorFromCode(CONCURRENT_TRANSACTIONS))
      )

      await transaction.commit()

      expect(broker.endTxn).toHaveBeenCalledTimes(2)
      expect(transaction.isActive()).toBe(false)
    })

    test('a commit whose EndTxn fails makes the transaction abort-only, and abort() then ends it', async () => {
      const { broker, producer } = createTransactionalProducer()
      await producer.connect()
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: ONE })
      const abortable = new KafkaJSProtocolError(createErrorFromCode(TRANSACTION_ABORTABLE))
      broker.endTxn.mockRejectedValueOnce(abortable)

      const error = await transaction.commit().catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(error.cause).toBe(abortable)
      await expect(transaction.commit()).rejects.toBe(error)
      await transaction.abort()
      expect(broker.endTxn.mock.calls.at(-1)[0]).toMatchObject({ transactionResult: false })
    })

    test('abort() rejects with KafkaJSApplicationRecoverableError when its EndTxn fails fatally', async () => {
      const { broker, producer } = createTransactionalProducer()
      await producer.connect()
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: ONE })
      broker.endTxn.mockRejectedValueOnce(
        new KafkaJSProtocolError(createErrorFromCode(PRODUCER_FENCED))
      )

      const error = await transaction.abort().catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      await expect(producer.transaction()).rejects.toBe(error)
    })

    test('under transaction version 1, aborting ABORTABLE_ERROR bumps the epoch; a bump that fails is owed to the next transaction', async () => {
      let fail = true
      const { broker, producer } = createTransactionalProducer({
        level: 1,
        produce: async (request) => answer(request, fail ? INVALID_RECORD : 0),
      })
      await producer.connect()
      const transactionalInits = () =>
        broker.initProducerId.mock.calls.filter(
          ([{ transactionalId }]) => transactionalId === TRANSACTIONAL_ID
        )

      const first = await producer.transaction()
      await first.send({ topic: TOPIC, messages: ONE }).catch(() => {})
      broker.initProducerId.mockRejectedValueOnce(
        new KafkaJSProtocolError(createErrorFromCode(REQUEST_TIMED_OUT))
      )
      await first.abort()
      expect(transactionalInits()).toHaveLength(2)

      fail = false
      const second = await producer.transaction()
      await second.send({ topic: TOPIC, messages: ONE })
      await second.commit()

      expect(transactionalInits().map(([{ producerEpoch }]) => producerEpoch)).toEqual([-1, 0, 0])
      expect(broker.produce.mock.calls.at(-1)[0]).toMatchObject({ producerEpoch: 1 })
    })

    test('commit() and abort() after the end keep 2.2.4’s message', async () => {
      const { producer } = createTransactionalProducer()
      await producer.connect()
      const transaction = await endedTransaction(producer)

      for (const call of [
        () => transaction.commit(),
        () => transaction.abort(),
        () => transaction.send({ topic: TOPIC, messages: ONE }),
        () => transaction.sendBatch({ topicMessages: [{ topic: TOPIC, messages: ONE }] }),
        () => transaction.sendOffsets({ consumerGroupId: 'group-a', topics: [] }),
      ]) {
        await expect(call()).rejects.toThrow(
          new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
        )
      }
    })

    test('a transaction that ended keeps that message while a later one can only be aborted', async () => {
      const { producer } = createTransactionalProducer({
        produce: async (request) => answer(request, INVALID_RECORD),
      })
      await producer.connect()
      const first = await producer.transaction()
      await first.commit()
      const second = await producer.transaction()
      await second.send({ topic: TOPIC, messages: ONE }).catch(() => {})

      await expect(first.send({ topic: TOPIC, messages: ONE })).rejects.toThrow(
        new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
      )
      await expect(second.send({ topic: TOPIC, messages: ONE })).rejects.toBeInstanceOf(
        KafkaJSTransactionAbortableError
      )
      await second.abort()
    })
  })
})
