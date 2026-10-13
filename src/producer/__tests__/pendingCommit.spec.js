const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const TransactionState = require('../state/transactionState')
const { createLogger, LEVELS } = require('../../loggers')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSConnectionError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSRequestTimeoutError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')

const TOPIC = 'topic-a'
const TRANSACTIONAL_ID = 'transactional-a'
// A commit's EndTxn is tried three times before its outcome is unknown: counted below.
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 2 }
const ATTEMPTS = RETRY.retries + 1
const ONE = [{ key: 'key', value: 'value' }]

const REQUEST_TIMED_OUT = 7
const INVALID_TXN_STATE = 48
const PRODUCER_FENCED = 90
const TRANSACTION_ABORTABLE = 120

/** A promise and the function that resolves it. */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/** Lets every pending promise callback and timer of 0 ms run. */
const settle = () => new Promise((done) => setTimeout(done, 5))

/** The causes after which nobody knows whether an EndTxn reached the coordinator. */
const UNKNOWN_OUTCOMES = [
  ['a retriable code', () => new KafkaJSProtocolError(createErrorFromCode(REQUEST_TIMED_OUT))],
  ['a request timeout', () => new KafkaJSRequestTimeoutError('Request EndTxn timed out')],
  ['a lost connection', () => new KafkaJSConnectionError('Connection error: socket hang up')],
]

/**
 * A transactional producer over the fake cluster, its coordinator at transaction version `level`,
 * with one transaction that sent a record; `entries` holds every warning the producer logged.
 * @param {{ level?: 1|2, retry?: object }} [options]
 */
const createCommittingProducer = async ({ level = 2, retry = RETRY } = {}) => {
  const entries = []
  const logger = createLogger({
    level: LEVELS.WARN,
    logCreator: () => (entry) => entries.push(entry),
  })
  const cluster = createFakeCluster({})
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
  // Version 2 hands back the producer's next epoch; a version 1 answer carries no producer id.
  broker.endTxn.mockImplementation(async ({ producerEpoch }) =>
    level === 2 ? { producerId: '1000', producerEpoch: producerEpoch + 1 } : {}
  )
  const producer = createProducer({
    cluster,
    logger,
    transactionalId: TRANSACTIONAL_ID,
    retry,
  })
  await producer.connect()
  const transaction = await producer.transaction()
  await transaction.send({ topic: TOPIC, messages: ONE })
  const warnings = () =>
    entries.filter(({ log }) => /commit/.test(log.message)).map(({ log }) => log)
  return { cluster, broker, producer, transaction, warnings }
}

/** Makes every EndTxn fail with `error()`, a fresh error each time; returns the last one. */
const failEndTxn = (broker, error) => {
  const thrown = []
  broker.endTxn.mockImplementation(async () => {
    thrown.push(error())
    throw thrown.at(-1)
  })
  return () => thrown.at(-1)
}

/** A transaction whose first commit() ran its EndTxn retries out on a lost connection. */
const pendingCommit = async (options) => {
  const harness = await createCommittingProducer(options)
  const answer = harness.broker.endTxn.getMockImplementation()
  failEndTxn(harness.broker, () => new KafkaJSConnectionError('Connection error'))
  const error = await harness.transaction.commit().catch((e) => e)
  harness.broker.endTxn.mockImplementation(answer)
  harness.broker.endTxn.mockClear()
  return { ...harness, error }
}

/**
 * Holds the Produce of an unrelated `producer.send()`, so that a `disconnect()`'s delivery waits
 * until `release()`; resolves once that Produce was sent.
 * @returns {Promise<{ sending: Promise<unknown>, release: () => void }>}
 */
const holdDelivery = async (broker, producer) => {
  const held = deferred()
  const reached = deferred()
  const produce = broker.produce.getMockImplementation()
  broker.produce.mockImplementationOnce(async (request) => {
    reached.resolve()
    await held.promise
    return produce(request)
  })
  const sending = producer.send({ topic: TOPIC, messages: ONE })
  await reached.promise
  return { sending, release: held.resolve }
}

/**
 * Makes the next EndTxn succeed only once `answer()` is called; `sent` resolves when it is sent.
 * @returns {{ sent: Promise<void>, answer: () => void }}
 */
const holdEndTxn = (broker) => {
  const sent = deferred()
  const answer = deferred()
  broker.endTxn.mockImplementationOnce(async () => {
    sent.resolve()
    await answer.promise
    return { producerId: '1000', producerEpoch: 1 }
  })
  return { sent: sent.promise, answer: answer.resolve }
}

describe('Producer > a commit whose outcome is unknown', () => {
  test.each(UNKNOWN_OUTCOMES)(
    'after %s on every attempt, rejects with the last error itself, which is retriable',
    async (_, error) => {
      const { broker, transaction } = await createCommittingProducer()
      const last = failEndTxn(broker, error)

      const rejected = await transaction.commit().catch((e) => e)

      expect(rejected).toBe(last())
      expect(rejected.retriable).toBe(true)
      expect(['retriable', 'refreshRetriable']).toContain(rejected.category)
      expect(rejected).not.toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(rejected).not.toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(broker.endTxn).toHaveBeenCalledTimes(ATTEMPTS)
    }
  )

  test('after the retries outlived retry.maxTotalTime, rejects with the last error itself', async () => {
    // The first wait, about 50 ms, would end past the 20 ms deadline
    const { broker, transaction } = await createCommittingProducer({
      retry: { retries: 5, initialRetryTime: 50, maxTotalTime: 20 },
    })
    const last = failEndTxn(broker, () => new KafkaJSConnectionError('Connection error'))

    const rejected = await transaction.commit().catch((e) => e)

    expect(rejected).toBe(last())
    expect(broker.endTxn).toHaveBeenCalledTimes(1)
    await expect(transaction.abort()).rejects.toEqual(TransactionState.pendingCommitError('abort'))
  })

  test('after a coordinator lookup that ran out of its own retries, rejects with its last error', async () => {
    const { cluster, transaction } = await createCommittingProducer()
    const refused = new KafkaJSConnectionError('Connection error: connect ECONNREFUSED', {
      code: 'ECONNREFUSED',
    })
    cluster.findGroupCoordinator.mockRejectedValue(
      new KafkaJSNumberOfRetriesExceeded(refused, { retryCount: 2, retryTime: 1 })
    )

    await expect(transaction.commit()).rejects.toBe(refused)
  })

  test('after a coordinator lookup that gave up on an error that is not retriable, is not pending: the transaction can only be aborted', async () => {
    const { cluster, transaction } = await createCommittingProducer()
    cluster.findGroupCoordinator.mockRejectedValue(
      new KafkaJSNumberOfRetriesExceeded(new KafkaJSNonRetriableError('denied'), {
        retryCount: 2,
        retryTime: 1,
      })
    )

    const rejected = await transaction.commit().catch((e) => e)

    expect(rejected).toBeInstanceOf(KafkaJSTransactionAbortableError)
    await expect(transaction.abort()).resolves.toBeUndefined()
  })

  test.each([
    ['abort', (transaction) => transaction.abort()],
    ['send', (transaction) => transaction.send({ topic: TOPIC, messages: ONE })],
    [
      'sendBatch',
      (transaction) => transaction.sendBatch({ topicMessages: [{ topic: TOPIC, messages: ONE }] }),
    ],
    [
      'sendOffsets',
      (transaction) => transaction.sendOffsets({ consumerGroupId: 'group-a', topics: [] }),
    ],
    ['beginTransaction', (_, producer) => producer.transaction()],
  ])('then %s is refused, naming the pending commit', async (method, call) => {
    const { broker, producer, transaction } = await pendingCommit()

    const refused = await call(transaction, producer).catch((e) => e)

    expect(refused).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(refused).toEqual(TransactionState.pendingCommitError(method))
    expect(broker.endTxn).not.toHaveBeenCalled()
    expect(broker.produce).toHaveBeenCalledTimes(1)
  })

  test('the next commit() sends the same EndTxn again and, under transaction version 2, adopts the epoch it returns', async () => {
    const { broker, producer, transaction } = await pendingCommit()
    const sent = broker.endTxn.mock.calls

    await transaction.commit()

    expect(sent).toEqual([
      [expect.objectContaining({ producerId: '1000', producerEpoch: 0, transactionResult: true })],
    ])
    const next = await producer.transaction()
    await next.send({ topic: TOPIC, messages: ONE })
    expect(broker.produce.mock.calls.at(-1)[0]).toMatchObject({ producerEpoch: 1 })
    await next.commit()
  })

  test('under transaction version 1 the next commit() sends the same EndTxn and keeps the epoch', async () => {
    const { broker, producer, transaction } = await pendingCommit({ level: 1 })

    await transaction.commit()

    expect(broker.endTxn).toHaveBeenCalledWith(
      expect.objectContaining({ producerEpoch: 0, transactionResult: true, transactionVersion: 1 })
    )
    const next = await producer.transaction()
    await next.send({ topic: TOPIC, messages: ONE })
    expect(broker.produce.mock.calls.at(-1)[0]).toMatchObject({ producerEpoch: 0 })
    await next.commit()
  })

  describe('a commit sent again after the coordinator aborted the transaction on its timeout is refused with 48 or 90, depending on the transaction version and on whether the broker has KAFKA-20785 (fixed in 4.3.2 and 4.5.0)', () => {
    test.each([INVALID_TXN_STATE, PRODUCER_FENCED])(
      '%d rejects KafkaJSApplicationRecoverableError: the producer is unusable',
      async (code) => {
        const { broker, producer, transaction } = await pendingCommit()
        broker.endTxn.mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(code)))

        const error = await transaction.commit().catch((e) => e)

        expect(error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
        expect(error.cause).toMatchObject({ code })
        await expect(producer.transaction()).rejects.toBe(error)
        await expect(transaction.abort()).rejects.toBe(error)
      }
    )
  })

  test('a commit sent again and answered with an abortable code arms abort-only, which abort() ends', async () => {
    const { broker, producer, transaction } = await pendingCommit()
    broker.endTxn.mockRejectedValueOnce(
      new KafkaJSProtocolError(createErrorFromCode(TRANSACTION_ABORTABLE))
    )

    const error = await transaction.commit().catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
    await transaction.abort()
    expect(broker.endTxn.mock.calls.at(-1)[0]).toMatchObject({ transactionResult: false })
    await expect(producer.transaction()).resolves.toBeDefined()
  })

  describe('a commit() called while another is under way joins it', () => {
    test('while the commit is sent again: one EndTxn, and both resolve', async () => {
      const { broker, transaction } = await pendingCommit()
      const answer = deferred()
      broker.endTxn.mockImplementationOnce(async () => {
        await answer.promise
        return { producerId: '1000', producerEpoch: 1 }
      })

      const first = transaction.commit()
      await settle()
      const second = transaction.commit()
      answer.resolve()

      await expect(Promise.all([first, second])).resolves.toEqual([undefined, undefined])
      expect(second).toBe(first)
      expect(broker.endTxn).toHaveBeenCalledTimes(1)
    })

    test('while the commit is sent again and its outcome stays unknown: both reject with the same error', async () => {
      const { broker, transaction } = await pendingCommit()
      const last = failEndTxn(broker, () => new KafkaJSConnectionError('Connection error'))

      const first = transaction.commit().catch((e) => e)
      const second = transaction.commit().catch((e) => e)

      expect(await second).toBe(await first)
      expect(await first).toBe(last())
      expect(broker.endTxn).toHaveBeenCalledTimes(ATTEMPTS)
    })

    test('while the first commit is on the wire', async () => {
      const { broker, transaction } = await createCommittingProducer()

      await Promise.all([transaction.commit(), transaction.commit()])

      expect(broker.endTxn).toHaveBeenCalledTimes(1)
    })

    test('but not a commit() of a transaction that ended, which keeps its message', async () => {
      const { broker, producer, transaction } = await createCommittingProducer()
      await transaction.commit()
      const next = await producer.transaction()
      await next.send({ topic: TOPIC, messages: ONE })
      const answer = deferred()
      broker.endTxn.mockImplementationOnce(async () => {
        await answer.promise
        return { producerId: '1000', producerEpoch: 2 }
      })

      const committing = next.commit()
      await settle()
      const ended = transaction.commit().catch((e) => e)
      answer.resolve()

      expect(await ended).toEqual(
        new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
      )
      await committing
    })
  })
})

describe('Producer > disconnect() and the commit of a transaction', () => {
  test('a pending commit is sent once more; when it succeeds the producer disconnects quietly', async () => {
    const { broker, cluster, producer, warnings } = await pendingCommit()

    await producer.disconnect()

    expect(broker.endTxn).toHaveBeenCalledTimes(1)
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    expect(warnings()).toEqual([])
  })

  test('a pending commit whose outcome stays unknown is logged, naming the transactional id, and the producer disconnects', async () => {
    const { broker, cluster, producer, warnings } = await pendingCommit()
    failEndTxn(broker, () => new KafkaJSConnectionError('Connection error'))

    await expect(producer.disconnect()).resolves.toBeUndefined()

    expect(broker.endTxn).toHaveBeenCalledTimes(ATTEMPTS)
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    expect(warnings()).toEqual([
      expect.objectContaining({ transactionalId: TRANSACTIONAL_ID, error: 'Connection error' }),
    ])
  })

  test.each([INVALID_TXN_STATE, PRODUCER_FENCED])(
    'a pending commit answered %d is logged with its code; the producer disconnects and is unusable',
    async (code) => {
      const { broker, cluster, producer, warnings } = await pendingCommit()
      broker.endTxn.mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(code)))

      await expect(producer.disconnect()).resolves.toBeUndefined()

      expect(cluster.disconnect).toHaveBeenCalledTimes(1)
      expect(warnings()).toEqual([
        expect.objectContaining({ transactionalId: TRANSACTIONAL_ID, code }),
      ])
      await expect(producer.send({ topic: TOPIC, messages: ONE })).rejects.toThrow(
        'The producer is disconnected'
      )
      await expect(producer.transaction()).rejects.toBeInstanceOf(
        KafkaJSApplicationRecoverableError
      )
    }
  )

  test('a commit() still under way is waited for, and disconnect() sends no EndTxn of its own', async () => {
    const { broker, cluster, producer, transaction } = await createCommittingProducer()
    const answer = deferred()
    broker.endTxn.mockImplementationOnce(async () => {
      await answer.promise
      return { producerId: '1000', producerEpoch: 1 }
    })

    const committing = transaction.commit()
    await settle()
    const disconnecting = producer.disconnect()
    await settle()
    expect(cluster.disconnect).not.toHaveBeenCalled()
    answer.resolve()

    await Promise.all([committing, disconnecting])
    expect(broker.endTxn).toHaveBeenCalledTimes(1)
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
  })

  test('a commit() still under way that ends pending is logged, and nothing more is sent', async () => {
    const { broker, producer, transaction, warnings } = await createCommittingProducer()
    failEndTxn(broker, () => new KafkaJSConnectionError('Connection error'))

    const committing = transaction.commit().catch((e) => e)
    const disconnecting = producer.disconnect()

    expect(await committing).toBeInstanceOf(KafkaJSConnectionError)
    await disconnecting
    expect(broker.endTxn).toHaveBeenCalledTimes(ATTEMPTS)
    expect(warnings()).toHaveLength(1)
  })

  test('a commit() under way when disconnect() begins is joined, not sent again, although it ends pending while an unrelated send holds delivery', async () => {
    const { broker, cluster, producer, transaction, warnings } = await createCommittingProducer()
    // Delivery outlasts the commit's retries.
    const delivery = await holdDelivery(broker, producer)
    failEndTxn(broker, () => new KafkaJSConnectionError('Connection error'))

    const committing = transaction.commit().catch((e) => e)
    const disconnecting = producer.disconnect()

    expect(await committing).toBeInstanceOf(KafkaJSConnectionError)
    await settle()
    expect(cluster.disconnect).not.toHaveBeenCalled()
    delivery.release()
    await Promise.all([delivery.sending, disconnecting])
    expect(broker.endTxn).toHaveBeenCalledTimes(ATTEMPTS)
    expect(warnings()).toHaveLength(1)
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
  })

  test('called twice while a commit is pending, the second joins the first: one EndTxn', async () => {
    const { broker, cluster, producer } = await pendingCommit()
    const answer = deferred()
    broker.endTxn.mockImplementationOnce(async () => {
      await answer.promise
      return { producerId: '1000', producerEpoch: 1 }
    })

    const first = producer.disconnect()
    const second = producer.disconnect()
    await settle()
    answer.resolve()

    await Promise.all([first, second])
    expect(broker.endTxn).toHaveBeenCalledTimes(1)
    expect(cluster.disconnect).toHaveBeenCalledTimes(2)
  })

  test('called twice, the first joining a commit() that ends pending and the second sending it again, closes nothing before that EndTxn is answered', async () => {
    const { broker, cluster, producer, transaction, warnings } = await createCommittingProducer()
    const delivery = await holdDelivery(broker, producer)
    failEndTxn(broker, () => new KafkaJSConnectionError('Connection error'))
    const committing = transaction.commit().catch((e) => e)
    const joining = producer.disconnect()
    expect(await committing).toBeInstanceOf(KafkaJSConnectionError)
    const resend = holdEndTxn(broker)

    const resending = producer.disconnect()
    delivery.release()
    await resend.sent
    await settle()

    expect(cluster.disconnect).not.toHaveBeenCalled()
    resend.answer()
    await Promise.all([delivery.sending, joining, resending])
    expect(broker.endTxn).toHaveBeenCalledTimes(ATTEMPTS + 1)
    expect(warnings()).toHaveLength(1)
    expect(cluster.disconnect).toHaveBeenCalledTimes(2)
  })

  test('called twice, the first sending a pending commit again and the second joining a commit() called again that ends pending, closes nothing before that EndTxn is answered', async () => {
    const { broker, cluster, producer, transaction, warnings } = await pendingCommit()
    const delivery = await holdDelivery(broker, producer)
    const resending = producer.disconnect()
    failEndTxn(broker, () => new KafkaJSConnectionError('Connection error'))
    const retried = transaction.commit().catch((e) => e)
    const joining = producer.disconnect()
    expect(await retried).toBeInstanceOf(KafkaJSConnectionError)
    const resend = holdEndTxn(broker)

    delivery.release()
    await resend.sent
    await settle()

    expect(cluster.disconnect).not.toHaveBeenCalled()
    resend.answer()
    await Promise.all([delivery.sending, resending, joining])
    expect(broker.endTxn).toHaveBeenCalledTimes(ATTEMPTS + 1)
    expect(warnings()).toHaveLength(1)
    expect(cluster.disconnect).toHaveBeenCalledTimes(2)
  })

  test('waits for every commit() the application starts again while it waits, and closes nothing before the last is answered', async () => {
    const { broker, cluster, producer, transaction } = await createCommittingProducer()
    const outcome = (promise) =>
      promise.then(
        () => null,
        (error) => error
      )
    /** Makes every EndTxn wait for `release()`, then fail as unknown or succeed. */
    const gate = (unknown) => {
      const sent = deferred()
      const answer = deferred()
      broker.endTxn.mockImplementation(async ({ producerEpoch }) => {
        sent.resolve()
        await answer.promise
        if (unknown) throw new KafkaJSConnectionError('Connection error')
        return { producerId: '1000', producerEpoch: producerEpoch + 1 }
      })
      return { sent: sent.promise, release: answer.resolve }
    }

    const first = gate(true)
    const firstCommit = outcome(transaction.commit())
    await first.sent
    const closing = producer.disconnect()
    await settle()
    first.release()
    expect((await firstCommit).retriable).toBe(true)

    const second = gate(true)
    const secondCommit = outcome(transaction.commit())
    await second.sent
    await settle()
    expect(cluster.disconnect).not.toHaveBeenCalled()
    second.release()
    expect((await secondCommit).retriable).toBe(true)
    expect(cluster.disconnect).not.toHaveBeenCalled()

    const third = gate(false)
    const thirdCommit = transaction.commit()
    await third.sent
    await settle()
    expect(cluster.disconnect).not.toHaveBeenCalled()
    third.release()
    await Promise.all([closing, thirdCommit])
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
  })

  test('after a commit that settled, sends no EndTxn and logs nothing', async () => {
    const { broker, producer, transaction, warnings } = await createCommittingProducer()
    await transaction.commit()
    broker.endTxn.mockClear()

    await producer.disconnect()

    expect(broker.endTxn).not.toHaveBeenCalled()
    expect(warnings()).toEqual([])
  })
})

describe('Producer > a transaction coordinator that cannot be found while a transaction ends', () => {
  test('abort() still ends the transaction, and the next one first bumps the epoch', async () => {
    const { broker, cluster, producer, transaction } = await createCommittingProducer()
    const lost = new KafkaJSConnectionError('Connection error')
    cluster.findGroupCoordinator.mockRejectedValue(
      new KafkaJSNumberOfRetriesExceeded(lost, { retryCount: 2, retryTime: 1 })
    )

    await expect(transaction.abort()).resolves.toBeUndefined()

    cluster.findGroupCoordinator.mockResolvedValue(broker)
    const next = await producer.transaction()
    expect(broker.initProducerId.mock.calls.at(-1)[0]).toMatchObject({
      transactionalId: TRANSACTIONAL_ID,
      producerId: '1000',
      producerEpoch: 0,
    })
    await next.abort()
  })
})

describe('Producer > a transaction whose pending commit disconnect() completed', () => {
  test('has ended: its calls are refused and send nothing, and the next transaction commits', async () => {
    const { broker, producer, transaction } = await pendingCommit()
    await producer.disconnect()
    await producer.connect()
    const next = await producer.transaction()
    await next.send({ topic: TOPIC, messages: ONE })
    broker.endTxn.mockClear()
    broker.produce.mockClear()

    for (const call of [
      () => transaction.abort(),
      () => transaction.commit(),
      () => transaction.send({ topic: TOPIC, messages: ONE }),
    ]) {
      await expect(call()).rejects.toEqual(
        new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
      )
    }
    expect(broker.endTxn).not.toHaveBeenCalled()
    expect(broker.produce).not.toHaveBeenCalled()

    await next.commit()
    expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: true }))
  })
})
