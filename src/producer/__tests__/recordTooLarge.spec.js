const { held } = require('../state/recordSize')
const {
  KafkaJSNonRetriableError,
  KafkaJSRecordTooLargeError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const {
  bufferedProducer,
  countersOf,
  flushAll,
  quiescent,
  transactionalProducer,
  watch,
} = require('./fixtures/bufferHarness')

const TOPIC = 'topic-a'

/**
 * Observes prompt refusal before awaiting a send the predecessor never refuses.
 * @param {Promise<*>} promise the send under test
 * @returns {Promise<Error>} its rejection, after asserting that it has rejected
 */
const refusalOf = async (promise) => {
  const result = watch(promise)
  await flushAll()
  expect(result.state).toBe('rejected')
  return result.value
}

describe('Producer > a send that can never fit in bufferMemory', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  test.each([
    ['idempotent', { idempotent: true }],
    ['not idempotent', { idempotent: false }],
  ])(
    '%s: a record above bufferMemory is refused at once, before anything is queued, with no timer',
    async (_, options) => {
      const { producer, cluster, admission } = bufferedProducer({ capacity: 1000, ...options })
      await producer.connect()
      try {
        const timers = jest.getTimerCount()
        const record = { value: 'x'.repeat(1000) }

        const error = await refusalOf(producer.send({ topic: TOPIC, messages: [record] }))

        expect(error).toBeInstanceOf(KafkaJSRecordTooLargeError)
        expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
        expect(error).toMatchObject({
          name: 'KafkaJSRecordTooLargeError',
          retriable: false,
          category: 'invalidConfiguration',
          type: 'MESSAGE_TOO_LARGE',
          code: 10,
          reason: 'bufferMemory',
          scope: 'record',
          size: held(record),
          limit: 1000,
          topic: TOPIC,
        })
        expect(jest.getTimerCount()).toBe(timers)
        expect(countersOf(admission)).toEqual(quiescent(admission))
        expect(cluster.broker.produce).not.toHaveBeenCalled()
      } finally {
        await flushAll()
        await producer.disconnect()
      }
    }
  )

  test('a send whose records fit one by one but not together is refused as a whole, with no topic named', async () => {
    const { producer, cluster, admission } = bufferedProducer({ capacity: 3000 })
    await producer.connect()
    try {
      const first = { value: 'a'.repeat(800) }
      const second = { value: 'b'.repeat(800) }

      const error = await refusalOf(
        producer.sendBatch({
          topicMessages: [
            { topic: TOPIC, messages: [first] },
            { topic: 'topic-b', messages: [second] },
          ],
        })
      )

      expect(error).toBeInstanceOf(KafkaJSRecordTooLargeError)
      expect(error).toMatchObject({
        scope: 'send',
        size: 1536 + held(first) + held(second),
        limit: 3000,
      })
      expect(error).not.toHaveProperty('topic')
      expect(countersOf(admission)).toEqual(quiescent(admission))
      expect(cluster.broker.produce).not.toHaveBeenCalled()
    } finally {
      await flushAll()
      await producer.disconnect()
    }
  })

  test('the backing store a small Buffer keeps alive can make a send never fit', async () => {
    const { producer, admission } = bufferedProducer({ capacity: 9000 })
    await producer.connect()
    try {
      const view = Buffer.alloc(16384).subarray(0, 100)

      const error = await refusalOf(producer.send({ topic: TOPIC, messages: [{ value: view }] }))

      expect(error).toBeInstanceOf(KafkaJSRecordTooLargeError)
      expect(error).toMatchObject({
        scope: 'send',
        size: held({ value: view }) + 1536 + 16284,
        limit: 9000,
      })
      expect(countersOf(admission)).toEqual(quiescent(admission))
    } finally {
      await flushAll()
      await producer.disconnect()
    }
  })

  test("a send whose first record's backing store takes it past bufferMemory is refused there, as a whole, whatever records come after it", async () => {
    const { producer, cluster, admission } = bufferedProducer({ capacity: 2000, idempotent: false })
    await producer.connect()
    try {
      const first = {
        topic: 'first-topic',
        messages: [{ value: Buffer.alloc(16384).subarray(0, 1) }],
      }
      const later = { topic: 'later-topic', messages: [{ value: 'x'.repeat(2001) }] }

      const alone = await refusalOf(producer.sendBatch({ topicMessages: [first] }))
      const error = await refusalOf(producer.sendBatch({ topicMessages: [first, later] }))

      // 1536 + 89 (21 + 64 + 1 + 2 + 1) + 16383: the first prefix, before the later record's own refusal
      for (const refusal of [alone, error]) {
        expect(refusal).toBeInstanceOf(KafkaJSRecordTooLargeError)
        expect(refusal).toMatchObject({
          reason: 'bufferMemory',
          scope: 'send',
          size: 18008,
          limit: 2000,
        })
        expect(refusal).not.toHaveProperty('topic')
      }
      expect(countersOf(admission)).toEqual(quiescent(admission))
      expect(cluster.broker.produce).not.toHaveBeenCalled()
    } finally {
      await flushAll()
      await producer.disconnect()
    }
  })

  test('inside a transaction the refusal arms the abortable state; the send keeps its error, nothing of it is written, and the next transaction works', async () => {
    const { producer, transaction, cluster, admission, release } = await transactionalProducer({
      capacity: 4096,
    })
    try {
      const before = transaction.send({ topic: TOPIC, messages: [{ value: 'kept' }] })
      await release()
      await expect(before).resolves.toHaveLength(1)
      await transaction.sendOffsets({
        consumerGroupId: 'group-a',
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
      })
      const produced = cluster.broker.produce.mock.calls.length

      const refused = await refusalOf(
        transaction.send({ topic: TOPIC, messages: [{ value: 'x'.repeat(5000) }] })
      )

      expect(refused).toBeInstanceOf(KafkaJSRecordTooLargeError)
      await expect(transaction.send({ topic: TOPIC, messages: [{ value: 'v' }] })).rejects.toThrow(
        KafkaJSTransactionAbortableError
      )
      await expect(
        transaction.sendOffsets({
          consumerGroupId: 'group-a',
          topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '6' }] }],
        })
      ).rejects.toThrow(KafkaJSTransactionAbortableError)
      const commit = await transaction.commit().catch((e) => e)
      expect(commit).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(commit.cause).toBe(refused)
      expect(cluster.broker.produce.mock.calls.length).toBe(produced)
      expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()

      await transaction.abort()
      const next = await producer.transaction()
      const sent = next.send({ topic: TOPIC, messages: [{ value: 'after' }] })
      await release()
      await expect(sent).resolves.toHaveLength(1)
      await expect(next.commit()).resolves.toBeUndefined()
      expect(countersOf(admission)).toEqual(quiescent(admission))
      await flushAll()
    } finally {
      await release()
      await transaction.abort().catch(() => {})
      await release()
      await producer.disconnect()
    }
  })
})
