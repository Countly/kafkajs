const { Kafka } = require('../../../index')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSRecordTooLargeError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const {
  countersOf,
  flushAll,
  quiescent,
  transactionalProducer,
  watch,
} = require('./fixtures/bufferHarness')

const TOPIC = 'oversized-topic'
const TOPIC_ID = '00000000-0000-4000-8000-000000000007'
const CAP = 1048576
/**
 * The most record bytes one request carries for this topic with the scripted cluster's client
 * id: 1048576 less the request framing (32 + 16 for `scripted-cluster`), the topic's (32 + 15),
 * the partition's (16) and the batch header (61).
 */
const CAPACITY = CAP - 48 - 47 - 16 - 61
/** A keyless record of `bytes` wire bytes: 21 + 1 for the null key + 3 + the value + 1. */
const recordOf = (bytes) => ({ value: 'x'.repeat(bytes - 26) })

describe('Producer > a record that no request can carry', () => {
  let producer, cluster, frames

  const start = async (options = {}) => {
    cluster = createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
    frames = []
    const factory = cluster.socketFactory
    cluster.socketFactory = (socketOptions) => {
      const socket = factory(socketOptions)
      const write = socket.write
      socket.write = (buffer, ...rest) => {
        if (Buffer.from(buffer).readInt16BE(4) === 0) frames.push(buffer.length)
        return write(buffer, ...rest)
      }
      return socket
    }
    cluster
      .answer('Metadata', () => ({
        brokers: [{ nodeId: 0, host: 'node0', port: 9092, rack: null }],
        controllerId: 0,
        topics: [
          {
            errorCode: 0,
            name: TOPIC,
            topicId: TOPIC_ID,
            isInternal: false,
            topicAuthorizedOperations: -2147483648,
            partitions: [
              {
                errorCode: 0,
                partitionIndex: 0,
                leaderId: 0,
                leaderEpoch: 5,
                replicas: [0],
                isr: [0],
                offlineReplicas: [],
              },
            ],
          },
        ],
      }))
      .answer('Produce', ({ topics }) => ({
        responses: topics.map(({ partitions, ...key }) => ({
          ...key,
          partitions: partitions.map(({ partition }) => ({
            partition,
            errorCode: 0,
            baseOffset: '0',
            logAppendTime: '-1',
            logStartOffset: '0',
            leader: null,
          })),
        })),
        nodeEndpoints: [],
      }))
    producer = new Kafka(cluster.kafkaOptions()).producer({ linger: 0, ...options })
    await producer.connect()
  }

  const produced = () =>
    cluster
      .requests()
      .filter(({ api }) => api === 'Produce')
      .flatMap(({ fields }) => fields.topics[0].partitions[0].batches)

  afterEach(async () => {
    if (producer) await producer.disconnect()
    if (cluster) cluster.assertClean()
    producer = cluster = null
  })

  test.each([
    ['plain, acks 0', { idempotent: false }, 0],
    ['plain, acks 1', { idempotent: false }, 1],
    ['plain, acks -1', { idempotent: false }, -1],
    ['idempotent, acks -1', { idempotent: true }, -1],
  ])(
    '%s: a record of maxRequestSize + 1 bytes is refused at the call, typed, with no Produce written',
    async (_, options, acks) => {
      await start(options)

      const error = await producer
        .send({ topic: TOPIC, acks, messages: [recordOf(CAP + 1)] })
        .catch((failure) => failure)

      expect(error).toBeInstanceOf(KafkaJSRecordTooLargeError)
      expect(error).toMatchObject({
        name: 'KafkaJSRecordTooLargeError',
        type: 'MESSAGE_TOO_LARGE',
        code: 10,
        retriable: false,
        reason: 'maxRequestSize',
        scope: 'record',
        size: CAP + 1,
        limit: CAPACITY,
        topic: TOPIC,
      })
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(0)
    }
  )

  test.each([
    ['plain', { idempotent: false }],
    ['idempotent', { idempotent: true }],
  ])(
    '%s: a record at exactly the capacity goes out alone within the cap, and one byte more is refused before anything is written',
    async (_, options) => {
      await start(options)

      await producer.send({ topic: TOPIC, messages: [recordOf(CAPACITY)] })
      const error = await producer
        .send({ topic: TOPIC, messages: [recordOf(CAPACITY + 1)] })
        .catch((failure) => failure)

      expect(frames).toHaveLength(1)
      expect(frames[0]).toBeLessThanOrEqual(CAP)
      expect(produced().map(({ recordCount }) => recordCount)).toEqual([1])
      expect(error).toMatchObject({ reason: 'maxRequestSize', size: CAPACITY + 1, limit: CAPACITY })
    }
  )

  test('the next send is numbered as if the refused one had never been made', async () => {
    await start({ idempotent: true })

    await producer.send({ topic: TOPIC, messages: [{ value: 'before' }] })
    await producer.send({ topic: TOPIC, messages: [recordOf(CAP)] }).catch(() => {})
    await producer.send({ topic: TOPIC, messages: [{ value: 'after' }] })

    const [before, after] = produced()
    expect([after.producerId, after.producerEpoch]).toEqual([
      before.producerId,
      before.producerEpoch,
    ])
    expect([before.baseSequence, after.baseSequence]).toEqual([0, 1])
  })

  test('a record above both maxRequestSize and bufferMemory is refused for maxRequestSize, as the Java producer checks it first', async () => {
    await start({ idempotent: false, bufferMemory: 4096 })

    const error = await producer
      .send({ topic: TOPIC, messages: [recordOf(CAP + 1)] })
      .catch((failure) => failure)

    expect(error).toMatchObject({ reason: 'maxRequestSize' })
  })

  test.each([
    ['an explicit idempotent producer', { idempotent: true }],
    ['a transactional producer', { transactionalId: 'tx-acks' }],
  ])('%s keeps refusing acks other than -1 as a configuration error', async (_, options) => {
    await start(options)

    const error = await producer
      .send({ topic: TOPIC, acks: 1, messages: [{ value: 'v' }] })
      .catch((failure) => failure)

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(0)
  })
})

describe('Producer > a size refusal inside a transaction', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  test('arms the abortable state, the send keeps its own error, nothing of it is written or published, and abort() starts over', async () => {
    const { producer, transaction, cluster, admission, release } = await transactionalProducer({
      maxRequestSize: 4096,
    })
    const before = transaction.send({ topic: 'topic-a', messages: [{ value: 'first' }] })
    await flushAll()
    await release()
    await expect(before).resolves.toHaveLength(1)
    const produced = cluster.broker.produce.mock.calls.length

    const refused = await transaction
      .send({ topic: 'topic-a', messages: [{ value: 'x'.repeat(5000) }] })
      .catch((e) => e)

    expect(refused).toBeInstanceOf(KafkaJSRecordTooLargeError)
    expect(refused).toMatchObject({ reason: 'maxRequestSize', scope: 'record' })
    const later = watch(transaction.send({ topic: 'topic-a', messages: [{ value: 'v' }] }))
    const batch = watch(
      transaction.sendBatch({ topicMessages: [{ topic: 'topic-a', messages: [{ value: 'v' }] }] })
    )
    const offsets = watch(
      transaction.sendOffsets({
        consumerGroupId: 'group-a',
        topics: [{ topic: 'topic-a', partitions: [{ partition: 0, offset: '5' }] }],
      })
    )
    await flushAll()
    expect(later.value).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(batch.value).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(offsets.value).toBeInstanceOf(KafkaJSTransactionAbortableError)
    const commit = await transaction.commit().catch((e) => e)
    expect(commit).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(commit.cause).toBe(refused)
    expect(cluster.broker.produce.mock.calls.length).toBe(produced)
    expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()

    await transaction.abort()
    const next = await producer.transaction()
    const sent = next.send({ topic: 'topic-a', messages: [{ value: 'after' }] })
    await flushAll()
    await release()
    await expect(sent).resolves.toHaveLength(1)
    await expect(next.commit()).resolves.toBeUndefined()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })
  test('a post-guard transactional refusal with queued chunks preserves the original size error object before arming siblings', async () => {
    const Manager = require('../state')
    const arm = jest.spyOn(Manager.prototype, 'arm')
    const { producer, transaction, cluster, admission } = await transactionalProducer({
      maxRequestSize: 4096,
    })
    cluster.broker.produce.mockImplementation(async (request) => {
      const refusal = request.guard(4097)
      if (refusal) throw refusal
      throw new Error('the size guard must refuse this frame')
    })
    const error = await transaction
      .send({
        topic: 'topic-a',
        messages: Array.from({ length: 12 }, () => ({ value: 'x'.repeat(1000) })),
      })
      .catch((failure) => failure)
    const original = arm.mock.calls
      .map(([failure]) => failure)
      .find((failure) => failure instanceof KafkaJSRecordTooLargeError)
    expect(original).toBeInstanceOf(KafkaJSRecordTooLargeError)
    expect(error).toBe(original)
    expect(cluster.broker.produce.mock.calls.length).toBeGreaterThan(1)
    for (const operation of [
      () => transaction.send({ topic: 'topic-a', messages: [{ value: 'later' }] }),
      () =>
        transaction.sendBatch({
          topicMessages: [{ topic: 'topic-a', messages: [{ value: 'later' }] }],
        }),
      () =>
        transaction.sendOffsets({
          consumerGroupId: 'g',
          topics: [{ topic: 'topic-a', partitions: [{ partition: 0, offset: '1' }] }],
        }),
      () => transaction.commit(),
    ]) {
      const armed = await operation().catch((failure) => failure)
      expect(armed).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(armed.cause).toBe(original)
    }
    await transaction.abort()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
    arm.mockRestore()
  })
})
