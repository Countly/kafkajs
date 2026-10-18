jest.mock('./groupMessagesPerPartition')
const { newLogger } = require('testHelpers')
const { errorCodes, createErrorFromCode } = require('../protocol/error')
const {
  KafkaJSConnectionClosedError,
  KafkaJSConnectionError,
  KafkaJSInvalidFrameError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSRequestTimeoutError,
} = require('../errors')
const { partitionResults } = require('../protocol/requests/produce/partitionResults')
const retry = require('../retry')
const Cluster = require('../cluster')
const createSendMessages = require('./sendMessages')

/**
 * A broker's answer to a non-idempotent Produce request: every partition it carried succeeds, at
 * an offset equal to its partition number.
 * @param {{ topicData: { topic: string, partitions: { partition: number }[] }[] }} request
 */
const okResults = ({ topicData }) =>
  partitionResults({
    topics: topicData.map(({ topic, partitions }) => ({
      topicName: topic,
      partitions: partitions.map(({ partition }) => ({
        partition,
        errorCode: 0,
        baseOffset: `${partition}`,
        logAppendTime: '-1',
        logStartOffset: '0',
      })),
    })),
  })

/** The `RecordMetadata` `okResults` resolves a partition of `topicName` to. */
const okMetadata = (topicName, partition) => ({
  topicName,
  partition,
  errorCode: 0,
  baseOffset: `${partition}`,
  logAppendTime: '-1',
  logStartOffset: '0',
})

/**
 * One send through the factory's two entry points, as the producer makes it: the records are
 * assigned to partitions once, then those batches are sent.
 * @param {ReturnType<typeof createSendMessages>} sender
 */
const sendThrough =
  (sender) =>
  async ({ acks = -1, timeout = 30000, compression, topicMessages }) =>
    sender.sendPartitioned({
      acks,
      timeout,
      compression,
      batches: await sender.partition(topicMessages),
    })

describe('Producer > sendMessages', () => {
  const topic = 'topic-name'
  let leaderOf
  let mockProducerId, mockProducerEpoch, mockTransactionalId

  let messages,
    partitioner,
    brokers,
    cluster,
    messagesPerPartition,
    topicPartitionMetadata,
    eosManager,
    retrier

  beforeEach(() => {
    messages = []
    partitioner = jest.fn()
    brokers = {
      1: { nodeId: 1, produce: jest.fn(okResults) },
      2: { nodeId: 2, produce: jest.fn(okResults) },
      3: { nodeId: 3, produce: jest.fn(okResults) },
      4: { nodeId: 4, produce: jest.fn(okResults) },
    }
    leaderOf = { 0: 1, 1: 2, 2: 3 }
    topicPartitionMetadata = [
      {
        isr: [2],
        leader: 1,
        partitionErrorCode: 0,
        partitionId: 0,
        replicas: [2],
      },
    ]

    cluster = {
      addMultipleTargetTopics: jest.fn(),
      refreshMetadata: jest.fn(),
      refreshMetadataIfNecessary: jest.fn(),
      findTopicPartitionMetadata: jest.fn(() => topicPartitionMetadata),
      findLeaderForPartitions: jest.fn((_, partitions) =>
        partitions.reduce((leaders, partition) => {
          const leader = leaderOf[partition]
          return { ...leaders, [leader]: [...(leaders[leader] || []), Number(partition)] }
        }, {})
      ),
      findBroker: jest.fn(({ nodeId }) => brokers[nodeId]),
      // No topic in this suite has a known id, so every produce stays on the pre-topic-id path
      // (topicIds undefined, capped at the last name-keyed version) - exactly this suite's
      // existing expectations.
      topicId: jest.fn(() => undefined),
      targetTopics: new Set(),
      isConnected: jest.fn(() => true),
    }
    messagesPerPartition = {
      0: [{ key: '3' }, { key: '6' }, { key: '9' }],
      1: [{ key: '1' }, { key: '4' }, { key: '7' }],
      2: [{ key: '2' }, { key: '5' }, { key: '8' }],
    }

    mockProducerId = -1
    mockProducerEpoch = -1
    mockTransactionalId = undefined

    eosManager = {
      getProducerId: jest.fn(() => mockProducerId),
      getProducerEpoch: jest.fn(() => mockProducerEpoch),
      getSequence: jest.fn(() => 0),
      getTransactionalId: jest.fn(() => mockTransactionalId),
      updateSequence: jest.fn(),
      isTransactional: jest.fn().mockReturnValue(false),
      addPartitionsToTransaction: jest.fn(),
    }

    retrier = retry({ retries: 5 })

    require('./groupMessagesPerPartition').mockImplementation(() => messagesPerPartition)
  })

  test('only retry failed brokers', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )

    brokers[1].produce
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementationOnce(okResults)

    brokers[3].produce
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementationOnce(okResults)

    const response = await send({ topicMessages: [{ topic, messages }] })

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()
    expect(eosManager.addPartitionsToTransaction).not.toHaveBeenCalled()

    expect(brokers[1].produce).toHaveBeenCalledTimes(2)
    expect(brokers[2].produce).toHaveBeenCalledTimes(1)
    expect(brokers[3].produce).toHaveBeenCalledTimes(3)
    expect(brokers[1].produce.mock.calls[1][0].topicData[0].partitions).toEqual([
      expect.objectContaining({ partition: 0 }),
    ])
    expect(brokers[3].produce.mock.calls[2][0].topicData[0].partitions).toEqual([
      expect.objectContaining({ partition: 2 }),
    ])
    expect(response).toEqual([okMetadata(topic, 1), okMetadata(topic, 0), okMetadata(topic, 2)])
  })

  const PRODUCE_ERRORS = [
    'UNKNOWN_TOPIC_OR_PARTITION',
    'LEADER_NOT_AVAILABLE',
    'NOT_LEADER_FOR_PARTITION',
  ]

  for (const errorType of PRODUCE_ERRORS) {
    test(`refresh stale metadata on ${errorType}`, async () => {
      const send = sendThrough(
        createSendMessages({
          retrier,
          logger: newLogger(),
          cluster,
          partitioner,
          eosManager,
        })
      )
      brokers[1].produce
        .mockImplementationOnce(() => {
          throw createErrorFromCode(errorCodes.find(({ type }) => type === errorType).code)
        })
        .mockImplementationOnce(okResults)

      await send({ topicMessages: [{ topic, messages }] })
      expect(brokers[1].produce).toHaveBeenCalledTimes(2)
      expect(cluster.refreshMetadata).toHaveBeenCalled()
    })
  }

  test('does not re-produce messages to brokers that are no longer leaders after metadata refresh', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )

    brokers[2].produce.mockImplementationOnce(() => {
      leaderOf[1] = 4 // Broker 4 replaces broker 2 as leader for partition 1
      throw createErrorFromCode(
        errorCodes.find(({ type }) => type === 'NOT_LEADER_FOR_PARTITION').code
      )
    })

    const response = await send({ topicMessages: [{ topic, messages }] })

    expect(brokers[2].produce).toHaveBeenCalledTimes(1)
    expect(brokers[4].produce).toHaveBeenCalledTimes(1)
    expect(brokers[4].produce.mock.calls[0][0].topicData[0].partitions).toEqual([
      expect.objectContaining({ partition: 1 }),
    ])
    expect(response).toEqual([okMetadata(topic, 0), okMetadata(topic, 2), okMetadata(topic, 1)])
  })

  test('refreshes metadata if partition metadata is empty', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    await send({ topicMessages: [{ topic, messages }] })

    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('a failing reconnect/refresh after a disconnect does not replace the original send error', async () => {
    // retries: 0 so the retrier's own wrap (KafkaJSNumberOfRetriesExceeded) happens on the first
    // failure, whose `cause` is what the catch block actually threw - exactly what the (buggy)
    // unguarded `await cluster.connect(); await cluster.refreshMetadata()` would have replaced
    // with "connect boom" instead of the original KafkaJSConnectionClosedError.
    const send = sendThrough(
      createSendMessages({
        retrier: retry({ retries: 0 }),
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )

    const sendError = new KafkaJSConnectionClosedError('closed', { host: 'a', port: 1 })
    brokers[1].produce.mockImplementationOnce(() => {
      throw sendError
    })
    cluster.removeBroker = jest.fn()
    cluster.isConnected.mockReturnValue(false)
    cluster.connect = jest.fn(() => Promise.reject(new Error('connect boom')))
    cluster.refreshMetadata.mockImplementation(() => Promise.reject(new Error('refresh boom')))

    await expect(send({ topicMessages: [{ topic, messages }] })).rejects.toMatchObject({
      cause: expect.objectContaining({ name: 'KafkaJSConnectionClosedError' }),
    })
    expect(cluster.connect).toHaveBeenCalled()
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test.each([
    ['a connection error', () => new KafkaJSConnectionError('refused', { broker: 'a:1' })],
    [
      'an invalid response frame',
      () => new KafkaJSInvalidFrameError('bad frame', { reason: 'negative', size: -1 }),
    ],
    [
      'a connection closed under the request',
      () => new KafkaJSConnectionClosedError('closed', { host: 'a', port: 1 }),
    ],
  ])('refreshes the metadata before it sends again after %s', async (_name, build) => {
    const send = sendThrough(
      createSendMessages({
        retrier: retry({ retries: 1, initialRetryTime: 1, maxRetryTime: 2 }),
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )
    brokers[1].produce.mockImplementationOnce(() => {
      throw build()
    })
    cluster.removeBroker = jest.fn()

    await send({ topicMessages: [{ topic, messages }] })

    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('sends again without a refresh after a request timeout, which says nothing about the metadata', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier: retry({ retries: 1, initialRetryTime: 1, maxRetryTime: 2 }),
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )
    brokers[1].produce.mockImplementationOnce(() => {
      throw new KafkaJSRequestTimeoutError('timed out')
    })

    await send({ topicMessages: [{ topic, messages }] })

    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  test('a failing best-effort refresh after a stale-metadata error does not replace the original error', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier: retry({ retries: 0 }),
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )

    brokers[1].produce.mockImplementationOnce(() => {
      throw createErrorFromCode(
        errorCodes.find(({ type }) => type === 'NOT_LEADER_FOR_PARTITION').code
      )
    })
    cluster.refreshMetadata.mockImplementation(() => Promise.reject(new Error('refresh boom')))

    await expect(send({ topicMessages: [{ topic, messages }] })).rejects.toMatchObject({
      cause: expect.objectContaining({ type: 'NOT_LEADER_FOR_PARTITION' }),
    })
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('a metadata lookup refused without retry on a later attempt rejects the batches still unsettled with its own error', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )
    const refused = createErrorFromCode(
      errorCodes.find(({ type }) => type === 'TOPIC_AUTHORIZATION_FAILED').code
    )
    brokers[1].produce.mockImplementationOnce(() => {
      throw createErrorFromCode(5)
    })
    cluster.refreshMetadataIfNecessary
      .mockImplementationOnce(async () => {}) // assigning the partitions
      .mockImplementationOnce(async () => {}) // the first attempt
      .mockImplementationOnce(async () => {
        throw refused
      })

    const error = await send({ topicMessages: [{ topic, messages }] }).catch((e) => e)

    expect(error).toBe(refused)
    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalledTimes(3)
    expect(brokers[1].produce).toHaveBeenCalledTimes(1)
    expect(brokers[2].produce).toHaveBeenCalledTimes(1)
    expect(brokers[3].produce).toHaveBeenCalledTimes(1)
  })

  test('a non-idempotent send takes no lock, numbers nothing and adds no partition to a transaction', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )
    eosManager.isTransactional.mockReturnValue(true)

    await send({ topicMessages: [{ topic, messages }] })

    for (const broker of [brokers[1], brokers[2], brokers[3]]) {
      const [{ topicData }] = broker.produce.mock.calls[0]
      expect(topicData[0].partitions[0]).not.toHaveProperty('firstSequence')
    }
    expect(eosManager.getSequence).not.toHaveBeenCalled()
    expect(eosManager.updateSequence).not.toHaveBeenCalled()
    expect(eosManager.addPartitionsToTransaction).not.toHaveBeenCalled()
  })

  test('if transactional produces with the transactional id and producer id & epoch', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    eosManager.isTransactional.mockReturnValue(true)

    mockProducerId = 1000
    mockProducerEpoch = 1
    mockTransactionalId = 'transactionalid'

    await send({
      topicMessages: [{ topic, messages }],
    })

    expect(brokers[1].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: mockTransactionalId,
        producerEpoch: mockProducerEpoch,
      })
    )
    expect(brokers[3].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: mockTransactionalId,
        producerEpoch: mockProducerEpoch,
      })
    )
    expect(brokers[3].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: mockTransactionalId,
        producerEpoch: mockProducerEpoch,
      })
    )
  })

  test('if idempotent produces with the producer id & epoch without the transactional id', async () => {
    const send = sendThrough(
      createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    eosManager.isTransactional.mockReturnValue(false)

    mockProducerId = 1000
    mockProducerEpoch = 1
    mockTransactionalId = 'transactionalid'

    await send({
      topicMessages: [{ topic, messages }],
    })

    expect(brokers[1].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: undefined,
        producerEpoch: mockProducerEpoch,
      })
    )
    expect(brokers[3].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: undefined,
        producerEpoch: mockProducerEpoch,
      })
    )
    expect(brokers[3].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        producerId: mockProducerId,
        transactionalId: undefined,
        producerEpoch: mockProducerEpoch,
      })
    )
  })

  test("leaves recordErrors unresolved when the reported partition is not in this broker's topicData", async () => {
    // A malformed or unexpected `partition` on the thrown error (KIP-467) must not throw out of
    // attachFailingRecords - it should just leave recordErrors as the broker sent them.
    const send = sendThrough(
      createSendMessages({
        retrier: retry({ retries: 0 }),
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
    )

    const sendError = Object.assign(createErrorFromCode(87), {
      topic,
      partition: 999, // never assigned to broker 1 in this suite's fixtures
      recordErrors: [{ batchIndex: 0, errorMessage: 'record too large' }],
    })
    brokers[1].produce.mockImplementationOnce(() => {
      throw sendError
    })

    const error = await send({ topicMessages: [{ topic, messages }] }).catch((e) => e)

    expect(error.recordErrors).toEqual([{ batchIndex: 0, errorMessage: 'record too large' }])
  })

  describe('a partition without a leader among partitions that have one', () => {
    const FAST_RETRY = {
      retries: 1,
      initialRetryTime: 1,
      maxRetryTime: 1,
      factor: 0,
      multiplier: 1,
    }

    /**
     * One send of a batch per partition, each settling on its own settler, through the real
     * `Cluster` lookups, which throw for a negative leader.
     * @param {(number|undefined|null)[]} leaders the leader of partition `n` is `leaders[n]`;
     *   `undefined` is metadata with no leader field, and `null` is a partition the cache does not
     *   hold at all, which still gets a batch
     */
    const sendTo = async (leaders) => {
      const metadataCache = {
        brokerPool: {
          metadata: {
            topicMetadata: [
              {
                topic,
                partitionMetadata: leaders.flatMap((leader, partitionId) =>
                  leader === null ? [] : [{ partitionId, leader }]
                ),
              },
            ],
          },
        },
      }
      metadataCache.findTopicPartitionMetadata =
        Cluster.prototype.findTopicPartitionMetadata.bind(metadataCache)
      cluster.findTopicPartitionMetadata = metadataCache.findTopicPartitionMetadata
      cluster.findLeaderForPartitions =
        Cluster.prototype.findLeaderForPartitions.bind(metadataCache)
      const settlers = leaders.map(() => ({ resolve: jest.fn(), reject: jest.fn() }))

      const error = await createSendMessages({
        retrier: retry(FAST_RETRY),
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
        .sendPartitioned({
          acks: -1,
          timeout: 30000,
          batches: settlers.map((settler, partition) => ({
            topic,
            partition,
            messages: [{ value: `v-${partition}` }],
            settlers: [settler],
          })),
        })
        .catch((e) => e)
      return { settlers, error }
    }

    /** The partitions each Produce request to `nodeId` carried. */
    const producedTo = (nodeId) =>
      brokers[nodeId].produce.mock.calls.map(([{ topicData }]) =>
        topicData.flatMap(({ partitions }) => partitions.map(({ partition }) => partition))
      )

    test('the partitions with a leader are produced once and the one without is rejected alone', async () => {
      const { settlers, error } = await sendTo([1, -1, 1])

      expect(producedTo(1)).toEqual([[0, 2]])
      for (const partition of [0, 2]) {
        expect(settlers[partition].resolve).toHaveBeenCalledWith(okMetadata(topic, partition))
        expect(settlers[partition].reject).not.toHaveBeenCalled()
      }
      expect(settlers[1].resolve).not.toHaveBeenCalled()
      expect(settlers[1].reject).toHaveBeenCalledTimes(1)
      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause).toMatchObject({ type: 'LEADER_NOT_AVAILABLE' })
    })

    test('every partition of the topic without a leader is rejected and none is produced', async () => {
      const { settlers } = await sendTo([-1, -1])

      expect(brokers[1].produce).not.toHaveBeenCalled()
      settlers.forEach(({ resolve, reject }) => {
        expect(resolve).not.toHaveBeenCalled()
        expect(reject).toHaveBeenCalledTimes(1)
        expect(reject.mock.calls[0][0].cause).toMatchObject({ type: 'LEADER_NOT_AVAILABLE' })
      })
    })

    test('a partition whose metadata is invalid is retried within the budget, then rejected alone', async () => {
      const { settlers, error } = await sendTo([1, undefined])

      expect(producedTo(1)).toEqual([[0]])
      expect(settlers[0].resolve).toHaveBeenCalledTimes(1)
      expect(settlers[1].reject).toHaveBeenCalledWith(error)
      expect(error.name).toEqual('KafkaJSNumberOfRetriesExceeded')
      expect(error.cause.retriable).toBe(true)
      expect(error.message).toEqual('Invalid partition metadata')
    })

    test('a partition the cache does not hold is retried with the others and rejected beside the one without a leader', async () => {
      const { settlers, error } = await sendTo([1, -1, null])

      expect(producedTo(1)).toEqual([[0]])
      expect(settlers[0].resolve).toHaveBeenCalledTimes(1)
      for (const partition of [1, 2]) {
        expect(settlers[partition].resolve).not.toHaveBeenCalled()
        expect(settlers[partition].reject).toHaveBeenCalledTimes(1)
        expect(settlers[partition].reject).toHaveBeenCalledWith(error)
      }
      expect(error.name).toEqual('KafkaJSNumberOfRetriesExceeded')
      expect(error.cause.retriable).toBe(true)
    })

    test('a partition the cache does not hold gets the error it gets when no lookup fails, even beside one without a leader', async () => {
      const { settlers, error } = await sendTo([null, -1, 1])

      expect(producedTo(1)).toEqual([[2]])
      expect(settlers[2].resolve).toHaveBeenCalledTimes(1)
      for (const partition of [0, 1]) {
        expect(settlers[partition].reject).toHaveBeenCalledWith(error)
      }
      expect(error.cause).toMatchObject({
        name: 'KafkaJSMetadataNotLoaded',
        message: 'Producing to a partition without metadata',
        retriable: true,
      })
    })

    test('a partition the cache does not hold is rejected with that error when no lookup fails', async () => {
      const { settlers, error } = await sendTo([1, 1, null])

      expect(producedTo(1)).toEqual([[0, 1]])
      expect(settlers[2].reject).toHaveBeenCalledWith(error)
      expect(error.cause).toMatchObject({
        name: 'KafkaJSMetadataNotLoaded',
        message: 'Producing to a partition without metadata',
      })
    })
  })
})
