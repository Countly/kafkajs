jest.mock('./groupMessagesPerPartition')
const { newLogger } = require('testHelpers')
const { errorCodes, createErrorFromCode } = require('../protocol/error')
const retry = require('../retry')
const createSendMessages = require('./sendMessages')

const createProducerResponse = (topicName, partition) => ({
  topics: [
    {
      topicName,
      partitions: [
        {
          errorCode: 0,
          offset: `${partition}`,
          partition,
          timestamp: '-1',
        },
      ],
    },
  ],
})

describe('Producer > sendMessages', () => {
  const topic = 'topic-name'
  const partitionsPerLeader = {
    1: [0],
    2: [1],
    3: [2],
  }
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
      1: { nodeId: 1, produce: jest.fn(() => createProducerResponse(topic, 0)) },
      2: { nodeId: 2, produce: jest.fn(() => createProducerResponse(topic, 1)) },
      3: { nodeId: 3, produce: jest.fn(() => createProducerResponse(topic, 2)) },
      4: { nodeId: 4, produce: jest.fn(() => createProducerResponse(topic, 1)) },
    }
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
      findLeaderForPartitions: jest.fn(() => partitionsPerLeader),
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
      acquireBrokerLock: jest.fn(),
      releaseBrokerLock: jest.fn(),
    }

    retrier = retry({ retries: 5 })

    require('./groupMessagesPerPartition').mockImplementation(() => messagesPerPartition)
  })

  test('only retry failed brokers', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    brokers[1].produce
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementationOnce(() => createProducerResponse(topic, 0))

    brokers[3].produce
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementationOnce(() => {
        throw createErrorFromCode(5)
      })
      .mockImplementationOnce(() => createProducerResponse(topic, 2))

    const response = await sendMessages({ topicMessages: [{ topic, messages }] })

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()
    expect(eosManager.addPartitionsToTransaction).not.toHaveBeenCalled()

    expect(brokers[1].produce).toHaveBeenCalledTimes(2)
    expect(brokers[2].produce).toHaveBeenCalledTimes(1)
    expect(brokers[3].produce).toHaveBeenCalledTimes(3)
    expect(response).toEqual([
      { errorCode: 0, offset: '1', partition: 1, timestamp: '-1', topicName: 'topic-name' },
      { errorCode: 0, offset: '0', partition: 0, timestamp: '-1', topicName: 'topic-name' },
      { errorCode: 0, offset: '2', partition: 2, timestamp: '-1', topicName: 'topic-name' },
    ])
  })

  const PRODUCE_ERRORS = [
    'UNKNOWN_TOPIC_OR_PARTITION',
    'LEADER_NOT_AVAILABLE',
    'NOT_LEADER_FOR_PARTITION',
  ]

  for (const errorType of PRODUCE_ERRORS) {
    test(`refresh stale metadata on ${errorType}`, async () => {
      const sendMessages = createSendMessages({
        retrier,
        logger: newLogger(),
        cluster,
        partitioner,
        eosManager,
      })
      brokers[1].produce
        .mockImplementationOnce(() => {
          throw createErrorFromCode(errorCodes.find(({ type }) => type === errorType).code)
        })
        .mockImplementationOnce(() => createProducerResponse(topic, 0))

      await sendMessages({ topicMessages: [{ topic, messages }] })
      expect(brokers[1].produce).toHaveBeenCalledTimes(2)
      expect(cluster.refreshMetadata).toHaveBeenCalled()
    })
  }

  test('does not re-produce messages to brokers that are no longer leaders after metadata refresh', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    brokers[2].produce
      .mockImplementationOnce(() => {
        throw createErrorFromCode(
          errorCodes.find(({ type }) => type === 'NOT_LEADER_FOR_PARTITION').code
        )
      })
      .mockImplementationOnce(() => createProducerResponse(topic, 0))
    cluster.findLeaderForPartitions
      .mockImplementationOnce(() => partitionsPerLeader)
      .mockImplementationOnce(() => ({
        1: [0],
        4: [1], // Broker 4 replaces broker 2 as leader for partition 1
        3: [2],
      }))

    const response = await sendMessages({ topicMessages: [{ topic, messages }] })

    expect(response).toEqual([
      { errorCode: 0, offset: '0', partition: 0, timestamp: '-1', topicName: 'topic-name' },
      { errorCode: 0, offset: '2', partition: 2, timestamp: '-1', topicName: 'topic-name' },
      { errorCode: 0, offset: '1', partition: 1, timestamp: '-1', topicName: 'topic-name' },
    ])
  })

  test('refreshes metadata if partition metadata is empty', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    await sendMessages({ topicMessages: [{ topic, messages }] })

    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('a failing reconnect/refresh after a disconnect does not replace the original send error', async () => {
    // retries: 0 so the retrier's own wrap (KafkaJSNumberOfRetriesExceeded) happens on the first
    // failure, whose `cause` is what the catch block actually threw - exactly what the (buggy)
    // unguarded `await cluster.connect(); await cluster.refreshMetadata()` would have replaced
    // with "connect boom" instead of the original KafkaJSConnectionClosedError.
    const sendMessages = createSendMessages({
      retrier: retry({ retries: 0 }),
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    const sendError = Object.assign(new Error('closed'), {
      name: 'KafkaJSConnectionClosedError',
      host: 'a',
      port: 1,
    })
    brokers[1].produce.mockImplementationOnce(() => {
      throw sendError
    })
    cluster.removeBroker = jest.fn()
    cluster.isConnected.mockReturnValue(false)
    cluster.connect = jest.fn(() => Promise.reject(new Error('connect boom')))
    cluster.refreshMetadata.mockImplementation(() => Promise.reject(new Error('refresh boom')))

    await expect(sendMessages({ topicMessages: [{ topic, messages }] })).rejects.toMatchObject({
      cause: expect.objectContaining({ name: 'KafkaJSConnectionClosedError' }),
    })
    expect(cluster.connect).toHaveBeenCalled()
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('a failing best-effort refresh after a stale-metadata error does not replace the original error', async () => {
    const sendMessages = createSendMessages({
      retrier: retry({ retries: 0 }),
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    brokers[1].produce.mockImplementationOnce(() => {
      throw createErrorFromCode(
        errorCodes.find(({ type }) => type === 'NOT_LEADER_FOR_PARTITION').code
      )
    })
    cluster.refreshMetadata.mockImplementation(() => Promise.reject(new Error('refresh boom')))

    await expect(sendMessages({ topicMessages: [{ topic, messages }] })).rejects.toMatchObject({
      cause: expect.objectContaining({ type: 'NOT_LEADER_FOR_PARTITION' }),
    })
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('retrieves sequence information from the transaction manager and updates', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    eosManager.getSequence.mockReturnValue(5)

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    await sendMessages({
      topicMessages: [{ topic, messages }],
    })

    expect(brokers[1].produce.mock.calls[0][0].topicData[0].partitions[0]).toHaveProperty(
      'firstSequence',
      5
    )
    expect(brokers[2].produce.mock.calls[0][0].topicData[0].partitions[0]).toHaveProperty(
      'firstSequence',
      5
    )
    expect(brokers[3].produce.mock.calls[0][0].topicData[0].partitions[0]).toHaveProperty(
      'firstSequence',
      5
    )

    expect(eosManager.updateSequence).toHaveBeenCalledWith(
      'topic-name',
      0,
      messagesPerPartition[0].length
    )
    expect(eosManager.updateSequence).toHaveBeenCalledWith(
      'topic-name',
      1,
      messagesPerPartition[1].length
    )
    expect(eosManager.updateSequence).toHaveBeenCalledWith(
      'topic-name',
      2,
      messagesPerPartition[2].length
    )
  })

  test('adds partitions to the transaction if transactional', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    eosManager.isTransactional.mockReturnValue(true)

    await sendMessages({
      topicMessages: [{ topic, messages }],
    })

    const numTargetBrokers = 3
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledTimes(numTargetBrokers)
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      {
        topic: 'topic-name',
        partitions: [expect.objectContaining({ partition: 0 })],
      },
    ])
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      {
        topic: 'topic-name',
        partitions: [expect.objectContaining({ partition: 1 })],
      },
    ])
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      {
        topic: 'topic-name',
        partitions: [expect.objectContaining({ partition: 2 })],
      },
    ])
  })

  test('if transactional produces with the transactional id and producer id & epoch', async () => {
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    eosManager.isTransactional.mockReturnValue(true)

    mockProducerId = 1000
    mockProducerEpoch = 1
    mockTransactionalId = 'transactionalid'

    await sendMessages({
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
    const sendMessages = createSendMessages({
      retrier,
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    cluster.findTopicPartitionMetadata
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => topicPartitionMetadata)

    eosManager.isTransactional.mockReturnValue(false)

    mockProducerId = 1000
    mockProducerEpoch = 1
    mockTransactionalId = 'transactionalid'

    await sendMessages({
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
    const sendMessages = createSendMessages({
      retrier: retry({ retries: 0 }),
      logger: newLogger(),
      cluster,
      partitioner,
      eosManager,
    })

    const sendError = Object.assign(createErrorFromCode(87), {
      topic,
      partition: 999, // never assigned to broker 1 in this suite's fixtures
      recordErrors: [{ batchIndex: 0, errorMessage: 'record too large' }],
    })
    brokers[1].produce.mockImplementationOnce(() => {
      throw sendError
    })

    const error = await sendMessages({ topicMessages: [{ topic, messages }] }).catch((e) => e)

    expect(error.recordErrors).toEqual([{ batchIndex: 0, errorMessage: 'record too large' }])
  })
})
