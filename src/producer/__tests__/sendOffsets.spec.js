const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const encodeTxnOffsetCommit = require('../../protocol/requests/txnOffsetCommit/v5/request')
const { newLogger } = require('testHelpers')

const TRANSACTIONAL_ID = 'transactional-a'
const GROUP_ID = 'group-a'
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 2 }
const GROUP_METADATA = {
  groupId: GROUP_ID,
  generationId: 7,
  memberId: 'member-a',
  groupInstanceId: 'instance-a',
}
/** Offsets as `consumer.uncommittedOffsets()` reports them: string partitions and offsets. */
const TOPICS = [
  {
    topic: 'topic-a',
    partitions: [
      { partition: '0', offset: '11', leaderEpoch: 3 },
      { partition: '1', offset: '7' },
    ],
  },
]

const COORDINATOR_LOAD_IN_PROGRESS = 14
const NOT_COORDINATOR = 16
const ILLEGAL_GENERATION = 22
const GROUP_AUTHORIZATION_FAILED = 30
const UNKNOWN_MEMBER_ID = 25
const FENCED_INSTANCE_ID = 82
const STALE_MEMBER_EPOCH = 113

/**
 * A transactional producer whose one broker coordinates its transactions and the group, at
 * transaction version `level`, with an open transaction. Its EndTxn hands back no new epoch, so an
 * abort from an error state re-initializes, and InitProducerId answers one epoch up.
 * @param {{ level?: number }} [options]
 */
const openTransaction = async ({ level = 2 } = {}) => {
  const cluster = createFakeCluster()
  const { broker } = cluster
  Object.assign(broker, {
    finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: level } },
    finalizedFeaturesEpoch: 1,
    negotiatedVersion: () => 5,
    addOffsetsToTxn: jest.fn(async () => ({})),
    txnOffsetCommit: jest.fn(async () => ({})),
  })
  broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  broker.endTxn.mockImplementation(async () => ({ producerId: '-1', producerEpoch: -1 }))
  const producer = createProducer({
    cluster,
    logger: newLogger(),
    transactionalId: TRANSACTIONAL_ID,
    retry: RETRY,
  })
  await producer.connect()
  const transaction = await producer.transaction()
  return { cluster, broker, producer, transaction }
}

/** A protocol error the broker would answer with `code`. */
const protocolError = (code) => new KafkaJSProtocolError(createErrorFromCode(code))

describe('Producer > transaction.sendOffsets', () => {
  test('consumerGroupMetadata puts the generation, member and instance on the TxnOffsetCommit', async () => {
    const { cluster, broker, transaction } = await openTransaction()

    await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

    expect(broker.txnOffsetCommit).toHaveBeenCalledWith({
      transactionalId: TRANSACTIONAL_ID,
      producerId: '1000',
      producerEpoch: 0,
      groupId: GROUP_ID,
      topics: TOPICS,
      generationId: 7,
      memberId: 'member-a',
      groupInstanceId: 'instance-a',
      transactionVersion: 2,
    })
    expect(cluster.markOffsetAsCommitted.mock.calls.map(([call]) => call)).toEqual([
      { groupId: GROUP_ID, topic: 'topic-a', partition: '0', offset: '11' },
      { groupId: GROUP_ID, topic: 'topic-a', partition: '1', offset: '7' },
    ])
  })

  test('the 2.2.4 consumerGroupId sends no member to fence, and the same group id to the cache', async () => {
    const { cluster, broker, transaction } = await openTransaction()

    await transaction.sendOffsets({ consumerGroupId: GROUP_ID, topics: TOPICS })

    expect(broker.txnOffsetCommit).toHaveBeenCalledWith(
      expect.objectContaining({
        groupId: GROUP_ID,
        generationId: -1,
        memberId: '',
        groupInstanceId: null,
      })
    )
    expect(cluster.markOffsetAsCommitted).toHaveBeenCalledWith(
      expect.objectContaining({ groupId: GROUP_ID })
    )
  })

  test.each([
    ['both identifiers', { consumerGroupId: GROUP_ID, consumerGroupMetadata: GROUP_METADATA }],
    ['neither identifier', {}],
  ])('%s throw, and nothing is sent or armed', async (_, identifiers) => {
    const { broker, transaction } = await openTransaction()

    await expect(transaction.sendOffsets({ ...identifiers, topics: TOPICS })).rejects.toThrow(
      new KafkaJSNonRetriableError(
        'sendOffsets takes exactly one of consumerGroupMetadata and consumerGroupId'
      )
    )

    expect(broker.txnOffsetCommit).not.toHaveBeenCalled()
    await expect(transaction.commit()).resolves.toBeUndefined()
  })

  test('under transaction version 2 the TxnOffsetCommit adds the offsets itself: no AddOffsetsToTxn', async () => {
    const { broker, transaction } = await openTransaction({ level: 2 })

    await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
    await transaction.commit()

    expect(broker.addOffsetsToTxn).not.toHaveBeenCalled()
    expect(broker.endTxn).toHaveBeenCalledWith(
      expect.objectContaining({ transactionResult: true, transactionVersion: 2 })
    )
  })

  test('under transaction version 1 AddOffsetsToTxn goes first, then TxnOffsetCommit at version 1', async () => {
    const { broker, transaction } = await openTransaction({ level: 1 })
    const order = []
    broker.addOffsetsToTxn.mockImplementation(async () => order.push('AddOffsetsToTxn'))
    broker.txnOffsetCommit.mockImplementation(async ({ transactionVersion }) =>
      order.push(`TxnOffsetCommit at transaction version ${transactionVersion}`)
    )

    await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

    expect(order).toEqual(['AddOffsetsToTxn', 'TxnOffsetCommit at transaction version 1'])
    expect(broker.addOffsetsToTxn).toHaveBeenCalledWith({
      transactionalId: TRANSACTIONAL_ID,
      producerId: '1000',
      producerEpoch: 0,
      groupId: GROUP_ID,
    })
  })

  test.each([
    ['ILLEGAL_GENERATION', ILLEGAL_GENERATION],
    ['UNKNOWN_MEMBER_ID', UNKNOWN_MEMBER_ID],
    ['FENCED_INSTANCE_ID', FENCED_INSTANCE_ID],
    ['STALE_MEMBER_EPOCH', STALE_MEMBER_EPOCH],
  ])(
    '%s from TxnOffsetCommit makes the transaction abort-only: abort, rejoin, reprocess',
    async (_, code) => {
      const { broker, producer, transaction } = await openTransaction()
      const stale = protocolError(code)
      broker.txnOffsetCommit.mockRejectedValue(stale)

      const error = await transaction
        .sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
        .catch((e) => e)
      const commitError = await transaction.commit().catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(error.cause).toBe(stale)
      expect(stale.category).toEqual('abortable')
      expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
      expect(commitError).toBe(error)
      await transaction.abort()
      await expect(producer.transaction()).resolves.toBeDefined()
    }
  )

  test('an AddOffsetsToTxn refused for the group ACL makes the transaction abort-only', async () => {
    const { broker, transaction } = await openTransaction({ level: 1 })
    const refused = protocolError(GROUP_AUTHORIZATION_FAILED)
    broker.addOffsetsToTxn.mockRejectedValue(refused)

    const error = await transaction
      .sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(error.cause).toBe(refused)
    expect(refused.category).toEqual('invalidConfiguration')
    expect(broker.txnOffsetCommit).not.toHaveBeenCalled()
    await expect(transaction.commit()).rejects.toBe(error)
  })

  test('a moved group coordinator is looked up again; a loading one is asked again', async () => {
    const { cluster, broker, transaction } = await openTransaction()
    broker.txnOffsetCommit
      .mockRejectedValueOnce(protocolError(NOT_COORDINATOR))
      .mockRejectedValueOnce(protocolError(COORDINATOR_LOAD_IN_PROGRESS))
    cluster.findGroupCoordinator.mockClear()

    await transaction.sendOffsets({ consumerGroupMetadata: GROUP_METADATA, topics: TOPICS })

    expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(3)
    expect(
      cluster.findGroupCoordinator.mock.calls.filter(
        ([{ coordinatorType }]) => coordinatorType === COORDINATOR_TYPES.GROUP
      )
    ).toHaveLength(2)
  })

  test('a TxnOffsetCommit that runs out of retries surfaces as KafkaJSTransactionAbortableError', async () => {
    const { broker, transaction } = await openTransaction()
    broker.txnOffsetCommit.mockRejectedValue(protocolError(COORDINATOR_LOAD_IN_PROGRESS))

    const error = await transaction
      .sendOffsets({ consumerGroupId: GROUP_ID, topics: TOPICS })
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(error.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    await expect(transaction.commit()).rejects.toBe(error)
    await expect(transaction.abort()).resolves.toBeUndefined()
    expect(broker.endTxn).toHaveBeenCalledWith(
      expect.objectContaining({ transactionResult: false })
    )
  })

  test('an offset the client cannot encode makes the transaction abort-only, and abort() recovers the producer', async () => {
    const { broker, producer, transaction } = await openTransaction()
    broker.txnOffsetCommit.mockImplementation(async (request) => {
      await encodeTxnOffsetCommit(request).encode()
      return {}
    })

    const error = await transaction
      .sendOffsets({
        consumerGroupId: GROUP_ID,
        topics: [{ topic: 'topic-a', partitions: [{ partition: '0' }] }],
      })
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(error.cause).toBeInstanceOf(TypeError)
    await expect(transaction.commit()).rejects.toBe(error)
    await expect(transaction.abort()).resolves.toBeUndefined()
    await expect(producer.transaction()).resolves.toBeDefined()
  })

  test('commit() waits for a sendOffsets not awaited', async () => {
    const { broker, transaction } = await openTransaction()
    let answer
    broker.txnOffsetCommit.mockImplementation(
      () =>
        new Promise((resolve) => {
          answer = resolve
        })
    )

    const sending = transaction.sendOffsets({ consumerGroupId: GROUP_ID, topics: TOPICS })
    const committing = transaction.commit()
    await new Promise((resolve) => setImmediate(resolve))
    expect(broker.endTxn).not.toHaveBeenCalled()
    answer({})
    await Promise.all([sending, committing])

    expect(broker.endTxn).toHaveBeenCalledTimes(1)
  })

  describe('an offset collection with no partition', () => {
    test.each([
      ['no topic', []],
      ['a topic with no partition', [{ topic: 'topic-a', partitions: [] }]],
    ])(
      'under transaction version 2 %s starts nothing: no TxnOffsetCommit, and commit() sends no EndTxn',
      async (_, topics) => {
        const { broker, transaction } = await openTransaction({ level: 2 })
        broker.endTxn.mockRejectedValue(protocolError(48))

        await transaction.sendOffsets({ consumerGroupId: GROUP_ID, topics })

        await expect(transaction.commit()).resolves.toBeUndefined()
        expect(broker.addOffsetsToTxn).not.toHaveBeenCalled()
        expect(broker.txnOffsetCommit).not.toHaveBeenCalled()
        expect(broker.endTxn).not.toHaveBeenCalled()
      }
    )

    test.each([
      ['offsets', TOPICS],
      [
        'offsets next to a topic with no partition',
        [{ topic: 'topic-b', partitions: [] }, ...TOPICS],
      ],
    ])(
      'under transaction version 2 %s still start the transaction: TxnOffsetCommit, then EndTxn',
      async (_, topics) => {
        const { broker, transaction } = await openTransaction({ level: 2 })

        await transaction.sendOffsets({ consumerGroupId: GROUP_ID, topics })
        await transaction.commit()

        expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
        expect(broker.endTxn).toHaveBeenCalledTimes(1)
        expect(broker.endTxn).toHaveBeenCalledWith(
          expect.objectContaining({ transactionResult: true })
        )
      }
    )

    test('under transaction version 2 it leaves a transaction that sent records to commit them', async () => {
      const { broker, transaction } = await openTransaction({ level: 2 })

      await transaction.send({ topic: 'topic-a', messages: [{ value: 'v' }] })
      await transaction.sendOffsets({ consumerGroupId: GROUP_ID, topics: [] })
      await transaction.commit()

      expect(broker.txnOffsetCommit).not.toHaveBeenCalled()
      expect(broker.endTxn).toHaveBeenCalledTimes(1)
      expect(broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ transactionResult: true })
      )
    })

    test('under transaction version 1 it still sends AddOffsetsToTxn, which starts the transaction', async () => {
      const { broker, transaction } = await openTransaction({ level: 1 })

      await transaction.sendOffsets({ consumerGroupId: GROUP_ID, topics: [] })
      await transaction.commit()

      expect(broker.addOffsetsToTxn).toHaveBeenCalledTimes(1)
      expect(broker.endTxn).toHaveBeenCalledTimes(1)
      expect(broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ transactionResult: true })
      )
    })
  })
})
