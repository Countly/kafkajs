const OffsetOperations = require('../../consumer/offsetManager/offsetOperations')
const { newLogger } = require('testHelpers')
const createEosManager = require('.')
const Generation = require('../generation')
const FencedRetry = require('../../retry/fenced')
const ProducerStateManager = require('../state')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSConnectionError,
  KafkaJSDeliveryTimeout,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProducerClosedError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { createErrorFromCode } = require('../../protocol/error')

describe('Producer > eosManager', () => {
  const topic = 'topic-name'
  const producerId = 1000
  const producerEpoch = 1
  const mockInitProducerIdResponse = {
    producerId,
    producerEpoch,
  }

  let cluster, broker

  beforeEach(() => {
    broker = {
      initProducerId: jest.fn().mockReturnValue(mockInitProducerIdResponse),
      addPartitionsToTxn: jest.fn(),
      endTxn: jest.fn(),
      addOffsetsToTxn: jest.fn(),
      txnOffsetCommit: jest.fn(),
    }
    cluster = {
      offsetOperations: () => new OffsetOperations(),
      markOffsetAsCommitted: jest.fn(),
      refreshMetadataIfNecessary: jest.fn(),
      topicId: jest.fn(),
      findGroupCoordinator: jest.fn().mockReturnValue(broker),
      findControllerBroker: jest.fn().mockReturnValue(broker),
      findConnectedBroker: jest.fn().mockReturnValue(broker),
    }
  })

  test('initializing the producer id and epoch', async () => {
    const eosManager = createEosManager({
      logger: newLogger(),
      cluster,
      transactionTimeout: 30000,
    })

    expect(eosManager.getProducerId()).toEqual(-1)
    expect(eosManager.getProducerEpoch()).toEqual(0)
    expect(eosManager.getSequence(topic, 1)).toEqual(0)
    expect(eosManager.isInitialized()).toEqual(false)

    await eosManager.initProducerId()

    expect(cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled()
    expect(cluster.findControllerBroker).not.toHaveBeenCalled()
    expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(1)
    expect(broker.initProducerId).toHaveBeenCalledWith({
      transactionalId: null,
      transactionTimeout: 30000,
      producerId: -1,
      producerEpoch: -1,
    })

    expect(eosManager.getProducerId()).toEqual(mockInitProducerIdResponse.producerId)
    expect(eosManager.getProducerEpoch()).toEqual(mockInitProducerIdResponse.producerEpoch)
    expect(eosManager.isInitialized()).toEqual(true)
  })

  test.each([
    ['PRODUCER_FENCED', 90],
    ['TRANSACTION_ABORTABLE', 120],
  ])(
    '%s from initProducerId has its classified outcome, with no retry loop',
    async (type, code) => {
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionTimeout: 30000,
      })

      broker.initProducerId.mockImplementation(() => {
        throw createErrorFromCode(code)
      })

      await expect(eosManager.initProducerId()).rejects.toMatchObject(
        code === 90
          ? {
              name: 'KafkaJSApplicationRecoverableError',
              category: 'applicationRecoverable',
              cause: { name: 'KafkaJSProtocolError', type, code, retriable: false },
            }
          : { name: 'KafkaJSProtocolError', type, code, retriable: false }
      )

      expect(broker.initProducerId).toHaveBeenCalledTimes(1)
    }
  )

  test.each([47, 90])(
    'code %s from the first initProducerId is handed to onFenced',
    async (code) => {
      const onFenced = jest.fn()
      const eosManager = createEosManager({ logger: newLogger(), cluster, onFenced })
      broker.initProducerId.mockRejectedValue(createErrorFromCode(code))

      const error = await eosManager.initProducerId().catch((failure) => failure)

      expect(error.name).toBe('KafkaJSApplicationRecoverableError')
      expect(onFenced).toHaveBeenCalledTimes(1)
      expect(onFenced.mock.calls[0][0]).toBe(error)
      expect(eosManager.stateManager.transaction.armedError()).toBe(error)
    }
  )

  test('only a fenced first initProducerId reaches onFenced', async () => {
    const onFenced = jest.fn()
    const eosManager = createEosManager({
      logger: newLogger(),
      cluster,
      transactional: true,
      transactionalId: 'fence-notice',
      retry: { retries: 0 },
      onFenced,
    })
    broker.initProducerId.mockRejectedValueOnce(createErrorFromCode(120))
    await expect(eosManager.initProducerId()).rejects.toMatchObject({ code: 120 })
    await eosManager.initProducerId()
    await eosManager.beginTransaction()
    broker.initProducerId.mockRejectedValue(createErrorFromCode(47))
    await expect(eosManager.abort({ droppedSequences: true })).rejects.toMatchObject({
      name: 'KafkaJSApplicationRecoverableError',
    })
    expect(eosManager.stateManager.state()).toBe('FATAL_ERROR')
    expect(onFenced).not.toHaveBeenCalled()
  })

  test('getting & updating the sequence per topic-partition', async () => {
    const eosManager = createEosManager({ logger: newLogger(), cluster })

    expect(eosManager.getSequence(topic, 1)).toEqual(0)
    eosManager.updateSequence(topic, 1, 10) // No effect if we haven't initialized
    expect(eosManager.getSequence(topic, 1)).toEqual(0)

    await eosManager.initProducerId()

    expect(eosManager.getSequence(topic, 1)).toEqual(0)
    eosManager.updateSequence(topic, 1, 5)
    eosManager.updateSequence(topic, 1, 10)
    expect(eosManager.getSequence(topic, 1)).toEqual(15)

    expect(eosManager.getSequence(topic, 2)).toEqual(0) // Different partition
    expect(eosManager.getSequence('foobar', 1)).toEqual(0) // Different topic

    eosManager.updateSequence(topic, 3, Math.pow(2, 31) - 100)
    expect(eosManager.getSequence(topic, 3)).toEqual(Math.pow(2, 31) - 100) // Rotates once we reach 2 ^ 31 (max Int32)
    eosManager.updateSequence(topic, 3, 100)
    expect(eosManager.getSequence(topic, 3)).toEqual(0) // Rotates once we reach 2 ^ 31 (max Int32)

    await eosManager.initProducerId()
    expect(eosManager.getSequence(topic, 1)).toEqual(0) // Sequences reset by initProducerId
  })

  describe('if transactional=true', () => {
    let transactionalId

    beforeEach(() => {
      transactionalId = `transactional-id`
    })

    test('initializing the producer id and epoch with the transactional id', async () => {
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionTimeout: 30000,
        transactional: true,
        transactionalId,
      })

      expect(eosManager.getProducerId()).toEqual(-1)
      expect(eosManager.getProducerEpoch()).toEqual(0)
      expect(eosManager.getSequence(topic, 1)).toEqual(0)
      expect(eosManager.isInitialized()).toEqual(false)

      await eosManager.initProducerId()

      expect(cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled()
      expect(cluster.findConnectedBroker).not.toHaveBeenCalled()
      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      expect(broker.initProducerId).toHaveBeenCalledWith({
        transactionalId,
        transactionTimeout: 30000,
        producerId: -1,
        producerEpoch: -1,
      })

      expect(eosManager.getProducerId()).toEqual(mockInitProducerIdResponse.producerId)
      expect(eosManager.getProducerEpoch()).toEqual(mockInitProducerIdResponse.producerEpoch)
      expect(eosManager.isInitialized()).toEqual(true)
    })

    test('adding partitions to transaction', async () => {
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionalId,
        transactional: true,
      })
      await eosManager.initProducerId()
      await eosManager.beginTransaction()

      const topicData = [
        {
          topic: 'test-1',
          partitions: [{ partition: 1 }, { partition: 2 }],
        },
        {
          topic: 'test-2',
          partitions: [{ partition: 1 }],
        },
      ]

      cluster.findGroupCoordinator.mockClear()
      await eosManager.addPartitionsToTransaction(topicData)

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(1)
      expect(broker.addPartitionsToTxn).toHaveBeenCalledWith({
        transactionalId,
        producerId,
        producerEpoch,
        topics: [
          {
            topic: 'test-1',
            partitions: [1, 2],
          },
          {
            topic: 'test-2',
            partitions: [1],
          },
        ],
      })

      broker.addPartitionsToTxn.mockClear()
      await eosManager.addPartitionsToTransaction(topicData)
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(0) // No call if nothing new

      broker.addPartitionsToTxn.mockClear()
      await eosManager.addPartitionsToTransaction([
        ...topicData,
        { topic: 'test-2', partitions: [{ partition: 2 }] },
        { topic: 'test-3', partitions: [{ partition: 1 }] },
      ])
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(1) // Called if some new
      expect(broker.addPartitionsToTxn).toHaveBeenCalledWith({
        transactionalId,
        producerId,
        producerEpoch,
        topics: [
          {
            topic: 'test-2',
            partitions: [2],
          },
          {
            topic: 'test-3',
            partitions: [1],
          },
        ],
      })
    })

    test('committing a transaction', async () => {
      const consumerGroupId = 'consumer-group-id'
      const topics = [{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionTimeout: 30000,
        transactional: true,
        transactionalId,
      })

      await expect(eosManager.commit()).rejects.toEqual(
        new KafkaJSNonRetriableError(
          'Transaction state exception: Cannot call "commit" in state "UNINITIALIZED"'
        )
      )
      await eosManager.initProducerId()
      await expect(eosManager.commit()).rejects.toEqual(
        new KafkaJSNonRetriableError(
          'Transaction state exception: Cannot call "commit" in state "READY"'
        )
      )
      await eosManager.beginTransaction()

      cluster.findGroupCoordinator.mockClear()
      await eosManager.addPartitionsToTransaction(topics)
      await eosManager.commit()

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      expect(broker.endTxn).toHaveBeenCalledWith({
        producerId,
        producerEpoch,
        transactionalId,
        transactionResult: true,
        transactionVersion: 1,
      })

      await eosManager.beginTransaction()

      cluster.findGroupCoordinator.mockClear()
      broker.endTxn.mockClear()

      await eosManager.sendOffsets({ consumerGroupId, topics })
      await eosManager.commit()

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      expect(broker.endTxn).toHaveBeenCalledWith({
        producerId,
        producerEpoch,
        transactionalId,
        transactionResult: true,
        transactionVersion: 1,
      })
    })

    test('aborting a transaction', async () => {
      const consumerGroupId = 'consumer-group-id'
      const topics = [{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionTimeout: 30000,
        transactional: true,
        transactionalId,
      })

      await expect(eosManager.abort()).rejects.toEqual(
        new KafkaJSNonRetriableError(
          'Transaction state exception: Cannot call "abort" in state "UNINITIALIZED"'
        )
      )
      await eosManager.initProducerId()
      await expect(eosManager.abort()).rejects.toEqual(
        new KafkaJSNonRetriableError(
          'Transaction state exception: Cannot call "abort" in state "READY"'
        )
      )
      await eosManager.beginTransaction()

      cluster.findGroupCoordinator.mockClear()
      await eosManager.addPartitionsToTransaction(topics)
      await eosManager.abort()

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      expect(broker.endTxn).toHaveBeenCalledWith({
        producerId,
        producerEpoch,
        transactionalId,
        transactionResult: false,
        transactionVersion: 1,
      })

      await eosManager.beginTransaction()

      cluster.findGroupCoordinator.mockClear()
      broker.endTxn.mockClear()

      await eosManager.sendOffsets({ consumerGroupId, topics })
      await eosManager.abort()

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      expect(broker.endTxn).toHaveBeenCalledWith({
        producerId,
        producerEpoch,
        transactionalId,
        transactionResult: false,
        transactionVersion: 1,
      })
    })

    test('sending offsets', async () => {
      const consumerGroupId = 'consumer-group-id'
      const topics = [{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionTimeout: 30000,
        transactional: true,
        transactionalId,
      })

      await expect(eosManager.sendOffsets()).rejects.toEqual(
        new KafkaJSNonRetriableError(
          'Transaction state exception: Cannot call "sendOffsets" in state "UNINITIALIZED"'
        )
      )
      await eosManager.initProducerId()
      await expect(eosManager.sendOffsets()).rejects.toEqual(
        new KafkaJSNonRetriableError(
          'Transaction state exception: Cannot call "sendOffsets" in state "READY"'
        )
      )
      await eosManager.beginTransaction()

      cluster.findGroupCoordinator.mockClear()

      await eosManager.sendOffsets({ consumerGroupId, topics })

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: consumerGroupId,
        coordinatorType: COORDINATOR_TYPES.GROUP,
      })
      expect(broker.addOffsetsToTxn).toHaveBeenCalledWith({
        producerId,
        producerEpoch,
        transactionalId,
        groupId: consumerGroupId,
      })
      expect(broker.txnOffsetCommit).toHaveBeenCalledWith({
        transactionalId,
        producerId,
        producerEpoch,
        groupId: consumerGroupId,
        topics,
        generationId: -1,
        memberId: '',
        groupInstanceId: null,
        transactionVersion: 1,
      })
    })

    describe('retrying addOffsetsToTxn on CONCURRENT_TRANSACTIONS', () => {
      test('code 51 is retried against a freshly looked-up transaction coordinator', async () => {
        const consumerGroupId = 'consumer-group-id'
        const topics = [{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]
        const eosManager = createEosManager({
          logger: newLogger(),
          cluster,
          transactionTimeout: 30000,
          transactional: true,
          transactionalId,
        })
        await eosManager.initProducerId()
        await eosManager.beginTransaction()

        let attempt = 0
        broker.addOffsetsToTxn.mockImplementation(() => {
          attempt += 1
          if (attempt === 1) throw createErrorFromCode(51)
        })

        await expect(eosManager.sendOffsets({ consumerGroupId, topics })).resolves.not.toThrow()

        expect(broker.addOffsetsToTxn).toHaveBeenCalledTimes(2)
        expect(broker.txnOffsetCommit).toHaveBeenCalled()
      })
    })

    describe('recovering from coordinator errors on txnOffsetCommit', () => {
      let consumerGroupId, topics

      beforeEach(() => {
        consumerGroupId = 'consumer-group-id'
        topics = [{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]
      })

      // sendOffsets also looks up the *transaction* coordinator (for addOffsetsToTxn), so count
      // only the group-coordinator lookups used for txnOffsetCommit.
      const groupCoordinatorLookups = () =>
        cluster.findGroupCoordinator.mock.calls.filter(
          ([{ coordinatorType }]) => coordinatorType === COORDINATOR_TYPES.GROUP
        ).length

      test('code 14 (coordinator loading) is retried against the same coordinator', async () => {
        const eosManager = createEosManager({
          logger: newLogger(),
          cluster,
          transactionTimeout: 30000,
          transactional: true,
          transactionalId,
        })
        await eosManager.initProducerId()
        await eosManager.beginTransaction()

        let attempt = 0
        broker.txnOffsetCommit.mockImplementation(() => {
          attempt += 1
          if (attempt === 1) throw createErrorFromCode(14)
        })

        cluster.findGroupCoordinator.mockClear()

        await expect(eosManager.sendOffsets({ consumerGroupId, topics })).resolves.not.toThrow()

        expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(2)
        // Only the initial lookup from sendOffsets itself -- no extra re-discovery on retry.
        expect(groupCoordinatorLookups()).toBe(1)
      })

      test.each([[15], [16]])(
        'code %i refreshes the group coordinator before retrying',
        async (code) => {
          const eosManager = createEosManager({
            logger: newLogger(),
            cluster,
            transactionTimeout: 30000,
            transactional: true,
            transactionalId,
          })
          await eosManager.initProducerId()
          await eosManager.beginTransaction()

          let attempt = 0
          broker.txnOffsetCommit.mockImplementation(() => {
            attempt += 1
            if (attempt === 1) throw createErrorFromCode(code)
          })

          cluster.findGroupCoordinator.mockClear()

          await expect(eosManager.sendOffsets({ consumerGroupId, topics })).resolves.not.toThrow()

          expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(2)
          // Initial lookup from sendOffsets itself, plus one re-discovery after the error.
          expect(groupCoordinatorLookups()).toBe(2)
          expect(cluster.findGroupCoordinator).toHaveBeenLastCalledWith({
            groupId: consumerGroupId,
            coordinatorType: COORDINATOR_TYPES.GROUP,
          })
        }
      )
    })

    test('aborting transaction when no operation have been made should not send EndTxn', async () => {
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionTimeout: 30000,
        transactional: true,
        transactionalId,
      })

      await eosManager.initProducerId()
      await eosManager.beginTransaction()

      await expect(eosManager.abort()).resolves.not.toThrow()
      expect(eosManager.isInTransaction()).toEqual(false)
      expect(broker.endTxn).not.toHaveBeenCalled()
    })

    test('commiting transaction when no operation have been made should not send EndTxn', async () => {
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionTimeout: 30000,
        transactional: true,
        transactionalId,
      })

      await eosManager.initProducerId()
      await eosManager.beginTransaction()

      await expect(eosManager.commit()).resolves.not.toThrow()
      expect(eosManager.isInTransaction()).toEqual(false)
      expect(broker.endTxn).not.toHaveBeenCalled()
    })
  })

  describe('if transactional=false', () => {
    let eosManager

    beforeEach(async () => {
      eosManager = createEosManager({ logger: newLogger(), cluster })
      await eosManager.initProducerId()
    })

    function testTransactionalGuardAsync(method) {
      test(`${method} throws`, async () => {
        const eosManager = createEosManager({ logger: newLogger(), cluster })

        await expect(eosManager[method]()).rejects.toEqual(
          new KafkaJSNonRetriableError(
            `Transaction state exception: Cannot call "${method}" in state "UNINITIALIZED"`
          )
        )
      })
    }

    test(`beginTransaction rejects`, async () => {
      await expect(eosManager.beginTransaction()).rejects.toThrow(
        new KafkaJSNonRetriableError('Method unavailable if non-transactional')
      )
    })

    testTransactionalGuardAsync('addPartitionsToTransaction')
    testTransactionalGuardAsync('sendOffsets')
    testTransactionalGuardAsync('commit')
    testTransactionalGuardAsync('abort')
  })

  describe('over the producer state manager', () => {
    const transactionalId = 'transactional-id'
    const create = (options = {}) =>
      createEosManager({ logger: newLogger(), cluster, transactionTimeout: 30000, ...options })
    const transactionalManager = () => create({ transactional: true, transactionalId })

    test('exposes the manager it delegates to, read-only', () => {
      const eosManager = create()

      expect(eosManager.stateManager).toBeInstanceOf(ProducerStateManager)
      expect(Object.getOwnPropertyDescriptor(eosManager, 'stateManager').set).toBeUndefined()
    })

    test.each([
      ['getSequence', [topic, 1], 'nextSequence'],
      ['updateSequence', [topic, 1, 5], 'advance'],
    ])('%s forwards to the manager once, with the same arguments', async (method, args, member) => {
      const eosManager = create()
      await eosManager.initProducerId()
      const spy = jest.spyOn(eosManager.stateManager, member)

      eosManager[method](...args)

      expect(spy).toHaveBeenCalledTimes(1)
      expect(spy).toHaveBeenCalledWith(...args)
    })

    test('initProducerId, isInitialized, getProducerId and getProducerEpoch read the manager', async () => {
      const eosManager = create()
      const { stateManager } = eosManager
      const initialize = jest.spyOn(stateManager, 'initialize')
      const isInitialized = jest.spyOn(stateManager, 'isInitialized')
      const id = jest.spyOn(stateManager, 'producerId', 'get')
      const epoch = jest.spyOn(stateManager, 'producerEpoch', 'get')

      await eosManager.initProducerId()
      expect(initialize).toHaveBeenCalledTimes(1)
      expect(initialize).toHaveBeenCalledWith()
      initialize.mockClear()
      id.mockClear()
      epoch.mockClear()

      expect(eosManager.isInitialized()).toBe(true)
      expect(eosManager.getProducerId()).toEqual(producerId)
      expect(eosManager.getProducerEpoch()).toEqual(producerEpoch)
      expect(isInitialized).toHaveBeenCalled()
      expect(id).toHaveBeenCalledTimes(1)
      expect(epoch).toHaveBeenCalledTimes(1)
      expect(initialize).not.toHaveBeenCalled()
    })

    test('beginTransaction, commit, abort and isInTransaction move and read the manager state', async () => {
      const eosManager = transactionalManager()
      const transitions = jest.spyOn(eosManager.stateManager, 'transitionTo')
      await eosManager.initProducerId()

      await eosManager.beginTransaction()
      expect(eosManager.isInTransaction()).toBe(true)
      await eosManager.commit()
      await eosManager.beginTransaction()
      await eosManager.abort()

      expect(transitions.mock.calls.map(([state]) => state)).toEqual([
        'READY',
        'IN_TRANSACTION',
        'COMMITTING',
        'READY',
        'IN_TRANSACTION',
        'ABORTING',
        'READY',
      ])
      expect(eosManager.isInTransaction()).toBe(false)
    })

    test('addPartitionsToTransaction registers each partition with the manager once', async () => {
      const eosManager = transactionalManager()
      await eosManager.initProducerId()
      await eosManager.beginTransaction()
      const register = jest.spyOn(eosManager.stateManager, 'addPartitionToTransaction')

      await eosManager.addPartitionsToTransaction([
        { topic: 'test-1', partitions: [{ partition: 1 }, { partition: 2 }] },
      ])

      expect(register.mock.calls).toEqual([
        ['test-1', 1],
        ['test-1', 2],
      ])
    })

    test('sendOffsets marks the offsets as part of the transaction on the manager', async () => {
      const eosManager = transactionalManager()
      await eosManager.initProducerId()
      await eosManager.beginTransaction()
      const mark = jest.spyOn(eosManager.stateManager, 'markOffsetsAdded')

      await eosManager.sendOffsets({
        consumerGroupId: 'consumer-group-id',
        topics: [{ topic: 'test-topic-1', partitions: [{ partition: 0, offset: '1' }] }],
      })

      expect(mark).toHaveBeenCalledTimes(1)
      expect(eosManager.stateManager.transactionStarted()).toBe(true)
    })

    test('a producer id another call acquired during the retry backoff is kept, not replaced', async () => {
      const eosManager = transactionalManager()
      let requests = 0
      broker.initProducerId.mockImplementation(async () => {
        requests += 1
        if (requests === 1) {
          throw createErrorFromCode(51)
        }
        return { producerId: String(requests * 1000), producerEpoch: 0 }
      })

      await Promise.all([eosManager.initProducerId(), eosManager.initProducerId()])

      expect(broker.initProducerId).toHaveBeenCalledTimes(2)
      expect(eosManager.getProducerId()).toEqual('2000')
    })

    test('initProducerId on an initialized producer still acquires a new producer id after a retry', async () => {
      const eosManager = transactionalManager()
      await eosManager.initProducerId()
      broker.initProducerId.mockRejectedValueOnce(createErrorFromCode(51))
      broker.initProducerId.mockResolvedValueOnce({ producerId: '2000', producerEpoch: 0 })

      await eosManager.initProducerId()

      expect(broker.initProducerId).toHaveBeenCalledTimes(3)
      expect(eosManager.getProducerId()).toEqual('2000')
    })

    test('beginTransaction outside READY is refused before the coordinator lookup', async () => {
      const eosManager = transactionalManager()

      await expect(eosManager.beginTransaction()).rejects.toThrow(
        'Transaction state exception: Cannot call "beginTransaction" in state "UNINITIALIZED"'
      )
      expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
    })

    test('initProducerId inside a transaction is refused before any request', async () => {
      const eosManager = transactionalManager()
      await eosManager.initProducerId()
      await eosManager.beginTransaction()

      await expect(eosManager.initProducerId()).rejects.toThrow(
        'Transaction state exception: Cannot call "initProducerId" in state "IN_TRANSACTION"'
      )
      expect(broker.initProducerId).toHaveBeenCalledTimes(1)
      expect(eosManager.isInTransaction()).toBe(true)
    })

    test('commit() waits for an AddPartitionsToTxn still in flight before deciding whether to send EndTxn', async () => {
      const eosManager = transactionalManager()
      await eosManager.initProducerId()
      await eosManager.beginTransaction()
      let added
      broker.addPartitionsToTxn.mockImplementation(
        () =>
          new Promise((resolve) => {
            added = resolve
          })
      )

      const adding = eosManager.addPartitionsToTransaction([
        { topic: 'test-1', partitions: [{ partition: 1 }] },
      ])
      const committing = eosManager.commit()
      await new Promise((resolve) => setImmediate(resolve))
      added({})
      await Promise.all([adding, committing])

      expect(broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ transactionResult: true })
      )
    })

    test('an AddPartitionsToTxn that fails while commit() waits for it makes commit() reject with the armed error', async () => {
      const eosManager = transactionalManager()
      await eosManager.initProducerId()
      await eosManager.beginTransaction()
      broker.addPartitionsToTxn.mockRejectedValue(createErrorFromCode(120))

      const adding = eosManager
        .addPartitionsToTransaction([{ topic: 'test-1', partitions: [{ partition: 1 }] }])
        .catch((e) => e)
      const error = await eosManager.commit().catch((e) => e)

      expect(await adding).toMatchObject({ code: 120 })
      expect(error).toBe(eosManager.stateManager.transaction.armedError())
      expect(broker.endTxn).not.toHaveBeenCalled()
    })

    test.each([
      ['TRANSACTIONAL_ID_AUTHORIZATION_FAILED', 53],
      ['GROUP_AUTHORIZATION_FAILED', 30],
    ])(
      'an AddOffsetsToTxn refused with %s leaves the transaction unstarted: abort() sends no EndTxn',
      async (_, code) => {
        broker.negotiatedVersion = jest.fn(() => 5)
        broker.initProducerId.mockImplementation(
          async ({ producerId: id, producerEpoch: epoch }) =>
            id === -1 ? mockInitProducerIdResponse : { producerId: id, producerEpoch: epoch + 1 }
        )
        broker.addOffsetsToTxn.mockRejectedValue(createErrorFromCode(code))
        const eosManager = transactionalManager()
        await eosManager.initProducerId()
        await eosManager.beginTransaction()

        await expect(
          eosManager.sendOffsets({
            consumerGroupId: 'consumer-group-id',
            topics: [{ topic: 'test-topic-1', partitions: [{ partition: 0, offset: '1' }] }],
          })
        ).rejects.toThrow()
        expect(eosManager.stateManager.transactionStarted()).toBe(false)
        await eosManager.abort().catch(() => {})

        expect(broker.txnOffsetCommit).not.toHaveBeenCalled()
        expect(broker.endTxn).not.toHaveBeenCalled()
      }
    )

    test('isTransactional and getTransactionalId answer from the options it was built with', () => {
      expect(transactionalManager().isTransactional()).toBe(true)
      expect(transactionalManager().getTransactionalId()).toEqual(transactionalId)
      expect(create().isTransactional()).toBeFalsy()
    })

    test('a partition that joined one transaction joins the next one again', async () => {
      const eosManager = transactionalManager()
      await eosManager.initProducerId()
      const topicData = [{ topic: 'test-1', partitions: [{ partition: 1 }] }]

      await eosManager.beginTransaction()
      await eosManager.addPartitionsToTransaction(topicData)
      await eosManager.commit()
      await eosManager.beginTransaction()
      await eosManager.addPartitionsToTransaction(topicData)

      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(2)
    })

    test('CLUSTER_AUTHORIZATION_FAILED from InitProducerId is a configuration error naming both remedies, not retried', async () => {
      const eosManager = create()
      broker.initProducerId.mockImplementation(() => {
        throw createErrorFromCode(31)
      })

      const initialized = eosManager.initProducerId()

      await expect(initialized).rejects.toThrow(KafkaJSInvalidConfigurationError)
      await expect(initialized).rejects.toThrow(/IDEMPOTENT_WRITE.*idempotent: false/)
      expect(broker.initProducerId).toHaveBeenCalledTimes(1)
      expect(eosManager.isInitialized()).toBe(false)
    })

    describe('a sequence left unresolved, or a batch still in flight, at the end of a transaction', () => {
      const LEADERS = { [topic]: { 1: [0] } }
      const REQUEST_TIMED_OUT = 7

      /** A transaction (version 1) holding partition 0 of `topic`, whose one batch was sent once. */
      const sentOnce = async () => {
        broker.negotiatedVersion = jest.fn(() => 5)
        broker.initProducerId.mockImplementation(
          async ({ producerId: id, producerEpoch: epoch }) =>
            id === -1 ? mockInitProducerIdResponse : { producerId: id, producerEpoch: epoch + 1 }
        )
        const eosManager = create({ transactional: true, transactionalId, retry: { retries: 1 } })
        await eosManager.initProducerId()
        await eosManager.beginTransaction()
        await eosManager.addPartitionsToTransaction([{ topic, partitions: [{ partition: 0 }] }])
        const { stateManager } = eosManager
        const batch = {
          topic,
          partition: 0,
          messages: [{ value: 'value' }],
          acks: -1,
          timeout: 30000,
          compression: 0,
          firstSequence: null,
          producerId: null,
          producerEpoch: null,
          attempt: 0,
          settlers: [],
          backoffUntil: 0,
          createdAt: 0,
          sequenceReset: false,
        }
        stateManager.enqueue([batch])
        expect(stateManager.drain({ nodeId: 1 }, LEADERS)).toEqual([batch])
        broker.initProducerId.mockClear()
        return { eosManager, stateManager, batch }
      }

      /** The same, the batch re-sent until its retries ran out: its sequence is unresolved, nothing armed. */
      const unresolved = async () => {
        const sent = await sentOnce()
        const { stateManager, batch } = sent
        expect(stateManager.fail(batch, createErrorFromCode(REQUEST_TIMED_OUT))).toEqual(
          'RETRY_IN_ORDER'
        )
        stateManager.sequencer.requeue(batch, 0)
        expect(stateManager.drain({ nodeId: 1 }, LEADERS)).toEqual([batch])
        expect(stateManager.fail(batch, createErrorFromCode(REQUEST_TIMED_OUT))).toEqual(
          'FAIL_BATCH'
        )
        expect(stateManager.state()).toEqual('IN_TRANSACTION')
        return sent
      }

      test('commit() checks for it before EndTxn: it rejects, the transaction can only be aborted', async () => {
        const { eosManager } = await unresolved()

        const error = await eosManager.commit().catch((e) => e)

        expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(error.message).toMatch(/hasn't received acknowledgment/)
        expect(eosManager.stateManager.state()).toEqual('ABORTABLE_ERROR')
        expect(broker.endTxn).not.toHaveBeenCalled()
      })

      test('abort() checks for it before deciding on the epoch: under transaction version 1 it bumps', async () => {
        const { eosManager } = await unresolved()

        await eosManager.abort()

        expect(broker.endTxn).toHaveBeenCalledWith(
          expect.objectContaining({ transactionResult: false, transactionVersion: 1 })
        )
        expect(broker.initProducerId).toHaveBeenCalledTimes(1)
        expect(broker.initProducerId).toHaveBeenCalledWith(
          expect.objectContaining({ producerId, producerEpoch })
        )
        expect(eosManager.getProducerEpoch()).toEqual(producerEpoch + 1)
      })

      test.each(['commit', 'abort'])(
        '%s() sends no EndTxn while a batch is in flight, and the producer is unusable',
        async (end) => {
          const { eosManager } = await sentOnce()

          const error = await eosManager[end]().catch((e) => e)

          expect(error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
          expect(error.cause).toBeInstanceOf(KafkaJSInvariantViolation)
          expect(broker.endTxn).not.toHaveBeenCalled()
        }
      )
    })

    describe('an abort that ends ABORTABLE_ERROR', () => {
      /** A transaction holding a partition, armed abortable by TRANSACTION_ABORTABLE. */
      const armed = async () => {
        broker.negotiatedVersion = jest.fn(() => 5)
        broker.initProducerId.mockImplementation(
          async ({ producerId: id, producerEpoch: epoch }) =>
            id === -1 ? mockInitProducerIdResponse : { producerId: id, producerEpoch: epoch + 1 }
        )
        const eosManager = transactionalManager()
        await eosManager.initProducerId()
        await eosManager.beginTransaction()
        await eosManager.addPartitionsToTransaction([{ topic, partitions: [{ partition: 0 }] }])
        const error = createErrorFromCode(120)
        eosManager.stateManager.classify(error, 'txn')
        eosManager.stateManager.arm(error)
        expect(eosManager.stateManager.state()).toEqual('ABORTABLE_ERROR')
        broker.initProducerId.mockClear()
        return eosManager
      }

      test('sends the EndTxn abort and then bumps the epoch with the current producer id', async () => {
        const eosManager = await armed()

        await eosManager.abort()

        expect(broker.endTxn).toHaveBeenCalledWith({
          producerId,
          producerEpoch,
          transactionalId,
          transactionResult: false,
          transactionVersion: 1,
        })
        expect(broker.initProducerId).toHaveBeenCalledTimes(1)
        expect(broker.initProducerId).toHaveBeenCalledWith({
          transactionalId,
          transactionTimeout: 30000,
          producerId,
          producerEpoch,
        })
        expect(eosManager.getProducerEpoch()).toEqual(producerEpoch + 1)
        expect(eosManager.stateManager.state()).toEqual('READY')
      })

      test('retries the bump while the coordinator still answers CONCURRENT_TRANSACTIONS', async () => {
        const eosManager = await armed()
        broker.initProducerId.mockRejectedValueOnce(createErrorFromCode(51))

        await eosManager.abort()

        expect(broker.initProducerId).toHaveBeenCalledTimes(2)
        expect(eosManager.getProducerEpoch()).toEqual(producerEpoch + 1)
      })

      test('an ordinary abort bumps nothing', async () => {
        const eosManager = transactionalManager()
        await eosManager.initProducerId()
        await eosManager.beginTransaction()
        await eosManager.addPartitionsToTransaction([{ topic, partitions: [{ partition: 0 }] }])
        broker.initProducerId.mockClear()

        await eosManager.abort()

        expect(broker.endTxn).toHaveBeenCalledTimes(1)
        expect(broker.initProducerId).not.toHaveBeenCalled()
      })
    })

    describe('the retry option on a cluster that has none', () => {
      const concurrent = () => createErrorFromCode(51)

      test('bounds the InitProducerId retries on CONCURRENT_TRANSACTIONS', async () => {
        let attempts = 0
        broker.initProducerId.mockImplementation(async () => {
          attempts += 1
          // Answers beyond the budget only so a retrier that ignores it fails fast, not by timeout
          if (attempts > 3) return mockInitProducerIdResponse
          throw concurrent()
        })
        const eosManager = create({
          transactional: true,
          transactionalId,
          retry: { retries: 1, initialRetryTime: 1 },
        })

        await expect(eosManager.initProducerId()).rejects.toBeInstanceOf(
          KafkaJSNumberOfRetriesExceeded
        )

        expect(broker.initProducerId).toHaveBeenCalledTimes(2)
      })

      test('outlasts the default budget when the option is larger', async () => {
        let attempts = 0
        // 17 refusals: past the default budget of a run (5 retries) and of a test run (15)
        broker.initProducerId.mockImplementation(async () => {
          attempts += 1
          if (attempts <= 17) throw concurrent()
          return mockInitProducerIdResponse
        })
        const eosManager = create({
          transactional: true,
          transactionalId,
          retry: { retries: 20, initialRetryTime: 1, maxRetryTime: 1 },
        })

        await eosManager.initProducerId()

        expect(broker.initProducerId).toHaveBeenCalledTimes(18)
        expect(eosManager.getProducerId()).toEqual(producerId)
      })

      test('bounds the InitProducerId retries by maxTotalTime', async () => {
        let attempts = 0
        broker.initProducerId.mockImplementation(async () => {
          attempts += 1
          // Answers beyond the deadline only so a retrier that ignores it fails fast
          if (attempts > 3) return mockInitProducerIdResponse
          throw concurrent()
        })
        // The first wait (about 100 ms) would end past the 50 ms deadline
        const eosManager = create({
          transactional: true,
          transactionalId,
          retry: { retries: 20, initialRetryTime: 100, maxTotalTime: 50 },
        })

        await expect(eosManager.initProducerId()).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)

        expect(broker.initProducerId).toHaveBeenCalledTimes(1)
      })

      test('outlasts the default budget on AddOffsetsToTxn when the option is larger', async () => {
        const eosManager = create({
          transactional: true,
          transactionalId,
          retry: { retries: 20, initialRetryTime: 1, maxRetryTime: 1 },
        })
        await eosManager.initProducerId()
        await eosManager.beginTransaction()
        let attempts = 0
        broker.addOffsetsToTxn.mockImplementation(async () => {
          attempts += 1
          if (attempts <= 17) throw concurrent()
        })

        await eosManager.sendOffsets({
          consumerGroupId: 'consumer-group-id',
          topics: [{ topic, partitions: [{ partition: 0, offset: '0' }] }],
        })

        expect(broker.addOffsetsToTxn).toHaveBeenCalledTimes(18)
        expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
      })
    })
  })
})

describe('Epoch bump failure at transaction boundaries', () => {
  const { createFakeCluster } = require('../__tests__/fixtures/fakeCluster')
  /** Builds a manager that can bump its identity. @returns {Promise<object>} manager and broker @throws {Error} on initialization. */
  const fixture = async () => {
    const cluster = createFakeCluster()
    const broker = cluster.broker
    broker.negotiatedVersion = () => 5
    broker.initProducerId.mockImplementation(async ({ producerEpoch }) => ({
      producerId: '1000',
      producerEpoch: producerEpoch + 1,
    }))
    const manager = createEosManager({
      cluster,
      logger: newLogger(),
      transactional: true,
      transactionalId: 'bump',
      retry: { retries: 0 },
    })
    await manager.initProducerId()
    await manager.beginTransaction()
    return { manager, broker }
  }
  test('abort resolves a nonfatal bump failure and the next begin retries the owed bump', async () => {
    const { manager, broker } = await fixture()
    const failure = createErrorFromCode(7)
    broker.initProducerId.mockRejectedValueOnce(failure)
    await expect(manager.abort({ droppedSequences: true })).resolves.toBeUndefined()
    expect(manager.stateManager.state()).toBe('READY')
    expect(broker.initProducerId).toHaveBeenCalledTimes(2)
    await manager.beginTransaction()
    expect(broker.initProducerId).toHaveBeenCalledTimes(3)
    expect(manager.getProducerEpoch()).toBe(1)
    await manager.abort()
  })
  test('begin reports the same nonfatal failure and keeps the bump owed', async () => {
    const { manager, broker } = await fixture()
    const failure = createErrorFromCode(7)
    broker.initProducerId.mockRejectedValue(failure)
    await manager.abort({ droppedSequences: true })
    await expect(manager.beginTransaction()).rejects.toMatchObject({ code: 7 })
    expect(manager.stateManager.state()).toBe('READY')
    broker.initProducerId.mockImplementation(async () => ({ producerId: '1000', producerEpoch: 1 }))
    await manager.beginTransaction()
    expect(manager.getProducerEpoch()).toBe(1)
    await manager.abort()
  })
  test.each(['abort', 'begin'])('a fatal bump failure rejects at %s', async (boundary) => {
    const { manager, broker } = await fixture()
    if (boundary === 'begin') {
      broker.initProducerId.mockRejectedValueOnce(createErrorFromCode(7))
      await manager.abort({ droppedSequences: true })
    }
    broker.initProducerId.mockRejectedValue(createErrorFromCode(47))
    await expect(
      boundary === 'abort' ? manager.abort({ droppedSequences: true }) : manager.beginTransaction()
    ).rejects.toMatchObject({
      name: boundary === 'abort' ? 'KafkaJSApplicationRecoverableError' : 'KafkaJSProtocolError',
      category: 'applicationRecoverable',
    })
    expect(manager.stateManager.state()).toBe('FATAL_ERROR')
    await expect(manager.beginTransaction()).rejects.toMatchObject({
      name: 'KafkaJSApplicationRecoverableError',
    })
  })
})

describe('Initialization whose lifecycle closes after its retry succeeds', () => {
  const { createFakeCluster } = require('../__tests__/fixtures/fakeCluster')

  test('rejects with the fatal error and is not made ready', async () => {
    const generation = new Generation()
    const manager = createEosManager({
      cluster: createFakeCluster(),
      logger: newLogger(),
      getGeneration: () => generation,
    })
    const fatal = new KafkaJSApplicationRecoverableError('boundary fence', {
      cause: createErrorFromCode(47),
    })
    const finish = FencedRetry.prototype.finish
    const spy = jest
      .spyOn(FencedRetry.prototype, 'finish')
      .mockImplementation(function (error, value) {
        const succeeds = !this.done && !error && this.configs.fence === generation
        const result = finish.call(this, error, value)
        if (succeeds) {
          manager.stateManager.arm(fatal)
          generation.close(fatal)
        }
        return result
      })
    try {
      await expect(manager.initProducerId()).rejects.toBe(fatal)
      expect(manager.stateManager.state()).toBe('FATAL_ERROR')
    } finally {
      spy.mockRestore()
    }
  })
})

describe('The transaction coordinator lookup of a begin and of InitProducerId', () => {
  const { createFakeCluster } = require('../__tests__/fixtures/fakeCluster')
  const RETRY = { retries: 4, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
  const reset = () =>
    new KafkaJSConnectionError('Connection error: read ECONNRESET', { code: 'ECONNRESET' })
  const settle = () => new Promise((resolve) => setImmediate(resolve))
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

  /**
   * A transactional manager over the fake cluster, its producer id acquired when `begun` is set,
   * and its lookups counted from there.
   * @param {object} [options] manager options laid over the test's
   * @param {boolean} [options.begun=false] whether to initialize first
   * @returns {Promise<{ manager: object, cluster: object, broker: object }>}
   */
  const build = async ({ begun = false, ...options } = {}) => {
    const cluster = createFakeCluster()
    const { broker } = cluster
    const manager = createEosManager({
      cluster,
      logger: newLogger(),
      transactional: true,
      transactionalId: 'lookup',
      retry: RETRY,
      ...options,
    })
    if (begun) await manager.initProducerId()
    cluster.findGroupCoordinator.mockClear()
    broker.initProducerId.mockClear()
    return { manager, cluster, broker }
  }

  /** @param {object} cluster @param {number} count @param {() => Error} [error] @returns {void} */
  const failLookups = (cluster, count, error = reset) => {
    for (let i = 0; i < count; i++) cluster.findGroupCoordinator.mockRejectedValueOnce(error())
  }

  const ENTRIES = [
    ['beginTransaction', { begun: true }],
    ['initProducerId', {}],
  ]

  describe.each(ENTRIES)('%s', (method, options) => {
    test.each([1, 4])('rides %i connection resets within the retry budget', async (count) => {
      const { manager, cluster, broker } = await build(options)
      failLookups(cluster, count)

      await expect(manager[method]()).resolves.toBeUndefined()

      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(count + 1)
      expect(broker.initProducerId).toHaveBeenCalledTimes(method === 'initProducerId' ? 1 : 0)
      expect(manager.stateManager.state()).toBe(
        method === 'initProducerId' ? 'READY' : 'IN_TRANSACTION'
      )
    })

    test('asks for the coordinator once when nothing fails', async () => {
      const { manager, cluster } = await build(options)

      await manager[method]()

      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    })

    test('rejects with the spent budget over the retriable error when the resets outlast it', async () => {
      const { manager, cluster, broker } = await build({
        ...options,
        retry: { ...RETRY, retries: 2 },
      })
      cluster.findGroupCoordinator.mockImplementation(async () => {
        throw reset()
      })

      const error = await manager[method]().catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause).toMatchObject({ code: 'ECONNRESET', retriable: true })
      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(3)
      expect(broker.initProducerId).not.toHaveBeenCalled()
      expect(manager.stateManager.state()).toBe(options.begun ? 'READY' : 'UNINITIALIZED')
    })

    test.each([
      ['TRANSACTIONAL_ID_AUTHORIZATION_FAILED', () => createErrorFromCode(53)],
      ['a TypeError', () => new TypeError('boom')],
      ['a coordinator that was not found', () => new KafkaJSGroupCoordinatorNotFound('none')],
    ])('rejects at once, as before, on %s', async (_name, make) => {
      const { manager, cluster, broker } = await build(options)
      const error = make()
      cluster.findGroupCoordinator.mockRejectedValueOnce(error)

      await expect(manager[method]()).rejects.toBe(error)

      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
      expect(broker.initProducerId).not.toHaveBeenCalled()
    })

    test('rejects at once, as it is, on an error that is not retriable after a reset', async () => {
      const { manager, cluster, broker } = await build(options)
      const refused = createErrorFromCode(53)
      cluster.findGroupCoordinator.mockRejectedValueOnce(reset()).mockRejectedValueOnce(refused)

      await expect(manager[method]()).rejects.toBe(refused)

      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
      expect(broker.initProducerId).not.toHaveBeenCalled()
    })

    test('stops retrying when the producer lifecycle closes meanwhile, and writes nothing late', async () => {
      const generation = new Generation()
      const { manager, cluster, broker } = await build({
        ...options,
        getGeneration: () => generation,
        retry: { ...RETRY, retries: 10, initialRetryTime: 40, maxRetryTime: 40 },
      })
      cluster.findGroupCoordinator.mockImplementation(async () => {
        throw reset()
      })

      const outcome = manager[method]().catch((e) => e)
      await settle()
      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
      const closed = new KafkaJSProducerClosedError('The producer was closed', {
        outcome: 'unknown',
      })
      generation.close(closed)

      expect(await outcome).toBe(closed)
      await sleep(120)
      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
      expect(broker.initProducerId).not.toHaveBeenCalled()
      expect(manager.stateManager.state()).toBe(options.begun ? 'READY' : 'UNINITIALIZED')
    })
  })

  test('a begin does not outlive a producer made unusable while it retries', async () => {
    const generation = new Generation()
    const { manager, cluster } = await build({
      begun: true,
      getGeneration: () => generation,
      retry: { ...RETRY, retries: 10, initialRetryTime: 40, maxRetryTime: 40 },
    })
    cluster.findGroupCoordinator.mockImplementation(async () => {
      throw reset()
    })
    const fatal = new KafkaJSApplicationRecoverableError('fenced', {
      cause: createErrorFromCode(90),
    })

    const outcome = manager.beginTransaction().catch((e) => e)
    await settle()
    manager.stateManager.arm(fatal)
    generation.close(fatal)

    expect(await outcome).toBe(fatal)
    await sleep(120)
    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    expect(manager.stateManager.state()).toBe('FATAL_ERROR')
  })

  test('the lookup of an owed epoch bump rides a reset too', async () => {
    const { manager, cluster, broker } = await build({ begun: true })
    broker.negotiatedVersion = () => 5
    broker.initProducerId.mockImplementation(async ({ producerEpoch }) => ({
      producerId: '1000',
      producerEpoch: producerEpoch + 1,
    }))
    await manager.beginTransaction()
    broker.initProducerId.mockRejectedValueOnce(createErrorFromCode(7))
    await manager.abort({ droppedSequences: true })
    cluster.findGroupCoordinator.mockClear()
    cluster.findGroupCoordinator
      .mockResolvedValueOnce(broker)
      .mockRejectedValueOnce(reset())
      .mockResolvedValueOnce(broker)

    await expect(manager.beginTransaction()).resolves.toBeUndefined()

    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(3)
    expect(manager.stateManager.state()).toBe('IN_TRANSACTION')
  })
})
