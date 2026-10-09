const { newLogger } = require('testHelpers')
const createEosManager = require('.')
const ProducerStateManager = require('../state')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('../../errors')
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
      refreshMetadataIfNecessary: jest.fn(),
      findGroupCoordinator: jest.fn().mockReturnValue(broker),
      findControllerBroker: jest.fn().mockReturnValue(broker),
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

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()
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
    '%s from initProducerId reaches the caller unchanged, with no retry loop',
    async (type, code) => {
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster,
        transactionTimeout: 30000,
      })

      broker.initProducerId.mockImplementation(() => {
        throw createErrorFromCode(code)
      })

      await expect(eosManager.initProducerId()).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        type,
        code,
        retriable: false,
      })

      expect(broker.initProducerId).toHaveBeenCalledTimes(1)
    }
  )

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

      expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()
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
  })
})
