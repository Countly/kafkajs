const { newLogger } = require('testHelpers')
const createEosManager = require('.')
const { KafkaJSNonRetriableError, KafkaJSLockTimeout } = require('../../errors')
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
    expect(broker.initProducerId).toHaveBeenCalledWith({ transactionTimeout: 30000 })

    expect(eosManager.getProducerId()).toEqual(mockInitProducerIdResponse.producerId)
    expect(eosManager.getProducerEpoch()).toEqual(mockInitProducerIdResponse.producerEpoch)
    expect(eosManager.isInitialized()).toEqual(true)
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

      expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()
      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      expect(broker.initProducerId).toHaveBeenCalledWith({
        transactionalId,
        transactionTimeout: 30000,
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
      eosManager.beginTransaction()

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

    test(`beginTransaction throws`, async () => {
      expect(() => eosManager.beginTransaction()).toThrow(
        new KafkaJSNonRetriableError('Method unavailable if non-transactional')
      )
    })

    testTransactionalGuardAsync('addPartitionsToTransaction')
    testTransactionalGuardAsync('sendOffsets')
    testTransactionalGuardAsync('commit')
    testTransactionalGuardAsync('abort')
  })

  describe('broker mutex instrumentation', () => {
    it('passes description/instrumentationEmitter through to the per-broker Lock, which emits on timeout', async () => {
      jest.useFakeTimers()

      try {
        const emit = jest.fn()
        const lockedBroker = { nodeId: 3, brokerAddress: '10.0.1.4:9092' }
        const eosManager = createEosManager({
          logger: newLogger(),
          cluster,
          instrumentationEmitter: { emit },
        })

        await eosManager.initProducerId()
        await eosManager.acquireBrokerLock(lockedBroker) // first caller acquires immediately

        const secondAcquire = eosManager.acquireBrokerLock(lockedBroker) // queues behind the first
        jest.advanceTimersByTime(0xffff)

        await expect(secondAcquire).rejects.toThrow(KafkaJSLockTimeout)

        expect(emit).toHaveBeenCalledTimes(1)
        const [eventName, payload] = emit.mock.calls[0]
        expect(eventName).toEqual(expect.stringContaining('lock_timeout'))
        expect(payload).toMatchObject({
          description: lockedBroker.brokerAddress,
          waiting: 1,
          timeout: 0xffff,
        })
      } finally {
        jest.useRealTimers()
      }
    })
  })
})
