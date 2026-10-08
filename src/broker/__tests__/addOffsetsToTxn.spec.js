const Broker = require('../index')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { secureRandom, createConnectionPool, newLogger, retryProtocol } = require('testHelpers')
const { KafkaJSProtocolError } = require('../../errors')

describe('Broker > AddOffsetsToTxn', () => {
  let broker, seedBroker, transactionalId, producerId, producerEpoch, consumerGroupId

  beforeEach(async () => {
    transactionalId = `transactional-id-${secureRandom()}`
    consumerGroupId = `group-id-${secureRandom()}`

    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })

    await seedBroker.connect()

    const {
      coordinator: { host, port },
    } = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () =>
        await seedBroker.findGroupCoordinator({
          groupId: transactionalId,
          coordinatorType: COORDINATOR_TYPES.TRANSACTION,
        })
    )

    broker = new Broker({
      connectionPool: createConnectionPool({ host, port }),
      logger: newLogger(),
    })

    await broker.connect()
    // The transaction coordinator can still be loading __transaction_state on a cold 4.x cluster
    // (codes 14/16, which kafkajs still names GROUP_LOAD_IN_PROGRESS / NOT_COORDINATOR_FOR_GROUP).
    const result = await retryProtocol('GROUP_LOAD_IN_PROGRESS', () =>
      retryProtocol('NOT_COORDINATOR_FOR_GROUP', () =>
        broker.initProducerId({
          transactionalId,
          transactionTimeout: 30000,
        })
      )
    )

    producerId = result.producerId
    producerEpoch = result.producerEpoch
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    const result = await broker.addOffsetsToTxn({
      transactionalId,
      producerId,
      producerEpoch,
      groupId: consumerGroupId,
    })

    expect(result).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
    })
  })

  test('throws for incorrect producer id', async () => {
    await expect(
      broker.addOffsetsToTxn({
        transactionalId,
        producerId: 12345,
        producerEpoch,
        groupId: consumerGroupId,
      })
    ).rejects.toEqual(
      new KafkaJSProtocolError(
        'The producer attempted to use a producer id which is not currently assigned to its transactional id'
      )
    )
  })

  test('throws PRODUCER_FENCED for a producer epoch the coordinator does not hold', async () => {
    // From AddOffsetsToTxn v2 the broker answers PRODUCER_FENCED here; it rewrites that to
    // INVALID_PRODUCER_EPOCH only for v0-v1 requests (handleAddOffsetsToTxnRequest,
    // https://github.com/apache/kafka/blob/4.3/core/src/main/scala/kafka/server/KafkaApis.scala).
    await expect(
      broker.addOffsetsToTxn({
        transactionalId,
        producerId,
        producerEpoch: producerEpoch + 1,
        groupId: consumerGroupId,
      })
    ).rejects.toMatchObject({ name: 'KafkaJSProtocolError', type: 'PRODUCER_FENCED', code: 90 })
  })
})
