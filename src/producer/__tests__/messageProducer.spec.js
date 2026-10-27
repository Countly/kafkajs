const createMessageProducer = require('../messageProducer')
const createEosManager = require('../eosManager')
const Idempotence = require('../idempotence')
const { DefaultPartitioner } = require('../partitioners')
const { Admission } = require('../state/admission')
const { RequestBudget } = require('../state/recordSize')
const createRetry = require('../../retry')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CONNECTION_STATUS } = require('../../network/connectionStatus')
const { KafkaJSError, KafkaJSNonRetriableError } = require('../../errors')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const MESSAGES = [{ key: 'key-0', value: 'value-0' }]
const LINGER = 20

/**
 * A message producer of a producer that is not idempotent and has no lifecycle getter, over the
 * fake cluster; the case moves its connection status through `state`.
 * @returns {{ messageProducer: ReturnType<typeof createMessageProducer>, cluster: object,
 *   admission: Admission, state: { status: string } }}
 */
const createWiring = () => {
  const logger = newLogger()
  const cluster = createFakeCluster()
  const admission = new Admission({ capacity: 1048576, maxBlockMs: 1000 })
  const state = { status: CONNECTION_STATUS.CONNECTED }
  const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 2 }
  const messageProducer = createMessageProducer({
    logger,
    cluster,
    partitioner: DefaultPartitioner(),
    eosManager: createEosManager({ logger, cluster, transactional: false, retry }),
    idempotence: new Idempotence({ enabled: false, explicit: true, logger }),
    retrier: createRetry(retry),
    retry,
    linger: LINGER,
    batchSize: 16384,
    admission,
    budget: new RequestBudget(),
    getConnectionStatus: () => state.status,
    isClosing: () => false,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    INITIALIZED: 'producer.initialized',
  })
  return { messageProducer, cluster, admission, state }
}

describe('Producer > message producer > batches that reach the send path', () => {
  test('a send of a connected producer with no lifecycle getter reaches the broker', async () => {
    const { messageProducer, cluster } = createWiring()

    await expect(messageProducer.ensureInitialized()).resolves.toBeUndefined()
    await expect(messageProducer.send({ topic: TOPIC, messages: MESSAGES })).resolves.toEqual([
      expect.objectContaining({ topicName: TOPIC, partition: 0, errorCode: 0 }),
    ])

    expect(cluster.broker.produce).toHaveBeenCalledTimes(1)
    expect(cluster.broker.initProducerId).not.toHaveBeenCalled()
  })

  test.each([
    [
      CONNECTION_STATUS.DISCONNECTING,
      KafkaJSNonRetriableError,
      "The producer is disconnecting; therefore, it can't safely accept messages anymore",
    ],
    [CONNECTION_STATUS.DISCONNECTED, KafkaJSError, 'The producer is disconnected'],
  ])(
    'a non-idempotent send accepted while connected is refused, unsent, once the producer is %s',
    async (status, errorClass, message) => {
      const { messageProducer, cluster, admission, state } = createWiring()
      const sent = messageProducer.send({ topic: TOPIC, messages: MESSAGES })
      state.status = status

      const error = await sent.catch((thrown) => thrown)

      expect(error).toBeInstanceOf(errorClass)
      expect(error.message).toBe(message)
      expect(cluster.broker.produce).not.toHaveBeenCalled()
      expect(admission.reserved).toBe(0)
    }
  )
})
