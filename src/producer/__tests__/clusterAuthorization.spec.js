const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const { newLogger } = require('testHelpers')

const CLUSTER_AUTHORIZATION_FAILED = 31

/** A producer whose brokers refuse InitProducerId for want of the ACL. */
const refusedProducer = (options = {}) => {
  const cluster = createFakeCluster()
  cluster.broker.initProducerId.mockRejectedValue(createErrorFromCode(CLUSTER_AUTHORIZATION_FAILED))
  return createProducer({ cluster, logger: newLogger(), ...options })
}

/** The error the first send of a producer ends with. */
const sendError = async (producer) => {
  await producer.connect().catch(() => {})
  return producer.send({ topic: 'topic-a', messages: [{ value: 'v' }] }).catch((e) => e)
}

describe('Producer > InitProducerId refused with CLUSTER_AUTHORIZATION_FAILED', () => {
  it('tells an idempotent producer to grant the ACL or turn idempotence off', async () => {
    const error = await sendError(refusedProducer())

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(error.message).toMatch(/IDEMPOTENT_WRITE on the cluster/)
    expect(error.message).toMatch(/WRITE on any topic, from Kafka 2.8/)
    expect(error.message).toMatch(/idempotent: false/)
  })

  it('treats an empty transactional id as none, so idempotence can still be turned off', async () => {
    const error = await sendError(refusedProducer({ transactionalId: '' }))

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(error.message).toMatch(/IDEMPOTENT_WRITE on the cluster/)
    expect(error.message).toMatch(/idempotent: false/)
  })

  it('tells a transactional producer only which ACL to grant, never to turn idempotence off', async () => {
    const error = await sendError(refusedProducer({ transactionalId: 'transactional-a' }))

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(error.message).toMatch(/IDEMPOTENT_WRITE on the cluster/)
    expect(error.message).toMatch(/WRITE on any topic, from Kafka 2.8/)
    expect(error.message).not.toMatch(/idempotent: false/)
  })
})
