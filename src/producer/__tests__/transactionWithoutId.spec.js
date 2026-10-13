const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { KafkaJSNonRetriableError } = require('../../errors')
const { newLogger } = require('testHelpers')

describe('Producer > transaction() without a transactional id', () => {
  it('rejects, naming what is missing, and starts nothing', async () => {
    const cluster = createFakeCluster()
    const producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()

    const error = await producer.transaction().catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toMatch('Must provide transactional id for transactional producer')
    expect(cluster.broker.addPartitionsToTxn).not.toHaveBeenCalled()
  })
})
