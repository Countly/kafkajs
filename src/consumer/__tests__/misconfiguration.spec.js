const createConsumer = require('../index')
const { KafkaJSNonRetriableError } = require('../../errors')

const { createCluster, newLogger } = require('testHelpers')

describe('Consumer', () => {
  it('throws when heartbeatInterval is lower or equal to sessionTimeout', () => {
    const errorMessage =
      'Consumer heartbeatInterval (10000) must be lower than sessionTimeout (10000). It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.'

    expect(() =>
      createConsumer({
        cluster: createCluster(),
        logger: newLogger(),
        groupId: 'test-group-id',
        heartbeatInterval: 10000,
        sessionTimeout: 10000,
      })
    ).toThrowWithMessage(KafkaJSNonRetriableError, errorMessage)
  })

  it.each([null, '', 1])('throws when groupId is %p', (groupId) => {
    const errorMessage = 'Consumer groupId must be a non-empty string.'

    expect(() =>
      createConsumer({
        cluster: createCluster(),
        logger: newLogger(),
        groupId,
      })
    ).toThrowWithMessage(KafkaJSNonRetriableError, errorMessage)
  })

  it('accepts a missing groupId for a consumer that only assigns partitions', () => {
    expect(() => createConsumer({ cluster: createCluster(), logger: newLogger() })).not.toThrow()
  })
})
