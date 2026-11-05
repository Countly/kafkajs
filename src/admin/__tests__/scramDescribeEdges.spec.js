const createAdmin = require('..')
const { KafkaJSInvalidConfigurationError, KafkaJSInvariantViolation } = require('../../errors')

const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const SHA256 = 1

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

const userRow = (user, credentialInfos = [{ mechanism: SHA256, iterations: 4096 }]) => ({
  user,
  errorCode: 0,
  credentialInfos,
})

/** An admin whose broker answers every describe with `results`. */
const adminAnswering = (results) => {
  const broker = { describeUserScramCredentials: jest.fn(async () => ({ results })) }
  const cluster = { findConnectedBroker: jest.fn(async () => broker) }
  return { broker, cluster, admin: createAdmin({ cluster, logger, retry }) }
}

describe('SCRAM credential description edges', () => {
  test('a non-retriable failure of the all-users read is not retried', async () => {
    const denied = Object.assign(new Error('denied'), { retriable: false })
    const cluster = { findConnectedBroker: jest.fn().mockRejectedValue(denied) }

    const error = await rejection(
      createAdmin({ cluster, logger, retry }).describeUserScramCredentials()
    )

    expect(error).toBe(denied)
    expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(1)
  })

  test('a retriable failure of the all-users read is retried', async () => {
    const flap = Object.assign(new Error('flap'), { retriable: true })
    const { cluster, admin } = adminAnswering([userRow('alice')])
    cluster.findConnectedBroker.mockRejectedValueOnce(flap)

    const result = await admin.describeUserScramCredentials()

    expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(2)
    expect(result).toEqual([
      {
        user: 'alice',
        errorCode: 0,
        errorMessage: null,
        error: null,
        credentials: [{ mechanism: 'SCRAM-SHA-256', iterations: 4096 }],
      },
    ])
  })

  test.each([
    ['results that are not an array', undefined],
    ['an empty user name', [userRow('')]],
    ['a repeated user', [userRow('alice'), userRow('alice')]],
  ])('rejects an all-users answer with %s', async (_label, results) => {
    const { admin } = adminAnswering(results)

    const error = await rejection(admin.describeUserScramCredentials())

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid SCRAM result identity')
  })

  test.each([
    ['a null row', [null]],
    ['a row without credential infos', [userRow('alice', null)]],
  ])('rejects a named-user answer with %s', async (_label, results) => {
    const { admin } = adminAnswering(results)

    const error = await rejection(admin.describeUserScramCredentials({ users: ['alice'] }))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid SCRAM credentials')
  })

  test.each([
    ['an unknown mechanism', [{ mechanism: 99, iterations: 4096 }]],
    [
      'a mechanism listed twice',
      [
        { mechanism: SHA256, iterations: 4096 },
        { mechanism: SHA256, iterations: 8192 },
      ],
    ],
  ])('rejects a named-user answer with %s', async (_label, credentialInfos) => {
    const { admin } = adminAnswering([userRow('alice', credentialInfos)])

    const error = await rejection(admin.describeUserScramCredentials({ users: ['alice'] }))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid SCRAM mechanism')
  })
})

describe('SCRAM credential alteration edges', () => {
  test('called without options it names the missing credential changes', async () => {
    const cluster = { refreshMetadata: jest.fn(), findControllerBroker: jest.fn() }

    const error = await rejection(
      createAdmin({ cluster, logger, retry }).alterUserScramCredentials()
    )

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(error.message).toContain('Invalid credential changes')
    expect(cluster.findControllerBroker).not.toHaveBeenCalled()
  })
})
