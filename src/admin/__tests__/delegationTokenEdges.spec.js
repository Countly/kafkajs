const createAdmin = require('..')
const { KafkaJSInvalidConfigurationError, KafkaJSInvariantViolation } = require('../../errors')

const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const hmac = Buffer.from('hmac-marker-only-test')

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

const wireToken = (overrides = {}) => ({
  tokenId: 'token-1',
  hmac,
  principalType: 'User',
  principalName: 'owner',
  issueTimestampMs: '100',
  expiryTimestampMs: '200',
  maxTimestampMs: '300',
  ...overrides,
})

/** An admin whose one broker answers each token call with the given wire answers. */
const adminOver = (answers) => {
  const broker = Object.fromEntries(
    Object.entries(answers).map(([method, answer]) => [method, jest.fn(async () => answer)])
  )
  const cluster = { findConnectedBroker: jest.fn(async () => broker) }
  return { broker, cluster, admin: createAdmin({ cluster, logger, retry }) }
}

describe('delegation token edges', () => {
  test('createDelegationToken without options asks for the broker defaults', async () => {
    const { admin, broker } = adminOver({ createDelegationToken: wireToken({ renewers: [] }) })

    const token = await admin.createDelegationToken()

    expect(broker.createDelegationToken).toHaveBeenCalledWith({
      renewers: [],
      maxLifetimeMs: '-1',
      ownerPrincipalType: null,
      ownerPrincipalName: null,
    })
    expect(token).toEqual({
      tokenId: 'token-1',
      hmac,
      owner: { principalType: 'User', principalName: 'owner' },
      renewers: [],
      issueTimestampMs: '100',
      expiryTimestampMs: '200',
      maxTimestampMs: '300',
    })
  })

  test.each([
    ['an empty hmac', { hmac: Buffer.alloc(0) }],
    ['a text hmac', { hmac: 'not-a-buffer' }],
    ['a missing token id', { tokenId: undefined }],
    ['an empty token id', { tokenId: '' }],
  ])('rejects a broker token with %s', async (_label, overrides) => {
    const { admin } = adminOver({ createDelegationToken: wireToken(overrides) })

    const error = await rejection(admin.createDelegationToken())

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid token result')
  })

  test.each(['renewDelegationToken', 'expireDelegationToken'])(
    '%s without options names the missing hmac and sends nothing',
    async (method) => {
      const { admin, cluster } = adminOver({})

      const error = await rejection(admin[method]())

      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.message).toContain('Invalid hmac')
      expect(cluster.findConnectedBroker).not.toHaveBeenCalled()
    }
  )

  test('rejects two described tokens with one id', async () => {
    const { admin } = adminOver({
      describeDelegationToken: { tokens: [wireToken(), wireToken({ principalName: 'other' })] },
    })

    const error = await rejection(admin.describeDelegationToken())

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Duplicate token result')
  })

  test('a retriable failure to reach a broker is retried', async () => {
    const { admin, cluster } = adminOver({ describeDelegationToken: { tokens: [wireToken()] } })
    cluster.findConnectedBroker.mockRejectedValueOnce(
      Object.assign(new Error('flap'), { retriable: true })
    )

    const tokens = await admin.describeDelegationToken()

    expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(2)
    expect(tokens.map((token) => token.tokenId)).toEqual(['token-1'])
  })
})
