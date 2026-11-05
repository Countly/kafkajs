const createAdmin = require('..')
const { KafkaJSInvalidConfigurationError, KafkaJSInvariantViolation } = require('../../errors')

const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const COMPONENTS = [{ entityType: 'user', matchType: 'any' }]
const user = { entityType: 'user', entityName: 'alice' }
const client = { entityType: 'client-id', entityName: 'app' }
const rate = { key: 'producer_byte_rate', value: 100 }

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

/** An admin whose broker answers every describe with `answer`. */
const adminAnswering = (answer) => {
  const broker = { describeClientQuotas: jest.fn(async () => answer) }
  const cluster = { findConnectedBroker: jest.fn(async () => broker) }
  return { broker, cluster, admin: createAdmin({ cluster, logger, retry }) }
}

describe('client quota edges', () => {
  test.each([
    ['describeClientQuotas', 'Invalid components'],
    ['alterClientQuotas', 'Invalid entries'],
  ])('%s without options names the missing field', async (method, message) => {
    const { admin, cluster } = adminAnswering({ entries: [] })

    const error = await rejection(admin[method]())

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(error.message).toContain(message)
    expect(cluster.findConnectedBroker).not.toHaveBeenCalled()
  })

  test('an answer without entries describes no quotas', async () => {
    const { admin } = adminAnswering({ entries: null })

    await expect(admin.describeClientQuotas({ components: COMPONENTS })).resolves.toEqual([])
  })

  test('rejects one entity repeated with its components in another order', async () => {
    const { admin } = adminAnswering({
      entries: [
        { entity: [user, client], values: [rate] },
        { entity: [client, user], values: [rate] },
      ],
    })

    const error = await rejection(admin.describeClientQuotas({ components: COMPONENTS }))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Duplicate quota entity')
  })

  test('rejects one quota key repeated within an entity', async () => {
    const { admin } = adminAnswering({ entries: [{ entity: [user], values: [rate, rate] }] })

    const error = await rejection(admin.describeClientQuotas({ components: COMPONENTS }))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Duplicate quota key')
  })

  test('a retriable failure of the read is retried and values become a plain object', async () => {
    const { admin, cluster } = adminAnswering({ entries: [{ entity: [user], values: [rate] }] })
    cluster.findConnectedBroker.mockRejectedValueOnce(
      Object.assign(new Error('flap'), { retriable: true })
    )

    const result = await admin.describeClientQuotas({ components: COMPONENTS })

    expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(2)
    expect(result).toEqual([{ entity: [user], values: { producer_byte_rate: 100 } }])
  })
})
