const { itemRetry } = require('../itemRetry')
const { KafkaJSInvariantViolation } = require('../../errors')

const options = (overrides = {}) => ({
  items: [{ name: 'a' }],
  keyOf: (item) => item.name,
  retry: { retries: 0 },
  retryable: () => false,
  callFailure: () => false,
  sanitize: (message) => message,
  ...overrides,
})

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

describe('admin item retry contracts', () => {
  test('rejects two caller cells with one identity before sending anything', async () => {
    const send = jest.fn(async () => [])

    const error = await rejection(
      itemRetry(options({ items: [{ name: 'a' }, { name: 'a' }] }), send)
    )

    expect(error.message).toBe('Duplicate item retry keys')
    expect(send).not.toHaveBeenCalled()
  })

  test('rejects a sender that answers with something other than an array of rows', async () => {
    const error = await rejection(itemRetry(options(), async () => ({ name: 'a', errorCode: 0 })))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Missing item results')
  })

  test('records a null message when the sender throws a value that has none', async () => {
    const thrown = { code: 'ECONNRESET' }

    const results = await itemRetry(options(), async () => {
      throw thrown
    })

    expect(results).toEqual([{ name: 'a', errorCode: -1, error: thrown, errorMessage: null }])
  })
})
