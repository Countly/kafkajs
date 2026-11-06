const createAdmin = require('..')
const { createErrorFromCode } = require('../../protocol/error')

const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const first = '00112233-4455-6677-8899-aabbccddeeff'
const second = '10112233-4455-6677-8899-aabbccddeeff'

const fixture = () => {
  const broker = { deleteTopics: jest.fn(async () => {}) }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => broker),
    targetTopics: new Set(),
  }
  const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
  return { broker, cluster, admin: createAdmin({ cluster, logger, retry }) }
}

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

describe('delete topics by id retries', () => {
  test('a NOT_CONTROLLER answer refreshes metadata and sends the same ids again', async () => {
    const { admin, broker, cluster } = fixture()
    broker.deleteTopics.mockRejectedValueOnce(createErrorFromCode(41))

    await admin.deleteTopics({ topicIds: [first] })

    expect(broker.deleteTopics.mock.calls.map(([request]) => request.topicIds)).toEqual([
      [first],
      [first],
    ])
    // each attempt refreshes first, the NOT_CONTROLLER answer refreshes again, and one follows the delete
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(4)
  })

  test('a quota refusal that names no pending id is not retried', async () => {
    const { admin, broker } = fixture()
    const refusal = Object.assign(createErrorFromCode(89), {
      topicErrors: [{ topic: null, topicId: second, errorCode: 89 }],
    })
    broker.deleteTopics.mockRejectedValue(refusal)

    const error = await rejection(admin.deleteTopics({ topicIds: [first] }))

    expect(error).toBe(refusal)
    expect(broker.deleteTopics).toHaveBeenCalledTimes(1)
  })

  test('a NOT_CONTROLLER answer to the refresh after the delete is retried', async () => {
    const { admin, broker, cluster } = fixture()
    const calls = []
    cluster.refreshMetadata
      .mockImplementationOnce(async () => {
        calls.push('refresh')
      })
      .mockImplementationOnce(async () => {
        calls.push('refresh failed')
        throw createErrorFromCode(41)
      })
      .mockImplementation(async () => {
        calls.push('refresh')
      })
    broker.deleteTopics.mockImplementation(async () => {
      calls.push('delete')
    })

    await admin.deleteTopics({ topicIds: [first] })

    expect(calls).toEqual(['refresh', 'delete', 'refresh failed', 'refresh'])
    expect(broker.deleteTopics.mock.calls.map(([request]) => request.topicIds)).toEqual([[first]])
  })

  test('any other failure of the refresh after the delete is not retried', async () => {
    const { admin, broker, cluster } = fixture()
    const denied = createErrorFromCode(31)
    cluster.refreshMetadata.mockResolvedValueOnce(undefined).mockRejectedValue(denied)

    const error = await rejection(admin.deleteTopics({ topicIds: [first] }))

    expect(error).toBe(denied)
    expect(broker.deleteTopics).toHaveBeenCalledTimes(1)
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
  })
})
