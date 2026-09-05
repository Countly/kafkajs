const createAdmin = require('../index')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')

const TOPIC = 'test-topic'
const FENCED_LEADER_EPOCH = 74
const UNKNOWN_LEADER_EPOCH = 75
const TOPIC_AUTHORIZATION_FAILED = 29

/**
 * A cluster stub reduced to what the two offset admin methods touch. `fetchTopicsOffset` is the
 * seam under test: on a real cluster it refreshes metadata itself before rethrowing a fenced
 * epoch, which is why the admin only has to let the error reach its retrier.
 */
const stubCluster = (fetchTopicsOffset) => ({
  addTargetTopic: async () => {},
  refreshMetadataIfNecessary: async () => {},
  refreshMetadata: async () => {},
  findTopicPartitionMetadata: () => [{ partitionId: 0 }],
  fetchTopicsOffset,
})

// Rebuilt per call: the admin methods `pop()` the array they are handed.
const offsets = () => [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }]

describe('Admin > offset APIs > a fenced leader epoch is retried, not bailed', () => {
  const retry = { retries: 3, initialRetryTime: 1, maxRetryTime: 2 }

  const failFirstCall = (code) => {
    const state = { calls: 0 }
    state.fetchTopicsOffset = async () => {
      state.calls += 1

      if (state.calls === 1) {
        throw createErrorFromCode(code)
      }

      return offsets()
    }
    return state
  }

  test.each([
    [FENCED_LEADER_EPOCH, 'FENCED_LEADER_EPOCH'],
    [UNKNOWN_LEADER_EPOCH, 'UNKNOWN_LEADER_EPOCH'],
  ])('fetchTopicOffsets succeeds after one %s (%s)', async (code) => {
    const state = failFirstCall(code)
    const admin = createAdmin({
      logger: newLogger(),
      retry,
      cluster: stubCluster(state.fetchTopicsOffset),
    })

    await expect(admin.fetchTopicOffsets(TOPIC)).resolves.toEqual([
      { partition: 0, offset: '5', high: '5', low: '5' },
    ])
    expect(state.calls).toBeGreaterThan(1)
  })

  test('fetchTopicOffsetsByTimestamp succeeds after one fenced leader epoch', async () => {
    const state = failFirstCall(FENCED_LEADER_EPOCH)
    const admin = createAdmin({
      logger: newLogger(),
      retry,
      cluster: stubCluster(state.fetchTopicsOffset),
    })

    await expect(admin.fetchTopicOffsetsByTimestamp(TOPIC, 1)).resolves.toEqual([
      { partition: 0, offset: '5' },
    ])
    expect(state.calls).toBeGreaterThan(1)
  })

  test('an unrelated protocol error still fails fast, on the first attempt', async () => {
    let calls = 0
    const admin = createAdmin({
      logger: newLogger(),
      retry,
      cluster: stubCluster(async () => {
        calls += 1
        throw createErrorFromCode(TOPIC_AUTHORIZATION_FAILED)
      }),
    })

    await expect(admin.fetchTopicOffsets(TOPIC)).rejects.toMatchObject({
      type: 'TOPIC_AUTHORIZATION_FAILED',
    })
    expect(calls).toEqual(1)
  })
})
