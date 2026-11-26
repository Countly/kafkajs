const createAdmin = require('..')
const Cluster = require('../../cluster')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const listOffsetsV9 = require('../../../testHelpers/wire/listOffsetsV9')
const { newLogger } = require('testHelpers')

const TOPIC = 't'

/**
 * @param {...[number, string]} rows partition and offset of each row
 * @returns {object} the answer's one topic entry
 */
const answered = (...rows) => ({
  name: TOPIC,
  partitions: rows.map(([partitionIndex, offset]) => ({
    partitionIndex,
    errorCode: 0,
    timestamp: '-1',
    offset,
    leaderEpoch: 5,
  })),
})

describe('Admin > offset lookups whose answer does not cover the request', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  /**
   * An admin over the real `Cluster#fetchTopicsOffset` fan-out and a real broker over a fake
   * socket that answers its ListOffsets requests one after the other from `answers`.
   * @param {object[]} answers the topic entry of each answer, in the order requests arrive
   * @returns {object} the admin
   */
  const adminOver = (answers) => {
    const queue = [...answers]
    harness = createGroupApiBroker({
      versions: { [apiKeys.ListOffsets]: { minVersion: 0, maxVersion: 9 } },
      respond: () => listOffsetsV9.buildResponse({ throttleTime: 0, topics: [queue.shift()] }),
    })
    const cluster = {
      isolationLevel: 1,
      metadataCache: { leaderEpoch: () => -1 },
      findLeaderForPartitions: (topic, partitions) => ({ 0: partitions }),
      findBroker: async () => harness.broker,
      defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
      addTargetTopic: async () => {},
      refreshMetadataIfNecessary: async () => {},
      refreshMetadata: async () => {},
      findTopicPartitionMetadata: () => [{ partitionId: 0 }, { partitionId: 1 }],
      fetchTopicsOffset: Cluster.prototype.fetchTopicsOffset,
    }
    return createAdmin({ cluster, logger: newLogger(), retry: { retries: 0 } })
  }

  test('fetchTopicOffsets whose low-offset answer omits a partition rejects at once with the retriable coverage error', async () => {
    const admin = adminOver([answered([0, '100'], [1, '200']), answered([0, '3'])])

    await expect(admin.fetchTopicOffsets(TOPIC)).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: expect.stringMatching(/^ListOffsets response does not name partition 1 of topic t/),
    })
    expect(harness.requests).toHaveLength(2)
  })

  test('fetchTopicOffsets whose answers cover every partition returns both bounds', async () => {
    const admin = adminOver([answered([0, '100'], [1, '200']), answered([0, '3'], [1, '4'])])

    await expect(admin.fetchTopicOffsets(TOPIC)).resolves.toEqual([
      { partition: 0, offset: '100', high: '100', low: '3' },
      { partition: 1, offset: '200', high: '200', low: '4' },
    ])
  })

  test('fetchTopicOffsetsByTimestamp with a -1 row falls back to the high watermark', async () => {
    const admin = adminOver([answered([0, '100'], [1, '200']), answered([0, '-1'], [1, '5'])])

    await expect(admin.fetchTopicOffsetsByTimestamp(TOPIC, 123)).resolves.toEqual([
      { partition: 0, offset: '100' },
      { partition: 1, offset: '5' },
    ])
  })
})
