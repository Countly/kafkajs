const createAdmin = require('..')
const { newLogger } = require('testHelpers')

const TOPIC = 'test-topic'
const GROUP = 'test-group'

describe('Admin > fetchOffsets > stored offsets', () => {
  /**
   * An admin whose coordinator answers every OffsetFetch with `offset` for partition 0 of `TOPIC`.
   * @param {string} offset the stored offset the answer carries
   * @returns {object} the admin
   */
  const adminStoring = (offset) => {
    const coordinator = {
      offsetFetch: async () => ({
        responses: [
          {
            topic: TOPIC,
            partitions: [{ partition: 0, offset, metadata: null, errorCode: 0 }],
          },
        ],
      }),
    }
    const cluster = {
      findGroupCoordinator: async () => coordinator,
      addTargetTopic: async () => {},
      refreshMetadataIfNecessary: async () => {},
      findTopicPartitionMetadata: () => [{ partitionId: 0 }],
    }
    return createAdmin({ cluster, logger: newLogger(), retry: { retries: 0 } })
  }

  test.each(['-500', '-2', '-1', '7'])('reports a stored %s as it is', async (offset) => {
    const admin = adminStoring(offset)

    await expect(admin.fetchOffsets({ groupId: GROUP, topics: [TOPIC] })).resolves.toEqual([
      { topic: TOPIC, partitions: [{ partition: 0, offset, metadata: null }] },
    ])
  })
})
