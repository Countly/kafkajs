const createAdmin = require('..')
const Cluster = require('../../cluster')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }

/** Creates a brokerless admin whose real cluster fan-out records the isolation byte.
 * @returns {object} admin, request bytes and read-only cache baseline
 */
const fixture = () => {
  const bytes = []
  const wire = require('../../protocol/requests/listOffsets/v2/request')
  const broker = {
    listOffsets: jest.fn(async (request) => {
      bytes.push((await wire({ ...request, replicaId: -1 }).encode()).buffer[4])
      return {
        responses: request.topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition, timestamp }) => ({
            partition,
            offset: timestamp === -1 ? '100' : '7',
          })),
        })),
      }
    }),
  }
  const cluster = {
    isolationLevel: 1,
    metadataCache: { leaderEpoch: () => -1 },
    findLeaderForPartitions: () => ({ 0: [0] }),
    findBroker: async () => broker,
    defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
    addTargetTopic: async () => {},
    refreshMetadataIfNecessary: async () => {},
    findTopicPartitionMetadata: () => [{ partitionId: 0 }],
    fetchTopicsOffset: Cluster.prototype.fetchTopicsOffset,
  }
  return { admin: createAdmin({ cluster, logger, retry: { retries: 0 } }), bytes, cluster }
}
describe('offset isolation', () => {
  it.each([undefined, 'read_committed', 'read_uncommitted'])(
    'maps isolation %p for both calls',
    async (isolationLevel) => {
      const { admin, bytes, cluster } = fixture()
      const options = { isolationLevel }
      await admin.fetchTopicOffsets('t', options)
      await admin.fetchTopicOffsetsByTimestamp('t', 123, options)
      expect(bytes).toEqual(Array(4).fill(isolationLevel === 'read_uncommitted' ? 0 : 1))
      expect(cluster.isolationLevel).toBe(1)
    }
  )
  it.each([null, true, 0, 'READ_COMMITTED', ''])(
    'refuses invalid isolation %p before discovery',
    async (isolationLevel) => {
      const { admin, bytes } = fixture()
      await expect(admin.fetchTopicOffsets('t', { isolationLevel })).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      await expect(
        admin.fetchTopicOffsetsByTimestamp('t', 1, { isolationLevel })
      ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(bytes).toEqual([])
    }
  )
})
