const { createCluster, secureRandom } = require('testHelpers')

describe('Cluster > findLeaderForPartitions', () => {
  let cluster, topic

  beforeEach(() => {
    topic = `test-topic-${secureRandom()}`
    cluster = createCluster()
  })

  test('returns the partition metadata of a topic', async () => {
    cluster.brokerPool.metadata = {
      topicMetadata: [
        {
          topic,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 2,
              leader: 2,
              replicas: [0, 1, 2],
              isr: [2, 0, 1],
            },
            {
              partitionErrorCode: 0,
              partitionId: 5,
              leader: 2,
              replicas: [0, 1, 2],
              isr: [2, 1, 0],
            },
            {
              partitionErrorCode: 0,
              partitionId: 4,
              leader: 1,
              replicas: [0, 1, 2],
              isr: [1, 0, 2],
            },
            {
              partitionErrorCode: 0,
              partitionId: 1,
              leader: 1,
              replicas: [0, 1, 2],
              isr: [1, 2, 0],
            },
            {
              partitionErrorCode: 0,
              partitionId: 3,
              leader: 0,
              replicas: [0, 1, 2],
              isr: [0, 2, 1],
            },
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 0,
              replicas: [0, 1, 2],
              isr: [0, 1, 2],
            },
          ],
        },
      ],
    }

    const partitions = [0, 5]
    expect(cluster.findLeaderForPartitions(topic, partitions)).toEqual({ 0: [0], 2: [5] })
  })

  it('does not include leaders for topics without metadata', () => {
    cluster.brokerPool.metadata = {
      topicMetadata: [
        {
          topic,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 2,
              leader: 2,
              replicas: [0, 1, 2],
              isr: [2, 0, 1],
            },
          ],
        },
      ],
    }

    const anotherTopic = `test-topic-${secureRandom()}`
    const partitions = [0]
    expect(cluster.findLeaderForPartitions(anotherTopic, partitions)).toEqual({})
  })

  describe('a partition whose cached leader is negative has no leader', () => {
    const cache = (leader, errorFields = { partitionErrorCode: 5 }) => {
      cluster.brokerPool.metadata = {
        topicMetadata: [
          {
            topic,
            partitionMetadata: [
              { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1] },
              { ...errorFields, partitionId: 1, leader, replicas: [2], isr: [] },
            ],
          },
        ],
      }
    }

    test.each([-1, -2])(
      'a leader of %d throws LEADER_NOT_AVAILABLE, never a node to route to',
      (leader) => {
        cache(leader)

        expect(() => cluster.findLeaderForPartitions(topic, [0, 1])).toThrow(
          expect.objectContaining({
            name: 'KafkaJSProtocolError',
            type: 'LEADER_NOT_AVAILABLE',
            code: 5,
            retriable: true,
          })
        )
      }
    )

    test.each([
      ['a partition error code of 0', { partitionErrorCode: 0 }],
      ['no partition error code', {}],
    ])(
      'a leader of -1 throws LEADER_NOT_AVAILABLE with %s: the leader decides, not the code',
      (_, errorFields) => {
        cache(-1, errorFields)

        expect(() => cluster.findLeaderForPartitions(topic, [0, 1])).toThrow(
          expect.objectContaining({ type: 'LEADER_NOT_AVAILABLE', code: 5, retriable: true })
        )
      }
    )

    test.each([null, undefined])(
      'a leader of %s is invalid partition metadata, not a node to route to',
      (leader) => {
        cache(leader)

        expect(() => cluster.findLeaderForPartitions(topic, [0, 1])).toThrow(
          'Invalid partition metadata'
        )
      }
    )

    test('a partition that was not asked for does not matter', () => {
      cache(-1)

      expect(cluster.findLeaderForPartitions(topic, [0])).toEqual({ 1: [0] })
    })
  })
})
