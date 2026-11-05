const createAdmin = require('..')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

const partitionRow = (partition, extra = {}) => ({
  partition,
  offset: '10',
  metadata: null,
  errorCode: 0,
  ...extra,
})
const groupRow = (groupId, partitions) => ({
  groupId,
  errorCode: 0,
  topics: [{ topic: 't', partitions }],
})

/** A coordinator that negotiated `version` and answers OffsetFetch through `answer`. */
const coordinator = (nodeId, version, answer) => ({
  nodeId,
  lookupRequestWithMax: () => () => ({ request: { apiVersion: version } }),
  offsetFetch: jest.fn(answer),
})

describe('delete group offsets edges', () => {
  test.each([
    ['a null topic row', [null]],
    ['an empty topic name', [{ topic: '', partitions: [0] }]],
  ])('rejects %s before any coordinator lookup', async (_label, topics) => {
    const cluster = { findGroupCoordinator: jest.fn() }
    const admin = createAdmin({ cluster, logger, retry })

    const error = await rejection(admin.deleteOffsets({ groupId: 'g', topics }))

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(error.message).toContain('Invalid topics')
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })

  test('a whole-response NOT_COORDINATOR answer looks the coordinator up again', async () => {
    const moved = {
      offsetDelete: jest.fn(async () => ({ errorCode: 16, topics: [] })),
    }
    const current = {
      offsetDelete: jest.fn(async () => ({
        errorCode: 0,
        topics: [
          {
            topic: 't',
            partitions: [
              { partition: 0, errorCode: 0 },
              { partition: 1, errorCode: 0 },
            ],
          },
        ],
      })),
    }
    const cluster = {
      findGroupCoordinator: jest.fn().mockResolvedValueOnce(moved).mockResolvedValueOnce(current),
    }

    const result = await createAdmin({ cluster, logger, retry }).deleteOffsets({
      groupId: 'g',
      topics: [{ topic: 't', partitions: [0, 1] }],
    })

    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    expect(moved.offsetDelete).toHaveBeenCalledTimes(1)
    expect(current.offsetDelete).toHaveBeenCalledTimes(1)
    expect(result.topics[0].partitions.map((p) => [p.partition, p.errorCode])).toEqual([
      [0, 0],
      [1, 0],
    ])
  })
})

describe('fetch offsets for groups edges', () => {
  const groups = [{ groupId: 'g0', topics: [{ topic: 't', partitions: [0, 1, 2] }] }]

  test('reports a missing, -1 or absent committed leader epoch as null and keeps a real one', async () => {
    const broker = coordinator(0, 9, async () => ({
      groups: [
        groupRow('g0', [
          partitionRow(0, { committedLeaderEpoch: -1 }),
          partitionRow(1),
          partitionRow(2, { committedLeaderEpoch: 3 }),
        ]),
      ],
    }))
    const cluster = { findGroupCoordinator: async () => broker }

    const [result] = await createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups({
      groups,
    })

    expect(result.topics[0].partitions.map((p) => [p.partition, p.leaderEpoch])).toEqual([
      [0, null],
      [1, null],
      [2, 3],
    ])
  })

  test('a single-group answer without an error code counts as success', async () => {
    const broker = coordinator(0, 7, async () => ({
      responses: [{ topic: 't', partitions: [partitionRow(0), partitionRow(1), partitionRow(2)] }],
    }))
    const cluster = { findGroupCoordinator: async () => broker }

    const [result] = await createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups({
      groups,
    })

    expect({ groupId: result.groupId, errorCode: result.errorCode, error: result.error }).toEqual({
      groupId: 'g0',
      errorCode: 0,
      error: null,
    })
    expect(result.topics[0].partitions.map((p) => p.partition)).toEqual([0, 1, 2])
  })

  test.each([9, 7])(
    'a NOT_COORDINATOR failure of the request at version %i finds the coordinator again',
    async (version) => {
      const answer = async (request) =>
        request.groups
          ? {
              groups: [groupRow('g0', [partitionRow(0), partitionRow(1), partitionRow(2)])],
            }
          : {
              responses: groupRow('g0', [partitionRow(0), partitionRow(1), partitionRow(2)]).topics,
            }
      const moved = coordinator(0, version, answer)
      moved.offsetFetch.mockRejectedValueOnce(createErrorFromCode(16))
      const current = coordinator(1, version, answer)
      const cluster = {
        findGroupCoordinator: jest.fn().mockResolvedValueOnce(moved).mockResolvedValueOnce(current),
      }

      const [result] = await createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups({
        groups,
      })

      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
      expect(moved.offsetFetch).toHaveBeenCalledTimes(1)
      expect(current.offsetFetch).toHaveBeenCalledTimes(1)
      expect(result.errorCode).toBe(0)
      expect(result.topics[0].partitions.map((p) => p.partition)).toEqual([0, 1, 2])
    }
  )
})
