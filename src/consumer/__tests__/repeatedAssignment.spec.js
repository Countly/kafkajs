const { applyRows, ASSIGNER_NAME, GROUP_ID, TOPIC } = require('./fixtures/repeatedAssignmentApply')

const SESSIONS = [
  ['off', false],
  ['on', true],
]

/** @returns {number} negative, zero or positive, to sort partitions by value */
const byValue = (a, b) => a - b

/** @returns {number[]} the partitions that occur again after their first place */
const repeats = (partitions) => partitions.filter((p, index) => partitions.indexOf(p) !== index)

/** @returns {number[][]} an empty list for every request, which is what a clean run carries */
const none = (requests) => requests.map(() => [])

/**
 * What the group sent and applied for one assignment, checked the same way for every case:
 * the member took the partitions once each, in `applied` order, and no Fetch or OffsetFetch
 * request carried a partition twice.
 * @param {object} seen what `applyRows` returned
 * @param {number[]} applied the partitions of the topic, in the order the member took them
 * @returns {void}
 */
const expectAppliedOnce = (seen, applied) => {
  expect(seen.crashes).toEqual([])
  expect(seen.joins).toHaveLength(1)
  expect(seen.joins[0].memberAssignment).toEqual({ [TOPIC]: applied })
  expect(seen.assigned).toHaveLength(1)
  expect(seen.assigned[0].flatMap(({ partitions }) => partitions)).toEqual(applied)
  expect([...seen.fetches[0]].sort(byValue)).toEqual([...applied].sort(byValue))
  expect(seen.fetches.map(repeats)).toEqual(none(seen.fetches))
  expect([...seen.offsetFetches[0]].sort(byValue)).toEqual([...applied].sort(byValue))
  expect(seen.offsetFetches.map(repeats)).toEqual(none(seen.offsetFetches))
}

describe('Consumer > repeated assignment', () => {
  test.each(SESSIONS)(
    'a partition the assigner names twice is applied, fetched and offset-fetched once, and warned about once, with fetch sessions %s',
    async (_name, fetchSessions) => {
      const seen = await applyRows({
        rows: [{ topic: TOPIC, partitions: [2, 0, 2, 1, 0] }],
        fetchSessions,
      })

      expectAppliedOnce(seen, [2, 0, 1])
      expect(seen.warnings).toHaveLength(1)
      expect(seen.warnings[0]).toMatchObject({
        message: expect.stringContaining('more than once'),
        groupId: GROUP_ID,
        memberId: 'member-1',
        generationId: 1,
        groupProtocol: ASSIGNER_NAME,
        repeatedPartitions: [{ topic: TOPIC, partitions: [2, 0] }],
        repeatedTopics: [],
      })
    }
  )

  test.each(SESSIONS)(
    'two rows of one topic from the assigner give the member the partitions of both rows, once each, with fetch sessions %s',
    async (_name, fetchSessions) => {
      const seen = await applyRows({
        rows: [
          { topic: TOPIC, partitions: [0] },
          { topic: TOPIC, partitions: [2, 1] },
        ],
        fetchSessions,
      })

      expectAppliedOnce(seen, [0, 2, 1])
      expect(seen.warnings).toHaveLength(1)
      expect(seen.warnings[0]).toMatchObject({
        groupId: GROUP_ID,
        memberId: 'member-1',
        generationId: 1,
        groupProtocol: ASSIGNER_NAME,
        repeatedPartitions: [],
        repeatedTopics: [TOPIC],
      })
    }
  )

  test.each(SESSIONS)(
    'a partition named in two rows of one topic is applied once and the warning names the topic and the partition with fetch sessions %s',
    async (_name, fetchSessions) => {
      const seen = await applyRows({
        rows: [
          { topic: TOPIC, partitions: [0, 1] },
          { topic: TOPIC, partitions: [1, 2] },
        ],
        fetchSessions,
      })

      expectAppliedOnce(seen, [0, 1, 2])
      expect(seen.warnings).toHaveLength(1)
      expect(seen.warnings[0]).toMatchObject({
        repeatedPartitions: [{ topic: TOPIC, partitions: [1] }],
        repeatedTopics: [TOPIC],
      })
    }
  )

  test.each(SESSIONS)(
    'an assignment that names each partition once is applied in its order and logs no warning with fetch sessions %s',
    async (_name, fetchSessions) => {
      const seen = await applyRows({
        rows: [{ topic: TOPIC, partitions: [2, 0, 1] }],
        fetchSessions,
      })

      expectAppliedOnce(seen, [2, 0, 1])
      expect(seen.warnings).toEqual([])
    }
  )
})
