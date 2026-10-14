const { assignSticky } = require('../stickyAssigner/stickyCore')
const sticky = require('../stickyAssigner')
const cooperativeSticky = require('../cooperativeStickyAssigner')
const { MemberAssignment } = require('../../assignerProtocol')
const {
  CHECK_ONLY,
  stag,
  wide,
  groupMembers,
  assign,
  checkInvariants,
  nonSubscriberGrants,
} = require('../../../../scripts/benchmarks/stickyAssign')

/**
 * @param {string} memberId
 * @param {string[]} topics
 * @param {{[topic: string]: number[]}} owned
 * @param {number} generationId
 * @param {string|null} [groupInstanceId]
 * @returns {object} a member in the shape `assignSticky` consumes
 */
const member = (memberId, topics, owned, generationId, groupInstanceId = null) => ({
  memberId,
  groupInstanceId,
  topics,
  ownedPartitions: Object.keys(owned).map((topic) => ({ topic, partitions: owned[topic] })),
  generationId,
})

const partitions = (count) => Array.from({ length: count }, (_, i) => i)

/**
 * @param {Array<{memberId: string, memberAssignment: Buffer}>} result
 * @returns {{[memberId: string]: {[topic: string]: number[]}}} each member's decoded assignment
 */
const decoded = (result) =>
  Object.fromEntries(
    result.map(({ memberId, memberAssignment }) => [
      memberId,
      MemberAssignment.decode(memberAssignment).assignment,
    ])
  )

describe('Consumer > assigners > stickyCore general path at scale', () => {
  test('assigns 150 owners and 150 joiners narrowly subscribed to 100 topics within a third of the session timeout', async () => {
    const shape = wide(150, 150)
    const members = groupMembers(shape)

    const started = performance.now()
    const result = await assign(sticky, shape, members)
    const elapsedMs = performance.now() - started

    expect(result).toHaveLength(300)
    expect(checkInvariants(shape, result)).toEqual([])
    // The leader holds the group between JoinGroup and SyncGroup, against the default 30 s
    // session timeout.
    expect(elapsedMs).toBeLessThan(10000)
  })

  test('stops balancing once no move helps, so a member is not handed back a partition of a topic it left', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(11), 'topic-B': partitions(4), 'topic-C': [0] },
      members: [
        member('member-1', ['topic-A', 'topic-B'], { 'topic-A': [9] }, 2),
        member('member-2', ['topic-A', 'topic-C'], {}, -1),
        member('member-3', ['topic-A', 'topic-B'], { 'topic-A': [10], 'topic-B': [1] }, -1),
        member('member-4', ['topic-A', 'topic-B'], { 'topic-B': [0] }, 0),
        // It held topic-A 7 in an older generation and no longer subscribes to topic-A.
        member('member-5', ['topic-B'], { 'topic-A': [7] }, -1),
        member('member-6', ['topic-C'], {}, 2),
      ],
    })

    expect(Object.fromEntries(assignment)).toEqual({
      'member-1': { 'topic-A': [0, 3, 7, 9] },
      'member-2': { 'topic-A': [1, 4, 8] },
      'member-3': { 'topic-A': [5, 10], 'topic-B': [1] },
      'member-4': { 'topic-A': [2, 6], 'topic-B': [0] },
      'member-5': { 'topic-B': [2, 3] },
      'member-6': { 'topic-C': [0] },
    })
  })

  test('keeps the partitions where they went before when a hand-back to a member that left the topic would still give every partition to a subscriber', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { A: partitions(2), B: partitions(2), C: partitions(1) },
      members: [
        member('e', ['A'], {}, 1),
        // It left topic A, which it owned in an older generation than the newest.
        member('d', ['B'], { A: [0] }, -1),
        member('c', ['A'], {}, 0),
        member('b', ['B'], {}, 1, 'i'),
        member('a', ['A', 'C'], { A: [0, 1] }, 2),
      ],
    })

    expect(Object.fromEntries(assignment)).toEqual({
      a: { C: [0] },
      b: { B: [0] },
      c: { A: [1] },
      d: { B: [1] },
      e: { A: [0] },
    })
  })

  test.each([
    [
      'sticky',
      sticky,
      'capacity-3',
      {
        'member-0001': { 'topic-B': [0, 1] },
        'member-0002': { 'topic-A': [0, 1, 2, 3, 4] },
        'member-0003': { 'topic-A': [5, 6, 7, 8, 9] },
      },
    ],
    [
      'cooperative-sticky',
      cooperativeSticky,
      'capacity-3',
      {
        'member-0001': {},
        'member-0002': { 'topic-A': [0, 1, 2, 3, 4] },
        'member-0003': { 'topic-A': [5, 6, 7, 8, 9] },
      },
    ],
    [
      'sticky',
      sticky,
      'hand-back-3',
      {
        'member-0001': { 'topic-B': [0] },
        'member-0002': { 'topic-A': [1, 2] },
        'member-0003': { 'topic-A': [0] },
      },
    ],
    [
      'cooperative-sticky',
      cooperativeSticky,
      'hand-back-3',
      {
        'member-0001': { 'topic-B': [0] },
        'member-0002': { 'topic-A': [1, 2] },
        'member-0003': {},
      },
    ],
  ])(
    '%s on %s gives the partitions of a topic a member left to the members subscribed to it',
    async (_name, factory, shapeName, expected) => {
      const shape = CHECK_ONLY[shapeName]()

      const result = await assign(factory, shape, groupMembers(shape))

      expect(nonSubscriberGrants(shape, result)).toEqual([])
      expect(decoded(result)).toEqual(expected)
    }
  )

  test.each(Object.keys(CHECK_ONLY))(
    'sticky keeps every invariant on %s, whose previous round names a topic its member left',
    async (name) => {
      const shape = CHECK_ONLY[name]()

      const result = await assign(sticky, shape, groupMembers(shape))

      expect(checkInvariants(shape, result)).toEqual([])
    }
  )

  test('deals partitions to the least loaded member and breaks a tie towards the lower member id', async () => {
    const shape = stag(0, 4, 40)

    const result = await assign(sticky, shape, groupMembers(shape))

    expect(decoded(result)).toEqual({
      'member-0000': {
        'topic-0000': [0, 1, 2, 3],
        'topic-0002': [0, 3],
        'topic-0003': [0, 1],
        'topic-0004': [0],
      },
      'member-0001': {
        'topic-0001': [0, 1, 2, 3],
        'topic-0002': [1],
        'topic-0004': [1],
        'topic-0005': [1],
        'topic-0006': [0, 3],
      },
      'member-0002': {
        'topic-0002': [2],
        'topic-0003': [2],
        'topic-0004': [2],
        'topic-0005': [2],
        'topic-0006': [1],
        'topic-0008': [0, 1, 2, 3],
      },
      'member-0003': {
        'topic-0003': [3],
        'topic-0004': [3],
        'topic-0005': [0, 3],
        'topic-0006': [2],
        'topic-0007': [0, 1, 2, 3],
      },
    })
  })
})
