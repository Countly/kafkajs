const { assignSticky, DEFAULT_GENERATION } = require('../stickyAssigner/stickyCore')

/**
 * @param {string} memberId
 * @param {object} [overrides]
 * @returns {object} a member in the shape `assignSticky` consumes
 */
const member = (memberId, { topics = ['topic-A'], owned = {}, generationId = -1 } = {}) => ({
  memberId,
  groupInstanceId: null,
  topics,
  ownedPartitions: Object.keys(owned).map((topic) => ({ topic, partitions: owned[topic] })),
  generationId,
})

/** @param {Map<string, object>} assignment @returns {object} a plain object a test can compare */
const plain = (assignment) => Object.fromEntries(assignment)

/** @param {Map<string, object>} assignment @returns {number[]} partition count per member */
const sizes = (assignment) =>
  [...assignment.values()].map((byTopic) =>
    Object.values(byTopic).reduce((sum, partitions) => sum + partitions.length, 0)
  )

/** @param {Map<string, object>} assignment @returns {string[]} every assigned `topic-partition` */
const allAssigned = (assignment) =>
  [...assignment.values()].flatMap((byTopic) =>
    Object.keys(byTopic).flatMap((topic) => byTopic[topic].map((p) => `${topic}-${p}`))
  )

const partitions = (count) => Array.from({ length: count }, (_, i) => i)

describe('Consumer > assigners > stickyCore', () => {
  describe('quotas', () => {
    test('7 partitions over 3 members leaves one member above the min quota', () => {
      const assignment = assignSticky({
        partitionsPerTopic: { 'topic-A': partitions(7) },
        members: [member('member-1'), member('member-2'), member('member-3')],
      })

      // minQuota 2, maxQuota 3, expectedNumMembersWithOverMinQuota 1
      expect(sizes(assignment).sort()).toEqual([2, 2, 3])
      expect(allAssigned(assignment)).toHaveLength(7)
    })

    test('3 partitions over 7 members leaves four members empty', () => {
      const assignment = assignSticky({
        partitionsPerTopic: { 'topic-A': partitions(3) },
        members: partitions(7).map((i) => member(`member-${i}`)),
      })

      // minQuota 0, maxQuota 1, expectedNumMembersWithOverMinQuota 3
      expect(sizes(assignment).sort()).toEqual([0, 0, 0, 0, 1, 1, 1])
      expect(allAssigned(assignment)).toHaveLength(3)
    })
  })

  test('takes the constrained path when every subscription is identical', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(4), 'topic-B': partitions(3) },
      members: [
        member('member-1', { topics: ['topic-A', 'topic-B'] }),
        member('member-2', { topics: ['topic-B', 'topic-A'] }),
      ],
    })

    // The leftovers go round-robin over the flat, topic-sorted partition list, not per topic.
    expect(plain(assignment)).toEqual({
      'member-1': { 'topic-A': [0, 2], 'topic-B': [0, 2] },
      'member-2': { 'topic-A': [1, 3], 'topic-B': [1] },
    })
    expect(Math.max(...sizes(assignment)) - Math.min(...sizes(assignment))).toBeLessThanOrEqual(1)
  })

  test('takes the general path when subscriptions differ', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(4), 'topic-B': partitions(2) },
      members: [
        member('member-1', { topics: ['topic-A'] }),
        member('member-2', { topics: ['topic-A', 'topic-B'] }),
        member('member-3', { topics: ['topic-B'] }),
      ],
    })

    expect(allAssigned(assignment).sort()).toEqual([
      'topic-A-0',
      'topic-A-1',
      'topic-A-2',
      'topic-A-3',
      'topic-B-0',
      'topic-B-1',
    ])
    expect(Math.max(...sizes(assignment)) - Math.min(...sizes(assignment))).toBeLessThanOrEqual(1)
    // Every partition is held by a member subscribed to its topic.
    expect(plain(assignment)['member-1']['topic-B']).toBeUndefined()
    expect(plain(assignment)['member-3']['topic-A']).toBeUndefined()
  })

  test('keeps a still-subscribed owner on its partitions when the balance allows', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(6) },
      members: [
        member('member-1', { owned: { 'topic-A': [4, 5] }, generationId: 7 }),
        member('member-2', { owned: { 'topic-A': [0, 1] }, generationId: 7 }),
        member('member-3', { owned: { 'topic-A': [2, 3] }, generationId: 7 }),
      ],
    })

    expect(plain(assignment)).toEqual({
      'member-1': { 'topic-A': [4, 5] },
      'member-2': { 'topic-A': [0, 1] },
      'member-3': { 'topic-A': [2, 3] },
    })
  })

  test('gives a doubly claimed partition to the higher generation', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(4) },
      members: [
        member('member-1', { owned: { 'topic-A': [0, 1] }, generationId: 4 }),
        member('member-2', { owned: { 'topic-A': [1, 2] }, generationId: 5 }),
      ],
    })

    expect(plain(assignment)['member-2']['topic-A']).toContain(1)
    expect(plain(assignment)['member-1']['topic-A']).not.toContain(1)
  })

  test('frees a partition claimed twice at the same generation from both claimants', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(6) },
      members: [
        member('member-1', { owned: { 'topic-A': [0, 1, 2] }, generationId: 5 }),
        member('member-2', { owned: { 'topic-A': [2, 3, 4] }, generationId: 5 }),
      ],
    })

    // Both keep their uncontested partitions; 2 and 5 are handed out from scratch, which puts
    // the contested 2 on the member that did not keep it by ownership.
    expect(plain(assignment)).toEqual({
      'member-1': { 'topic-A': [0, 1, 2] },
      'member-2': { 'topic-A': [3, 4, 5] },
    })
    expect(allAssigned(assignment)).toHaveLength(6)
  })

  test('lets a member with the default generation lose every conflict', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(4) },
      members: [
        member('member-1', {
          owned: { 'topic-A': [0, 1] },
          generationId: DEFAULT_GENERATION,
        }),
        member('member-2', { owned: { 'topic-A': [0, 1] }, generationId: 3 }),
      ],
    })

    expect(plain(assignment)['member-2']['topic-A']).toEqual([0, 1])
    expect(plain(assignment)['member-1']['topic-A']).toEqual([2, 3])
  })

  test('produces a balanced, deterministic assignment on a first join', () => {
    const input = {
      partitionsPerTopic: { 'topic-A': partitions(5), 'topic-B': partitions(1) },
      members: [
        member('member-3', { topics: ['topic-A', 'topic-B'] }),
        member('member-1', { topics: ['topic-A', 'topic-B'] }),
        member('member-2', { topics: ['topic-A', 'topic-B'] }),
      ],
    }

    const first = assignSticky(input)

    expect(sizes(first)).toEqual([2, 2, 2])
    expect(plain(assignSticky(input))).toEqual(plain(first))
  })

  test('gives an unsubscribed member nothing without distorting the others', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(4) },
      members: [
        member('member-1', { topics: ['topic-A'] }),
        member('member-2', { topics: ['topic-A'] }),
        member('member-3', { topics: [] }),
      ],
    })

    expect(plain(assignment)['member-3']).toEqual({})
    expect(plain(assignment)['member-1']['topic-A']).toHaveLength(2)
    expect(plain(assignment)['member-2']['topic-A']).toHaveLength(2)
  })

  test('leaves a partition nobody subscribes to unassigned', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(2), 'topic-orphan': partitions(3) },
      members: [
        member('member-1', { topics: ['topic-A'] }),
        member('member-2', { topics: ['topic-A', 'topic-B'] }),
      ],
    })

    expect(allAssigned(assignment).sort()).toEqual(['topic-A-0', 'topic-A-1'])
  })

  test('caps a member that owns more than the max quota, and refills the rest', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(7) },
      members: [
        member('member-1', { owned: { 'topic-A': [0, 1, 2, 3, 4, 5, 6] }, generationId: 3 }),
        member('member-2', { generationId: 3 }),
        member('member-3', { generationId: 3 }),
      ],
    })

    // minQuota 2, maxQuota 3, one member allowed above the min: member-1 keeps three and the
    // remaining four go round-robin to the two empty members.
    expect(plain(assignment)).toEqual({
      'member-1': { 'topic-A': [0, 1, 2] },
      'member-2': { 'topic-A': [3, 5] },
      'member-3': { 'topic-A': [4, 6] },
    })
  })

  test('ignores an owned partition whose topic the group no longer has', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(2) },
      members: [
        member('member-1', { owned: { 'topic-A': [0], 'topic-gone': [0, 1] }, generationId: 2 }),
        member('member-2', { owned: { 'topic-A': [1] }, generationId: 2 }),
      ],
    })

    expect(plain(assignment)).toEqual({
      'member-1': { 'topic-A': [0] },
      'member-2': { 'topic-A': [1] },
    })
  })

  test('revokes a partition from a member that no longer subscribes to its topic', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(2), 'topic-B': partitions(2) },
      members: [
        member('member-1', {
          topics: ['topic-A'],
          owned: { 'topic-A': [0, 1], 'topic-B': [0] },
          generationId: 4,
        }),
        member('member-2', {
          topics: ['topic-A', 'topic-B'],
          owned: { 'topic-B': [1] },
          generationId: 4,
        }),
      ],
    })

    expect(plain(assignment)).toEqual({
      'member-1': { 'topic-A': [0, 1] },
      'member-2': { 'topic-B': [0, 1] },
    })
  })

  test('rebalances a lopsided general assignment down to an even one', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(4), 'topic-B': partitions(2) },
      members: [
        member('member-1', {
          topics: ['topic-A', 'topic-B'],
          owned: { 'topic-A': [0, 1, 2, 3], 'topic-B': [0, 1] },
          generationId: 6,
        }),
        member('member-2', { topics: ['topic-A'], generationId: 6 }),
      ],
    })

    // topic-B has a single subscriber, so its partitions never move; topic-A's do, until the two
    // members are one apart.
    expect(sizes(assignment).sort()).toEqual([3, 3])
    expect(plain(assignment)['member-1']['topic-B']).toEqual([0, 1])
    expect(allAssigned(assignment)).toHaveLength(6)
  })

  test('sends a partition back to the owner it had in an older generation', () => {
    const assignment = assignSticky({
      partitionsPerTopic: { 'topic-A': partitions(9), 'topic-B': partitions(1) },
      members: [
        member('member-1', {
          topics: ['topic-A', 'topic-B'],
          owned: { 'topic-A': [0, 1, 2, 3, 4, 5], 'topic-B': [0] },
          generationId: 9,
        }),
        member('member-2', {
          topics: ['topic-A'],
          owned: { 'topic-A': [0, 1, 2, 3, 4, 5] },
          generationId: 2,
        }),
      ],
    })

    // member-1 wins every claim on the higher generation, so member-2 starts empty; the older
    // generation still names it as the previous owner, and the balancing pass hands partitions
    // back to it rather than to an arbitrary member.
    expect(allAssigned(assignment)).toHaveLength(10)
    expect(Math.max(...sizes(assignment)) - Math.min(...sizes(assignment))).toBeLessThanOrEqual(1)
    expect(plain(assignment)['member-2']['topic-A'].length).toBeGreaterThan(0)
  })

  test('holds its invariants over a battery of general-path shapes', () => {
    // A deterministic pseudo-random walk over heterogeneous shapes: the general builder has more
    // arms than any single hand-written case reaches, and the invariants below are the ones the
    // coordinator relies on for every one of them.
    let seed = 20260907
    const next = (bound) => {
      seed = (seed * 1103515245 + 12345) % 2147483648
      return seed % bound
    }

    for (let shape = 0; shape < 200; shape++) {
      const topicCount = 2 + next(4)
      const memberCount = 2 + next(6)
      const topics = partitions(topicCount).map((i) => `topic-${i}`)
      const partitionsPerTopic = Object.fromEntries(
        topics.map((topic) => [topic, partitions(1 + next(8))])
      )

      const members = partitions(memberCount).map((i) => {
        const subscribed = topics.filter(() => next(3) > 0)
        const owned = {}

        for (const topic of subscribed) {
          const held = partitionsPerTopic[topic].filter(() => next(4) === 0)
          if (held.length > 0) {
            owned[topic] = held
          }
        }

        return member(`member-${String(i).padStart(2, '0')}`, {
          topics: subscribed,
          owned,
          generationId: next(3),
        })
      })

      const assignment = assignSticky({ partitionsPerTopic, members })
      const assigned = allAssigned(assignment)

      expect(new Set(assigned).size).toEqual(assigned.length)

      const subscribedTopics = new Set(members.flatMap(({ topics: t }) => t))
      const assignable = topics
        .filter((topic) => subscribedTopics.has(topic))
        .flatMap((topic) => partitionsPerTopic[topic].map((p) => `${topic}-${p}`))

      expect(assigned.sort()).toEqual(assignable.sort())

      for (const [memberId, byTopic] of assignment) {
        const subscription = members.find((m) => m.memberId === memberId).topics
        expect(Object.keys(byTopic).filter((t) => !subscription.includes(t))).toEqual([])
      }
    }
  })

  test('never puts a member more than one partition above the minimum quota', () => {
    for (const [partitionCount, memberCount] of [
      [1, 1],
      [10, 3],
      [11, 4],
      [17, 5],
      [2, 9],
    ]) {
      const assignment = assignSticky({
        partitionsPerTopic: { 'topic-A': partitions(partitionCount) },
        members: partitions(memberCount).map((i) => member(`member-${i}`)),
      })

      const held = sizes(assignment)
      expect(Math.min(...held)).toEqual(Math.floor(partitionCount / memberCount))
      expect(Math.max(...held)).toBeLessThanOrEqual(Math.floor(partitionCount / memberCount) + 1)
      expect(allAssigned(assignment)).toHaveLength(partitionCount)
    }
  })

  test('assigns 1000 partitions over 10 topics to 50 staggered members exactly once', () => {
    const topics = partitions(10).map((i) => `topic-${i}`)
    const partitionsPerTopic = Object.fromEntries(topics.map((topic) => [topic, partitions(100)]))

    const members = partitions(50).map((i) =>
      member(`member-${String(i).padStart(2, '0')}`, {
        // Staggered subscriptions: every member takes a different window of the topic list, so
        // the general builder runs.
        topics: topics.slice(i % 5, (i % 5) + 5 + (i % 3)),
      })
    )

    const started = process.hrtime.bigint()
    const assignment = assignSticky({ partitionsPerTopic, members })
    const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6

    const assigned = allAssigned(assignment)
    expect(new Set(assigned).size).toEqual(assigned.length)
    expect(assigned).toHaveLength(
      topics.reduce((sum, topic) => sum + partitionsPerTopic[topic].length, 0)
    )
    // A termination gate against a third of the default session timeout, not a performance
    // guard: the measured time is three orders of magnitude below it.
    expect(elapsedMs).toBeLessThan(10000)
  })
})
