const RoundRobinAssigner = require('./index')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')
const { seeded } = require('../../../../scripts/benchmarks/stickyAssign')

/**
 * The assigner as it was before it read the members' subscriptions: every partition of every
 * topic in `topics`, dealt over the members sorted by id, leaving out a member dealt nothing. Kept
 * as the reference the uniform case must match byte for byte for every member that is dealt one.
 * @param {{ findTopicPartitionMetadata: (topic: string) => Array<{ partitionId: number }> }} cluster
 * @param {{ members: Array<{ memberId: string }>, topics: string[] }} group
 * @returns {Array<{ memberId: string, memberAssignment: Buffer }>}
 */
const previousAssign = (cluster, { members, topics }) => {
  const membersCount = members.length
  const sortedMembers = members.map(({ memberId }) => memberId).sort()
  const assignment = {}

  const topicsPartitions = topics.flatMap((topic) => {
    const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
    return partitionMetadata.map((m) => ({ topic, partitionId: m.partitionId }))
  })

  topicsPartitions.forEach((topicPartition, i) => {
    const assignee = sortedMembers[i % membersCount]

    if (!assignment[assignee]) {
      assignment[assignee] = Object.create(null)
    }

    if (!assignment[assignee][topicPartition.topic]) {
      assignment[assignee][topicPartition.topic] = []
    }

    assignment[assignee][topicPartition.topic].push(topicPartition.partitionId)
  })

  return Object.keys(assignment).map((memberId) => ({
    memberId,
    memberAssignment: MemberAssignment.encode({ version: 0, assignment: assignment[memberId] }),
  }))
}

/**
 * @param {string} memberId
 * @param {string[]} topics what the member subscribes to
 * @returns {{ memberId: string, memberMetadata: Buffer }} a member as the leader receives it
 */
const member = (memberId, topics) => ({
  memberId,
  memberMetadata: MemberMetadata.encode({ version: 0, topics }),
})

/**
 * @param {Array<{ memberId: string, memberAssignment: Buffer }>} result
 * @returns {{[memberId: string]: {[topic: string]: number[]}}}
 */
const decoded = (result) =>
  Object.fromEntries(
    result.map(({ memberId, memberAssignment }) => [
      memberId,
      { ...MemberAssignment.decode(memberAssignment).assignment },
    ])
  )

/**
 * @param {(bound: number) => number} next
 * @param {T[]} items
 * @returns {T[]} the items in a seeded random order
 * @template T
 */
const shuffled = (next, items) => {
  const copy = [...items]
  for (let i = copy.length - 1; i > 0; i--) {
    const j = next(i + 1)
    ;[copy[i], copy[j]] = [copy[j], copy[i]]
  }
  return copy
}

/**
 * Draws a group: 1 to 6 topics of 1 to 16 partitions each, listed in a shuffled metadata order,
 * and 1 to 8 members in a shuffled order.
 * @param {(bound: number) => number} next
 * @param {(topics: string[], i: number) => string[]} subscribe member i's subscription
 * @returns {{ metadata: object, topics: string[], members: object[] }}
 */
const randomGroup = (next, subscribe) => {
  const topics = Array.from({ length: 1 + next(6) }, (_, i) => `topic-${i}`)
  const metadata = Object.fromEntries(
    topics.map((topic) => [
      topic,
      shuffled(
        next,
        Array.from({ length: 1 + next(16) }, (_, partitionId) => ({ partitionId }))
      ),
    ])
  )
  const members = shuffled(
    next,
    Array.from({ length: 1 + next(8) }, (_, i) =>
      member(`member-${next(100)}-${i}`, subscribe(topics, i))
    )
  )

  return { metadata, topics, members }
}

describe('Consumer > assigners > RoundRobinAssigner > subscriptions', () => {
  let metadata, assigner

  beforeEach(() => {
    metadata = {}
    assigner = RoundRobinAssigner({
      cluster: { findTopicPartitionMetadata: (topic) => metadata[topic] },
    })
  })

  test('hands out the same buffers as before whenever every member subscribes to every topic', async () => {
    const next = seeded(11)

    for (let round = 0; round < 300; round++) {
      const group = randomGroup(next, (topics) =>
        shuffled(next, [...topics, ...(next(2) === 0 ? ['topic-elsewhere'] : [])])
      )
      metadata = group.metadata

      const result = await assigner.assign(group)

      const dealt = previousAssign(
        { findTopicPartitionMetadata: (topic) => metadata[topic] },
        group
      )
      const empty = MemberAssignment.encode({ version: 0, assignment: {} })
      expect(result).toEqual(
        group.members
          .map(({ memberId }) => memberId)
          .sort()
          .map(
            (memberId) =>
              dealt.find((entry) => entry.memberId === memberId) ?? {
                memberId,
                memberAssignment: empty,
              }
          )
      )
    }
  })

  test('gives each partition to the next member in id order that subscribes to its topic', async () => {
    metadata['topic-A'] = [{ partitionId: 0 }, { partitionId: 1 }]
    metadata['topic-B'] = [{ partitionId: 0 }, { partitionId: 1 }, { partitionId: 2 }]
    const members = [
      member('member-3', ['topic-A', 'topic-B']),
      member('member-1', ['topic-A']),
      member('member-2', ['topic-A', 'topic-B']),
    ]

    const result = await assigner.assign({ members, topics: ['topic-A', 'topic-B'] })

    // topic-B 1 skips member-1 and goes to member-2, and the deal carries on after member-2.
    expect(decoded(result)).toEqual({
      'member-1': { 'topic-A': [0] },
      'member-2': { 'topic-A': [1], 'topic-B': [1] },
      'member-3': { 'topic-B': [0, 2] },
    })
  })

  test('on random mixed subscriptions, gives each partition once, to a subscriber, evenly per topic', async () => {
    const next = seeded(5)

    for (let round = 0; round < 300; round++) {
      const group = randomGroup(next, (topics) => topics.filter(() => next(3) > 0))
      metadata = group.metadata
      const subscriptions = new Map(
        group.members.map(({ memberId, memberMetadata }) => [
          memberId,
          MemberMetadata.decode(memberMetadata).topics,
        ])
      )

      const assignment = decoded(await assigner.assign(group))

      for (const topic of group.topics) {
        const subscribers = [...subscriptions].filter(([, topics]) => topics.includes(topic))
        const held = subscribers.map(([memberId]) => (assignment[memberId] || {})[topic] || [])
        const dealt = held.flat().sort((a, b) => a - b)
        const expected = subscribers.length === 0 ? [] : metadata[topic].map((p) => p.partitionId)

        expect(dealt).toEqual(expected.sort((a, b) => a - b))
        if (held.length > 0) {
          const sizes = held.map((partitions) => partitions.length)
          expect(Math.max(...sizes) - Math.min(...sizes)).toBeLessThanOrEqual(1)
        }
      }

      for (const [memberId, byTopic] of Object.entries(assignment)) {
        Object.keys(byTopic).forEach((topic) =>
          expect(subscriptions.get(memberId)).toContain(topic)
        )
      }
    }
  })

  test('skips a topic no member subscribes to', async () => {
    metadata['topic-A'] = [{ partitionId: 0 }, { partitionId: 1 }]
    metadata['topic-X'] = [{ partitionId: 0 }]
    const members = [member('member-1', ['topic-A']), member('member-2', ['topic-A'])]

    const result = await assigner.assign({ members, topics: ['topic-X', 'topic-A'] })

    expect(decoded(result)).toEqual({
      'member-1': { 'topic-A': [0] },
      'member-2': { 'topic-A': [1] },
    })
  })

  test('takes a member that sent no metadata to subscribe to every topic', async () => {
    metadata['topic-A'] = [{ partitionId: 0 }]
    metadata['topic-B'] = [{ partitionId: 0 }, { partitionId: 1 }]
    const members = [{ memberId: 'member-1' }, member('member-2', ['topic-A'])]

    const result = await assigner.assign({ members, topics: ['topic-A', 'topic-B'] })

    expect(decoded(result)).toEqual({
      'member-1': { 'topic-A': [0], 'topic-B': [0, 1] },
      'member-2': {},
    })
  })
})
