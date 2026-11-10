const StickyAssigner = require('../stickyAssigner')
const CooperativeStickyAssigner = require('../cooperativeStickyAssigner')
const { stickyUserData } = require('../stickyAssigner/stickyUserData')
const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const Encoder = require('../../../protocol/encoder')

const TOPIC = 'topic-A'
const range = (from, to) => Array.from({ length: to - from + 1 }, (_, offset) => from + offset)

/** Builds one subscription of an assignor, owning `owned` partitions (null: a new member). */
const SUBSCRIPTIONS = {
  sticky: (memberId, owned, generationId) => ({
    memberId,
    memberMetadata: MemberMetadata.encode({
      version: 3,
      topics: [TOPIC],
      userData:
        owned === null
          ? null
          : stickyUserData.encode({
              assignment: [{ topic: TOPIC, partitions: owned }],
              generationId,
            }),
      ownedPartitions: [],
      generationId,
    }),
  }),
  'cooperative-sticky': (memberId, owned, generationId) => ({
    memberId,
    memberMetadata: MemberMetadata.encode({
      version: 3,
      topics: [TOPIC],
      userData: new Encoder().writeInt32(generationId).buffer,
      ownedPartitions: owned === null ? [] : [{ topic: TOPIC, partitions: owned }],
      generationId,
    }),
  }),
}
const ASSIGNORS = { sticky: StickyAssigner, 'cooperative-sticky': CooperativeStickyAssigner }

/**
 * One rebalance of two members with equal subscriptions over one topic.
 * @param {'sticky'|'cooperative-sticky'} name
 * @param {number} partitionCount partitions of the topic
 * @param {{ A: number[]|null, B: number[]|null }} prior what each member owned, null for a new member
 * @param {number} generationId the generation both members claim their partitions in
 * @returns {Promise<{ A: number[], B: number[] }>} the sorted partitions each member is given
 */
const rebalance = async (name, partitionCount, prior, generationId) => {
  const cluster = {
    findTopicPartitionMetadata: () =>
      range(0, partitionCount - 1).map((partitionId) => ({ partitionId })),
  }
  const assignor = ASSIGNORS[name]({ cluster })
  const result = await assignor.assign({
    members: ['A', 'B'].map((id) => SUBSCRIPTIONS[name](id, prior[id], generationId)),
    topics: [TOPIC],
  })
  const given = { A: [], B: [] }
  for (const { memberId, memberAssignment } of result) {
    const partitions = MemberAssignment.decode(memberAssignment).assignment[TOPIC] ?? []
    given[memberId] = [...partitions].sort((a, b) => a - b)
  }
  return given
}

/** Asserts no partition is given to both members, and that all of them are given when `complete`. */
const expectOwnedOnce = (given, partitionCount, complete = true) => {
  const all = [...given.A, ...given.B].sort((a, b) => a - b)
  expect(new Set(all).size).toBe(all.length)
  if (complete) expect(all).toEqual(range(0, partitionCount - 1))
}

/*
 * Fork-constructed from the Kafka Streams StickyTaskAssignor tests of apache/kafka PR #22006
 * (KAFKA-20198). The upstream defect is in Streams and the consumer sticky assignors are unchanged
 * between 4.3.2 and 4.4.0, so these cases show the fork's consumer quota behaviour, not Streams
 * equivalence.
 */
describe('Kafka 4.4 client fix audit', () => {
  test.each(['sticky', 'cooperative-sticky'])(
    'KAFKA-20198 %s keeps a ceiling fair share on scale-up and converges within two rounds',
    async (name) => {
      // V1: 450 partitions, A owns the lower half, B is new: A keeps all of it.
      const half = { A: range(0, 224), B: null }
      const v1 = await rebalance(name, 450, half, 1)
      expect(v1).toEqual({ A: range(0, 224), B: range(225, 449) })
      expectOwnedOnce(v1, 450)

      // V2: 451 partitions, A owns 0-225: the ceiling share of 226 stays with A, a floor share would move one.
      const odd = await rebalance(name, 451, { A: range(0, 225), B: null }, 1)
      expect(odd).toEqual({ A: range(0, 225), B: range(226, 450) })
      expectOwnedOnce(odd, 451)

      // V3: A owns everything; each result is fed back as the next prior ownership.
      const everything = range(0, 449)
      const first = await rebalance(name, 450, { A: everything, B: null }, 1)
      expectOwnedOnce(first, 450, name === 'sticky')
      const second = await rebalance(
        name,
        450,
        { A: first.A, B: first.B.length ? first.B : null },
        2
      )
      expectOwnedOnce(second, 450)
      expect(second.A).toHaveLength(225)
      expect(second.B).toHaveLength(225)
      expect(second.A.every((partition) => everything.includes(partition))).toBe(true)
      const third = await rebalance(name, 450, { A: second.A, B: second.B }, 3)
      expect(third).toEqual(second)

      // V4: 5 partitions with valid prior loads 3 and 2 sit on the ceiling boundary and stay put.
      const loads = { A: [0, 1, 2], B: [3, 4] }
      const kept = await rebalance(name, 5, loads, 1)
      expect(kept).toEqual(loads)
      expectOwnedOnce(kept, 5)
      expect(await rebalance(name, 5, kept, 2)).toEqual(kept)
    }
  )
})
