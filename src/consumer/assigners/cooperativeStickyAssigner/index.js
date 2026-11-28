const Encoder = require('../../../protocol/encoder')
const Decoder = require('../../../protocol/decoder')
const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const partitionMetadata = require('../partitionMetadata')
const { assignSticky, DEFAULT_GENERATION } = require('../stickyAssigner/stickyCore')
const dictionary = require('../../../utils/dictionary')

/**
 * The generation a member claims its owned partitions in: the subscription's `GenerationId` when
 * it is 0 or more (Java turns a negative one into an absent generation), otherwise the generation
 * this assigner writes as its four-byte userData, which is all a version-1 subscription carries.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/clients/consumer/CooperativeStickyAssignor.java
 * @param {{ generationId?: number, userData?: Buffer|null }} subscription decoded
 * @returns {number} `DEFAULT_GENERATION` when neither is readable
 */
const generationOf = ({ generationId, userData }) => {
  if (generationId != null && generationId >= 0) return generationId
  return userData == null || userData.length < 4
    ? DEFAULT_GENERATION
    : new Decoder(userData).readInt32()
}

/**
 * Who may hold each partition after this assigner's round, from the members' claims:
 *
 * | Claims on the partition                       | May be assigned it      | Must wait a round  |
 * |-----------------------------------------------|-------------------------|--------------------|
 * | none                                          | any one member          | nobody             |
 * | one member at the highest claimed generation  | that member (retention) | every other member |
 * | two or more members at the highest generation | nobody                  | every member       |
 *
 * A lower-generation claim is stale: it neither keeps the partition nor counts as a tie. The
 * outcome depends only on the claims, never on the order the members are listed in. Private to
 * this assigner: ranking claims is an assigner's choice, and the group leader does not rank them.
 */
class LiveOwners {
  /**
   * @param {Array<{ memberId: string, generationId: number,
   *   ownedPartitions: Array<{topic: string, partitions: number[]}> }>} members
   */
  constructor(members) {
    /** @type {Map<string, Map<number, { generation: number, members: Set<string> }>>} */
    this.top = new Map()

    for (const { memberId, generationId, ownedPartitions } of members) {
      for (const { topic, partitions } of ownedPartitions) {
        let byPartition = this.top.get(topic)
        if (byPartition === undefined) this.top.set(topic, (byPartition = new Map()))

        for (const partition of partitions) {
          const top = byPartition.get(partition)
          if (top === undefined || generationId > top.generation) {
            byPartition.set(partition, { generation: generationId, members: new Set([memberId]) })
          } else if (generationId === top.generation) {
            top.members.add(memberId)
          }
        }
      }
    }
  }

  /**
   * @param {string} memberId
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean} whether the table above lets this member be assigned the partition
   */
  mayHold(memberId, topic, partition) {
    const top = this.top.get(topic)?.get(partition)
    return top === undefined || (top.members.size === 1 && top.members.has(memberId))
  }
}

/**
 * Java's `CooperativeStickyAssignor`, under its wire name.
 *
 * The assignment itself is the sticky one, in which a newer claim ends an older tie. What makes it
 * cooperative is a single subtraction afterwards, by the {@link LiveOwners} table: a partition is
 * withheld this round from any member other than its one live owner, the member claiming it at the
 * highest generation. The owner losing it revokes it and triggers a follow-up round, and only then
 * does the new owner get it. A partition two members claim at the highest generation is withheld
 * from everyone until the next round says who owns it. A brand-new partition, or one whose
 * previous owner has left, is handed over immediately. Every plan it returns therefore gives a
 * partition only to a member that claims it, or to one member when nobody does, which is all the
 * group leader checks.
 *
 * Ownership comes from the subscription's `OwnedPartitions`, not from userData: under the
 * cooperative protocol a member still holds its partitions while it rejoins, so the protocol
 * field is the live truth. The four-byte userData carries only the generation, and is read only
 * when the subscription carries no `GenerationId`.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/CooperativeStickyAssignor.java
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'cooperative-sticky',
  version: 3,

  /** Set by `onAssignment`; the only state this assigner retains. */
  generationId: DEFAULT_GENERATION,

  /** @returns {string[]} cooperative first, so it wins the intersection when every peer has it */
  supportedProtocols() {
    return ['cooperative', 'eager']
  },

  /**
   * @param {{ members: import('types').GroupMember[], topics: string[] }} group
   * @returns {Promise<import('types').GroupMemberAssignment[]>}
   * @param {string[]} [group.groupTopics] sorted union; own topics stays unchanged
   * @param {ReadonlyMap<string, object[]>} [group.groupTopicMetadata] authoritative when present
   */
  async assign({ members, topics, groupTopics, groupTopicMetadata }) {
    const decoded = members.map((member) => {
      const metadata = MemberMetadata.decode(member.memberMetadata)
      const ownedPartitions = metadata.ownedPartitions || []

      return {
        memberId: member.memberId,
        groupInstanceId: member.groupInstanceId == null ? null : member.groupInstanceId,
        topics: metadata.topics,
        ownedPartitions,
        generationId: generationOf(metadata),
      }
    })

    const partitionsPerTopic = dictionary()
    for (const topic of groupTopics ?? topics) {
      partitionsPerTopic[topic] = partitionMetadata(cluster, groupTopicMetadata, topic).map(
        ({ partitionId }) => partitionId
      )
    }

    const assignment = assignSticky({
      partitionsPerTopic,
      members: decoded,
      tiesYieldToNewerClaims: true,
    })
    this.withholdTransferringPartitions(decoded, assignment)

    return [...assignment].map(([memberId, byTopic]) => ({
      memberId,
      memberAssignment: MemberAssignment.encode({
        version: this.version,
        assignment: byTopic,
      }),
    }))
  },

  /**
   * Removes every partition the {@link LiveOwners} table does not let its member hold this round:
   * one another member owns, or one two members claim at the highest generation.
   * @private
   * @param {Array<{memberId: string, generationId: number,
   *   ownedPartitions: Array<{topic: string, partitions: number[]}>}>} members
   * @param {Map<string, {[topic: string]: number[]}>} assignment mutated in place
   * @returns {void}
   */
  withholdTransferringPartitions(members, assignment) {
    const ownership = new LiveOwners(members)

    for (const [memberId, byTopic] of assignment) {
      for (const topic of Object.keys(byTopic)) {
        const kept = byTopic[topic].filter((p) => ownership.mayHold(memberId, topic, p))

        if (kept.length === 0) {
          delete byTopic[topic]
        } else {
          byTopic[topic] = kept
        }
      }
    }
  },

  /**
   * This member's own `userData`: exactly four bytes, the generation it last held.
   * @returns {Buffer}
   */
  subscriptionUserData() {
    return new Encoder().writeInt32(this.generationId).buffer
  },

  /**
   * @param {{ topics: string[], ownedPartitions: Array<{topic: string, partitions: number[]}>,
   *   generationId: number, rackId: string|null }} subscription
   * @returns {{ name: string, metadata: Buffer }}
   */
  protocol({ topics, ownedPartitions, generationId, rackId }) {
    return {
      name: this.name,
      metadata: MemberMetadata.encode({
        version: this.version,
        topics,
        ownedPartitions,
        generationId,
        rackId,
        userData: this.subscriptionUserData(topics),
      }),
    }
  },

  /**
   * Retains the generation only - the partitions travel in the subscription's `OwnedPartitions`.
   * A `null` assignment means the member left the group or lost its generation.
   * @param {object|null} assignment
   * @param {{ generationId: number }} metadata
   * @returns {void}
   */
  onAssignment(assignment, { generationId }) {
    this.generationId = assignment == null ? DEFAULT_GENERATION : generationId
  },
})
