const Encoder = require('../../../protocol/encoder')
const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const partitionMetadata = require('../partitionMetadata')
const { assignSticky, DEFAULT_GENERATION } = require('../stickyAssigner/stickyCore')
const dictionary = require('../../../utils/dictionary')
const {
  CooperativeOwnership,
  claimGeneration,
  COOPERATIVE_STICKY,
} = require('../cooperativeOwnership')

/**
 * Java's `CooperativeStickyAssignor`, under its wire name.
 *
 * The assignment itself is the sticky one; what makes it cooperative is a single subtraction
 * afterwards, by the rule of {@link CooperativeOwnership}: a partition is withheld this round from
 * any member other than its one live owner, the member claiming it at the highest generation. The
 * owner losing it revokes it and triggers a follow-up round, and only then does the new owner get
 * it. A partition two members claim at the highest generation is withheld from everyone until the
 * next round says who owns it. A brand-new partition, or one whose previous owner has left, is
 * handed over immediately. The group leader checks every plan by the same rule.
 *
 * Ownership comes from the subscription's `OwnedPartitions`, not from userData: under the
 * cooperative protocol a member still holds its partitions while it rejoins, so the protocol
 * field is the live truth. The four-byte userData carries only the generation, and is read only
 * when the subscription carries no `GenerationId` (see {@link claimGeneration}).
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/CooperativeStickyAssignor.java
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: COOPERATIVE_STICKY,
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
        generationId: claimGeneration(metadata, { userDataGeneration: true }),
      }
    })

    const partitionsPerTopic = dictionary()
    for (const topic of groupTopics ?? topics) {
      partitionsPerTopic[topic] = partitionMetadata(cluster, groupTopicMetadata, topic).map(
        ({ partitionId }) => partitionId
      )
    }

    const assignment = assignSticky({ partitionsPerTopic, members: decoded })
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
   * Removes every partition the {@link CooperativeOwnership} rule does not let its member hold
   * this round: one another member owns, or one two members claim at the highest generation.
   * @private
   * @param {Array<{memberId: string, generationId: number,
   *   ownedPartitions: Array<{topic: string, partitions: number[]}>}>} members
   * @param {Map<string, {[topic: string]: number[]}>} assignment mutated in place
   * @returns {void}
   */
  withholdTransferringPartitions(members, assignment) {
    const ownership = new CooperativeOwnership(members)

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
