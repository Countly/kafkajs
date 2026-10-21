const Encoder = require('../../../protocol/encoder')
const Decoder = require('../../../protocol/decoder')
const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const {
  assignSticky,
  DEFAULT_GENERATION,
  keyOf,
  topicOf,
  partitionOf,
} = require('../stickyAssigner/stickyCore')
const dictionary = require('../../../utils/dictionary')

/**
 * @param {Buffer|null} userData
 * @returns {number} the generation a peer encoded in its four-byte userData, or the default when
 *   there is nothing readable there
 */
const decodeGeneration = (userData) => {
  if (userData == null || userData.length < 4) {
    return DEFAULT_GENERATION
  }

  return new Decoder(userData).readInt32()
}

/**
 * The generation a member is claiming its partitions in.
 *
 * A subscription's `GenerationId` counts only when it is non-negative: the field's wire default is
 * `-1`, and a member that lost its generation sends that while its userData still carries the last
 * generation it was actually assigned. Java draws the same line by turning any negative value into
 * an absent `Optional` before the assignor ever sees it.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.java
 * @param {{ generationId?: number, userData?: Buffer|null }} metadata decoded subscription
 * @returns {number}
 */
const generationOf = (metadata) =>
  metadata.generationId == null || metadata.generationId < 0
    ? decodeGeneration(metadata.userData)
    : metadata.generationId

/**
 * @param {Array<{topic: string, partitions: number[]}>} byTopic
 * @returns {string[]} flat partition keys, for set arithmetic
 */
const flatten = (byTopic) =>
  byTopic.flatMap(({ topic, partitions }) => partitions.map((partition) => keyOf(topic, partition)))

/**
 * Java's `CooperativeStickyAssignor`, under its wire name.
 *
 * The assignment itself is the sticky one; what makes it cooperative is a single subtraction
 * afterwards. A partition that is being added to one member *and* revoked from another is
 * withheld this round: the member losing it revokes it and triggers a follow-up round, and only
 * then does the new owner get it. So is a partition two members of the same generation both
 * claim - one of them is working from a membership it has already lost, and neither may fetch it
 * until the next round says who owns it. Nothing else is withheld - a brand-new partition, or one
 * whose previous owner has left, is handed over immediately.
 *
 * Ownership comes from the subscription's `OwnedPartitions`, not from userData: under the
 * cooperative protocol a member still holds its partitions while it rejoins, so the protocol
 * field is the live truth. The four-byte userData carries only the generation, and is read only
 * when the peer is too old to send a `GenerationId`.
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
   */
  async assign({ members, topics }) {
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
    for (const topic of topics) {
      partitionsPerTopic[topic] = cluster
        .findTopicPartitionMetadata(topic)
        .map(({ partitionId }) => partitionId)
    }

    const contested = new Set()
    const assignment = assignSticky({ partitionsPerTopic, members: decoded, contested })
    this.withholdTransferringPartitions(decoded, assignment, contested)

    return [...assignment].map(([memberId, byTopic]) => ({
      memberId,
      memberAssignment: MemberAssignment.encode({ version: this.version, assignment: byTopic }),
    }))
  },

  /**
   * Removes every partition that is simultaneously added to one member and revoked from another,
   * and every partition two members of the same generation both claimed.
   * @private
   * @param {Array<{memberId: string, ownedPartitions: Array<{topic: string, partitions: number[]}>}>} members
   * @param {Map<string, {[topic: string]: number[]}>} assignment mutated in place
   * @param {Set<string>} contested flat keys claimed twice in one generation
   * @returns {void}
   */
  withholdTransferringPartitions(members, assignment, contested) {
    const holderOf = new Map()
    const added = new Set()
    const revoked = new Set()

    for (const { memberId, ownedPartitions } of members) {
      const owned = new Set(flatten(ownedPartitions))
      const byTopic = assignment.get(memberId)
      const assigned = new Set(
        flatten(Object.keys(byTopic).map((topic) => ({ topic, partitions: byTopic[topic] })))
      )

      for (const key of assigned) {
        holderOf.set(key, memberId)

        if (!owned.has(key)) {
          added.add(key)
        }
      }

      for (const key of owned) {
        if (!assigned.has(key)) {
          revoked.add(key)
        }
      }
    }

    for (const [key, memberId] of holderOf) {
      if (!contested.has(key) && !(added.has(key) && revoked.has(key))) {
        continue
      }

      const topic = topicOf(key)
      const partition = partitionOf(key)
      const byTopic = assignment.get(memberId)
      byTopic[topic] = byTopic[topic].filter((p) => p !== partition)

      if (byTopic[topic].length === 0) {
        delete byTopic[topic]
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
