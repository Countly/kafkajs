const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const { compareMembers } = require('../memberInfo')

/**
 * Java's `RangeAssignor`, under its wire name.
 *
 * Per topic: sort the subscribed members, divide the partition count by the member count, and
 * hand out contiguous ranges in ascending partition order, the first `partitions % members`
 * members getting one extra. Co-partitioned topics therefore line up, which is the property this
 * assignor exists for.
 *
 * Rack-aware co-partitioning is deliberately not implemented: `rackId` is sent on the wire and
 * ignored here.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/RangeAssignor.java
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'range',
  version: 3,

  /** @returns {string[]} */
  supportedProtocols() {
    return ['eager']
  },

  /**
   * @param {{ members: import('types').GroupMember[], topics: string[] }} group
   * @returns {Promise<import('types').GroupMemberAssignment[]>}
   */
  async assign({ members, topics }) {
    const decoded = members
      .map((member) => ({
        memberId: member.memberId,
        groupInstanceId: member.groupInstanceId == null ? null : member.groupInstanceId,
        metadata: MemberMetadata.decode(member.memberMetadata),
      }))
      .sort(compareMembers)

    const assignment = new Map(decoded.map(({ memberId }) => [memberId, {}]))

    for (const topic of topics) {
      const subscribed = decoded.filter(({ metadata }) => metadata.topics.includes(topic))

      if (subscribed.length === 0) {
        continue
      }

      const partitions = cluster
        .findTopicPartitionMetadata(topic)
        .map(({ partitionId }) => partitionId)
        .sort((a, b) => a - b)

      const n = Math.floor(partitions.length / subscribed.length)
      const extra = partitions.length % subscribed.length

      let cursor = 0
      subscribed.forEach(({ memberId }, index) => {
        const size = index < extra ? n + 1 : n

        if (size === 0) {
          return
        }

        assignment.get(memberId)[topic] = partitions.slice(cursor, cursor + size)
        cursor += size
      })
    }

    // Every member gets an entry, empty ones included: a member the leader leaves out of SyncGroup
    // is handed a zero-length assignment buffer by the coordinator, which the Java client cannot
    // decode - it reads the version off the buffer first.
    return [...assignment].map(([memberId, byTopic]) => ({
      memberId,
      memberAssignment: MemberAssignment.encode({ version: this.version, assignment: byTopic }),
    }))
  },

  /**
   * `range` holds no state, so its userData is always the wire NULL - which is what Java's
   * `RangeAssignor` sends, and what the goldens therefore contain. Returning `undefined` here
   * would write an empty byte string and never match them.
   * @returns {null}
   */
  subscriptionUserData() {
    return null
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
})
