const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')

/**
 * The `roundRobin` assigner as kafkajs 2.2.4 shipped it, under its own name: every partition of
 * every topic it is given, dealt over the members sorted by id, whatever each member subscribes
 * to. A leader running it hands members partitions of topics they do not subscribe to, which is
 * the case the consumer's `RECEIVED_UNSUBSCRIBED_TOPICS` handling exists for.
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'SubscriptionBlindAssigner',
  version: 0,

  /**
   * @param {{ members: Array<{ memberId: string }>, topics: string[] }} group
   * @returns {Promise<Array<{ memberId: string, memberAssignment: Buffer }>>}
   */
  async assign({ members, topics }) {
    const sortedMembers = members.map(({ memberId }) => memberId).sort()
    const assignment = {}
    let i = 0

    for (const topic of topics) {
      for (const { partitionId } of cluster.findTopicPartitionMetadata(topic)) {
        const assignee = sortedMembers[i++ % sortedMembers.length]
        assignment[assignee] = assignment[assignee] || {}
        assignment[assignee][topic] = [...(assignment[assignee][topic] || []), partitionId]
      }
    }

    return Object.keys(assignment).map((memberId) => ({
      memberId,
      memberAssignment: MemberAssignment.encode({
        version: this.version,
        assignment: assignment[memberId],
      }),
    }))
  },

  /**
   * @param {{ topics: string[] }} subscription
   * @returns {{ name: string, metadata: Buffer }}
   */
  protocol({ topics }) {
    return { name: this.name, metadata: MemberMetadata.encode({ version: this.version, topics }) }
  },
})
