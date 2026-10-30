const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const partitionMetadata = require('../partitionMetadata')

/**
 * RoundRobinAssigner: deals the partitions of `topics`, one topic after another in metadata order,
 * over the members sorted by id. A partition goes to the next member in that circular order that
 * subscribes to its topic, as Java's `RoundRobinAssignor` does; the deal carries on after the
 * member that took it. When every member subscribes to every topic, this is the plain deal by
 * position. A topic no member subscribes to is skipped. A member that sent no metadata is taken to
 * subscribe to every topic.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/RoundRobinAssignor.java
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'RoundRobinAssigner',
  version: 0,

  /**
   * Assign the topics to the provided members.
   *
   * The members array contains information about each member, `memberMetadata` is the result of the
   * `protocol` operation.
   *
   * @param {object} group
   * @param {import('types').GroupMember[]} group.members array of members, e.g:
                              [{ memberId: 'test-5f93f5a3', memberMetadata: Buffer }]
   * @param {string[]} group.topics
   * @returns {Promise<import('types').GroupMemberAssignment[]>} object partitions per topic per member, e.g:
   *                   [
   *                     {
   *                       memberId: 'test-5f93f5a3',
   *                       memberAssignment: {
   *                         'topic-A': [0, 2, 4, 6],
   *                         'topic-B': [1],
   *                       },
   *                     },
   *                     {
   *                       memberId: 'test-3d3d5341',
   *                       memberAssignment: {
   *                         'topic-A': [1, 3, 5],
   *                         'topic-B': [0, 2],
   *                       },
   *                     }
   *                   ]
   *   A member that is dealt nothing is left out.
   * @param {string[]} [group.groupTopics] sorted union; own topics stays unchanged
   * @param {ReadonlyMap<string, object[]>} [group.groupTopicMetadata] authoritative when present
   */
  async assign({ members, topics, groupTopics, groupTopicMetadata }) {
    const membersCount = members.length
    const sortedMembers = members.map(({ memberId }) => memberId).sort()
    const subscriptions = new Map(
      members.map(({ memberId, memberMetadata }) => [
        memberId,
        memberMetadata == null ? null : new Set(MemberMetadata.decode(memberMetadata).topics),
      ])
    )
    /** @param {string} memberId @param {string} topic @returns {boolean} */
    const subscribes = (memberId, topic) => {
      const subscribed = subscriptions.get(memberId)
      return subscribed === null || subscribed.has(topic)
    }
    const assignment = {}
    let cursor = 0

    for (const topic of groupTopics ?? topics) {
      if (!sortedMembers.some((memberId) => subscribes(memberId, topic))) {
        continue
      }

      for (const { partitionId } of partitionMetadata(cluster, groupTopicMetadata, topic)) {
        while (!subscribes(sortedMembers[cursor % membersCount], topic)) {
          cursor++
        }
        const assignee = sortedMembers[cursor++ % membersCount]

        if (!assignment[assignee]) {
          assignment[assignee] = Object.create(null)
        }

        if (!assignment[assignee][topic]) {
          assignment[assignee][topic] = []
        }

        assignment[assignee][topic].push(partitionId)
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

  protocol({ topics }) {
    return {
      name: this.name,
      metadata: MemberMetadata.encode({
        version: this.version,
        topics,
      }),
    }
  },
})
