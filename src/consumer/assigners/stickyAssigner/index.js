const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const { assignSticky, DEFAULT_GENERATION } = require('./stickyCore')
const { stickyUserData } = require('./stickyUserData')
const dictionary = require('../../../utils/dictionary')

/**
 * Java's `StickyAssignor`, under its wire name.
 *
 * Sticky in both senses: it keeps a member on the partitions it already held whenever the balance
 * allows, and it is stateful - one instance lives for the whole consumer, learning what it holds
 * through `onAssignment` and reporting it back in the next subscription's `userData`.
 *
 * Ownership is read from `userData` only, never from the subscription's `OwnedPartitions`: this
 * is an eager assignor, so by the time a member rejoins it has already revoked everything and
 * `OwnedPartitions` says nothing about the generation being replaced.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/StickyAssignor.java
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'sticky',
  version: 3,

  /** Set by `onAssignment`; `null` until this member has ever been assigned anything. */
  memberAssignment: null,
  generationId: DEFAULT_GENERATION,

  /** @returns {string[]} */
  supportedProtocols() {
    return ['eager']
  },

  /**
   * @param {{ members: import('types').GroupMember[], topics: string[] }} group
   * @returns {Promise<import('types').GroupMemberAssignment[]>}
   */
  async assign({ members, topics }) {
    const decoded = members.map((member) => {
      const metadata = MemberMetadata.decode(member.memberMetadata)
      const { assignment, generationId } = stickyUserData.decode(metadata.userData)

      return {
        memberId: member.memberId,
        groupInstanceId: member.groupInstanceId == null ? null : member.groupInstanceId,
        topics: metadata.topics,
        ownedPartitions: assignment,
        generationId,
      }
    })

    const partitionsPerTopic = dictionary()
    for (const topic of topics) {
      partitionsPerTopic[topic] = cluster
        .findTopicPartitionMetadata(topic)
        .map(({ partitionId }) => partitionId)
    }

    return [...assignSticky({ partitionsPerTopic, members: decoded })].map(
      ([memberId, assignment]) => ({
        memberId,
        memberAssignment: MemberAssignment.encode({ version: this.version, assignment }),
      })
    )
  },

  /**
   * This member's own `userData`, called from inside `protocol()` - the group never consults it.
   * @returns {Buffer|null} `null` before the first assignment, which the subscription codec
   *   writes as the wire NULL; a V1 buffer afterwards
   */
  subscriptionUserData() {
    if (this.memberAssignment === null) {
      return null
    }

    return stickyUserData.encode({
      assignment: this.memberAssignment,
      generationId: this.generationId,
    })
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
   * Retains what this member was given, so the next round can ask for it back. A `null`
   * assignment means the member left the group or lost its generation, and the retained state
   * goes with it - reporting a dead generation's partitions would make this member fight the
   * live owner for them.
   * @param {{ partitions: Array<{topic: string, partitions: number[]}> }|null} assignment
   * @param {{ generationId: number }} metadata
   * @returns {void}
   */
  onAssignment(assignment, { generationId }) {
    if (assignment == null) {
      this.memberAssignment = null
      this.generationId = DEFAULT_GENERATION
      return
    }

    this.memberAssignment = assignment.partitions
    this.generationId = generationId
  },
})
