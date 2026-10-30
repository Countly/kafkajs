const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const { compareMembers } = require('../memberInfo')
const dictionary = require('../../../utils/dictionary')
const partitionMetadata = require('../partitionMetadata')

/**
 * One topic's remaining range quotas and optional replica-rack matches. Extra slots are consumed
 * when a member exceeds the base quota, as in Java; they are not reserved for an earlier member.
 */
class TopicAssignmentState {
  /**
   * @param {object} options topic, sorted members, decoded partitions, broker racks and opt-in flag
   * @returns {TopicAssignmentState} assignment-local state; never retained by the factory
   */
  constructor({ topic, members, partitions, brokers, rackAware }) {
    this.topic = topic
    this.members = members
    this.partitions = partitions.map(({ partitionId }) => partitionId).sort((a, b) => a - b)
    this.remaining = new Set(this.partitions)
    this.assigned = new Map(members.map(({ memberId }) => [memberId, 0]))
    this.racks = new Map(
      members.map(({ memberId, metadata }) => [memberId, metadata.rackId || null])
    )
    this.base = members.length ? Math.floor(partitions.length / members.length) : 0
    this.extra = members.length ? partitions.length % members.length : 0
    this.partitionRacks = new Map(
      partitions.map(({ partitionId, replicas = [], replicaRacks }) => [
        partitionId,
        new Set(
          replicaRacks || replicas.map((id) => brokers.get(id)).filter((rack) => rack != null)
        ),
      ])
    )
    const consumers = new Set([...this.racks.values()].filter((rack) => rack != null))
    const all = new Set([...this.partitionRacks.values()].flatMap((racks) => [...racks]))
    this.needsRackMatching =
      rackAware &&
      consumers.size > 0 &&
      [...consumers].some((rack) => all.has(rack)) &&
      [...this.partitionRacks.values()].some((racks) => racks.size !== all.size)
  }

  /**
   * @param {string} member member id
   * @returns {number} remaining legal slots; balance always limits rack alignment
   */
  maxAssignable(member) {
    return Math.max(0, this.base + (this.extra > 0 ? 1 : 0) - this.assigned.get(member))
  }

  /**
   * @param {string} member member id
   * @param {number} partition partition id
   * @returns {boolean} true for a missing member rack or a matching replica rack
   */
  racksMatch(member, partition) {
    const rack = this.racks.get(member)
    return rack == null || Boolean(this.partitionRacks.get(partition)?.has(rack))
  }

  /**
   * Consumes quota only after successful assignment, keeping all co-partitioned topics together.
   * @param {string} member member id
   * @param {number[]} partitions selected ascending partition ids
   * @param {Map<string, object>} assignment output map
   * @returns {void}
   */
  assign(member, partitions, assignment) {
    if (!partitions.length) return
    const byTopic = assignment.get(member)
    if (!byTopic[this.topic]) byTopic[this.topic] = []
    for (const partition of partitions) byTopic[this.topic].push(partition)
    const count = this.assigned.get(member) + partitions.length
    this.assigned.set(member, count)
    if (count > this.base) this.extra -= 1
    for (const partition of partitions) this.remaining.delete(partition)
  }

  /**
   * @param {Map<string, object>} assignment output map
   * @param {boolean} matchRacks filter by rack before the ordinary range pass
   * @returns {void}
   */
  assignRanges(assignment, matchRacks) {
    for (const { memberId } of this.members) {
      const partitions = [...this.remaining]
        .filter((partition) => !matchRacks || this.racksMatch(memberId, partition))
        .slice(0, this.maxAssignable(memberId))
      this.assign(memberId, partitions, assignment)
    }
  }
}

/**
 * Java's `RangeAssignor`, under its wire name.
 *
 * Per topic: sort the subscribed members, divide the partition count by the member count, and
 * hand out contiguous ranges in ascending partition order, the first `partitions % members`
 * members getting one extra. Co-partitioned topics therefore line up, which is the property this
 * assignor exists for.
 *
 * Rack matching is opt-in. Quotas and co-partitioning follow the Java range algorithm;
 * replica racks include offline and out-of-ISR replicas, independently of Fetch routing.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/RangeAssignor.java
 * @param {{cluster: object, rackAware?: boolean}} options metadata source and opt-in range flag
 * @returns {import('types').Assigner} stateless range assignment factory result
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster, rackAware = false }) => ({
  name: 'range',
  version: 3,

  /** @returns {string[]} */
  supportedProtocols() {
    return ['eager']
  },

  /**
   * @param {{ members: import('types').GroupMember[], topics: string[], groupTopics?: string[],
   *   groupTopicMetadata?: ReadonlyMap<string, object[]> }} group authoritative round metadata
   * @returns {Promise<import('types').GroupMemberAssignment[]>}
   * @param {string[]} [group.groupTopics] sorted union; own topics stays unchanged
   * @param {ReadonlyMap<string, object[]>} [group.groupTopicMetadata] authoritative when present
   */
  async assign({ members, topics, groupTopics, groupTopicMetadata }) {
    const decoded = members
      .map((member) => ({
        memberId: member.memberId,
        groupInstanceId: member.groupInstanceId == null ? null : member.groupInstanceId,
        metadata: MemberMetadata.decode(member.memberMetadata),
      }))
      .sort(compareMembers)

    const assignment = new Map(decoded.map(({ memberId }) => [memberId, dictionary()]))

    const brokers = new Map(
      (cluster.brokerPool?.metadata?.brokers || []).map(({ nodeId, rack }) => [nodeId, rack])
    )
    const states = (groupTopics || topics).map(
      (topic) =>
        new TopicAssignmentState({
          topic,
          members: decoded.filter(({ metadata }) => metadata.topics.includes(topic)),
          partitions: partitionMetadata(cluster, groupTopicMetadata, topic),
          brokers,
          rackAware,
        })
    )
    const groups = new Map()
    for (const state of states) {
      const key = JSON.stringify([
        state.members.map(({ memberId }) => memberId),
        state.partitions.length,
      ])
      if (!groups.has(key)) groups.set(key, [])
      groups.get(key).push(state)
    }
    const useRackAware = states.some((state) => state.needsRackMatching)
    if (useRackAware)
      for (const group of groups.values()) {
        if (group.length === 1) {
          if (group[0].needsRackMatching) group[0].assignRanges(assignment, true)
        } else {
          const first = group[0]
          for (const partition of first.partitions) {
            const member = first.members.find(({ memberId }) =>
              group.every(
                (state) =>
                  state.maxAssignable(memberId) > 0 && state.racksMatch(memberId, partition)
              )
            )
            if (member)
              for (const state of group) state.assign(member.memberId, [partition], assignment)
          }
        }
      }
    for (const state of states) state.assignRanges(assignment, false)
    if (useRackAware)
      for (const byTopic of assignment.values()) {
        for (const partitions of Object.values(byTopic)) partitions.sort((a, b) => a - b)
      }

    // Every member gets an entry, empty ones included: a member the leader leaves out of SyncGroup
    // is handed a zero-length assignment buffer by the coordinator, which the Java client cannot
    // decode - it reads the version off the buffer first.
    return [...assignment].map(([memberId, byTopic]) => ({
      memberId,
      memberAssignment: MemberAssignment.encode({
        version: this.version,
        assignment: byTopic,
        ...(rackAware ? { userData: null } : {}),
      }),
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
