const { KafkaJSNonRetriableError } = require('../../errors')
const { readAssignmentRows } = require('../assignmentRows')

/**
 * The partitions the joined members report owning, as the group leader reads them before it asks
 * an assigner for a cooperative plan, and the one check that plan must pass. It applies to every
 * plan, whichever assigner made it.
 *
 * The leader reads only each subscription's `OwnedPartitions` (a version-0 subscription claims
 * nothing): no `GenerationId`, no userData and no protocol name. Which of several claimants keeps
 * a partition is the assigner's choice, since only the assigner can read its own metadata.
 *
 * - R1: a partition may be in only one member's assignment.
 * - R2: a partition that some member claims may go only to a member that claims it.
 *
 * Everything else passes: withholding a partition, giving an unclaimed partition to any one
 * member, and keeping a partition with any one of its claimants. A plan that passes gives each
 * partition to at most one member, and every other claimant finds it missing from its own
 * assignment, revokes it and rejoins, so no member starts on a partition another still holds.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java
 */
class OwnedPartitionClaims {
  /**
   * @param {Iterable<{ memberId: string,
   *   ownedPartitions?: Array<{topic: string, partitions: number[]}>|null }>} members one entry per
   *   joined member, with the owned partitions its subscription reported
   */
  constructor(members) {
    /** @type {Map<string, Map<number, Set<string>>>} topic, then partition, to its claimants */
    this.claimants = new Map()

    for (const { memberId, ownedPartitions } of members) {
      for (const { topic, partitions } of ownedPartitions || []) {
        let byPartition = this.claimants.get(topic)
        if (byPartition === undefined) this.claimants.set(topic, (byPartition = new Map()))

        for (const partition of partitions || []) {
          const claimants = byPartition.get(partition)
          if (claimants === undefined) byPartition.set(partition, new Set([memberId]))
          else claimants.add(memberId)
        }
      }
    }
  }

  /**
   * Refuses a plan that breaks R1 or R2.
   * @param {Array<{ memberId: string, memberAssignment: Buffer|null }>} plan the rows the leader
   *   sends; every topic row of each assignment is checked, repeated rows included
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} naming the topic, the partition, the receiving member and
   *   the other member involved
   * @throws {import('../../errors').KafkaJSPartialMessageError} for a truncated assignment
   */
  assertPlan(plan) {
    /** @type {Map<string, Map<number, string>>} */
    const holders = new Map()

    for (const { memberId, memberAssignment } of plan) {
      const rows = Buffer.isBuffer(memberAssignment) ? readAssignmentRows(memberAssignment) : null

      for (const { topic, partitions } of (rows && rows.topics) || []) {
        let byPartition = holders.get(topic)
        if (byPartition === undefined) holders.set(topic, (byPartition = new Map()))

        for (const partition of partitions || []) {
          const holder = byPartition.get(partition)
          if (holder !== undefined && holder !== memberId) {
            throw refusal(topic, partition, memberId, `${holder} is assigned it too`)
          }
          byPartition.set(partition, memberId)

          const claimants = this.claimants.get(topic)?.get(partition)
          if (claimants !== undefined && !claimants.has(memberId)) {
            const owners = [...claimants].sort()
            throw refusal(
              topic,
              partition,
              memberId,
              `${owners.join(' and ')} still ${owners.length === 1 ? 'owns' : 'own'} it`
            )
          }
        }
      }
    }
  }
}

/**
 * @param {string} topic
 * @param {number} partition
 * @param {string} memberId
 * @param {string} reason
 * @returns {KafkaJSNonRetriableError}
 */
const refusal = (topic, partition, memberId, reason) =>
  new KafkaJSNonRetriableError(
    `Cooperative rebalance would assign ${topic}-${partition} to ${memberId} while ${reason}; leave a partition out of every assignment until its owner has revoked it`
  )

module.exports = OwnedPartitionClaims
