const Decoder = require('../../protocol/decoder')
const { KafkaJSNonRetriableError } = require('../../errors')
const { readAssignmentRows } = require('../assignmentRows')

/** The protocol name of the built-in cooperative assigner, Java's `CooperativeStickyAssignor`. */
const COOPERATIVE_STICKY = 'cooperative-sticky'

/** The generation of a claim whose generation is unknown, older than every known one. */
const UNKNOWN_GENERATION = -1

/**
 * @param {Buffer|null|undefined} userData
 * @returns {number} the generation the built-in assigner writes as its four-byte userData, or
 *   unknown when there is nothing readable there
 */
const decodeGeneration = (userData) =>
  userData == null || userData.length < 4 ? UNKNOWN_GENERATION : new Decoder(userData).readInt32()

/**
 * The generation a member claims its owned partitions in.
 *
 * A subscription's `GenerationId` counts only when it is non-negative: the field's wire default is
 * `-1`, and Java turns any negative value into an absent generation. When it is absent or
 * negative, a `cooperative-sticky` subscription still carries the generation in its four-byte
 * userData (a version-1 subscription has no `GenerationId` at all), so that is read instead. Any
 * other protocol's userData belongs to its assigner and is never read: its claim is unknown.
 *
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/clients/consumer/CooperativeStickyAssignor.java
 * @param {{ generationId?: number, userData?: Buffer|null }} subscription decoded
 * @param {{ userDataGeneration: boolean }} options whether the userData is the built-in's
 * @returns {number}
 */
const claimGeneration = (subscription, { userDataGeneration }) => {
  const { generationId } = subscription
  if (generationId != null && generationId >= 0) return generationId
  return userDataGeneration ? decodeGeneration(subscription.userData) : UNKNOWN_GENERATION
}

/** @param {string} topic @param {number} partition @returns {string} */
const keyOf = (topic, partition) => `${topic}\u0000${partition}`

/**
 * Who may hold each partition after an incremental (cooperative) rebalance round, decided from the
 * partitions the members claim in their subscriptions. It is the one rule for every plan: the
 * built-in `cooperative-sticky` assigner withholds by it, and the group leader refuses any plan,
 * whoever produced it, that breaks it.
 *
 * | Claims on the partition                        | May be assigned it        | Must wait a round          |
 * |------------------------------------------------|---------------------------|----------------------------|
 * | none                                           | any one member            | nobody                     |
 * | one member at the highest claimed generation   | that member (retention)   | every other member         |
 * | two or more members at the highest generation | nobody                    | every member               |
 *
 * A claim at a lower generation than another claim on the same partition is stale: it neither
 * keeps the partition nor counts as a conflict. A partition may be in only one member's plan.
 * The outcome depends only on the claims, never on the order the broker lists the members in.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractStickyAssignor.java
 */
class CooperativeOwnership {
  /**
   * @param {Iterable<{ memberId: string, generationId: number,
   *   ownedPartitions?: Array<{topic: string, partitions: number[]}>|null }>} claims one entry per
   *   member, its generation already resolved by {@link claimGeneration}
   */
  constructor(claims) {
    /** @type {Map<string, { generation: number, members: string[] }>} the top claims per partition */
    this.top = new Map()

    for (const { memberId, generationId, ownedPartitions } of claims) {
      for (const { topic, partitions } of ownedPartitions || []) {
        for (const partition of partitions || []) {
          this.claim(memberId, generationId, keyOf(topic, partition))
        }
      }
    }
  }

  /**
   * @private
   * @param {string} memberId
   * @param {number} generation
   * @param {string} key
   * @returns {void}
   */
  claim(memberId, generation, key) {
    const top = this.top.get(key)
    if (top === undefined || generation > top.generation) {
      this.top.set(key, { generation, members: [memberId] })
    } else if (generation === top.generation && !top.members.includes(memberId)) {
      top.members.push(memberId)
    }
  }

  /**
   * @param {string} memberId
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean} whether the table above lets this member be assigned the partition
   */
  mayHold(memberId, topic, partition) {
    const top = this.top.get(keyOf(topic, partition))
    return top === undefined || (top.members.length === 1 && top.members[0] === memberId)
  }

  /**
   * Refuses a plan that breaks the rule.
   * @param {Array<{ memberId: string, memberAssignment: Buffer|null }>} plan the rows the leader
   *   sends; every topic row of each assignment is checked, repeated rows included
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} naming the topic, the partition and the members involved
   * @throws {import('../../errors').KafkaJSPartialMessageError} for a truncated assignment
   */
  assertPlan(plan) {
    const holderOf = new Map()

    for (const { memberId, memberAssignment } of plan) {
      const rows = Buffer.isBuffer(memberAssignment) ? readAssignmentRows(memberAssignment) : null

      for (const { topic, partitions } of (rows && rows.topics) || []) {
        for (const partition of partitions || []) {
          const key = keyOf(topic, partition)
          const holder = holderOf.get(key)
          if (holder !== undefined && holder !== memberId) {
            throw refusal(topic, partition, memberId, `${holder} is assigned it too`)
          }
          holderOf.set(key, memberId)

          if (this.mayHold(memberId, topic, partition)) continue

          const { generation, members } = this.top.get(key)
          const claimants = [...members].sort().join(' and ')
          throw refusal(
            topic,
            partition,
            memberId,
            members.length === 1
              ? `${claimants} still owns it`
              : `${claimants} claim it in the same generation ${generation}`
          )
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
    `Cooperative rebalance would assign ${topic}-${partition} to ${memberId} while ${reason}; use the cooperative-sticky assigner or an assignor that withholds moved partitions`
  )

module.exports = { CooperativeOwnership, claimGeneration, COOPERATIVE_STICKY, UNKNOWN_GENERATION }
