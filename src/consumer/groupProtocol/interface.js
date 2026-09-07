/**
 * The rebalance-protocol seam. `ClassicGroupProtocol` implements it over JoinGroup/SyncGroup;
 * a future protocol implements it over its own requests. Nothing here executes: the module
 * carries the two mode constants and the typedefs every implementation is checked against.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 */

/** Every member revokes its whole assignment before rejoining. */
const EAGER = 'eager'

/** A member keeps the partitions it retains and revokes only what moved (KIP-429). */
const COOPERATIVE = 'cooperative'

/**
 * @typedef {'eager' | 'cooperative'} RebalanceProtocol
 * @typedef {{ topic: string, partitions: number[] }} TopicPartitions
 *
 * @typedef {Object} RebalanceResult
 * @property {TopicPartitions[]} assignment the full target assignment after this round
 * @property {TopicPartitions[]} added      assignment minus what was previously owned
 * @property {TopicPartitions[]} revoked    previously owned minus assignment
 *
 * @typedef {Object} Membership
 * @property {string|null} memberId
 * @property {number} generationId
 * @property {string|null} leaderId
 * @property {string|null} groupInstanceId
 * @property {boolean} isLeader
 * @property {string|null} groupProtocol the assignor NAME the coordinator selected
 *
 * @typedef {Object} GroupProtocol
 * @property {() => Promise<void>} connect resolves the group coordinator
 * @property {(context: { topics: string[], topicsSubscribed: string[],
 *   beforeSync: () => void }) => Promise<Object>} rebalance
 *   ONE JoinGroup + SyncGroup exchange, returning the raw decoded sync result. Plain, not
 *   self-guarded and not a loop: the single flight, the round loop and the instrumentation
 *   events belong to the caller.
 * @property {(deps: { rebalanceCoordinator: Object, heartbeatManager: Object }) => void} attach
 *   Late-bound collaborators, forwarded by `ConsumerGroup#attach`.
 * @property {(options: { interval: number }) => Promise<void>} heartbeat
 * @property {() => Promise<void>} leave
 * @property {() => Membership} membership snapshot; allocates, so instrumentation only
 * @property {() => RebalanceProtocol} rebalanceProtocol
 * @property {(options?: { clearMemberId?: boolean }) => void} resetGeneration
 * @property {() => TopicPartitions[]} owned
 * @property {(topicPartitions: TopicPartitions[]) => void} narrowOwned shrink only
 * @property {(topicPartitions: TopicPartitions[]) => void} setOwned widen to what was applied
 */

module.exports = { EAGER, COOPERATIVE }
