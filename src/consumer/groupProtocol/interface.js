/**
 * The rebalance-protocol seam. `ClassicGroupProtocol` implements it over JoinGroup/SyncGroup;
 * a future protocol implements it over its own requests. The module carries the two mode
 * constants, the mode-selection rule, and the typedefs every implementation is checked against.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 */

const { KafkaJSNonRetriableError } = require('../../errors')

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
 * @typedef {Object} GroupProtocol
 * @property {'classic' | 'consumer'} kind which membership implementation this is; read by the
 *   round loop, the poll-expiry path and the error contract, none of which can reach the protocol
 *   except through `ConsumerGroup`
 * @property {number} memberEpoch the current member epoch, or null for a protocol that has none
 * @property {() => Promise<void>} connect resolves the group coordinator
 * @property {(context: { topics: string[], topicsSubscribed: string[],
 *   beforeSync: () => void, growSubscription?: (names: string[]) => Promise<void> }) => Promise<Object>} rebalance
 *   ONE exchange with the coordinator - a JoinGroup + SyncGroup for the classic protocol, one
 *   reconciliation step for the consumer protocol - returning the result the caller applies.
 *   Plain, not self-guarded and not a loop: the single flight, the round loop and the
 *   instrumentation events belong to the caller. `growSubscription` is supplied only by a caller
 *   whose subscription the coordinator may widen.
 * @property {(deps: { rebalanceCoordinator: Object, heartbeatManager: Object,
 *   commitBeforeRelease?: Function }) => void} attach
 *   Late-bound collaborators, forwarded by `ConsumerGroup#attach`.
 * @property {(options: { interval: number }) => Promise<void>} heartbeat
 * @property {() => number} heartbeatInterval how long until the next heartbeat should go out; the
 *   heartbeat manager reads this and nothing else when it schedules
 * @property {() => boolean} needsReconciliation whether the coordinator wants something this member
 *   does not have yet; always false for a protocol whose assignment is decided at join time
 * @property {(error: Error) => void} onMembershipError performs the state move a lost-membership
 *   error implies, wherever it was learned - the heartbeat, an offset commit or a fetch
 * @property {() => void} onStopRequested fails whatever the protocol is waiting on, because the
 *   consumer is stopping
 * @property {(options?: { leaveGroup?: boolean }) => Promise<void>} leave `leaveGroup` `true`
 *   removes the member from the group at once, `false` keeps it there until its session expires,
 *   and `undefined` is the default: a dynamic member leaves, a static one stays
 * @property {() => Promise<void>} leaveForPollExpiry the poll-overrun variant of `leave`: no
 *   commit, no partition events, the response ignored
 * @property {() => RebalanceProtocol} rebalanceProtocol
 * @property {(options?: { clearMemberId?: boolean }) => void} resetGeneration
 * @property {() => TopicPartitions[]} owned
 * @property {(topicPartitions: TopicPartitions[]) => void} narrowOwned shrink only
 * @property {(topicPartitions: TopicPartitions[]) => void} setOwned widen to what was applied
 */

/** Precedence: the highest id in the intersection wins, matching the Java client. */
const PROTOCOL_IDS = { [EAGER]: 0, [COOPERATIVE]: 1 }

/**
 * Chooses the rebalance protocol for a set of assigners, once, at consumer construction.
 *
 * The rule is Java's: intersect what every configured assigner supports and take the highest id.
 * An assigner that declares nothing supports eager only, which is what makes a rolling upgrade
 * work - a group running one old and one new assigner stays eager until every member has dropped
 * the old one.
 *
 * An empty assigner list is not rejected here: it has always been accepted at construction and
 * fails at JoinGroup instead, so it keeps that behaviour and reports the conservative protocol
 * rather than the whole universe the intersection would otherwise return.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 * @param {import('../../../types').Assigner[]} assigners
 * @returns {RebalanceProtocol}
 * @throws {KafkaJSNonRetriableError} when the assigners share no protocol
 */
const selectRebalanceProtocol = (assigners) => {
  if (assigners.length === 0) {
    return EAGER
  }

  const supported = assigners.map((assigner) => ({
    name: assigner.name,
    protocols: assigner.supportedProtocols ? assigner.supportedProtocols() : [EAGER],
  }))

  const intersection = supported.reduce(
    (shared, { protocols }) => shared.filter((protocol) => protocols.includes(protocol)),
    [EAGER, COOPERATIVE]
  )

  if (intersection.length === 0) {
    const described = supported
      .map(({ name, protocols }) => `${name} supports [${protocols.join(', ')}]`)
      .join('; ')

    throw new KafkaJSNonRetriableError(
      `The configured partition assigners share no rebalance protocol: ${described}`
    )
  }

  return intersection.reduce((best, protocol) =>
    PROTOCOL_IDS[protocol] > PROTOCOL_IDS[best] ? protocol : best
  )
}

module.exports = { EAGER, COOPERATIVE, selectRebalanceProtocol }
