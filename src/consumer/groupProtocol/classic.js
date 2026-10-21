const sharedPromiseTo = require('../../utils/sharedPromiseTo')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { MemberAssignment, MemberMetadata } = require('../assignerProtocol')
const { KafkaJSNonRetriableError } = require('../../errors')
const {
  events: { HEARTBEAT },
} = require('../instrumentationEvents')
const { EAGER, COOPERATIVE } = require('./interface')
const { stampGeneration, stampOwnership } = require('../rebalanceErrorContract')

/** The protocol type of every consumer group, sent on JoinGroup. */
const PROTOCOL_TYPE = 'consumer'

/**
 * What the coordinator logs when the consumer leaves; the Java client sends the same text.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
 */
const LEAVE_REASON = 'the consumer is being closed'

/**
 * Membership over the classic JoinGroup/SyncGroup protocol.
 *
 * Owns the coordinator handle, the four classic group requests, the membership fields the
 * coordinator hands back, the set of partitions this member currently owns, and the assignor
 * selection the leader performs. It owns no loop, no retry policy and no rebalance event: one
 * call to `rebalance()` is exactly one JoinGroup followed by one SyncGroup.
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @implements {import('./interface').GroupProtocol}
 */
module.exports = class ClassicGroupProtocol {
  /**
   * @param {object} options
   * @param {import('../groupOwnership')} [options.ownership=null] fences obsolete answers before membership changes
   * @param {import('../../../types').Cluster} options.cluster
   * @param {string} options.groupId
   * @param {string|null} options.groupInstanceId static membership id (KIP-345)
   * @param {number} options.sessionTimeout
   * @param {number|null} [options.heartbeatInterval] how long between heartbeats, in
   *   milliseconds; null leaves the cadence to whoever schedules the heartbeats
   * @param {number} options.rebalanceTimeout the value sent on the wire as RebalanceTimeoutMs
   * @param {import('../../../types').Assigner[]} options.assigners
   * @param {import('./interface').RebalanceProtocol} [options.mode]
   * @param {string} [options.rackId] the consumer's rack; the empty string means "no rack"
   * @param {import('../../instrumentation/emitter')} options.instrumentationEmitter
   * @param {import('../../../types').Logger} options.logger
   */
  constructor({
    cluster,
    groupId,
    groupInstanceId = null,
    sessionTimeout,
    rebalanceTimeout,
    heartbeatInterval = null,
    ownership = null,
    assigners,
    mode = EAGER,
    rackId = '',
    instrumentationEmitter,
    logger,
  }) {
    this.cluster = cluster
    this.ownership = ownership
    this.groupId = groupId
    this.groupInstanceId = groupInstanceId
    this.sessionTimeout = sessionTimeout
    this.rebalanceTimeout = rebalanceTimeout
    this.heartbeatIntervalMs = heartbeatInterval
    this.assigners = assigners
    this.mode = mode
    // The empty string is the shipped default and means "no rack", which on the wire is null.
    this.rackId = rackId === '' ? null : rackId
    this.instrumentationEmitter = instrumentationEmitter
    // The group's own logger object, NOT a new namespace: `logger.namespace()` returns a fresh
    // object, and existing spies on the group's logger must keep observing what is logged here.
    this.logger = logger

    this.coordinator = null
    this.generationId = null
    /**
     * How many JoinGroups the coordinator has accepted. Only ever goes up: the generation and the
     * member id can come back unchanged on a rejoin, so a membership that replaced another can be
     * told apart only by this count.
     */
    this.joinCount = 0
    this.leaderId = null
    this.memberId = null
    this.members = null
    this.groupProtocol = null
    this.protocolType = null
    this.lastRequest = Date.now()
    this.staticMembershipInactiveWarned = false

    /** @type {import('./interface').TopicPartitions[]} */
    this.ownedPartitions = []

    /** @type {Object|null} set by `attach`, absent until a `Runner` exists */
    this.rebalanceCoordinator = null
    /** @type {Object|null} set by `attach`, absent until a `Runner` exists */
    this.heartbeatManager = null

    this.sharedHeartbeat = sharedPromiseTo(async ({ interval }) => {
      const { groupId, generationId, memberId } = this
      const token = this.ownership?.token()
      const now = Date.now()

      if (memberId && now >= this.lastRequest + interval) {
        const payload = {
          groupId,
          memberId,
          groupGenerationId: generationId,
          groupInstanceId: this.groupInstanceId,
        }

        try {
          await this.coordinator.heartbeat(payload)
        } catch (e) {
          // A heartbeat sent before a rebalance and answered after it is answered about the
          // generation it was sent under, not the one this member holds by then.
          throw stampOwnership(stampGeneration(e, generationId), token)
        }

        this.instrumentationEmitter.emit(HEARTBEAT, payload)
        this.lastRequest = Date.now()
      }
    })
  }

  /**
   * Late-bound collaborators. The `Runner` builds both and cannot pass them through this object's
   * constructor, which runs earlier.
   * @param {{ rebalanceCoordinator: Object, heartbeatManager: Object }} deps
   * @returns {void}
   */
  attach({ rebalanceCoordinator, heartbeatManager }) {
    this.rebalanceCoordinator = rebalanceCoordinator
    this.heartbeatManager = heartbeatManager
  }

  /** @returns {import('./interface').RebalanceProtocol} */
  rebalanceProtocol() {
    return this.mode
  }

  /** @returns {'classic'} */
  get kind() {
    return 'classic'
  }

  /**
   * How long until the next heartbeat should go out: the configured interval, always. The heartbeat
   * manager reads this rather than its own constructor value so both membership implementations
   * answer the same question, and this one's answer is the one it always was.
   *
   * Null until a caller configures it, which is not a gap: the manager falls back to the interval
   * it was built with for a membership that does not answer a usable number, so an unconfigured
   * protocol keeps exactly the cadence it had rather than substituting a default of its own.
   * @returns {number|null}
   */
  heartbeatInterval() {
    return this.heartbeatIntervalMs
  }

  /** No-op: a lost membership is handled by the round, not by a state machine. @returns {void} */
  onMembershipError() {}

  /** No-op: nothing is awaited across a stop. @returns {void} */
  onStopRequested() {}

  /** No-op: a poll overrun leaves through the ordinary leave path. @returns {Promise<void>} */
  async leaveForPollExpiry() {}

  /** @returns {false} the assignment is decided at join time, so nothing is ever outstanding */
  needsReconciliation() {
    return false
  }

  /** @returns {boolean} */
  isLeader() {
    return this.leaderId && this.memberId === this.leaderId
  }

  /**
   * Resolves the group coordinator. Re-run on every join, so a coordinator that moved is
   * re-discovered rather than reused.
   * @returns {Promise<void>}
   */
  async connect() {
    this.coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId })
  }

  /**
   * One JoinGroup followed by one SyncGroup.
   * @param {{ topics: string[], topicsSubscribed: string[], beforeSync: () => void }} context
   * @returns {Promise<{ assignment: Object, userData: Buffer|null }>} the decoded member
   *   assignment, `{}` when the coordinator assigned nothing
   */
  async rebalance({ topics, topicsSubscribed, beforeSync }) {
    await this.join({ topicsSubscribed })
    return this.sync({ topics, topicsSubscribed, beforeSync })
  }

  /**
   * The JoinGroup half. Resolves the coordinator first, so a coordinator that moved between
   * rounds is found before the request rather than after it fails.
   *
   * The attached heartbeat manager, when there is one, is disabled for the duration of the
   * JoinGroup request and re-enabled in a `finally`, so a join that throws or is retried keeps
   * heartbeating.
   *
   * @param {{ topicsSubscribed: string[] }} context
   * @returns {Promise<void>}
   */
  async join({ topicsSubscribed }) {
    const { groupId, sessionTimeout, rebalanceTimeout, groupInstanceId } = this

    await this.connect()

    if (groupInstanceId) {
      this.warnIfStaticMembershipInactive()
    }

    let groupData

    try {
      // Heartbeats are suspended for exactly this request and no longer. The broker keeps an
      // existing member alive only while a join or sync future is pending, and the Java client
      // re-enables its heartbeat thread on the JoinGroup response - before SyncGroup - so
      // everything after this call, including the leader's assign window and any retry backoff,
      // must keep heartbeating.
      // @see https://github.com/apache/kafka/blob/4.3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/classic/ClassicGroupMember.java
      // @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
      if (this.heartbeatManager) {
        this.heartbeatManager.disable()
      }

      groupData = await this.coordinator.joinGroup({
        groupId,
        sessionTimeout,
        rebalanceTimeout,
        memberId: this.memberId || '',
        groupInstanceId,
        protocolType: PROTOCOL_TYPE,
        groupProtocols: this.assigners.map((assigner) =>
          this.subscriptionFor(assigner, topicsSubscribed)
        ),
      })
    } finally {
      // In the `finally`, so a join that threw, bailed or is about to be retried heartbeats
      // through its own backoff - which is precisely the window that needs them.
      if (this.heartbeatManager) {
        this.heartbeatManager.enable()
      }
    }

    this.joinCount += 1
    this.generationId = groupData.generationId
    this.leaderId = groupData.leaderId
    this.memberId = groupData.memberId
    this.members = groupData.members
    this.groupProtocol = groupData.groupProtocol
    this.protocolType = groupData.protocolType
    // JoinGroup v5's response carries no group_instance_id for this member: it only appears
    // per-entry in `members`, populated for the leader. `this.groupInstanceId` is the value the
    // caller configured and must not be overwritten with undefined here.
  }

  /**
   * The subscription entry one assigner contributes to JoinGroup, in configuration order.
   *
   * The argument is widened additively: an assigner that reads only `topics` emits exactly the
   * bytes it emitted before IF it declares version 0; the subscription codec is positional, so
   * the version it declares - not which keys it forwards - decides the layout.
   * `subscriptionUserData` is deliberately NOT consulted here - it is an input to bytes
   * `protocol()` owns, and the built-in assigners call their own from inside their own
   * `protocol()`.
   * @private
   * @param {import('../../../types').Assigner} assigner
   * @param {string[]} topicsSubscribed
   * @returns {{ name: string, metadata: Buffer }}
   */
  subscriptionFor(assigner, topicsSubscribed) {
    return assigner.protocol({
      topics: topicsSubscribed,
      // A copy: `ownedPartitions` reaches third-party code here, and an assigner that sorts or
      // splices its argument would otherwise corrupt the ownership set this object rebalances on.
      ownedPartitions: this.ownedPartitions.map(({ topic, partitions }) => ({
        topic,
        partitions: [...partitions],
      })),
      generationId: this.generationId == null ? -1 : this.generationId,
      rackId: this.rackId,
    })
  }

  /**
   * The SyncGroup half: the leader computes and uploads the assignment, every member reads its
   * own back. `beforeSync` runs between the leader's `assign()` and the request, which is where
   * the caller refreshes the partitions it knows about for its subscribed topics.
   * @param {{ topics: string[], topicsSubscribed: string[], beforeSync: () => void }} context
   * @returns {Promise<{ assignment: Object, userData: Buffer|null }>}
   * @throws {KafkaJSNonRetriableError} when the coordinator picked an assignor this member does
   *   not have
   */
  async sync({ topics, topicsSubscribed, beforeSync }) {
    let assignment = []
    const {
      groupId,
      generationId,
      memberId,
      members,
      groupProtocol,
      protocolType,
      coordinator,
      sessionTimeout,
      rebalanceTimeout,
      groupInstanceId,
    } = this

    if (this.isLeader()) {
      this.logger.debug('Chosen as group leader', { groupId, generationId, memberId, topics })
      const assigner = this.assigners.find(({ name }) => name === groupProtocol)

      if (!assigner) {
        throw new KafkaJSNonRetriableError(
          `Unsupported partition assigner "${groupProtocol}", the assigner wasn't found in the assigners list`
        )
      }

      await this.cluster.refreshMetadata()
      assignment = await assigner.assign({ members, topics: topicsSubscribed })

      // An incremental rebalance must never hand a partition to a member while another member
      // still owns it: the new owner would fetch it while the old one is still delivering.
      // `cooperative-sticky` guarantees this through its own withhold rule, so it is exempt; any
      // other assignor configured under this mode has to be checked.
      // @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
      if (this.mode === COOPERATIVE && groupProtocol !== 'cooperative-sticky') {
        this.assertNoDoubleAssignment({ members, assignment })
      }

      this.logger.debug('Group assignment', {
        groupId,
        generationId,
        groupProtocol,
        assignment,
        topics: topicsSubscribed,
      })
    }

    beforeSync()

    const { memberAssignment } = await coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupInstanceId,
      protocolType,
      protocolName: groupProtocol,
      groupAssignment: assignment,
      sessionTimeout,
      rebalanceTimeout,
    })

    const decoded = MemberAssignment.decode(memberAssignment)
    return {
      assignment: decoded != null ? decoded.assignment : {},
      userData: decoded != null ? decoded.userData : null,
    }
  }

  /**
   * Throws when a leader-computed assignment hands a partition to one member while another still
   * owns it, which an incremental rebalance must never do.
   *
   * A member whose subscription carries no owned partitions - a v0 subscription, or one this
   * member cannot decode - contributes nothing to the ownership set, so an unknown owner is never
   * treated as a conflict.
   *
   * @param {{ members: Array<{memberId: string, memberMetadata: Buffer}>,
   *   assignment: Array<{memberId: string, memberAssignment: Buffer}> }} round
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} naming the topic, the partition and both member ids
   */
  assertNoDoubleAssignment({ members, assignment }) {
    const ownerOf = new Map()

    for (const { memberId, memberMetadata } of members) {
      const decoded = this.decodeSubscription(memberMetadata)
      for (const { topic, partitions } of (decoded && decoded.ownedPartitions) || []) {
        for (const partition of partitions) {
          ownerOf.set(`${topic}|${partition}`, memberId)
        }
      }
    }

    for (const { memberId, memberAssignment } of assignment) {
      const decoded = MemberAssignment.decode(memberAssignment)
      for (const [topic, partitions] of Object.entries(decoded ? decoded.assignment : {})) {
        for (const partition of partitions) {
          const owner = ownerOf.get(`${topic}|${partition}`)

          if (owner !== undefined && owner !== memberId) {
            throw new KafkaJSNonRetriableError(
              `Cooperative rebalance would assign ${topic}-${partition} to ${memberId} while ${owner} still owns it; use the cooperative-sticky assigner or an assignor that withholds moved partitions`
            )
          }
        }
      }
    }
  }

  /**
   * Decodes one member's subscription, tolerating a buffer this client did not write.
   * @private
   * @param {Buffer} memberMetadata
   * @returns {Object|null} null when the buffer carries no readable version header
   */
  decodeSubscription(memberMetadata) {
    // Two bytes is the version header every version of the subscription starts with; anything
    // shorter cannot be decoded and belongs to a member whose ownership is simply unknown.
    return memberMetadata && memberMetadata.length >= 2
      ? MemberMetadata.decode(memberMetadata)
      : null
  }

  /**
   * @param {{ interval: number }} options
   * @returns {Promise<void>}
   */
  async heartbeat({ interval }) {
    return this.sharedHeartbeat({ interval })
  }

  /**
   * Hands the applied assignment to every assigner that wants it. Assigners are built once per
   * consumer and reused across every run and restart, so a stateful one (the sticky family)
   * learns what it owns only through this callback.
   * @param {{ partitions: import('./interface').TopicPartitions[], userData: Buffer|null }} assignment
   * @returns {void}
   */
  notifyAssignment(assignment) {
    for (const assigner of this.assigners) {
      if (assigner.onAssignment) {
        assigner.onAssignment(assignment, {
          generationId: this.generationId,
          groupProtocol: this.groupProtocol,
        })
      }
    }
  }

  /**
   * Tells every assigner to forget what it retained. Called when this member leaves the group or
   * loses its generation: retained state from a generation that no longer exists would otherwise
   * be used to compute the next assignment.
   * @returns {void}
   */
  clearAssignerState() {
    for (const assigner of this.assigners) {
      if (assigner.onAssignment) {
        assigner.onAssignment(null, { generationId: -1, groupProtocol: null })
      }
    }
  }

  /**
   * Leaves the group with the reason `the consumer is being closed`, then forgets the member id and
   * the assigners' retained state. Sends nothing without a member id.
   *
   * The forgetting is skipped when a join was accepted meanwhile. The request is awaited, so a
   * rebalance round can join during it - after a poll-interval overrun, the heartbeat failure that
   * follows starts one - and the answer is about the old membership: clearing then would strip the
   * new one of its member id, which silences every heartbeat, and of its assigners' state. The
   * count of accepted joins decides, not the ids: a rejoin can return the same member id and the
   * same generation.
   *
   * A static member's request carries both ids, so the coordinator checks the pair and removes the
   * instance's current member, or answers `FENCED_INSTANCE_ID` for a stale member id; Java's
   * classic leave sends the member id alone.
   * @param {{ leaveGroup?: boolean }} [options] `false` sends no request and only forgets the
   *   member id; `true` and `undefined` send it (nothing calls this for a static member that
   *   leaves by default)
   * @returns {Promise<void>}
   * @throws {import('../../errors').KafkaJSProtocolError} the coordinator's error for this member
   */
  async leave({ leaveGroup } = {}) {
    const { groupId, memberId, groupInstanceId, joinCount } = this

    if (memberId && leaveGroup !== false) {
      await this.coordinator.leaveGroup({
        groupId,
        memberId,
        groupInstanceId,
        reason: LEAVE_REASON,
      })
    }

    if (this.joinCount === joinCount) {
      this.memberId = null
      this.clearAssignerState()
    }
  }

  /**
   * Drops this member's generation so the next round joins as a new one. The member id is kept
   * unless the caller says otherwise: a stale generation does not invalidate the id, and clearing
   * it costs an extra MEMBER_ID_REQUIRED round trip.
   * @param {{ clearMemberId?: boolean }} [options]
   * @returns {void}
   */
  resetGeneration({ clearMemberId = false } = {}) {
    this.generationId = null
    this.leaderId = null
    this.members = null
    this.groupProtocol = null
    this.protocolType = null
    this.ownedPartitions = []

    if (clearMemberId) {
      this.memberId = null
    }

    this.clearAssignerState()
  }

  /** @returns {import('./interface').TopicPartitions[]} */
  owned() {
    return this.ownedPartitions
  }

  /**
   * Shrinks `owned` to its intersection with the given set — it never widens. Called before the
   * cooperative apply touches the offset manager or the subscription state, so a revoked partition
   * stops being advertised before anything can resolve an offset for it, while a partition this
   * member is only about to GAIN is not claimed before the apply has actually happened. The
   * widening half is `setOwned`, after the apply.
   * @param {import('./interface').TopicPartitions[]} topicPartitions
   * @returns {void}
   */
  narrowOwned(topicPartitions) {
    const keep = new Map(
      topicPartitions.map(({ topic, partitions }) => [topic, new Set(partitions)])
    )

    this.ownedPartitions = this.ownedPartitions
      .map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.filter((partition) => {
          const kept = keep.get(topic)
          return kept !== undefined && kept.has(partition)
        }),
      }))
      .filter(({ partitions }) => partitions.length > 0)
  }

  /**
   * Widens `owned` to what was actually applied. Both the next JoinGroup's OwnedPartitions and
   * the next round's `added = assigned - owned` read the result, so an apply that only ever
   * narrowed would advertise a stale set and over-report what it gained.
   * @param {import('./interface').TopicPartitions[]} topicPartitions
   * @returns {void}
   */
  setOwned(topicPartitions) {
    this.ownedPartitions = topicPartitions
  }

  /**
   * The difference between what this member owns and what it was just assigned.
   * @param {import('./interface').TopicPartitions[]} assignment
   * @returns {{ added: import('./interface').TopicPartitions[],
   *   revoked: import('./interface').TopicPartitions[] }}
   */
  diffAssignment(assignment) {
    const index = (topicPartitions) => {
      const map = new Map()
      for (const { topic, partitions } of topicPartitions) {
        map.set(topic, new Set(partitions))
      }
      return map
    }

    const subtract = (from, minus) => {
      const result = []
      for (const [topic, partitions] of from) {
        const other = minus.get(topic)
        const remaining = [...partitions].filter((partition) => !other || !other.has(partition))
        if (remaining.length > 0) {
          result.push({ topic, partitions: remaining.sort((a, b) => a - b) })
        }
      }
      return result
    }

    const assigned = index(assignment)
    const owned = index(this.ownedPartitions)

    return { added: subtract(assigned, owned), revoked: subtract(owned, assigned) }
  }

  /**
   * Warns once when `groupInstanceId` is configured but the coordinator negotiated a JoinGroup
   * below v5: only v5+ carries `group_instance_id` on the wire, so static membership is silently
   * inactive on an older negotiated version.
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
   * @returns {void}
   */
  warnIfStaticMembershipInactive() {
    if (this.staticMembershipInactiveWarned) {
      return
    }

    const negotiatedVersion = this.coordinator.negotiatedVersion(
      apiKeys.JoinGroup,
      requests.JoinGroup
    )

    if (negotiatedVersion == null) {
      return
    }

    if (negotiatedVersion < 5) {
      this.logger.warn(
        `groupInstanceId is set but the broker negotiated JoinGroup v${negotiatedVersion} - static membership is inactive`,
        { groupId: this.groupId, groupInstanceId: this.groupInstanceId, negotiatedVersion }
      )
    }

    this.staticMembershipInactiveWarned = true
  }
}
