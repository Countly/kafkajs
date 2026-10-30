const partitionMetadata = require('../assigners/partitionMetadata')
const sharedPromiseTo = require('../../utils/sharedPromiseTo')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { MemberAssignment, MemberMetadata } = require('../assignerProtocol')
const {
  KafkaJSNonRetriableError,
  KafkaJSConnectionError,
  KafkaJSRequestTimeoutError,
  isStaleCoordinator,
} = require('../../errors')
const {
  events: { HEARTBEAT },
} = require('../instrumentationEvents')
const { EAGER, COOPERATIVE } = require('./interface')
const LeaderUnionWatch = require('../leaderUnionWatch')
const {
  stampGeneration,
  stampOwnership,
  stampMembership,
  joinDeadlineError,
} = require('../rebalanceErrorContract')

/** The protocol type of every consumer group, sent on JoinGroup. */
const PROTOCOL_TYPE = 'consumer'

/**
 * What the coordinator logs when the consumer leaves; the Java client sends the same text.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
 */
const LEAVE_REASON = 'the consumer is being closed'

/**
 * Whether a heartbeat error leaves the cached coordinator unusable: the connection to it failed,
 * closed or timed out, or it answered that it is not, or not yet, the coordinator. The Java client
 * marks its coordinator unknown on the same errors and looks it up before the next heartbeat.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
 * @param {Error} error
 * @returns {boolean}
 */
const losesCoordinator = (error) =>
  error instanceof KafkaJSConnectionError ||
  error instanceof KafkaJSRequestTimeoutError ||
  isStaleCoordinator(error)

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
    /** Whether the last heartbeat lost the coordinator, so the next one looks it up first. */
    this.coordinatorUnknown = false
    /** @type {number|null} the generation of the joined membership; see the `generationId` accessor */
    this.currentGenerationId = null
    /**
     * How many JoinGroups the coordinator has accepted. Only ever goes up: the generation and the
     * member id can come back unchanged on a rejoin, so a membership that replaced another can be
     * told apart only by this count.
     */
    this.joinCount = 0
    this.leaderId = null
    /** @type {string|null} the member id of the joined membership; see the `memberId` accessor */
    this.currentMemberId = null
    /** @type {string|null} the id a MEMBER_ID_REQUIRED answer assigned, not yet joined under */
    this.pendingMemberId = null
    /**
     * Counts the memberships retired so far. Work captures it when its request starts and
     * `changeIdentity` compares it before any write, so an obsolete answer cannot touch a later
     * membership.
     */
    this.membershipRevision = 0
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
      const { membershipRevision } = this
      const now = Date.now()

      if (memberId && now >= this.lastRequest + interval) {
        const payload = {
          groupId,
          memberId,
          groupGenerationId: generationId,
          groupInstanceId: this.groupInstanceId,
        }

        try {
          const coordinator = await this.liveCoordinator()
          await coordinator.heartbeat(payload)
        } catch (e) {
          if (losesCoordinator(e)) {
            this.coordinatorUnknown = true
          }
          // A heartbeat sent before a rebalance and answered after it is answered about the
          // generation it was sent under, not the one this member holds by then.
          throw stampMembership(
            stampOwnership(stampGeneration(e, generationId), token),
            membershipRevision
          )
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

  /**
   * The id of the joined membership. A pending id is not one: heartbeats, commits and ownership
   * read this, and none of them may act on an id that has not joined.
   * @returns {string|null}
   */
  get memberId() {
    return this.currentMemberId
  }

  /**
   * Local reset or fixture write of the joined id, applied to the current membership; `null`
   * retires it. A response path uses `changeIdentity` with the revision its request began under.
   * @param {string|null} value
   */
  set memberId(value) {
    this.changeIdentity(this.membershipRevision, { memberId: value })
  }

  /**
   * The generation of the joined membership.
   * @returns {number|null}
   */
  get generationId() {
    return this.currentGenerationId
  }

  /**
   * Local write of the generation, applied to the current membership.
   * @param {number|null} value
   */
  set generationId(value) {
    this.changeIdentity(this.membershipRevision, { generationId: value })
  }

  /**
   * The one place classic member identity is written: the joined id and generation, the pending
   * id and the revision. Every response path captures `membershipRevision` when its request
   * starts and passes it here, so an answer that outlived its membership changes nothing.
   *
   * A `memberId` of `null` retires the membership: it drops the pending id with the joined one and
   * moves the revision on, so every reset forgets both.
   * @param {number} revision the membership revision the caller's request began under
   * @param {{ memberId?: string|null, pendingMemberId?: string|null, generationId?: number|null }} change
   *   the fields to write; an absent field is left alone
   * @returns {boolean} false when the membership was retired since `revision` and nothing was written
   */
  changeIdentity(revision, { memberId, pendingMemberId, generationId }) {
    if (revision !== this.membershipRevision) return false
    if (memberId !== undefined) {
      this.currentMemberId = memberId
      if (memberId === null) {
        this.pendingMemberId = null
        this.membershipRevision += 1
      }
    }
    if (pendingMemberId !== undefined) this.pendingMemberId = pendingMemberId
    if (generationId !== undefined) this.currentGenerationId = generationId
    return true
  }

  /**
   * Keeps the id a MEMBER_ID_REQUIRED answer assigned, for the next JoinGroup of the same
   * membership. A notification from a membership retired since its join began is dropped.
   * @private
   * @param {string} memberId
   * @param {number} revision the membership revision the join began under
   * @returns {boolean} whether it was kept
   */
  keepPendingMemberId(memberId, revision) {
    return this.changeIdentity(revision, { pendingMemberId: memberId })
  }

  /**
   * Forgets the joined and the pending id, for an UNKNOWN_MEMBER_ID answer or a local discard.
   * @param {number} [revision] the revision the answered request began under; the current
   *   membership when absent
   * @returns {boolean} whether the membership was current and forgotten
   */
  forgetMemberId(revision = this.membershipRevision) {
    return this.changeIdentity(revision, { memberId: null })
  }

  /** @returns {boolean} */
  isLeader() {
    return this.leaderId && this.memberId === this.leaderId
  }

  /**
   * Resolves the group coordinator. Re-run on every join, so a coordinator that moved is
   * re-discovered rather than reused. With a round, its fence ends the lookup's retries and dials
   * when the round retires.
   * @param {object} [round] the join's round; absent for a lookup outside a rebalance
   * @returns {Promise<void>}
   */
  async connect(round) {
    this.coordinator = await this.cluster.findGroupCoordinator({
      groupId: this.groupId,
      fence: round?.fence,
    })
    this.coordinatorUnknown = false
  }

  /**
   * The cached coordinator, found again first when it was retired since, the last heartbeat lost
   * it, or its connection is no longer open: a retired broker refuses every request, a dead or
   * moved coordinator answers nothing useful, and a broker another user of the cluster disconnected
   * would reconnect outside the cluster's broker pool, where no later disconnect closes it. Between
   * joins nothing else looks the coordinator up. Otherwise it is returned untouched. The Java
   * client checks the coordinator's connection the same way (`checkAndGetCoordinator`).
   * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
   * @private
   * @returns {Promise<import('../../../types').Broker>}
   */
  async liveCoordinator() {
    if (
      this.coordinatorUnknown ||
      this.coordinator.retired === true ||
      !this.coordinator.isConnected()
    ) {
      await this.connect()
    }

    return this.coordinator
  }

  /**
   * Takes the newest desired subscription. The classic protocol states its names in JoinGroup
   * only, and a join reads the round's pinned names, so there is nothing to keep here.
   * @param {{names: string[], revision: number}} snapshot the newest desired subscription
   * @returns {void}
   */
  updateSubscription() {}

  /**
   * One JoinGroup followed by one SyncGroup.
   * @param {{ topics: string[], topicsSubscribed: string[], beforeSync: () => void, round: object }} context
   * @returns {Promise<{ assignment: Object, userData: Buffer|null }>} the decoded member
   *   assignment, `{}` when the coordinator assigned nothing
   */
  async rebalance({ topics, topicsSubscribed, beforeSync, round }) {
    await round.wait(() => this.join({ topicsSubscribed, round }))
    return this.sync({ topics, topicsSubscribed, beforeSync, round })
  }

  /**
   * The JoinGroup half. Resolves the coordinator first, so a coordinator that moved between
   * rounds is found before the request rather than after it fails.
   *
   * The id a MEMBER_ID_REQUIRED answer assigns is kept as the pending id the moment the broker
   * reports it, so a follow-up JoinGroup that fails still leaves the next join of the same
   * membership sending it instead of asking for another. A successful join makes it the member id,
   * unless the membership was retired while the request was in flight: that success is refused
   * with the retriable join-deadline error, so a late response never restores a departed identity.
   * The same check guards both sends of the handshake, so a membership retired before the
   * MEMBER_ID_REQUIRED answer arrives writes no follow-up JoinGroup that could acquire a member
   * nothing owns.
   *
   * The attached heartbeat manager, when there is one, is disabled for the duration of the
   * JoinGroup request and re-enabled in a `finally`, so a join that throws or is retried keeps
   * heartbeating.
   *
   * @param {{ topicsSubscribed: string[], round?: object }} context
   * @returns {Promise<void>}
   * @throws {KafkaJSNonRetriableError} for a session timeout refused by the broker, naming
   *   the client option and broker settings; other failures retain their original identity.
   */
  async join({ topicsSubscribed, round }) {
    const { groupId, sessionTimeout, rebalanceTimeout, groupInstanceId, membershipRevision } = this

    await this.connect(round)

    if (groupInstanceId) {
      this.warnIfStaticMembershipInactive()
    }

    let groupData

    /** @returns {Error|null} the round's refusal, else the join-deadline error of a retired membership */
    const joinRefusal = () =>
      round?.guard() ||
      (this.membershipRevision === membershipRevision
        ? null
        : joinDeadlineError({ rebalanceTimeoutMs: rebalanceTimeout }))

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

      groupData = await this.coordinator.joinGroup(
        {
          groupId,
          sessionTimeout,
          rebalanceTimeout,
          memberId: this.memberId || this.pendingMemberId || '',
          groupInstanceId,
          protocolType: PROTOCOL_TYPE,
          groupProtocols: this.assigners.map((assigner) =>
            this.subscriptionFor(assigner, topicsSubscribed)
          ),
        },
        {
          guard: joinRefusal,
          onMemberIdRequired: (assigned) => this.keepPendingMemberId(assigned, membershipRevision),
        }
      )
      const stale = round?.guard()
      if (stale) throw stale
      const installed = this.changeIdentity(membershipRevision, {
        memberId: groupData.memberId,
        pendingMemberId: null,
        generationId: groupData.generationId,
      })
      if (!installed) throw joinDeadlineError({ rebalanceTimeoutMs: rebalanceTimeout })
    } catch (error) {
      if (error.type === 'INVALID_SESSION_TIMEOUT' && error.code === 26) {
        const refusal = new KafkaJSNonRetriableError(error, { cause: error })
        refusal.message = `Consumer sessionTimeout (${sessionTimeout}) was refused by the broker; check the broker settings group.min.session.timeout.ms and group.max.session.timeout.ms.`
        throw refusal
      }
      throw error
    } finally {
      // In the `finally`, so a join that threw, bailed or is about to be retried heartbeats
      // through its own backoff - which is precisely the window that needs them.
      if (this.heartbeatManager) {
        this.heartbeatManager.enable()
      }
    }

    this.joinCount += 1
    this.leaderId = groupData.leaderId
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
   * Builds the leader union and its authoritative partition snapshot. A member whose subscription
   * cannot be read adds no topic.
   * @param {object[]} members decoded JoinGroup members
   * @param {string[]} topicsSubscribed leader subscription, never mutated
   * @param {object} round captured run/attempt, guard and remaining-time wait
   * @returns {Promise<{groupTopics: string[], groupTopicMetadata: Map<string, object[]>}>} round-only data
   * @throws {Error} original snapshot failure or retriable round expiry
   */
  async leaderMetadata(members, topicsSubscribed, round) {
    const refused = round.guard()
    if (refused) throw refused
    const groupTopics = [
      ...new Set([
        ...topicsSubscribed,
        ...members.flatMap(
          ({ memberMetadata }) => this.decodeSubscription(memberMetadata)?.topics ?? []
        ),
      ]),
    ].sort()
    const own = new Set(topicsSubscribed)
    const groupTopicMetadata = new Map()
    for (const topic of topicsSubscribed) {
      const rows = this.cluster.findTopicPartitionMetadata(topic)
      groupTopicMetadata.set(
        topic,
        partitionMetadata.withReplicaRacks(rows, this.cluster.brokerPool?.metadata?.brokers || [])
      )
    }
    const missing = groupTopics.filter((topic) => !own.has(topic))
    if (missing.length) {
      const now = performance.now()
      const remaining = round.roundDeadlineAt - now
      if (remaining <= 0) throw round.guard()
      const snapshot = await round.wait(() =>
        this.cluster.groupMetadata({
          topics: missing,
          allowAutoTopicCreation: round.allowAutoTopicCreation,
          deadlineAt: now + remaining / 2,
          guard: round.guard,
        })
      )
      const wanted = new Set(missing)
      for (const {
        topic,
        topicErrorCode,
        partitionMetadata: partitions,
      } of snapshot.topicMetadata) {
        if (!wanted.has(topic)) continue
        if (topicErrorCode) {
          this.logger.warn('Skipping unavailable group topic', {
            topic,
            topicErrorCode,
          })
          continue
        }
        groupTopicMetadata.set(
          topic,
          partitionMetadata.withReplicaRacks(partitions, snapshot.brokers || [])
        )
      }
    }
    const error = round.guard()
    if (error) throw error
    return { groupTopics, groupTopicMetadata }
  }

  /**
   * The SyncGroup half: the leader computes and uploads the assignment, every member reads its
   * own back. `beforeSync` runs between the leader's `assign()` and the request, which is where
   * the caller refreshes the partitions it knows about for its subscribed topics.
   * @param {{ topics: string[], topicsSubscribed: string[], beforeSync: () => void, round: object }} context
   * @returns {Promise<{ assignment: Object, userData: Buffer|null,
   *   unionSnapshot: Map<string, number[]>|null }>} `unionSnapshot` is the leader's partition sets
   *   of the topics only other members subscribe to, for `LeaderUnionWatch`; null for a follower
   *   or when every member shares the leader's subscription
   * @throws {KafkaJSNonRetriableError} when the coordinator picked an assignor this member does
   *   not have
   */
  async sync({ topics, topicsSubscribed, beforeSync, round }) {
    let assignment = []
    let unionSnapshot = null
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
      this.logger.debug('Chosen as group leader', {
        groupId,
        generationId,
        memberId,
        topics,
      })
      const assigner = this.assigners.find(({ name }) => name === groupProtocol)

      if (!assigner) {
        throw new KafkaJSNonRetriableError(
          `Unsupported partition assigner "${groupProtocol}", the assigner wasn't found in the assigners list`
        )
      }

      await round.wait(() => this.cluster.refreshMetadata({ guard: round.guard }))
      const { groupTopics, groupTopicMetadata } = await this.leaderMetadata(
        members,
        topicsSubscribed,
        round
      )
      const own = new Set(topicsSubscribed)
      unionSnapshot = LeaderUnionWatch.snapshotOf(
        groupTopics.filter((topic) => !own.has(topic)),
        groupTopicMetadata
      )
      assignment = await round.wait(() =>
        assigner.assign({
          members,
          topics: topicsSubscribed,
          groupTopics,
          groupTopicMetadata,
        })
      )

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

    const refusal = round.guard()
    if (refusal) throw refusal
    beforeSync()

    const { memberAssignment } = await round.wait(() =>
      coordinator.syncGroup(
        {
          groupId,
          generationId,
          memberId,
          groupInstanceId,
          protocolType,
          protocolName: groupProtocol,
          groupAssignment: assignment,
          sessionTimeout,
          rebalanceTimeout,
        },
        { guard: round.guard }
      )
    )

    const decoded = MemberAssignment.decode(memberAssignment)
    return {
      assignment: decoded != null ? decoded.assignment : {},
      userData: decoded != null ? decoded.userData : null,
      unionSnapshot,
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
   * Sends a heartbeat when the interval has passed, on a coordinator found again if the cached one
   * was retired. Concurrent calls share one request.
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
   *
   * The request is skipped while the last heartbeat left the coordinator unknown, as in the Java
   * client: looking for one would hold up a stop, and an unsent leave costs only the session
   * timeout. Otherwise it goes to the cached coordinator, found again first if it was retired or
   * its connection is closed.
   * The member id, joined or pending, is forgotten whether the request succeeds, fails or is not
   * sent, unless a newer membership replaced it while the request was in flight.
   * @param {{ leaveGroup?: boolean }} [options] `false` sends no request and only forgets the
   *   member id; `true` and `undefined` send it (nothing calls this for a static member that
   *   leaves by default)
   * @returns {Promise<void>}
   * @throws {import('../../errors').KafkaJSProtocolError} the coordinator's error for this member
   */
  async leave({ leaveGroup } = {}) {
    const { groupId, groupInstanceId, joinCount, membershipRevision } = this
    const memberId = this.memberId || this.pendingMemberId

    try {
      if (memberId && leaveGroup !== false) {
        if (this.coordinatorUnknown) {
          this.logger.debug('Leaving without a LeaveGroup: no coordinator is known', { groupId })
        } else {
          const coordinator = await this.liveCoordinator()
          await coordinator.leaveGroup({
            groupId,
            memberId,
            groupInstanceId,
            reason: LEAVE_REASON,
          })
        }
      }
    } finally {
      if (this.joinCount === joinCount && this.forgetMemberId(membershipRevision)) {
        this.clearAssignerState()
      }
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
    this.changeIdentity(this.membershipRevision, {
      generationId: null,
      ...(clearMemberId && { memberId: null }),
    })
    this.leaderId = null
    this.members = null
    this.groupProtocol = null
    this.protocolType = null
    this.ownedPartitions = []

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

    return {
      added: subtract(assigned, owned),
      revoked: subtract(owned, assigned),
    }
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
        {
          groupId: this.groupId,
          groupInstanceId: this.groupInstanceId,
          negotiatedVersion,
        }
      )
    }

    this.staticMembershipInactiveWarned = true
  }
}
