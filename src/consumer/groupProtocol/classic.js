const sharedPromiseTo = require('../../utils/sharedPromiseTo')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { MemberAssignment } = require('../assignerProtocol')
const { KafkaJSNonRetriableError } = require('../../errors')
const {
  events: { HEARTBEAT },
} = require('../instrumentationEvents')
const { EAGER } = require('./interface')

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
   * @param {import('../../../types').Cluster} options.cluster
   * @param {string} options.groupId
   * @param {string|null} options.groupInstanceId static membership id (KIP-345)
   * @param {number} options.sessionTimeout
   * @param {number} options.rebalanceTimeout the value sent on the wire as RebalanceTimeoutMs
   * @param {import('../../../types').Assigner[]} options.assigners
   * @param {import('./interface').RebalanceProtocol} [options.mode]
   * @param {import('../../instrumentation/emitter')} options.instrumentationEmitter
   * @param {import('../../../types').Logger} options.logger
   */
  constructor({
    cluster,
    groupId,
    groupInstanceId = null,
    sessionTimeout,
    rebalanceTimeout,
    assigners,
    mode = EAGER,
    instrumentationEmitter,
    logger,
  }) {
    this.cluster = cluster
    this.groupId = groupId
    this.groupInstanceId = groupInstanceId
    this.sessionTimeout = sessionTimeout
    this.rebalanceTimeout = rebalanceTimeout
    this.assigners = assigners
    this.mode = mode
    this.instrumentationEmitter = instrumentationEmitter
    // The group's own logger object, NOT a new namespace: `logger.namespace()` returns a fresh
    // object, and existing spies on the group's logger must keep observing what is logged here.
    this.logger = logger

    this.coordinator = null
    this.generationId = null
    this.leaderId = null
    this.memberId = null
    this.members = null
    this.groupProtocol = null
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
      const now = Date.now()

      if (memberId && now >= this.lastRequest + interval) {
        const payload = {
          groupId,
          memberId,
          groupGenerationId: generationId,
          groupInstanceId: this.groupInstanceId,
        }

        await this.coordinator.heartbeat(payload)
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
   * @returns {Promise<{ assignment: Object }>} the decoded member assignment, `{}` when the
   *   coordinator assigned nothing
   */
  async rebalance({ topics, topicsSubscribed, beforeSync }) {
    await this.join({ topicsSubscribed })
    return this.sync({ topics, topicsSubscribed, beforeSync })
  }

  /**
   * The JoinGroup half. Resolves the coordinator first, so a coordinator that moved between
   * rounds is found before the request rather than after it fails.
   * @param {{ topicsSubscribed: string[] }} context
   * @returns {Promise<void>}
   */
  async join({ topicsSubscribed }) {
    const { groupId, sessionTimeout, rebalanceTimeout, groupInstanceId } = this

    await this.connect()

    if (groupInstanceId) {
      this.warnIfStaticMembershipInactive()
    }

    const groupData = await this.coordinator.joinGroup({
      groupId,
      sessionTimeout,
      rebalanceTimeout,
      memberId: this.memberId || '',
      groupInstanceId,
      groupProtocols: this.assigners.map((assigner) =>
        this.subscriptionFor(assigner, topicsSubscribed)
      ),
    })

    this.generationId = groupData.generationId
    this.leaderId = groupData.leaderId
    this.memberId = groupData.memberId
    this.members = groupData.members
    this.groupProtocol = groupData.groupProtocol
    // JoinGroup v5's response carries no group_instance_id for this member: it only appears
    // per-entry in `members`, populated for the leader. `this.groupInstanceId` is the value the
    // caller configured and must not be overwritten with undefined here.
  }

  /**
   * The subscription entry one assigner contributes to JoinGroup, in configuration order.
   * @param {import('../../../types').Assigner} assigner
   * @param {string[]} topicsSubscribed
   * @returns {{ name: string, metadata: Buffer }}
   */
  subscriptionFor(assigner, topicsSubscribed) {
    return assigner.protocol({ topics: topicsSubscribed })
  }

  /**
   * The SyncGroup half: the leader computes and uploads the assignment, every member reads its
   * own back. `beforeSync` runs between the leader's `assign()` and the request, which is where
   * the caller refreshes the partitions it knows about for its subscribed topics.
   * @param {{ topics: string[], topicsSubscribed: string[], beforeSync: () => void }} context
   * @returns {Promise<{ assignment: Object }>}
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
      groupAssignment: assignment,
      sessionTimeout,
      rebalanceTimeout,
    })

    const decoded = MemberAssignment.decode(memberAssignment)
    return { assignment: decoded != null ? decoded.assignment : {} }
  }

  /**
   * @param {{ interval: number }} options
   * @returns {Promise<void>}
   */
  async heartbeat({ interval }) {
    return this.sharedHeartbeat({ interval })
  }

  /** @returns {Promise<void>} */
  async leave() {
    const { groupId, memberId, groupInstanceId } = this
    if (memberId) {
      await this.coordinator.leaveGroup({ groupId, memberId, groupInstanceId })
      this.memberId = null
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
    this.ownedPartitions = []

    if (clearMemberId) {
      this.memberId = null
    }
  }

  /** @returns {import('./interface').Membership} allocates; instrumentation only */
  membership() {
    return {
      memberId: this.memberId,
      generationId: this.generationId,
      leaderId: this.leaderId,
      groupInstanceId: this.groupInstanceId,
      isLeader: this.isLeader(),
      groupProtocol: this.groupProtocol,
    }
  }

  /** @returns {import('./interface').TopicPartitions[]} */
  owned() {
    return this.ownedPartitions
  }

  /**
   * Shrinks `owned` to its intersection with the given set — it never widens. Called before
   * anything asynchronous in a cooperative apply, so a revoked partition stops being advertised
   * immediately, while a partition this member is only about to GAIN is not claimed before the
   * apply has actually happened. The widening half is `setOwned`, after the apply.
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
