const HeartbeatBackoff = require('../groupProtocol/heartbeatBackoff')
const { generateMemberId } = require('../groupProtocol/memberId')
const { MEMBER_STATES: S, transition } = require('../groupProtocol/memberState')
const { KafkaJSNonRetriableError } = require('../../errors')
const {
  membershipAction,
  fatalMembershipError,
  createErrorFromCode,
} = require('./shareErrorContract')

/** Until the coordinator has answered once there is no interval; this keeps the join retry alive. */
const INITIAL_HEARTBEAT_INTERVAL_MS = 1000

/** The member epoch of a leave. */
const LEAVE_EPOCH = -1

/**
 * One run's share membership. HeartbeatManager owns every timer; this class owns none.
 *
 * Identity: one generated member ID per run, kept across fences and rejoins, and one opaque,
 * never-reused `token` per membership. A response, a metadata answer or a notification that
 * outlives its token is discarded, so it can never settle, restart or crash a replacement.
 *
 * Assignments apply synchronously by topic ID: names the cluster knows are applied at once and
 * unknown IDs resolve independently, never holding up the known ones. Observer callbacks are
 * synchronous notifications; an exception in one cannot cancel retirement or a leave.
 *
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/ShareGroupHeartbeatRequest.json
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ShareHeartbeatRequestManager.java
 */
class ShareMembership {
  /**
   * @param {object} options Validated internal collaborators and subscription.
   * @param {object} options.cluster Coordinator lookup, topic-name lookup and the guarded
   *   `refreshMetadataForTopicIds(ids, { isCurrent })` helper.
   * @param {string} options.groupId Share group identifier.
   * @param {string[]} options.topics Subscribed names; copied at construction.
   * @param {string} [options.rackId=''] Rack of this client; an empty rack is sent as null.
   * @param {object} [options.retry] Heartbeat backoff settings, shared by heartbeat and metadata retries.
   * @param {object} options.logger Logger with `error()`.
   * @param {function(object):void} [options.onAssignment] Called with a snapshot when the resolved assignment changes.
   * @param {function(symbol,string):void} [options.onInvalidate] Synchronous hook, called with the old token and the
   *   reason (`joined`, `fenced`, `fatal`, `stopped` or `poll-expired`). It must not return a promise.
   * @param {function(Error):void} [options.onFatal] Synchronous terminal verdict, called once.
   * @throws {Error} When member-ID entropy fails.
   */
  constructor({
    cluster,
    groupId,
    topics,
    rackId = '',
    retry,
    logger,
    onAssignment = () => {},
    onInvalidate = () => {},
    onFatal = () => {},
  }) {
    this.cluster = cluster
    this.groupId = groupId
    this.topics = topics.slice()
    this.rackId = rackId || null
    this.logger = logger
    this.onAssignment = onAssignment
    this.onInvalidate = onInvalidate
    this.onFatal = onFatal
    this.memberId = generateMemberId()
    this.memberEpoch = 0
    this.state = transition(S.UNSUBSCRIBED, S.JOINING)
    this.token = Symbol('membership')
    this.targetRevision = Symbol('assignment')
    this.target = []
    this.assignment = []
    this.unresolved = []
    this.sentTopics = null
    this.sentRack = false
    this.interval = INITIAL_HEARTBEAT_INTERVAL_MS
    this.backoff = new HeartbeatBackoff(retry)
    this.metadataBackoff = new HeartbeatBackoff(retry)
    this.coordinator = null
    this.inFlight = null
    this.resolution = null
    this.leavePromise = null
    this.closed = false
    this.terminal = false
  }

  /**
   * Read by HeartbeatManager before every schedule.
   * @returns {number} Remaining failed-heartbeat backoff while one is armed, else the broker's interval.
   */
  heartbeatInterval() {
    return this.backoff.due ? this.backoff.remaining(performance.now()) : this.interval
  }

  /**
   * @returns {{memberId:string, memberEpoch:number, state:string, token:symbol, targetRevision:symbol,
   *   assignment:Array<{topicId:string, topic:string, partitions:number[]}>, unresolved:string[]}}
   *   A fresh copy; changing its arrays cannot change the membership.
   */
  snapshot() {
    return {
      memberId: this.memberId,
      memberEpoch: this.memberEpoch,
      state: this.state,
      token: this.token,
      targetRevision: this.targetRevision,
      assignment: this.assignment.map((row) => ({
        ...row,
        partitions: row.partitions.slice(),
      })),
      unresolved: this.unresolved.slice(),
    }
  }

  /**
   * Replaces the desired names without changing the run's identity; the next successful beat
   * sends them. Validation of a running subscription belongs to the runner.
   * @param {string[]} topics Validated names.
   * @returns {void}
   */
  updateSubscription(topics) {
    this.topics = topics.slice()
  }

  /**
   * Sends at most one current heartbeat; concurrent callers share the operation, and a call made
   * while a failed beat's backoff is armed sends nothing. The broker's interval wins; both the
   * membership token and the caller's ordinary authority must remain current.
   * @param {object} [options] Ordinary request authority.
   * @param {() => Error|null} [options.guard] The run's refusal after its freeze boundary.
   * @returns {Promise<void>} Settles without waiting for metadata resolution.
   * @throws {Error} The classified failure of a current heartbeat; a retired one has no effect.
   */
  heartbeat({ guard = () => null } = {}) {
    if (this.closed || this.state === S.FATAL || this.state === S.STALE || guard())
      return Promise.resolve()
    if (this.inFlight) return this.inFlight
    if (this.backoff.pending(performance.now())) return Promise.resolve()
    const operation = this.sendHeartbeat(guard)
    this.inFlight = operation
    const clear = () => {
      if (this.inFlight === operation) this.inFlight = null
    }
    operation.then(clear, clear)
    return operation
  }

  /**
   * Looks up the coordinator and sends under one captured token, checked at every await and again
   * by the Broker before the write. Fields are remembered only after a current, non-regressing,
   * successful answer; every epoch-zero payload is full.
   * @private
   * @param {() => Error|null} ordinaryGuard The caller's captured ordinary authority.
   * @returns {Promise<void>}
   * @throws {Error} The current membership failure, after its policy has been applied.
   */
  async sendHeartbeat(ordinaryGuard) {
    const token = this.token
    const current = () =>
      !this.closed && this.token === token && this.state !== S.FATAL && !ordinaryGuard()
    const guard = () =>
      current() ? null : new KafkaJSNonRetriableError('Share membership retired')
    const topics = this.topics.slice()
    const joining = this.memberEpoch === 0
    const changed =
      this.sentTopics === null ||
      topics.length !== this.sentTopics.length ||
      topics.some((topic, i) => topic !== this.sentTopics[i])
    const payload = {
      groupId: this.groupId,
      memberId: this.memberId,
      memberEpoch: this.memberEpoch,
      rackId: joining || !this.sentRack ? this.rackId : null,
      subscribedTopicNames: joining || changed ? topics : null,
    }
    this.backoff.takeDue()
    try {
      if (!this.coordinator) {
        const coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId })
        if (!current()) return
        this.coordinator = coordinator
      }
      const response = await this.coordinator.shareGroupHeartbeat(payload, { guard })
      if (!current()) return
      if (response.errorCode) throw createErrorFromCode(response.errorCode)
      if (response.memberEpoch < this.memberEpoch) return
      if (payload.memberEpoch === 0 && response.memberEpoch > 0) {
        const previous = this.token
        this.token = Symbol('membership')
        this.targetRevision = Symbol('assignment')
        this.resolution = null
        this.metadataBackoff.reset()
        const accepted = this.token
        this.notify(this.onInvalidate, previous, 'joined')
        if (this.closed || this.token !== accepted) return
      }
      this.memberEpoch = response.memberEpoch
      this.interval = response.heartbeatIntervalMs
      this.sentTopics = topics
      this.sentRack = true
      this.backoff.reset()
      if (this.memberEpoch > 0) this.state = transition(this.state, S.STABLE)
      if (response.assignment !== null) this.adoptTarget(response.assignment.topicPartitions)
      this.publishAssignment()
      this.refreshUnresolved(ordinaryGuard)
    } catch (error) {
      if (!current()) return
      throw this.onMembershipError(error, token)
    }
  }

  /**
   * Normalizes an answer's assignment and, when it differs from the current target, starts a new
   * target revision. An identical answer keeps the running metadata flight and its backoff, so it
   * is not another chance to bypass a pending retry.
   * @private
   * @param {Array<{topicId:string, partitions:number[]}>} topicPartitions Rows of a non-null
   *   assignment; an empty list replaces the target at once. A null assignment never reaches here
   *   and keeps the target.
   * @returns {void}
   */
  adoptTarget(topicPartitions) {
    const target = topicPartitions
      .map(({ topicId, partitions }) => ({
        topicId,
        partitions: [...new Set(partitions)].sort((a, b) => a - b),
      }))
      .sort((a, b) => a.topicId.localeCompare(b.topicId))
    if (JSON.stringify(target) === JSON.stringify(this.target)) return
    this.targetRevision = Symbol('assignment')
    this.target = target
    this.resolution = null
    this.metadataBackoff.reset()
  }

  /**
   * Applies a membership failure verdict exactly once, for a heartbeat or for a fetch or
   * acknowledgement that learned of the same membership. Callers capture the token before their
   * first await. A retired membership ignores every verdict: a closed one (leaving, stale or
   * unsubscribed) and a fatal one never act on a late answer, so a late code 69 cannot restart or
   * crash a replacement. For a current membership the action decides:
   * - `coordinator`: forget the coordinator and retry under backoff, keeping the membership;
   * - `fence`: retire the token and rejoin at epoch zero with the same member ID;
   * - `fatal`, `unsupported` and `groupNotFound` (code 69 answering anything but this member's
   *   own leave, which `leave` handles): enter FATAL with a non-retriable error and notify
   *   `onFatal` once.
   * @param {Error} error Original decoded or transport failure.
   * @param {symbol} token Originating membership identity.
   * @returns {Error|null} The classified error for a current verdict, null for a discarded one.
   */
  onMembershipError(error, token) {
    if (this.closed || this.state === S.FATAL || this.token !== token) return null
    this.sentTopics = null
    this.sentRack = false
    this.backoff.onFailedAttempt(performance.now())
    const action = membershipAction(error)
    if (action === 'coordinator') this.coordinator = null
    if (action === 'fence') {
      this.state = transition(this.state, S.FENCED)
      this.retire('fenced')
      this.memberEpoch = 0
      this.coordinator = null
      this.state = transition(this.state, S.JOINING)
    }
    if (action === 'fatal' || action === 'unsupported' || action === 'groupNotFound') {
      error = fatalMembershipError(error)
      this.state = transition(this.state, S.FATAL)
      this.retire('fatal')
      this.notify(this.onFatal, error)
    }
    return error
  }

  /**
   * Publishes the known target IDs; unknown IDs never hold up known partitions or revoke them.
   * Notifies `onAssignment` only when the resolved set changed.
   * @private
   * @returns {void}
   */
  publishAssignment() {
    const assignment = []
    const unresolved = []
    for (const { topicId, partitions } of this.target) {
      const topic = this.cluster.topicName(topicId)
      if (topic == null) unresolved.push(topicId)
      else assignment.push({ topicId, topic, partitions: partitions.slice() })
    }
    this.unresolved = unresolved
    const previous = JSON.stringify(this.assignment)
    this.assignment = assignment
    if (JSON.stringify(assignment) !== previous) this.notify(this.onAssignment, this.snapshot())
  }

  /**
   * Starts one metadata flight for the unresolved IDs when none is running and the metadata
   * backoff allows it. Every heartbeat answer is another chance, there is no attempt ceiling and
   * no metadata-age timer. The flight is bound to the captured token and target revision, and the
   * same predicate goes to the cluster helper, so a retired flight cannot install, follow up or
   * publish. Misses and failures both advance the backoff.
   * @private
   * @param {() => Error|null} [ordinaryGuard] The authority of the heartbeat that started this work.
   * @returns {void}
   */
  refreshUnresolved(ordinaryGuard = () => null) {
    if (
      this.closed ||
      this.state === S.FATAL ||
      !this.unresolved.length ||
      this.resolution ||
      this.metadataBackoff.pending(performance.now())
    )
      return
    const token = this.token
    const revision = this.targetRevision
    const flight = {}
    const isCurrent = () =>
      !this.closed &&
      this.state !== S.FATAL &&
      !ordinaryGuard() &&
      this.token === token &&
      this.targetRevision === revision
    this.resolution = flight
    const work = async () => {
      try {
        await this.cluster.refreshMetadataForTopicIds(this.unresolved.slice(), { isCurrent })
        if (!isCurrent()) return
        this.publishAssignment()
        if (this.unresolved.length) this.metadataBackoff.onFailedAttempt(performance.now())
        else this.metadataBackoff.reset()
      } catch (error) {
        if (isCurrent()) this.metadataBackoff.onFailedAttempt(performance.now())
      } finally {
        if (this.resolution === flight) this.resolution = null
      }
    }
    work()
  }

  /**
   * Replaces the token, clears the target and tells the runner, synchronously and in that order,
   * so old work is invalid before any hook settles old sessions or ledgers.
   * @private
   * @param {string} reason Retirement cause passed to `onInvalidate`.
   * @returns {void}
   */
  retire(reason) {
    const old = this.token
    this.token = Symbol('membership')
    this.targetRevision = Symbol('assignment')
    this.target = []
    this.resolution = null
    this.metadataBackoff.reset()
    this.notify(this.onInvalidate, old, reason)
    this.publishAssignment()
  }

  /**
   * Retires ordinary authority at once, then sends one best-effort leave under the caller's
   * captured closing authority. The promise is latched before any notification, so a re-entrant
   * or repeated call shares it and never upgrades the episode: only the first call's `stale`
   * counts, and ending a stale episode for good takes `stop()`. A lost or refused leave is logged
   * and never retried here.
   *
   * The leave goes through the cached coordinator only: no lookup and no new connection. A
   * refusal of the closing guard before the write sends nothing. A reply that arrives after the
   * guard refused is not acted on. GROUP_ID_NOT_FOUND (69) answering the leave means the group is
   * gone, so the leave is complete: no failure is logged, nothing is retried, and no fatal
   * verdict is raised. Any other error code is logged. Whatever the outcome, the episode ends by
   * moving to STALE (poll expiry) or UNSUBSCRIBED; the next episode cannot start before that,
   * because `rejoinAfterPoll` only accepts STALE.
   * @param {object} [options] Runner closing context.
   * @param {boolean} [options.stale=false] Poll expiry, which may rejoin, instead of a terminal stop.
   * @param {function():Promise<void>} [options.beforeLeave] Lazy, bounded final work run before the frame.
   * @param {function():Error|null} [options.guard] The original closing episode's refusal.
   * @returns {Promise<void>} Resolves when the episode ended; never rejects.
   * @throws {Error} Synchronously, when the current state cannot start a leave.
   */
  leave({ stale = false, beforeLeave = async () => {}, guard = () => null } = {}) {
    if (this.leavePromise) return this.leavePromise
    if (this.state === S.FATAL || (this.closed && this.terminal)) return Promise.resolve()
    this.state = transition(this.state, S.PREPARE_LEAVING)
    let complete
    this.leavePromise = new Promise((resolve) => {
      complete = resolve
    })
    this.closed = true
    if (!stale) this.terminal = true
    this.retire(stale ? 'poll-expired' : 'stopped')
    this.state = transition(this.state, S.LEAVING)
    const work = async () => {
      try {
        if (guard()) return
        await beforeLeave()
        if (guard()) return
        const coordinator = this.coordinator
        if (!coordinator) return
        const response = await coordinator.shareGroupHeartbeat(
          {
            groupId: this.groupId,
            memberId: this.memberId,
            memberEpoch: LEAVE_EPOCH,
            rackId: null,
            subscribedTopicNames: null,
          },
          { guard }
        )
        if (guard()) return
        if (response.errorCode) {
          const error = createErrorFromCode(response.errorCode)
          if (membershipAction(error) !== 'groupNotFound') throw error
        }
      } catch (error) {
        guard()
        this.logger.error('Share leave failed', { error: error.message })
      } finally {
        this.state = transition(this.state, stale ? S.STALE : S.UNSUBSCRIBED)
      }
    }
    work().then(complete, complete)
    return this.leavePromise
  }

  /**
   * Rejoins at epoch zero after a poll-expiry leave, once the runner shows that every captured
   * expired call finished or was abandoned. Does nothing in any other state, after `stop()`, after
   * a `leave()` without `stale` that began its own episode, or while a captured call is still
   * running. A `leave()` made while a stale episode's promise is still latched only shares it and
   * does not make the episode terminal, so only `stop()` ends recovery for good there.
   * @param {SharePollView} poll The run's own poll view.
   * @param {object[]} captured The expired call identities `poll.captureExpired()` returned.
   * @returns {void}
   */
  rejoinAfterPoll(poll, captured) {
    if (this.state !== S.STALE || this.terminal || !poll || !captured || !poll.drained(captured))
      return
    this.closed = false
    this.memberEpoch = 0
    this.sentTopics = null
    this.sentRack = false
    this.coordinator = null
    this.leavePromise = null
    this.backoff.reset()
    this.state = transition(this.state, S.JOINING)
  }

  /**
   * Retires the run for good without any traffic: the zero-budget close. It is not a prelude to a
   * leave that sends a frame, which `leave` retires itself.
   * @returns {void}
   */
  stop() {
    this.terminal = true
    if (this.closed) return
    this.closed = true
    this.retire('stopped')
  }

  /**
   * Runs a notification so an observer's exception cannot undo retirement or a leave.
   * @private
   * @param {function} callback Notification.
   * @param {...*} args Values passed to it.
   * @returns {void}
   */
  notify(callback, ...args) {
    try {
      callback(...args)
    } catch (error) {
      this.logger.error('Share membership observer failed', { error: error.message })
    }
  }
}

/**
 * One run's monotonic view of its active handler calls, read by HeartbeatManager in place of a
 * PollTimer. Each call has its own entry, so progress at one node never resets another's clock,
 * and an idle view is healthy. No timer per record or node.
 */
class SharePollView {
  /** @param {{maxPollInterval:number}} options Maximum duration of the oldest active call, in ms. */
  constructor({ maxPollInterval }) {
    this.maxPollInterval = maxPollInterval
    this.calls = new Map()
    this.stopped = false
  }

  /**
   * Tracks an existing handler invocation with its independent monotonic start time.
   * @param {import('./shareHandlerCall').ShareHandlerCall} call Original invocation identity.
   * @returns {object|null} The same invocation, or null after `stop()`.
   */
  begin(call) {
    if (this.stopped) return null
    this.calls.set(call, performance.now())
    return call
  }

  /**
   * Removes exactly this call, never another invocation of the same node.
   * @param {object} call Token from `begin`; a late return of an old run is harmless.
   * @returns {void}
   */
  finish(call) {
    this.calls.delete(call)
  }

  /** @returns {number} Age of the oldest active call in ms; idle is always zero. */
  elapsed() {
    if (this.stopped || !this.calls.size) return 0
    let oldest = Infinity
    for (const startedAt of this.calls.values()) oldest = Math.min(oldest, startedAt)
    return performance.now() - oldest
  }

  /** @returns {number} The configured maximum interval, for event payloads. */
  deadline() {
    return this.maxPollInterval
  }

  /** @returns {boolean} Whether the oldest call is strictly older than the maximum, as PollTimer. */
  expired() {
    return !this.stopped && this.elapsed() > this.maxPollInterval
  }

  /** @returns {object[]} Every call older than the maximum right now, for the delayed rejoin. */
  captureExpired() {
    const now = performance.now()
    return [...this.calls]
      .filter(([, start]) => now - start > this.maxPollInterval)
      .map(([call]) => call)
  }

  /**
   * @param {object[]} captured Identities from `captureExpired`.
   * @returns {boolean} Whether every one finished or was abandoned.
   */
  drained(captured) {
    return captured.every((call) => !this.calls.has(call))
  }

  /** Stops reporting and abandons every call; a new run creates a new view. @returns {void} */
  stop() {
    this.stopped = true
    this.calls.clear()
  }
}

module.exports = { ShareMembership, SharePollView }
