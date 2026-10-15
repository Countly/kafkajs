const sharedPromiseTo = require('../../utils/sharedPromiseTo')
const retryDefaults = require('../../retry/defaults')
const apiKeys = require('../../protocol/requests/apiKeys')
const { COOPERATIVE } = require('./interface')
const { generateMemberId } = require('./memberId')
const { SentFields, buildRequest } = require('./heartbeatState')
const {
  MEMBER_STATES,
  transition,
  canHandleNewAssignment,
  shouldHeartbeatNow,
  shouldSkipHeartbeat,
  isNotInGroup,
  isLeavingGroup,
} = require('./memberState')
const {
  stampGeneration,
  assignmentChangedError,
  consumerStoppingError,
  joinDeadlineError,
  unsupportedConsumerProtocolError,
  isConsumerProtocolFatal,
} = require('../rebalanceErrorContract')
const {
  events: { HEARTBEAT, PARTITIONS_REVOKED, PARTITIONS_LOST },
} = require('../instrumentationEvents')

const {
  UNSUBSCRIBED,
  JOINING,
  RECONCILING,
  ACKNOWLEDGING,
  STABLE,
  PREPARE_LEAVING,
  LEAVING,
  FENCED,
  FATAL,
  STALE,
} = MEMBER_STATES

/**
 * Until the coordinator has answered once there is no interval; a bounded default keeps the join
 * retry alive. Named for what it is - the interval before the first response - so it cannot be
 * read as this protocol's steady-state cadence, which is always the server's own value.
 */
const INITIAL_HEARTBEAT_INTERVAL_MS = 1000

const LEAVE_EPOCH_DYNAMIC = -1
const LEAVE_EPOCH_STATIC = -2

/**
 * The member epoch a leave carries. A static member says it is only going away for now (-2) unless
 * the caller asked for it to leave (`leaveGroup: true`), which has to use -1: the coordinator
 * removes a static member that leaves at -1 and keeps its slot for one that leaves at -2.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerMembershipManager.java
 * @param {boolean} isStatic whether the member has a `groupInstanceId`
 * @param {boolean} [leaveGroup] the caller's choice, `undefined` for the default
 * @returns {number}
 */
const leaveEpochFor = (isStatic, leaveGroup) =>
  isStatic && leaveGroup !== true ? LEAVE_EPOCH_STATIC : LEAVE_EPOCH_DYNAMIC

/**
 * How many refreshes may report the same topic id unresolved before it stops being asked for. An
 * id that is merely late is retried; one the cluster has now twice refused to name would otherwise
 * cost a Metadata request per metadata max age for the life of the membership.
 */
const MAX_UNRESOLVED_ATTEMPTS = 2

const COORDINATOR_LOAD_IN_PROGRESS = 14
const COORDINATOR_NOT_AVAILABLE = 15
const NOT_COORDINATOR = 16
const UNKNOWN_MEMBER_ID = 25
const TOPIC_AUTHORIZATION_FAILED = 29
const UNSUPPORTED_VERSION = 35
const GROUP_ID_NOT_FOUND = 69
const FENCED_MEMBER_EPOCH = 110
const INVALID_REGULAR_EXPRESSION = 128

/**
 * The prefix the Java client puts in front of a broker-rejected pattern, kept verbatim so the same
 * subscription produces the same message text in both clients.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerMembershipManager.java
 */
const INVALID_REGEX_PREFIX = 'Invalid RE2J SubscriptionPattern provided in the call to subscribe. '

/**
 * A promise plus its settlers, so a heartbeat response can complete a join the round is awaiting.
 * The spare `catch` observes the rejection on a DIFFERENT promise, which keeps a rejection that
 * settles before the round awaits from reaching the process as unhandled without swallowing it.
 * @returns {{ promise: Promise<void>, resolve: () => void, reject: (e: Error) => void }}
 */
const deferred = () => {
  let resolve
  let reject
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  promise.catch(() => {})
  return { promise, resolve, reject }
}

/**
 * @param {import('./interface').TopicPartitions[]} topicPartitions
 * @returns {Map<string, Set<number>>}
 */
const indexByTopic = (topicPartitions) => {
  const map = new Map()
  for (const { topic, partitions } of topicPartitions) {
    map.set(topic, new Set(partitions))
  }
  return map
}

/**
 * @param {Map<string, Set<number>>} from
 * @param {Map<string, Set<number>>} minus
 * @returns {import('./interface').TopicPartitions[]}
 */
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

/**
 * The payload `Broker#consumerGroupHeartbeat` takes, from a request built in wire field names.
 *
 * The two shapes differ on purpose: the diff and `SentFields` are written against the wire's own
 * names, while the broker method takes the repo's camelCase form and turns an omitted key into the
 * protocol's "unchanged since the last heartbeat" default. Translating here keeps that to one
 * place - the single call site - rather than teaching either side about the other.
 * @param {Object} request
 * @returns {Object}
 */
const toBrokerPayload = (request) => {
  const payload = {
    groupId: request.GroupId,
    memberId: request.MemberId,
    memberEpoch: request.MemberEpoch,
  }

  if (request.InstanceId !== undefined) {
    payload.instanceId = request.InstanceId
  }

  if (request.RackId !== undefined) {
    payload.rackId = request.RackId
  }

  if (request.RebalanceTimeoutMs !== undefined) {
    payload.rebalanceTimeoutMs = request.RebalanceTimeoutMs
  }

  if (request.SubscribedTopicNames !== undefined) {
    payload.subscribedTopicNames = request.SubscribedTopicNames
  }

  if (request.SubscribedTopicRegex !== undefined) {
    payload.subscribedTopicRegex = request.SubscribedTopicRegex
  }

  if (request.ServerAssignor !== undefined) {
    payload.serverAssignor = request.ServerAssignor
  }

  if (request.TopicPartitions !== undefined) {
    payload.topicPartitions = request.TopicPartitions.map(({ TopicId, Partitions }) => ({
      topicId: TopicId,
      partitions: Partitions,
    }))
  }

  return payload
}

/**
 * The exponential backoff a heartbeat error arms, with the same shape the client's retrier uses:
 * `initialRetryTime * multiplier ** attempts`, jittered by `factor` and capped at `maxRetryTime`.
 * Its remaining delay is what the membership reports as its heartbeat interval while it is armed,
 * because the heartbeat manager reads only that.
 */
class HeartbeatBackoff {
  /** @param {{ initialRetryTime?: number, maxRetryTime?: number, multiplier?: number, factor?: number }} [retry] */
  constructor(retry = {}) {
    const { initialRetryTime, maxRetryTime, multiplier, factor } = { ...retryDefaults, ...retry }
    this.initialRetryTime = initialRetryTime
    this.maxRetryTime = maxRetryTime
    this.multiplier = multiplier
    this.factor = factor
    this.reset()
  }

  /** Clears the backoff entirely, so the next attempt is immediate. @returns {void} */
  reset() {
    this.attempts = 0
    this.until = 0
    this.due = false
  }

  /**
   * Clears the backoff and marks one request due right away: an answer that says "ask the new
   * coordinator" is not a reason to wait, and the request must not be held by the interval gate
   * either.
   * @returns {void}
   */
  retryNow() {
    this.reset()
    this.due = true
  }

  /** @param {number} now @returns {void} */
  onFailedAttempt(now) {
    const base = Math.min(
      this.maxRetryTime,
      this.initialRetryTime * this.multiplier ** this.attempts
    )
    this.attempts += 1
    this.until = now + Math.round(base * (1 + this.factor * (Math.random() * 2 - 1)))
    this.due = true
  }

  /**
   * Whether the one retry this backoff owes has come due. It is a single token, not a mode: an
   * error that neither arms a backoff nor clears one - `TOPIC_AUTHORIZATION_FAILED` is the code
   * designed to recur forever without touching it - must not leave the interval gate bypassed for
   * every later call.
   * @param {number} now
   * @returns {boolean}
   */
  isDue(now) {
    return this.due && !this.pending(now)
  }

  /** Consumes the retry an elapsed backoff owed, so it lets exactly one request past. @returns {void} */
  takeDue() {
    this.due = false
  }

  /** @param {number} now @returns {boolean} */
  pending(now) {
    return this.until > now
  }

  /** @param {number} now @returns {number} milliseconds left, 0 when nothing is armed */
  remaining(now) {
    return Math.max(0, this.until - now)
  }
}

/**
 * Membership over the ConsumerGroupHeartbeat protocol, where the coordinator computes the
 * assignment and the member reconciles towards it one step at a time.
 *
 * One heartbeat carries what changed since the last one and comes back with a member epoch and,
 * when the coordinator wants something different, a target assignment keyed by topic id. The
 * member applies what it can, acknowledges by sending the partitions it now owns, and the epoch
 * moves. There is no JoinGroup, no SyncGroup, no leader and no client-side assignor.
 *
 * It owns no round loop and emits no rebalance event: `rebalance()` is ONE reconciliation step and
 * the rebalance coordinator owns the rest, exactly as it does for the classic protocol.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerMembershipManager.java
 * @implements {import('./interface').GroupProtocol}
 */
module.exports = class ConsumerGroupProtocol {
  /** The join deferred, private so nothing outside this class can settle it. */
  #firstMembership = null

  /**
   * @param {object} options
   * @param {import('../../../types').Cluster} options.cluster
   * @param {string} options.groupId
   * @param {string|null} [options.groupInstanceId] static membership id
   * @param {number} options.rebalanceTimeout the value sent as RebalanceTimeoutMs, and the deadline
   *   a join is allowed to take
   * @param {string} [options.rackId] the consumer's rack; the empty string means "no rack"
   * @param {string|null} [options.serverAssignor] the coordinator-side assignor to ask for
   * @param {string[]} [options.topics] the subscribed topic names
   * @param {string|null} [options.subscribedTopicRegex] the broker-side pattern, when there is one
   * @param {number} [options.metadataMaxAge] how often an unresolved topic id may trigger a refresh
   * @param {object} [options.retry] the client's retry configuration, for the heartbeat backoff
   * @param {import('../../instrumentation/emitter')} options.instrumentationEmitter
   * @param {import('../../../types').Logger} options.logger
   */
  constructor({
    cluster,
    groupId,
    groupInstanceId = null,
    rebalanceTimeout,
    rackId = '',
    serverAssignor = null,
    topics = [],
    subscribedTopicRegex = null,
    metadataMaxAge = 300000,
    retry = {},
    instrumentationEmitter,
    logger,
  }) {
    this.cluster = cluster
    this.groupId = groupId
    this.groupInstanceId = groupInstanceId
    this.rebalanceTimeoutMs = rebalanceTimeout
    // The empty string is the shipped default and means "no rack", which on the wire is null.
    this.rackId = rackId === '' ? null : rackId
    this.serverAssignor = serverAssignor
    this.metadataMaxAge = metadataMaxAge
    this.instrumentationEmitter = instrumentationEmitter
    // The group's own logger object, NOT a new namespace: existing spies on the group's logger
    // must keep observing what is logged here.
    this.logger = logger

    this.state = UNSUBSCRIBED
    this.memberEpoch = 0
    /**
     * How many joins the coordinator has accepted. Only ever goes up: a rejoin can come back at the
     * epoch the member left, and the member id never changes, so the count is the only thing that
     * tells a replacing membership from the one it replaced.
     */
    this.joinCount = 0
    this.coordinator = null
    this.heartbeatIntervalMs = INITIAL_HEARTBEAT_INTERVAL_MS
    this.generatedMemberId = generateMemberId()
    this.fatalError = null
    this.pollTimerExpired = false

    this.subscribedTopicNames = [...topics].sort()
    this.subscribedTopicRegex = subscribedTopicRegex
    this.regexUpdated = subscribedTopicRegex != null

    /** @type {{ localEpoch: number, byTopicId: Map<string, number[]> }|null} what the coordinator wants */
    this.target = null
    /** @type {import('./interface').TopicPartitions[]} what this member has */
    this.ownedPartitions = []
    /** @type {{ localEpoch: number, byTopicId: Map<string, number[]> }} what was last acknowledged */
    this.localAssignment = { localEpoch: 0, byTopicId: new Map() }
    /** @type {Set<string>} target ids whose topic name is not known yet */
    this.unresolvedTopicIds = new Set()
    /** @type {Map<string, number>} how often a refresh has reported an id still unresolved */
    this.unresolvedAttempts = new Map()
    /** @type {Map<string, string>} id to name, so a topic flushed from metadata still resolves */
    this.assignedTopicNames = new Map()
    /** @type {Set<string>} names already handed to the subscription, so it is widened once each */
    this.grownTopicNames = new Set()
    /** The resolvable subset the current step returned, adopted when the apply acknowledges it. */
    this.pendingResolved = null

    this.sentFields = new SentFields()
    this.lastRequestAt = 0
    this.immediatePending = false
    this.backoff = new HeartbeatBackoff(retry)
    // Seeded to now, not to 0: zero reads as "refreshed at the epoch", which makes the very first
    // heartbeat's staleness check fire a metadata request the reconciliation step is about to make
    // anyway. The heartbeat-path refresh is the slow retry for ids that stay unresolved.
    this.lastMetadataRefreshAt = Date.now()
    this.unauthorizedMessage = null

    /** @type {Object|null} set by `attach` */
    this.rebalanceCoordinator = null
    /** @type {Object|null} set by `attach` */
    this.heartbeatManager = null
    /** @type {((partitions: import('./interface').TopicPartitions[]) => Promise<void>)|null} */
    this.commitBeforeRelease = null
    /** @type {Promise<void>|null} the leave on the wire, whichever entry point started it */
    this.inFlightLeave = null

    // The classic protocol wraps its heartbeat the same way: `Runner#heartbeat()` is called per
    // message, per batch and on every empty fetch round, so without the single flight AND the
    // interval gate below a busy consumer would issue one request per record.
    this.sharedHeartbeat = sharedPromiseTo(async ({ interval }) => {
      if (shouldSkipHeartbeat(this.state)) {
        return
      }

      const now = Date.now()

      if (this.backoff.pending(now)) {
        return
      }

      if (this.immediatePending && shouldHeartbeatNow(this.state)) {
        await this.sendHeartbeat({ sendAllFields: this.state === JOINING })
        return
      }

      // A backoff that has ELAPSED is itself the schedule. The interval gate is measured from the
      // last request, which is always more recent than the shorter backoff, so leaving the gate in
      // front of a due retry would swallow it until the ordinary interval came round anyway - and
      // the backoff would never grow past its first step. It is ONE request, consumed by the send
      // below: this is the retry the backoff owed, not a mode the gate stays off in.
      if (this.backoff.isDue(now)) {
        await this.sendHeartbeat({ sendAllFields: this.state === JOINING })
        return
      }

      if (now < this.lastRequestAt + Math.max(interval, this.heartbeatIntervalMs)) {
        return
      }

      await this.sendHeartbeat({ sendAllFields: false })
    })

    this.sharedLeave = sharedPromiseTo((options) => this.performLeave(options))
  }

  /** @returns {'consumer'} */
  get kind() {
    return 'consumer'
  }

  /**
   * The member id, generated once and kept for the life of this membership.
   * @returns {string}
   */
  get memberId() {
    return this.generatedMemberId
  }

  /**
   * Ignored. The coordinator identifies this member by an id the client generated, so dropping it
   * on a lost generation - which the classic protocol does - would make the member unrecognisable
   * rather than re-registerable.
   * @param {string|null} value
   */
  set memberId(value) {
    this.logger.debug('Ignoring a member id assignment; this protocol keeps its own id', {
      groupId: this.groupId,
      memberId: this.generatedMemberId,
      attempted: value,
    })
  }

  /**
   * The member epoch, reported under the name every existing consumer of a generation uses, so
   * events, offset commits and the stale-answer guard all carry the epoch without change.
   * @returns {number}
   */
  get generationId() {
    return this.memberEpoch
  }

  /** Ignored: only the coordinator moves the epoch. @param {number|null} value */
  set generationId(value) {
    this.logger.debug('Ignoring a generation assignment; the member epoch is the coordinator’s', {
      groupId: this.groupId,
      attempted: value,
    })
  }

  /** @returns {null} there is no leader under this protocol */
  get leaderId() {
    return null
  }

  /** @returns {null} the member list is the coordinator's, and is not sent to members */
  get members() {
    return null
  }

  /** @returns {string} the server-side assignor once one is known, never null */
  get groupProtocol() {
    return this.serverAssignor || ''
  }

  /** @returns {boolean} */
  isLeader() {
    return false
  }

  /** @returns {import('./interface').RebalanceProtocol} */
  rebalanceProtocol() {
    return COOPERATIVE
  }

  /**
   * Late-bound collaborators.
   * @param {{ rebalanceCoordinator: Object, heartbeatManager: Object,
   *   commitBeforeRelease?: ((partitions: import('./interface').TopicPartitions[]) => Promise<void>)|null }} deps
   * @returns {void}
   */
  attach({ rebalanceCoordinator, heartbeatManager, commitBeforeRelease = null }) {
    this.rebalanceCoordinator = rebalanceCoordinator
    this.heartbeatManager = heartbeatManager
    this.commitBeforeRelease = commitBeforeRelease
  }

  /** Resolves the group coordinator. @returns {Promise<void>} */
  async connect() {
    this.coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId })
  }

  /**
   * How long until the next heartbeat should go out. While a backoff is armed this is its
   * remaining delay, which is how a backoff takes effect at all: the heartbeat manager reads this
   * and nothing else when it schedules.
   * @returns {number}
   */
  heartbeatInterval() {
    const remaining = this.backoff.remaining(Date.now())
    return remaining > 0 ? remaining : this.heartbeatIntervalMs
  }

  /**
   * Whether the coordinator wants something this member does not have yet - counting only the
   * partitions whose topic name is known. An unresolved topic id deliberately does NOT make this
   * true: an id that can never resolve would otherwise ask for a rebalance round forever.
   * @returns {boolean}
   */
  needsReconciliation() {
    return this.target != null && !this.matchesOwned(this.resolveFromCache(this.target))
  }

  /**
   * ONE reconciliation step.
   *
   * The round loop calls this through the group's retrier. It joins when the member is not in the
   * group, resolves the target's topic ids, widens the subscription with any name the coordinator
   * assigned that this consumer has not seen, and returns the resolvable subset for the caller to
   * apply. It never applies anything itself and never sends the acknowledgement: the apply calls
   * `setOwned`, and that is where the acknowledgement is triggered.
   *
   * @param {{ topics: string[], topicsSubscribed: string[], beforeSync: () => void,
   *   growSubscription?: (names: string[]) => Promise<void> }} context
   * @returns {Promise<{ assignment: Object, userData: null }>} the assignment in the name-keyed
   *   shape the apply expects
   * @throws {Error} the stored fatal error when this member may never rejoin; whatever the join
   *   failed with otherwise
   */
  async rebalance(context) {
    const { beforeSync, growSubscription } = context

    if (this.state === FATAL) {
      throw this.fatalError
    }

    // State as well as epoch: a fenced member keeps the epoch it was fenced at on purpose, so an
    // epoch-only gate would skip the join and report a successful round for a member that is not
    // in the group and sends nothing.
    if (this.memberEpoch <= 0 || isNotInGroup(this.state) || this.state === JOINING) {
      if (this.state !== JOINING) {
        // Entering JOINING from anywhere else starts a NEW join, so any deferred left over from a
        // previous one is cleared first. Reusing it would skip `armFirstMembership` - and with it
        // `immediatePending` and `requestTick()` - so no joining heartbeat would ever go out and
        // this round would wait on the abandoned attempt's deadline instead of its own.
        this.clearFirstMembership()
        this.transitionTo(JOINING)
        this.memberEpoch = 0
        this.pollTimerExpired = false
        this.sentFields.reset()
        this.clearTarget()
        this.localAssignment = { localEpoch: 0, byTopicId: new Map() }
      }

      await (this.#firstMembership || this.armFirstMembership()).promise
    }

    if (this.target == null) {
      return { assignment: this.ownedAsObject(), userData: null }
    }

    const resolved = await this.resolveAssignmentTopicNames(this.target)
    await this.growSubscriptionWith(growSubscription, resolved.names)

    // The coordinator sees no progress unless something is acknowledged, and an apply that changes
    // nothing never calls `setOwned` with a difference - so a subset that already equals what is
    // owned is acknowledged here, under the target's bumped local epoch.
    if (this.matchesOwned(resolved)) {
      this.adoptLocalAssignment(this.target.localEpoch, resolved)

      if (this.state === RECONCILING) {
        this.acknowledge()
      }

      return { assignment: this.ownedAsObject(), userData: null }
    }

    this.pendingResolved = resolved
    beforeSync()

    return { assignment: resolved.assignment, userData: null }
  }

  /**
   * Arms a fresh join attempt: a deferred that a heartbeat response resolves, a deadline that
   * bounds it, and one immediate joining heartbeat.
   *
   * The deadline is not optional. Without it a coordinator that keeps answering
   * `COORDINATOR_LOAD_IN_PROGRESS` - which is swallowed rather than raised - leaves the retrier
   * with nothing to retry and the round never settles, which holds a `stop()` open forever.
   *
   * @private
   * @returns {{ promise: Promise<void>, resolve: Function, reject: Function }}
   */
  armFirstMembership() {
    const record = deferred()
    record.deadline = setTimeout(
      () =>
        this.rejectFirstMembership(
          joinDeadlineError({ rebalanceTimeoutMs: this.rebalanceTimeoutMs })
        ),
      this.rebalanceTimeoutMs
    )

    this.#firstMembership = record
    this.immediatePending = true
    this.requestTick()

    return record
  }

  /**
   * Reports the join, once, and counts it. Clearing the deferred is what keeps a later attempt from
   * awaiting a settled promise.
   * @private
   * @returns {void}
   */
  resolveFirstMembership() {
    const record = this.#firstMembership
    if (!record) return

    this.joinCount += 1
    this.#firstMembership = null
    clearTimeout(record.deadline)
    record.resolve()
  }

  /**
   * Fails the join, once. Whatever settles the deferred with a rejection also clears it: re-awaiting
   * a settled rejection would burn every retry synchronously on a stale error, and a heartbeat that
   * succeeded meanwhile could not resolve a settled promise, so a member that IS in the group could
   * never report a join.
   * @private
   * @param {Error} error
   * @returns {void}
   */
  rejectFirstMembership(error) {
    const record = this.#firstMembership
    if (!record) return

    this.#firstMembership = null
    clearTimeout(record.deadline)
    record.reject(error)
  }

  /**
   * Drops an outstanding join attempt without failing anything: its deadline is cleared and its
   * promise is RESOLVED, not rejected.
   *
   * Called when a new join begins and when a poll overrun parks the member, both of which abandon
   * whatever attempt was in flight without the consumer going away. A caller that is still waiting
   * continues into the "no target yet" branch and gets an empty round, which the coordinator simply
   * runs again.
   * @private
   * @returns {void}
   */
  clearFirstMembership() {
    const record = this.#firstMembership
    if (!record) return

    this.#firstMembership = null
    clearTimeout(record.deadline)
    record.resolve()
  }

  /**
   * Fails an outstanding join because the consumer is stopping. Pushed by the runner rather than
   * polled: without it a `stop()` that lands while the join is in flight cancels the heartbeat
   * chain, so nothing can retry the joining heartbeat and the round never settles.
   * @returns {void}
   */
  onStopRequested() {
    this.rejectFirstMembership(consumerStoppingError())
  }

  /**
   * Resolves the target's topic ids to names, asking the cluster for metadata BY ID once when some
   * are unknown. Ids that stay unresolved are kept in the target and retried from the heartbeat
   * path; the member reconciles and acknowledges the resolvable subset meanwhile.
   * @private
   * @param {{ localEpoch: number, byTopicId: Map<string, number[]> }} target
   * @returns {Promise<{ assignment: Object, byTopicId: Map<string, number[]>, names: string[], unresolved: string[] }>}
   */
  async resolveAssignmentTopicNames(target) {
    let resolved = this.resolveFromCache(target)

    if (resolved.unresolved.length) {
      await this.refreshMetadataForTopicIds(resolved.unresolved)
      resolved = this.resolveFromCache(target)
    }

    this.recordResolution(resolved)
    return resolved
  }

  /**
   * The same resolution against what is already known, without any request. Pure: the caches are
   * written by `recordResolution`, so this can be called from a predicate.
   * @private
   * @param {{ byTopicId: Map<string, number[]> }} target
   * @returns {{ assignment: Object, byTopicId: Map<string, number[]>, names: string[], unresolved: string[] }}
   */
  resolveFromCache(target) {
    const assignment = {}
    const byTopicId = new Map()
    const names = []
    const unresolved = []

    for (const [topicId, partitions] of target.byTopicId) {
      const name = this.cluster.topicName(topicId) || this.assignedTopicNames.get(topicId)

      if (name) {
        const sorted = [...partitions].sort((a, b) => a - b)
        byTopicId.set(topicId, sorted)
        assignment[name] = sorted
        names.push(name)
      } else {
        unresolved.push(topicId)
      }
    }

    return { assignment, byTopicId, names, unresolved }
  }

  /**
   * Remembers what resolved and what did not.
   * @private
   * @param {{ byTopicId: Map<string, number[]>, names: string[], unresolved: string[] }} resolved
   * @returns {boolean} whether a name resolved that had not resolved before
   */
  recordResolution(resolved) {
    let grew = false
    let index = 0

    for (const topicId of resolved.byTopicId.keys()) {
      if (!this.assignedTopicNames.has(topicId)) {
        grew = true
      }
      this.assignedTopicNames.set(topicId, resolved.names[index++])
      this.unresolvedTopicIds.delete(topicId)
      this.unresolvedAttempts.delete(topicId)
    }

    for (const topicId of resolved.unresolved) {
      this.unresolvedTopicIds.add(topicId)
    }

    return grew
  }

  /**
   * One metadata request by topic id, for the ids still worth asking about.
   *
   * Failures are logged and swallowed: an id that cannot be resolved leaves its topic unserved for
   * this membership, which is the stated trade, and must never fail the round. An id the cluster
   * reports unresolved twice stops being asked for, so a topic this client may not see costs a
   * bounded number of requests rather than one per metadata max age forever.
   * @private
   * @param {string[]} topicIds
   * @returns {Promise<void>}
   */
  async refreshMetadataForTopicIds(topicIds) {
    const askable = topicIds.filter(
      (topicId) => (this.unresolvedAttempts.get(topicId) || 0) < MAX_UNRESOLVED_ATTEMPTS
    )

    if (!askable.length) return

    this.lastMetadataRefreshAt = Date.now()

    try {
      const outcome = await this.cluster.refreshMetadataForTopicIds(askable)

      for (const topicId of (outcome && outcome.unresolved) || []) {
        this.unresolvedAttempts.set(topicId, (this.unresolvedAttempts.get(topicId) || 0) + 1)
      }
    } catch (error) {
      this.logger.debug('Could not refresh metadata for the assigned topic ids', {
        groupId: this.groupId,
        topicIds: askable,
        error: error.message,
      })
    }
  }

  /**
   * Widens the consumer's subscription with names the coordinator assigned, once per name. Under a
   * pattern subscription this is what puts the topic into the subscribed set, into the offset
   * manager's per-topic configuration and into the cluster's target topics - without it the apply
   * would strip the whole assignment as unsubscribed.
   * @private
   * @param {((names: string[]) => Promise<void>)|undefined} growSubscription
   * @param {string[]} names
   * @returns {Promise<void>}
   */
  async growSubscriptionWith(growSubscription, names) {
    if (!growSubscription) return

    const fresh = names.filter((name) => !this.grownTopicNames.has(name))
    if (!fresh.length) return

    await growSubscription(fresh)
    for (const name of fresh) {
      this.grownTopicNames.add(name)
    }
  }

  /**
   * Records what was acknowledged, and prunes the id caches to the current target so a churning
   * pattern subscription does not grow them for the life of the consumer.
   * @private
   * @param {number} localEpoch
   * @param {{ byTopicId: Map<string, number[]> }} resolved
   * @returns {void}
   */
  adoptLocalAssignment(localEpoch, resolved) {
    this.localAssignment = { localEpoch, byTopicId: new Map(resolved.byTopicId) }

    const live = this.target ? new Set(this.target.byTopicId.keys()) : new Set()

    for (const topicId of [...this.assignedTopicNames.keys()]) {
      if (!live.has(topicId)) this.assignedTopicNames.delete(topicId)
    }

    for (const topicId of [...this.unresolvedTopicIds]) {
      if (!live.has(topicId)) this.unresolvedTopicIds.delete(topicId)
    }

    for (const topicId of [...this.unresolvedAttempts.keys()]) {
      if (!live.has(topicId)) this.unresolvedAttempts.delete(topicId)
    }
  }

  /**
   * Enters the acknowledging state and makes the next heartbeat go out now, carrying the owned set.
   * @private
   * @returns {void}
   */
  acknowledge() {
    this.transitionTo(ACKNOWLEDGING)
    this.immediatePending = true
    this.requestTick()
  }

  /**
   * Asks for the next heartbeat to go out now. Tolerates an unattached manager: the join arms its
   * first heartbeat before the runner exists, and that window is exactly what the guard covers.
   * @private
   * @returns {void}
   */
  requestTick() {
    if (this.heartbeatManager) {
      this.heartbeatManager.requestTick()
    }
  }

  /**
   * Moves the member state, raising an invariant violation on an edge the table forbids.
   * @private
   * @param {string} state
   * @returns {void}
   */
  transitionTo(state) {
    this.state = transition(this.state, state)
  }

  /**
   * Forgets the outstanding target and everything derived from it, so no later step can reconcile
   * against an assignment this member is no longer entitled to.
   * @private
   * @returns {void}
   */
  clearTarget() {
    this.target = null
    this.pendingResolved = null
    this.unresolvedTopicIds.clear()
    this.unresolvedAttempts.clear()
    this.assignedTopicNames.clear()
    // Cleared here and nowhere else, so it moves in lockstep with the subscription the consumer
    // resets on a fencing, a leave or a generation reset: a name still remembered as "already
    // grown" after that reset would never be handed back, and the apply would strip the whole
    // assignment as unsubscribed.
    this.grownTopicNames.clear()
  }

  /** @private @returns {Object} the owned set in the name-keyed shape the apply expects */
  ownedAsObject() {
    return Object.fromEntries(
      this.ownedPartitions.map(({ topic, partitions }) => [topic, partitions])
    )
  }

  /**
   * @private
   * @param {{ assignment: Object }} resolved
   * @returns {boolean} whether the resolvable subset is exactly what this member owns
   */
  matchesOwned({ assignment }) {
    const owned = indexByTopic(this.ownedPartitions)
    const topics = Object.keys(assignment)

    if (topics.length !== owned.size) {
      return false
    }

    return topics.every((topic) => {
      const partitions = owned.get(topic)
      return (
        partitions != null &&
        partitions.size === assignment[topic].length &&
        assignment[topic].every((partition) => partitions.has(partition))
      )
    })
  }

  /**
   * One heartbeat, gated exactly as the classic protocol's is: single-flight, and interval-gated
   * unless the state owes an immediate one or a backoff has come due. The gate is load-bearing -
   * the runner calls this per message, per batch and on every empty fetch round.
   * @param {{ interval: number }} options
   * @returns {Promise<void>}
   */
  async heartbeat({ interval }) {
    return this.sharedHeartbeat({ interval })
  }

  /**
   * Sends one heartbeat, gate-free. Also the leave path: `leaveEpoch` replaces the member epoch and
   * makes the response irrelevant.
   *
   * The "on generated" transitions run against the state the request was BUILT in, not the state
   * its answer arrives in, which is where `onHeartbeatRequestGenerated` runs them upstream: an
   * acknowledgement that failed has still moved the state, and a heartbeat already on the wire when
   * the acknowledgement was decided must not consume it.
   *
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java
   * @param {{ sendAllFields?: boolean, leaveEpoch?: number|null }} options
   * @returns {Promise<void>}
   * @throws {Error} the classified error, for every path except a leave, whose errors are swallowed
   */
  async sendHeartbeat({ sendAllFields = false, leaveEpoch = null }) {
    if (!this.coordinator) {
      // A leave never LOOKS for a coordinator. `connect()` goes through the cluster's retrier, so
      // waiting for it here would hold `stop()` open for tens of seconds whenever the coordinator
      // has moved - and the member is going away either way, so an unsent leave costs only the
      // session timeout. Catching the rejection is not enough: the wait itself is the problem.
      if (leaveEpoch != null) {
        this.logger.debug('Leaving without a heartbeat: no coordinator is known', {
          groupId: this.groupId,
        })
        this.onRequestGenerated({ leaveEpoch, stateAtRequest: this.state })
        return
      }

      try {
        await this.connect()
      } catch (error) {
        this.backoff.onFailedAttempt(Date.now())
        throw error
      }
    }

    const request = buildRequest(this, { sendAllFields, leaveEpoch })
    const sentUnderEpoch = request.MemberEpoch
    const stateAtRequest = this.state

    this.lastRequestAt = Date.now()
    this.immediatePending = false
    this.backoff.takeDue()

    try {
      const response = await this.coordinator.consumerGroupHeartbeat(toBrokerPayload(request))

      this.sentFields.remember(request)
      if (request.SubscribedTopicRegex !== undefined) {
        this.regexUpdated = false
      }
      this.backoff.reset()

      this.instrumentationEmitter.emit(HEARTBEAT, {
        groupId: this.groupId,
        memberId: this.memberId,
        groupGenerationId: sentUnderEpoch,
        groupInstanceId: this.groupInstanceId,
      })

      this.onSuccess(response)
    } catch (error) {
      this.sentFields.reset()

      if (leaveEpoch != null) {
        // A leave's answer is deliberately NOT classified. The classification exists to produce a
        // verdict and a state move, and a leave raises no verdict: running the table here would
        // move a leaving member to FATAL or FENCED - states the leave cannot complete out of - and
        // could ask the stopped heartbeat manager for a tick on the way.
        this.logger.debug('The leave heartbeat failed; leaving anyway', {
          groupId: this.groupId,
          error: error.message,
        })
        return
      }

      throw this.classifyHeartbeatError(stampGeneration(error, sentUnderEpoch))
    } finally {
      this.onRequestGenerated({ leaveEpoch, stateAtRequest })
    }
  }

  /**
   * The state moves that belong to a request having been GENERATED rather than answered.
   * @private
   * @param {{ leaveEpoch: number|null, stateAtRequest: string }} options
   * @returns {void}
   */
  onRequestGenerated({ leaveEpoch, stateAtRequest }) {
    if (leaveEpoch != null) {
      if (stateAtRequest === LEAVING && this.state === LEAVING) {
        this.transitionTo(this.pollTimerExpired ? STALE : UNSUBSCRIBED)
      }
      return
    }

    // Both halves matter: the request must have been the acknowledgement, and nothing since
    // (a fencing, a fatal answer) may have moved the member out of ACKNOWLEDGING.
    if (stateAtRequest !== ACKNOWLEDGING || this.state !== ACKNOWLEDGING) {
      return
    }

    if (this.needsReconciliation()) {
      this.transitionTo(RECONCILING)
      this.requestRejoinForAssignmentChange()
    } else {
      this.transitionTo(STABLE)
    }
  }

  /**
   * A successful heartbeat response: the interval, the epoch, and possibly a new target.
   * @private
   * @param {{ memberEpoch: number, heartbeatIntervalMs: number, assignment: Object|null }} response
   * @returns {void}
   */
  onSuccess(response) {
    if (response.heartbeatIntervalMs > 0) {
      this.heartbeatIntervalMs = response.heartbeatIntervalMs
    }

    // The response's own MemberId is ignored on every version: this client generates its id and
    // keeps it.
    // Epoch 0 is what this member sends to JOIN, never what a coordinator that accepted it answers.
    // Taken as a membership it would install a target and move the machine to RECONCILING, from
    // which the next round's step-0 gate (`memberEpoch <= 0`) would attempt an illegal rejoin.
    if (isNotInGroup(this.state) || isLeavingGroup(this.state) || response.memberEpoch <= 0) {
      this.logger.debug('Ignoring a heartbeat response this member has no use for', {
        groupId: this.groupId,
        state: this.state,
        memberEpoch: response.memberEpoch,
      })
      return
    }

    this.memberEpoch = response.memberEpoch

    if (this.memberEpoch > 0) {
      this.resolveFirstMembership()
    }

    const targetChanged =
      response.assignment != null && canHandleNewAssignment(this.state)
        ? this.replaceTarget(response.assignment)
        : false

    if (this.target != null) {
      this.evaluateTarget({ targetChanged })
    }
  }

  /**
   * Replaces the target with what the coordinator sent, bumping its local epoch only when the
   * partitions actually changed. The epoch is what makes a re-acknowledgement of an unchanged
   * partition set visible to the coordinator.
   * @private
   * @param {{ topicPartitions: Array<{ topicId: string, partitions: number[] }> }} assignment
   * @returns {boolean} whether the partitions changed
   */
  replaceTarget(assignment) {
    const byTopicId = new Map(
      assignment.topicPartitions.map(({ topicId, partitions }) => [
        topicId,
        [...partitions].sort((a, b) => a - b),
      ])
    )

    const previous = this.target
    const changed =
      previous == null ||
      previous.byTopicId.size !== byTopicId.size ||
      [...byTopicId].some(([topicId, partitions]) => {
        const before = previous.byTopicId.get(topicId)
        return (
          before == null ||
          before.length !== partitions.length ||
          before.some((partition, index) => partition !== partitions[index])
        )
      })

    this.target = {
      localEpoch: changed ? (previous ? previous.localEpoch : 0) + 1 : previous.localEpoch,
      byTopicId,
    }

    if (changed) {
      // A new target is the coordinator asserting these ids exist, so the attempt budget starts
      // over: an id given up on under an older target is worth asking about again.
      for (const topicId of byTopicId.keys()) {
        this.unresolvedAttempts.delete(topicId)
      }
    }

    return changed
  }

  /**
   * Decides what the current target means for the state machine, from live state: whether the
   * member is where the coordinator wants it, and whether a round has to be asked for.
   * @private
   * @param {{ targetChanged: boolean }} options
   * @returns {void}
   */
  evaluateTarget({ targetChanged }) {
    const wasJoining = this.state === JOINING
    const wasReconciling = this.state === RECONCILING
    const resolved = this.resolveFromCache(this.target)
    const grew = this.recordResolution(resolved)
    const converged = this.matchesOwned(resolved)

    if (converged) {
      if (wasReconciling || wasJoining) {
        this.transitionTo(STABLE)
      }
    } else {
      if (!wasReconciling && canHandleNewAssignment(this.state)) {
        this.transitionTo(RECONCILING)
      }

      if (targetChanged || grew || !wasReconciling) {
        this.requestRejoinForAssignmentChange()
      }
    }

    // A reconciliation step resolves unknown ids by id itself, and one is coming for every target
    // this member has not converged on yet - so the converged case is the only one that can strand
    // an id with nothing to resolve it.
    const stepWillResolve = wasJoining || wasReconciling || !converged

    this.refreshUnresolvedTopicIds({ immediate: (targetChanged || grew) && !stepWillResolve })
  }

  /**
   * Asks the cluster to name the target's unknown topic ids, and re-reads the target when a name
   * arrives.
   *
   * Immediately when the coordinator has just handed over ids this member cannot name and no
   * reconciliation step is coming to resolve them - the case nothing else covers, and without it a
   * topic assigned after the member reached its steady state (a regex match, or a subscribed name
   * whose topic was created later) is picked up only after a whole metadata max age. Otherwise on
   * the metadata max age, as the slow retry for ids that stayed unresolved, bounded per id by the
   * attempt cap in `refreshMetadataForTopicIds`.
   *
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractMembershipManager.java
   * @private
   * @param {{ immediate: boolean }} options
   * @returns {void}
   */
  refreshUnresolvedTopicIds({ immediate }) {
    if (!this.unresolvedTopicIds.size) return
    if (!immediate && Date.now() - this.lastMetadataRefreshAt < this.metadataMaxAge) return

    const topicIds = [...this.unresolvedTopicIds]
    const { localEpoch } = this.target

    // Deliberately not awaited - a heartbeat must not wait on metadata - so nothing raised here
    // has a caller: the refresh swallows its own failures and the re-read is logged rather than
    // left to the process as an unhandled rejection.
    this.refreshMetadataForTopicIds(topicIds)
      .then(() => this.onTopicIdsRefreshed(topicIds, localEpoch))
      .catch((error) => {
        this.logger.debug('Could not re-read the target after refreshing its topic ids', {
          groupId: this.groupId,
          error: error.message,
        })
      })
  }

  /**
   * Re-evaluates the target once a by-id refresh has run, so a name that has just arrived starts
   * its round now rather than on the next heartbeat.
   *
   * Does nothing unless the refresh actually named one of the ids it asked about and the target it
   * was resolving is still this member's: a refresh landing after a fencing, a leave or a newer
   * target must not move the machine. The local epoch is the identity that matters - the target
   * object is replaced by every heartbeat carrying an assignment, changed or not.
   * @private
   * @param {string[]} topicIds the ids the refresh asked about
   * @param {number} localEpoch the local epoch of the target it was resolving
   * @returns {void}
   */
  onTopicIdsRefreshed(topicIds, localEpoch) {
    if (this.target == null || this.target.localEpoch !== localEpoch) return
    if (isNotInGroup(this.state) || isLeavingGroup(this.state)) return
    if (!topicIds.some((topicId) => this.cluster.topicName(topicId) != null)) return

    this.evaluateTarget({ targetChanged: false })
  }

  /**
   * Tells the round loop that the coordinator wants a different assignment, so a reconciliation
   * step runs. A no-op before the coordinator is attached: the join itself will reconcile.
   * @private
   * @returns {void}
   */
  requestRejoinForAssignmentChange() {
    if (!this.rebalanceCoordinator) return

    this.rebalanceCoordinator.requestRejoin({
      kind: 'revoked',
      reason: 'assignment-changed',
      error: assignmentChangedError(),
    })
  }

  /**
   * Whether this failure is "the cluster cannot serve ConsumerGroupHeartbeat at all".
   *
   * Version negotiation raises `KafkaJSServerDoesNotSupportApiKey` before a request is ever framed,
   * so the failure carries no protocol error code: without this test it reads as a transport
   * failure and the member retries a request the cluster can never answer, for as long as the
   * process runs.
   * @private
   * @param {Error} error
   * @returns {boolean}
   */
  isUnsupportedApi(error) {
    return (
      error.name === 'KafkaJSServerDoesNotSupportApiKey' &&
      error.apiKey === apiKeys.ConsumerGroupHeartbeat
    )
  }

  /**
   * Turns a failed heartbeat into a state move plus the error the verdict path will read.
   *
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java
   * @private
   * @param {Error} error
   * @returns {Error} the error to raise
   */
  classifyHeartbeatError(error) {
    const { code } = error

    if (code === NOT_COORDINATOR || code === COORDINATOR_NOT_AVAILABLE) {
      // No backoff, and the request is marked due rather than merely scheduled: `immediatePending`
      // is honoured only in the three states that owe a heartbeat now, and a coordinator move is
      // likeliest to land in STABLE or RECONCILING - where the tick armed below would send nothing
      // and the re-find would wait out a whole interval.
      this.coordinator = null
      this.backoff.retryNow()
      this.requestTick()
      return error
    }

    if (code === COORDINATOR_LOAD_IN_PROGRESS) {
      this.logger.info('The group coordinator is still loading; retrying the heartbeat', {
        groupId: this.groupId,
      })
      this.backoff.onFailedAttempt(Date.now())
      return error
    }

    if (code === UNKNOWN_MEMBER_ID || code === FENCED_MEMBER_EPOCH) {
      this.transitionToFenced(error)
      return error
    }

    if (code === UNSUPPORTED_VERSION || this.isUnsupportedApi(error)) {
      return this.transitionToFatal(unsupportedConsumerProtocolError(error))
    }

    if (code === INVALID_REGULAR_EXPRESSION) {
      if (!error.message.startsWith(INVALID_REGEX_PREFIX)) {
        error.message = `${INVALID_REGEX_PREFIX}${error.message}`
      }
      return this.transitionToFatal(error)
    }

    if (code === TOPIC_AUTHORIZATION_FAILED) {
      // Deliberately not fatal and not a verdict: the member stays where it is so it recovers on
      // its own if the missing ACLs are added.
      if (this.unauthorizedMessage !== error.message) {
        this.unauthorizedMessage = error.message
        this.logger.error(
          'Not authorized to read an assigned topic; the member stays in the group',
          {
            groupId: this.groupId,
            error: error.message,
          }
        )
      }
      return error
    }

    if (code === GROUP_ID_NOT_FOUND) {
      return this.state === UNSUBSCRIBED ? error : this.transitionToFatal(error)
    }

    if (isConsumerProtocolFatal(error)) {
      return this.transitionToFatal(error)
    }

    if (error.name === 'KafkaJSInvariantViolation') {
      // This client's own bug, raised from inside the response handling. Backing off and dropping
      // the coordinator would treat a defect that recurs on every heartbeat as a network problem.
      return error
    }

    if (typeof code !== 'number') {
      // A transport failure, not an answer: drop the cached coordinator so the next attempt
      // rediscovers it, and back off.
      this.coordinator = null
      this.backoff.onFailedAttempt(Date.now())
      return error
    }

    return this.transitionToFatal(error)
  }

  /**
   * Performs the state move a fencing implies, wherever it was learned - the heartbeat, an offset
   * commit or a fetch. Idempotent, because the same fencing can arrive twice.
   *
   * The epoch is deliberately NOT zeroed here: the stale-answer guard compares the epoch an error
   * was sent under against the one the member holds, so zeroing it now would make every fencing
   * verdict look stale and be dropped, leaving a member that sends nothing and never rejoins. The
   * rejoin zeroes it when it consumes the verdict.
   * @param {Error} error
   * @returns {void}
   */
  onMembershipError(error) {
    const { code } = error

    if (code === UNKNOWN_MEMBER_ID || code === FENCED_MEMBER_EPOCH) {
      this.transitionToFenced(error)
      return
    }

    if (isConsumerProtocolFatal(error)) {
      this.transitionToFatal(error)
    }
  }

  /**
   * @private
   * @param {Error} error the fencing that caused it, which also fails an outstanding join
   * @returns {void}
   */
  transitionToFenced(error) {
    if (isNotInGroup(this.state)) return

    this.transitionTo(FENCED)
    this.sentFields.reset()
    this.clearTarget()
    // A fencing that lands while the join is awaited has to reach the round, or the step would sit
    // there until its deadline for an error the round knows how to recover from.
    this.rejectFirstMembership(error)
  }

  /**
   * @private
   * @param {Error} error
   * @returns {Error} the same error, so callers can `return this.transitionToFatal(e)`
   */
  transitionToFatal(error) {
    this.fatalError = error

    // FENCED and STALE included: a fatal error learned on the commit or fetch path while the
    // member is out of the group must still end the membership, or `rebalance()` - which only
    // throws `fatalError` from FATAL - would rejoin straight back into it.
    if (this.state !== FATAL) {
      this.transitionTo(FATAL)
      this.sentFields.reset()
      this.clearTarget()
    }

    this.logger.error('The consumer group membership failed fatally', {
      groupId: this.groupId,
      memberId: this.memberId,
      error: error.message,
    })

    this.rejectFirstMembership(error)

    return error
  }

  /**
   * Leaves the group: commit what is owned, report it, and send a heartbeat carrying the leave
   * epoch. Legal from every state - a member that is already out sends nothing rather than
   * throwing, which matters because a poll-interval overrun is the commonest way a consumer ends
   * up being stopped.
   *
   * `leaveGroup` picks the leave heartbeat's epoch and whether it is sent at all: `true` is -1 for
   * every member, `undefined` is -1 for a dynamic member and -2 for a static one, and `false` is -2
   * for a static member and nothing for a dynamic one, which is then left to expire. Everything
   * else a leave does (the commit, the revocation, the clearing) happens whatever its value. A call
   * that joins a leave already under way keeps that leave's choice.
   * @param {{ leaveGroup?: boolean }} [options]
   * @returns {Promise<void>}
   */
  async leave(options) {
    if (isLeavingGroup(this.state)) {
      return this.joinInFlightLeave()
    }

    return this.trackLeave(this.sharedLeave(options))
  }

  /**
   * Waits for the leave that is already on the wire instead of starting a second one.
   *
   * The two leave paths are different entry points, so the single flight around `leave()` does not
   * cover a stop that lands while a poll-expiry leave is in flight - and a second leave would run
   * `PREPARE_LEAVING` from `LEAVING`, an edge the machine forbids. The invariant violation it
   * raised was swallowed by the runner's shutdown catch, so the stop looked clean.
   * @private
   * @returns {Promise<void>}
   */
  async joinInFlightLeave() {
    await this.inFlightLeave
  }

  /**
   * Records the leave that is on the wire, so the other entry point can join it. Both paths go
   * through this, and the field is cleared however the leave settles.
   * @private
   * @param {Promise<void>} leaving
   * @returns {Promise<void>}
   */
  trackLeave(leaving) {
    this.inFlightLeave = leaving.finally(() => {
      this.inFlightLeave = null
    })

    return this.inFlightLeave
  }

  /**
   * The leave itself, run once however many callers asked for it: commit, report the partitions,
   * send the leave heartbeat, park the member out of the group.
   * @private
   * @param {{ leaveGroup?: boolean }} [options] see `leave`
   * @returns {Promise<void>}
   */
  async performLeave(options) {
    // Whatever join was in flight is abandoned by the leave, from every state, and it is FAILED
    // rather than quietly resolved: a leave runs only from a stop, and a round told the join
    // succeeded would go on to apply an assignment for a member that is on its way out. The
    // deferred carries its own `catch`, so a rejection nobody awaits stays observed.
    this.rejectFirstMembership(consumerStoppingError())

    if (this.state === FENCED) {
      this.clearTarget()
      this.ownedPartitions = []
      this.transitionTo(UNSUBSCRIBED)
      return
    }

    if (isNotInGroup(this.state)) {
      return
    }

    this.transitionTo(PREPARE_LEAVING)

    const releasing = this.ownedPartitions

    if (this.memberEpoch > 0) {
      await this.releaseOwnedPartitions(releasing)
      this.emitPartitions(PARTITIONS_REVOKED, releasing)
    } else if (releasing.length) {
      // Already out of the group: nothing may be committed under an epoch the coordinator dropped.
      this.emitPartitions(PARTITIONS_LOST, releasing)
    }

    await this.sendLeaveHeartbeat(options?.leaveGroup)
  }

  /**
   * The poll-expiry variant: no commit, no partition events, the response ignored, and the member
   * parks in a state that sends nothing until the recovering round rejoins it.
   * @returns {Promise<void>}
   */
  async leaveForPollExpiry() {
    if (isNotInGroup(this.state)) return

    if (isLeavingGroup(this.state)) {
      return this.joinInFlightLeave()
    }

    return this.trackLeave(this.performPollExpiryLeave())
  }

  /**
   * The poll-expiry leave itself, once the guards have let it start.
   * @private
   * @returns {Promise<void>}
   */
  async performPollExpiryLeave() {
    this.clearFirstMembership()
    this.pollTimerExpired = true
    this.transitionTo(PREPARE_LEAVING)
    this.ownedPartitions = []

    await this.sendLeaveHeartbeat()
  }

  /**
   * Sends the one heartbeat that announces the leave, at the negative epoch the member's kind of
   * membership calls for, and clears everything the membership held. A dynamic member that was told
   * to remain in the group (`leaveGroup: false`) clears the same things and sends nothing, and the
   * leave ends the way one does when no coordinator is known.
   * @private
   * @param {boolean} [leaveGroup] the caller's choice, `undefined` for the default
   * @returns {Promise<void>}
   */
  async sendLeaveHeartbeat(leaveGroup) {
    this.ownedPartitions = []
    this.clearTarget()
    this.sentFields.reset()
    this.localAssignment = { localEpoch: 0, byTopicId: new Map() }
    const isStatic = Boolean(this.groupInstanceId)
    this.memberEpoch = leaveEpochFor(isStatic, leaveGroup)
    this.transitionTo(LEAVING)

    if (leaveGroup === false && !isStatic) {
      this.logger.debug('Leaving without a heartbeat: the member was told to remain in the group', {
        groupId: this.groupId,
      })
      this.onRequestGenerated({ leaveEpoch: this.memberEpoch, stateAtRequest: LEAVING })
      return
    }

    this.immediatePending = true

    // Sent directly, never through the heartbeat manager: by the time a leave runs the chain has
    // already been cancelled, and re-arming it would leave heartbeats running after the member has
    // gone and a timer holding the process open.
    await this.sendHeartbeat({ sendAllFields: false, leaveEpoch: this.memberEpoch })
  }

  /**
   * @private
   * @param {import('./interface').TopicPartitions[]} partitions
   * @returns {Promise<void>}
   */
  async releaseOwnedPartitions(partitions) {
    if (!this.commitBeforeRelease || !partitions.length) return

    try {
      await this.commitBeforeRelease(partitions)
    } catch (error) {
      this.logger.warn('Commit before leaving the group failed; leaving anyway', {
        groupId: this.groupId,
        error: error.message,
      })
    }
  }

  /**
   * @private
   * @param {string} event
   * @param {import('./interface').TopicPartitions[]} partitions
   * @returns {void}
   */
  emitPartitions(event, partitions) {
    if (!this.rebalanceCoordinator || !partitions.length) return

    this.rebalanceCoordinator.emitPartitions(event, partitions)
  }

  /**
   * Drops this member's epoch and everything held under it. The member id is kept whatever the
   * caller asks: the coordinator knows this member by an id the client generated, and a new one
   * would be a new member.
   * @param {{ clearMemberId?: boolean }} [options]
   * @returns {void}
   */
  resetGeneration({ clearMemberId = false } = {}) {
    if (clearMemberId) {
      this.logger.debug('Keeping the member id: this protocol generates it once per membership', {
        groupId: this.groupId,
        memberId: this.memberId,
      })
    }

    this.memberEpoch = 0
    this.ownedPartitions = []
    this.localAssignment = { localEpoch: 0, byTopicId: new Map() }
    this.clearTarget()
    this.sentFields.reset()
  }

  /** @returns {import('./interface').TopicPartitions[]} */
  owned() {
    return this.ownedPartitions
  }

  /**
   * Shrinks `owned` to its intersection with the given set - it never widens. Called before the
   * apply touches the offset manager, so a revoked partition stops being advertised before an
   * offset can be resolved for it.
   * @param {import('./interface').TopicPartitions[]} topicPartitions
   * @returns {void}
   */
  narrowOwned(topicPartitions) {
    const keep = indexByTopic(topicPartitions)

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
   * Widens `owned` to what was applied AND, when a reconciliation was under way, acknowledges it.
   *
   * What is acknowledged is what was **applied**, which is not always what the step returned: the
   * apply legitimately drops topics the consumer is no longer subscribed to before it calls back
   * here. Acknowledging the returned set instead would tell the coordinator this member holds
   * partitions it does not, and the coordinator would never reassign them.
   *
   * The state guard is required: the round also calls this with an empty set before the join of
   * every lost round, when the member is fenced or stale and the acknowledging transition would be
   * an illegal edge.
   * @param {import('./interface').TopicPartitions[]} topicPartitions the set actually applied
   * @returns {void}
   */
  setOwned(topicPartitions) {
    this.ownedPartitions = topicPartitions

    const resolved = this.pendingResolved
    this.pendingResolved = null

    if (this.state !== RECONCILING) {
      return
    }

    if (resolved) {
      this.adoptLocalAssignment(
        this.target ? this.target.localEpoch : this.localAssignment.localEpoch,
        this.appliedSubsetOf(resolved, topicPartitions)
      )
    }

    this.acknowledge()
  }

  /**
   * Narrows a resolved subset to the partitions that were actually applied, keeping the topic-id
   * keys the wire form needs. A topic left with nothing applied drops out entirely.
   * @private
   * @param {{ byTopicId: Map<string, number[]> }} resolved what the step returned
   * @param {import('./interface').TopicPartitions[]} topicPartitions what the apply kept
   * @returns {{ byTopicId: Map<string, number[]> }}
   */
  appliedSubsetOf(resolved, topicPartitions) {
    const applied = indexByTopic(topicPartitions)
    const byTopicId = new Map()

    for (const [topicId, partitions] of resolved.byTopicId) {
      const name = this.cluster.topicName(topicId) || this.assignedTopicNames.get(topicId)
      const kept = name ? applied.get(name) : undefined
      const keptPartitions = kept ? partitions.filter((partition) => kept.has(partition)) : []

      if (keptPartitions.length) byTopicId.set(topicId, keptPartitions)
    }

    return { byTopicId }
  }

  /**
   * The difference between what this member owns and what it was just assigned.
   * @param {import('./interface').TopicPartitions[]} assignment
   * @returns {{ added: import('./interface').TopicPartitions[],
   *   revoked: import('./interface').TopicPartitions[] }}
   */
  diffAssignment(assignment) {
    const assigned = indexByTopic(assignment)
    const owned = indexByTopic(this.ownedPartitions)

    return { added: subtract(assigned, owned), revoked: subtract(owned, assigned) }
  }

  /** Static membership is available from the first version of this protocol. @returns {void} */
  warnIfStaticMembershipInactive() {}

  /** There is no client-side assignor to notify. @returns {void} */
  notifyAssignment() {}

  /** There is no client-side assignor state to clear. @returns {void} */
  clearAssignerState() {}
}
