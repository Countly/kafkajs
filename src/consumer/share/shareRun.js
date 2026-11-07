const createRetry = require('../../retry')
const apiKeys = require('../../protocol/requests/apiKeys')
const { ConsumerOperation } = require('../groupProtocol/interface')
const { MEMBER_STATES } = require('../groupProtocol/memberState')
const HeartbeatManager = require('../heartbeatManager')
const { ShareMembership, SharePollView } = require('./shareMembership')
const { ShareSession } = require('./shareSession')
const { ShareFetchLoop } = require('./shareFetchLoop')
const { ShareHandlerCall, countWithin } = require('./shareHandlerCall')
const { ShareCloseEpisode } = require('./shareCloseEpisode')
const { events } = require('./instrumentationEvents')
const { KafkaJSNonRetriableError, KafkaJSShareGroupsNotSupportedError } = require('../../errors')

const INITIAL_HEARTBEAT_MS = 1000

/**
 * @returns {{promise: Promise<void>, resolve: () => void, reject: (error: Error) => void, settled: boolean}}
 *   A promise settled by hand; its rejection is observed here, so an unawaited one never escapes.
 */
const deferred = () => {
  const gate = { settled: false }
  gate.promise = new Promise((resolve, reject) => {
    gate.resolve = () => {
      gate.settled = true
      resolve()
    }
    gate.reject = (error) => {
      gate.settled = true
      reject(error)
    }
  })
  gate.promise.catch(() => {})
  return gate
}

/**
 * @param {Error} error A membership failure.
 * @returns {Error} The capability error when the membership wrapped one, else the error itself.
 */
const unwrapCapability = (error) =>
  error.cause instanceof KafkaJSShareGroupsNotSupportedError ? error.cause : error

/**
 * One run's authority and effects: its membership and heartbeat, one fetch loop per leader node,
 * handler invocation and the captured close. Every effect checks the run's own `authorityToken`,
 * the membership identity that was current when the work was captured, so a late answer, callback
 * or continuation of an old membership or run can never act on a replacement.
 *
 * A close freezes eligible terminal work synchronously, before ordinary invalidation, and hands
 * it to one {@link ShareCloseEpisode}; ordinary and closing authority never substitute for each
 * other. The class owns no timer except the ones its operations and the heartbeat manager own.
 */
class ShareRun {
  /**
   * @param {object} options Run dependencies.
   * @param {import('./shareRunner').ShareRunner} options.runner Owning client.
   * @param {Function} options.handler The application's `eachMessage` or `eachBatch`.
   * @param {boolean} options.batch Whether the handler is `eachBatch`.
   * @returns {ShareRun} An unstarted run with a fresh member ID, poll view and heartbeat manager.
   */
  constructor({ runner, handler, batch }) {
    const { options } = runner
    this.runner = runner
    this.options = options
    this.handler = handler
    this.batch = batch
    this.stopGeneration = runner.stopGeneration
    this.retired = false
    this.admitting = true
    this.stopRequested = false
    this.crashed = false
    this.finished = false
    this.closed = false
    this.closing = null
    this.refusalError = null
    this.nodes = new Map()
    this.assignment = new Map()
    this.applying = 0
    this.committed = []
    this.tap = null
    this.heartbeatRevision = Symbol('heartbeat')
    this.coordinator = null
    this.facade = null
    this.waiters = new Set()
    this.lookups = new Set()
    this.firstJoin = deferred()
    this.membership = new ShareMembership({
      cluster: this.membershipCluster(),
      groupId: options.groupId,
      topics: runner.subscription,
      rackId: options.rackId,
      retry: options.retry,
      logger: options.logger,
      onAssignment: (snapshot) => this.assignmentChanged(snapshot),
      onInvalidate: (oldToken, reason) => this.onInvalidate(oldToken, reason),
      onFatal: (error) => this.crash(unwrapCapability(error)),
    })
    this.authorityToken = this.membership.token
    this.poll = new SharePollView({ maxPollInterval: options.maxPollInterval })
    this.manager = new HeartbeatManager({
      groupMembership: {
        heartbeat: () => this.heartbeat(),
        heartbeatInterval: () => this.membership.heartbeatInterval(),
      },
      pollTimer: this.poll,
      heartbeatInterval: INITIAL_HEARTBEAT_MS,
      logger: options.logger,
      onError: (error) => this.onHeartbeatError(error),
      onPollExpired: () => this.onPollExpired(),
    })
    this.unrouted = options.cluster.onRoutingChange(() => this.syncNodes())
    this.operation = new ConsumerOperation({
      guard: () => this.ordinaryRefusal(this.authorityToken),
    })
  }

  /**
   * @returns {Array<{topic: string, partitions: number[]}>} A fresh snapshot of the broker's
   *   current assignment; empty before the first join and after the run ends.
   */
  assignmentSnapshot() {
    return [...this.assignment.values()].map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.slice(),
    }))
  }

  /**
   * @param {symbol|null} issued The membership identity the caller captured before its first await.
   * @returns {Error|null} A refusal once the run retired or `issued` is no longer the current
   *   membership. One cached error per run, so a hot-path check allocates nothing.
   */
  ordinaryRefusal(issued) {
    if (this.retired || issued === null || issued !== this.authorityToken) {
      this.refusalError ??= new KafkaJSNonRetriableError('Share run is no longer current')
      return this.refusalError
    }
    return null
  }

  /**
   * @private
   * @returns {boolean} Whether the run still owns the client and was not asked to stop.
   */
  isActive() {
    return !this.retired && !this.stopRequested && this.runner.current === this
  }

  /**
   * The narrow Cluster facade the membership runs on. Coordinator lookup is delegated to the
   * original Cluster under an operation bound to the asking membership, gated by the share
   * capability, and returns a cached per-run object whose only transport method forwards to the
   * original Broker and observes the answer.
   * @private
   * @returns {object} `{ findGroupCoordinator, topicName, refreshMetadataForTopicIds }`.
   */
  membershipCluster() {
    const { cluster } = this.options
    return {
      topicName: (topicId) => cluster.topicName(topicId),
      refreshMetadataForTopicIds: (ids, scope) => cluster.refreshMetadataForTopicIds(ids, scope),
      findGroupCoordinator: ({ groupId }) => this.lookupCoordinator(groupId),
    }
  }

  /**
   * Finds the group coordinator for the membership that asks now. The lookup is fenced by an
   * operation that refuses once the run retired or that membership closed or was replaced, so a
   * late answer installs nothing, opens no connection and sends no frame, and the retries stop.
   * Every refusal coincides with the membership no longer being current, so the membership
   * discards the failure instead of classifying it.
   * @private
   * @param {string} groupId Share group id.
   * @returns {Promise<{broker: object, shareGroupHeartbeat: Function}>} The cached coordinator facade.
   * @throws {Error} The refusal of the retired lookup, or the capability error of the coordinator.
   */
  async lookupCoordinator(groupId) {
    const { membership } = this
    const issued = membership.token
    const lookup = new ConsumerOperation({
      guard: () =>
        this.retired || membership.closed || membership.token !== issued
          ? new KafkaJSNonRetriableError('Share membership is no longer current')
          : null,
    })
    this.lookups.add(lookup)
    try {
      const broker = await this.options.cluster.findGroupCoordinator({ groupId, fence: lookup })
      lookup.check()
      this.runner.checkCapability(broker, [apiKeys.ShareGroupHeartbeat])
      if (this.facade?.broker !== broker) {
        this.facade = {
          broker,
          shareGroupHeartbeat: (request, options) => this.transmit(broker, request, options),
        }
      }
      return this.facade
    } finally {
      this.lookups.delete(lookup)
      lookup.finish()
    }
  }

  /**
   * Retires every coordinator lookup in flight, so none can outlive the membership it served.
   * Callers run it once that membership is closed, replaced or frozen for captured traffic.
   * @private
   * @returns {void}
   */
  retireLookups() {
    if (this.lookups.size === 0) return
    const refusal = new KafkaJSNonRetriableError('Share membership is no longer current')
    for (const lookup of this.lookups) lookup.retire(refusal)
  }

  /**
   * Sends a membership frame through the original Broker and, for a heartbeat (not a leave),
   * records what was answered under which guard result without publishing anything.
   * @private
   * @param {object} broker Original coordinator Broker.
   * @param {object} request The exact ShareGroupHeartbeat fields.
   * @param {{guard?: () => Error|null}} [options] The exact guard.
   * @returns {Promise<object>} The decoded answer, unchanged.
   */
  async transmit(broker, request, options) {
    const response = await broker.shareGroupHeartbeat(request, options)
    if (request.memberEpoch >= 0) {
      this.tap = {
        epoch: request.memberEpoch,
        response,
        current: !options?.guard || options.guard() === null,
      }
      if (this.tap.current && response.errorCode === 0)
        this.coordinator = this.bindCoordinator(broker)
    }
    return response
  }

  /**
   * @private
   * @param {object} broker Original coordinator Broker after a successful answer.
   * @returns {{broker: object, pool: object, connection: object, generation: number, current: () => boolean}}
   *   The captured coordinator socket (slot 0) and its generation.
   */
  bindCoordinator(broker) {
    const pool = broker.connectionPool
    const binding = {
      broker,
      pool,
      connection: pool.pool[0],
      generation: pool.generationFor(apiKeys.ShareGroupHeartbeat),
    }
    binding.current = () =>
      broker.connectionPool === pool &&
      binding.connection.isConnected() &&
      pool.generationFor(apiKeys.ShareGroupHeartbeat) === binding.generation
    return binding
  }

  /**
   * Begins the run: one immediate join attempt through the same coalesced adapter, then the
   * heartbeat manager's chain for retries and steady beats.
   * @returns {Promise<void>} Settles after the first attempt; failures are classified, not thrown.
   */
  async start() {
    try {
      await this.heartbeat()
    } catch (error) {
      this.onHeartbeatError(error)
    }
    if (this.isActive() && !this.closing) this.manager.ensureStarted()
  }

  /**
   * Sends or shares one membership heartbeat, then publishes what that answer committed, once.
   * @returns {Promise<void>}
   * @throws {Error} The classified failure of a current heartbeat, as `ShareMembership#heartbeat`.
   */
  async heartbeat() {
    const revision = this.heartbeatRevision
    const guard = () =>
      this.retired || revision === null || revision !== this.heartbeatRevision
        ? new KafkaJSNonRetriableError('Share heartbeat is no longer current')
        : null
    this.applying++
    try {
      await this.membership.heartbeat({ guard })
    } finally {
      this.applying--
      if (this.applying === 0) this.publishCommitted()
    }
  }

  /**
   * Turns the tapped answer into committed events, only if the membership accepted it, then
   * publishes the batch in order: heartbeat, group_join, assignment_changed. State changes and
   * the first-join settlement happen before any listener runs; handlers start afterwards.
   * @private
   * @returns {void}
   */
  publishCommitted() {
    const tap = this.tap
    this.tap = null
    const { membership, options } = this
    const batch = []
    if (
      tap &&
      tap.current &&
      tap.response.errorCode === 0 &&
      membership.memberEpoch === tap.response.memberEpoch &&
      !membership.closed
    ) {
      const { memberEpoch, heartbeatIntervalMs } = tap.response
      batch.push([
        events.HEARTBEAT,
        {
          groupId: options.groupId,
          memberId: membership.memberId,
          memberEpoch,
          heartbeatIntervalMs,
        },
      ])
      if (tap.epoch === 0 && memberEpoch > 0) {
        batch.push([
          events.GROUP_JOIN,
          { groupId: options.groupId, memberId: membership.memberId, memberEpoch },
        ])
        this.firstJoin.settled || this.firstJoin.resolve()
      }
    }
    batch.push(...this.committed.splice(0))
    for (const [name, payload] of batch) this.runner.emit(name, payload)
    this.syncNodes()
  }

  /**
   * @private
   * @param {Error} error A heartbeat failure whose policy the membership already applied.
   * @returns {void} Terminal membership errors reach `crash` through the membership itself; the
   *   rest is retried by the manager's chain under the membership's backoff.
   */
  onHeartbeatError(error) {
    this.options.logger.warn('Share heartbeat failed', { error: error.message })
  }

  /**
   * Applies a new assignment synchronously: the wanted sets change at once, while the public
   * event joins the response batch being committed, or is published directly when the change
   * came from asynchronous metadata resolution.
   * @private
   * @param {{assignment: Array<{topicId: string, topic: string, partitions: number[]}>}} snapshot
   *   The membership's resolved assignment.
   * @returns {void}
   */
  assignmentChanged(snapshot) {
    const next = new Map(
      snapshot.assignment.map(({ topicId, topic, partitions }) => [topicId, { topic, partitions }])
    )
    const keyed = (assignment) => {
      const keys = new Map()
      for (const [topicId, { topic, partitions }] of assignment)
        for (const partition of partitions)
          keys.set(`${topicId}:${partition}`, { topic, partition })
      return keys
    }
    const before = keyed(this.assignment)
    const after = keyed(next)
    this.assignment = next
    const group = (keys, other) => {
      const byTopic = new Map()
      for (const [key, { topic, partition }] of keys) {
        if (other.has(key)) continue
        if (!byTopic.has(topic)) byTopic.set(topic, { topic, partitions: [] })
        byTopic.get(topic).partitions.push(partition)
      }
      return [...byTopic.values()]
    }
    const added = group(after, before)
    const removed = group(before, after)
    if (added.length || removed.length) {
      this.committed.push([
        events.ASSIGNMENT_CHANGED,
        {
          groupId: this.options.groupId,
          memberId: this.membership.memberId,
          memberEpoch: this.membership.memberEpoch,
          assignment: this.assignmentSnapshot(),
          added,
          removed,
        },
      ])
    }
    if (this.applying === 0) this.publishCommitted()
  }

  /**
   * Routes the assignment to leader nodes: new nodes start their loop, existing nodes get their
   * current wanted sets, and a node with nothing left ends. Moved partitions lose their old-node
   * acquisitions synchronously; revoked partitions keep their handler completion/release policy.
   * Never runs while a close owns the run.
   * @private
   * @returns {void}
   */
  syncNodes() {
    if (!this.admitting || this.retired || this.closing || this.stopRequested) return
    const { cluster } = this.options
    const wanted = new Map()
    const routed = new Set()
    for (const [topicId, { topic, partitions }] of this.assignment) {
      for (const partition of partitions) {
        const leader = cluster.cachedLeader(topic, partition)
        if (leader < 0) continue
        if (!wanted.has(leader)) wanted.set(leader, [])
        wanted.get(leader).push({ topicId, topic, partition })
        routed.add(`${topicId}:${partition}`)
      }
    }
    const moved = []
    for (const [nodeId, node] of this.nodes) {
      if (node.ended) continue
      const rows = wanted.get(nodeId) || []
      const keys = new Set(rows.map((row) => `${row.topicId}:${row.partition}`))
      for (const key of node.keys) if (!keys.has(key) && routed.has(key)) moved.push({ node, key })
      node.wanted = rows
      node.keys = keys
      node.ended = rows.length === 0
    }
    for (const { node, key } of moved) node.loop?.retireKey(key, 'session-lost')
    if (!this.admitting || this.retired || this.closing || this.stopRequested) return
    for (const [nodeId, rows] of wanted) {
      const node = this.nodes.get(nodeId)
      if (node?.ended) this.nodes.delete(nodeId)
      if (!node || node.ended) this.startNode(nodeId, rows)
    }
  }

  /**
   * @private
   * @param {number} nodeId Leader node id.
   * @param {Array<{topicId: string, topic: string, partition: number}>} rows Its wanted partitions.
   * @returns {void}
   */
  startNode(nodeId, rows) {
    const issued = this.authorityToken
    const node = {
      id: nodeId,
      issued,
      wanted: rows,
      keys: new Set(rows.map((row) => `${row.topicId}:${row.partition}`)),
      ended: false,
      loop: null,
      session: null,
      captured: null,
      queue: null,
      active: null,
      dropped: 0,
      lockDuration: null,
      retryTime: null,
    }
    node.operation = new ConsumerOperation({ guard: () => this.ordinaryRefusal(issued) })
    node.pacing = new ConsumerOperation({ guard: () => this.ordinaryRefusal(issued) })
    node.drive = this.driveNode(node)
    this.nodes.set(nodeId, node)
  }

  /**
   * Finds the node's Broker under its own operation, builds its session and loop, and polls
   * until the node ends or loses authority. A failure that still has authority is terminal.
   * @private
   * @param {object} node The node record.
   * @returns {Promise<void>} Never rejects.
   */
  async driveNode(node) {
    const { cluster, retry, logger } = this.options
    try {
      const broker = await node.operation.retry(
        () => cluster.findBroker({ nodeId: node.id, fence: node.operation }),
        retry
      )
      node.operation.check()
      node.session = new ShareSession({ operation: node.operation })
      node.schedule = createRetry.backoff(retry)
      node.loop = this.createLoop(node, broker)
      while (!node.ended && this.admitting && this.ordinaryRefusal(node.issued) === null) {
        await this.pollNode(node)
      }
    } catch (error) {
      if (this.ordinaryRefusal(node.issued) === null && !node.ended && !this.closing) {
        logger.error('Share node failed', { nodeId: node.id, error: error.message })
        this.crash(error)
      }
    }
    if (node.ended && node.loop && !this.closing) {
      await node.loop.finishNode().catch(() => {})
      if (this.nodes.get(node.id) === node) this.nodes.delete(node.id)
    }
  }

  /**
   * @private
   * @param {object} node The node record.
   * @param {object} broker The node's Broker.
   * @returns {ShareFetchLoop} The node's loop, wired to this run's callbacks.
   */
  createLoop(node, broker) {
    const { options } = this
    return new ShareFetchLoop({
      session: node.session,
      groupId: options.groupId,
      memberId: this.membership.memberId,
      maxWaitTimeInMs: options.maxWaitTimeInMs,
      minBytes: options.minBytes,
      maxBytes: options.maxBytes,
      maxRecords: options.maxRecords,
      acquireMode: options.acquireMode,
      broker,
      cluster: options.cluster,
      membership: this.membership,
      logger: options.logger,
      wanted: () => node.wanted,
      onResults: (results, cohort) => this.onResults(node, results, cohort),
      onMove: (move) => this.onMove(node, move),
      onFatal: (error) => this.crash(error),
      onReset: (open, reason) => this.onSessionReset(node, reason),
      dispatch: (cohorts) => this.dispatch(node, cohorts),
      authority: { membershipToken: node.issued },
      retry: options.retry,
      checkCrcs: options.checkCrcs,
    })
  }

  /**
   * One fetching round of a node: the exchange, then its handlers. The round, not the FIFO,
   * spans the handlers, so a renewal or flush can use the queue while they run.
   * @private
   * @param {object} node The node record.
   * @returns {Promise<object>} The round result.
   */
  pollNode(node) {
    const { loop, session } = node
    return loop.round(
      async (open) => {
        const epoch = session.epoch
        const result = await loop.exchange({ apiKey: apiKeys.ShareFetch, kind: 'fetch' }, open)
        this.afterFetch(node, epoch, result)
        return result
      },
      (cohorts) => loop.dispatchCohorts(cohorts)
    )
  }

  /**
   * Publishes the fetch event of an answered fetching request and records its lock duration.
   * Idle requests, resets and finals publish nothing.
   * @private
   * @param {object} node The node record.
   * @param {number} epoch The session epoch the request carried.
   * @param {object} result The exchange result.
   * @returns {void}
   */
  afterFetch(node, epoch, result) {
    if (result.reset || result.closed || node.session.epoch === epoch) return
    if (result.lockDuration !== undefined) {
      node.lockDuration = result.lockDuration
      if (this.ordinaryRefusal(node.issued) === null)
        this.runner.latestLockDuration = result.lockDuration
    }
    let numberOfRecords = 0
    let numberOfGaps = 0
    for (const { walk } of result.reply) {
      numberOfRecords += walk.deliver.records.length
      numberOfGaps += walk.gaps.length
    }
    this.runner.emit(events.FETCH, {
      nodeId: node.id,
      sessionEpoch: epoch,
      numberOfRecords,
      numberOfGaps,
      acknowledgementsPiggybacked: result.results.length,
      duration: result.answeredAt - result.sentAt,
    })
  }

  /**
   * Counts the delivered records that a lost outcome discarded, and publishes each compact result
   * once. Automatic spans, records settled earlier and the nonterminal renew echo of a lost
   * renewal are not counted: only delivered records inside a lost terminal range are.
   * @private
   * @param {object} node The node record.
   * @param {object[]} results Compact ledger results.
   * @param {object} cohort The acquisition they belong to.
   * @returns {void}
   */
  onResults(node, results, cohort) {
    for (const outcome of results) {
      const reason = outcome.error?.reason
      if (outcome.type !== 'renew' && (reason === 'session-lost' || reason === 'membership-lost'))
        node.dropped += countWithin(
          cohort.walk.deliver.records,
          outcome.firstOffset,
          outcome.lastOffset
        )
    }
    this.runner.publishResults(cohort, results)
  }

  /**
   * @private
   * @param {object} node The node record.
   * @param {string} reason Why the session reset.
   * @returns {void}
   */
  onSessionReset(node, reason) {
    this.runner.emit(events.SESSION_RESET, {
      nodeId: node.id,
      reason,
      recordsDropped: node.dropped,
    })
    node.dropped = 0
  }

  /**
   * A partition moved to another leader: the old node's acquisitions were retired, and routing
   * follows the metadata the move installed.
   * @private
   * @param {object} node The old node.
   * @param {{reason: string}} move The loop's move description.
   * @returns {void}
   */
  onMove(node, move) {
    this.onSessionReset(node, move.reason)
    this.syncNodes()
  }

  /**
   * Hands one answer's acquisitions to the handlers, one cohort after another. Resolves only
   * after every record was handled, dropped or released.
   * @private
   * @param {object} node The node record.
   * @param {object[]} cohorts The adopted acquisitions of the answer.
   * @returns {Promise<void>}
   * @throws {Error} The explicit-omission verdict, after its releases were flushed.
   */
  async dispatch(node, cohorts) {
    node.queue = { cohorts, index: 0 }
    try {
      for (; node.queue.index < cohorts.length; node.queue.index++) {
        await this.dispatchCohort(node, cohorts[node.queue.index])
      }
    } finally {
      node.queue = null
      node.active = null
    }
  }

  /**
   * @private
   * @param {object} node The node record.
   * @param {object} cohort One acquisition.
   * @returns {Promise<void>}
   */
  async dispatchCohort(node, cohort) {
    const { records } = cohort.walk.deliver
    if (records.length === 0) return
    const frame = {
      ledger: cohort.ledger,
      records,
      mode: this.options.acknowledgementMode,
      batch: this.batch,
      authority: this.authorityOf(node, cohort),
    }
    cohort.key = `${cohort.topicId}:${cohort.partition}`
    cohort.lockDuration = node.lockDuration
    node.active = { cohort, records, next: 0, call: null }
    if (this.batch) await this.dispatchBatch(node, cohort, frame)
    else {
      for (let i = 0; i < records.length; i++) {
        if (!(await this.dispatchMessage(node, cohort, frame, i))) return
      }
    }
  }

  /**
   * The original authority of one acquisition, shared by every call made on it.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @returns {{current: () => boolean, running: () => boolean, stopping: () => boolean, renew: (offsets: string[]) => Promise<number>}}
   *   The predicates and the renewal adapter of the acquisition.
   */
  authorityOf(node, cohort) {
    return {
      current: () => this.ordinaryRefusal(node.issued) === null && node.loop.isLive(cohort),
      running: () => this.ordinaryRefusal(node.issued) === null && !this.stopRequested,
      stopping: () => this.stopRequested,
      renew: (offsets) => this.renew(node, cohort, offsets),
    }
  }

  /**
   * Checks the start gate of a handler call over `records[from, to)`: authority, wanted set, stop
   * latch, then the lock. Unstarted records of a revoked partition or a stopping run are
   * released; expired ones are dropped without acknowledgement. A partly expired range leaves its
   * dropped offsets on `cohort.expired`.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {number} from First record that would start.
   * @param {number} to One past the last record that would start.
   * @returns {'start'|'drop'|'stop'} Whether a handler may start, all records expired, or none may.
   */
  startGate(node, cohort, from, to) {
    if (this.ordinaryRefusal(node.issued) !== null || !node.loop.isLive(cohort)) return 'stop'
    const { records } = cohort.walk.deliver
    if (!node.keys.has(cohort.key) || this.stopRequested) {
      this.releaseFrom(node, cohort, from)
      return 'stop'
    }
    this.flushIfDue(node, cohort)
    const now = performance.now()
    let expired = null
    for (let i = from; i < to; i++) {
      if (!cohort.ledger.canStart(records[i].offset, now)) (expired ??= []).push(records[i].offset)
    }
    if (expired === null) return 'start'
    cohort.ledger.dropExpired(expired, now)
    this.runner.emit(events.LOCK_EXPIRED, {
      topic: cohort.topic,
      partition: cohort.partition,
      offsets: expired,
      stage: 'before-handler',
    })
    if (expired.length === to - from) return 'drop'
    cohort.expired = new Set(expired)
    return 'start'
  }

  /**
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {object} frame The acquisition's call context, shared by all its calls.
   * @param {number} index Index of the record in the acquisition.
   * @returns {Promise<boolean>} False when no further record of this acquisition may start.
   */
  async dispatchMessage(node, cohort, frame, index) {
    const { records, deliveryCounts } = cohort.walk.deliver
    const verdict = this.startGate(node, cohort, index, index + 1)
    if (verdict === 'stop') return false
    if (verdict === 'drop') return true
    records[index].deliveryCount = deliveryCounts[index]
    node.active.next = index + 1
    const call = new ShareHandlerCall(frame, index, index + 1)
    node.active.call = call
    await this.invoke(node, cohort, call, call.messagePayload(cohort.topic, cohort.partition))
    return this.ordinaryRefusal(node.issued) === null
  }

  /**
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {object} frame The acquisition's call context.
   * @returns {Promise<void>}
   */
  async dispatchBatch(node, cohort, frame) {
    const { deliveryCounts } = cohort.walk.deliver
    let { records } = cohort.walk.deliver
    const verdict = this.startGate(node, cohort, 0, records.length)
    if (verdict !== 'start') return
    if (cohort.expired) {
      records = records.filter((record) => !cohort.expired.has(record.offset))
      cohort.expired = null
    }
    const delivered = cohort.walk.deliver.records
    for (let i = 0; i < delivered.length; i++) delivered[i].deliveryCount = deliveryCounts[i]
    node.active.next = delivered.length
    const call = new ShareHandlerCall(records === frame.records ? frame : { ...frame, records })
    node.active.call = call
    const batch = {
      topic: cohort.topic,
      partition: cohort.partition,
      messages: records,
      acquisitionLockTimeoutMs: cohort.lockDuration,
    }
    await this.invoke(node, cohort, call, call.batchPayload(batch))
  }

  /**
   * Releases the unstarted records of an acquisition that must not start another handler.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {number} from Index of the first record to release.
   * @returns {void}
   */
  releaseFrom(node, cohort, from) {
    if (this.ordinaryRefusal(node.issued) !== null || !node.loop.isLive(cohort)) return
    const { records } = cohort.walk.deliver
    cohort.ledger.completeRange(records, from, records.length, 'release')
    if (node.active?.cohort === cohort) node.active.next = records.length
  }

  /**
   * Flushes the decided entries of an acquisition once less than half of a lock remains, without
   * waiting for it: the request takes its turn in the node's FIFO and its failure is observed.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @returns {void}
   */
  flushIfDue(node, cohort) {
    if (!cohort.ledger.needsFlush(performance.now())) return
    node.loop.acknowledge({ cohorts: [cohort] }).catch((error) => {
      this.options.logger.debug('Share boundary flush failed', { error: error.message })
    })
  }

  /**
   * Runs one handler call: poll accounting, the application call, lock-expiry observation, the
   * policy completion, and the handler pacing or the omission verdict.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {ShareHandlerCall} call The original invocation.
   * @param {object} payload The public payload built from `call`, handed to the application handler.
   * @returns {Promise<void>}
   * @throws {Error} The explicit-omission verdict.
   */
  async invoke(node, cohort, call, payload) {
    const pollCall = this.poll.begin(call)
    let failed = false
    let thrown
    try {
      if (pollCall === null) throw new KafkaJSNonRetriableError('Share run is no longer polling')
      await this.handler(payload)
    } catch (error) {
      failed = true
      thrown = error
    } finally {
      this.poll.finish(pollCall)
      this.notifyDrained()
    }
    if (!call.open || !call.authority.current()) {
      call.complete(failed)
      return
    }
    this.observeLateReturn(cohort, call)
    const verdict = call.complete(failed)
    if (failed) await this.afterFailure(node, cohort, call, thrown)
    else {
      node.retryTime = null
      this.flushIfDue(node, cohort)
      if (verdict) await this.afterOmission(node, cohort, verdict)
    }
  }

  /**
   * Emits `lock_expired` for the records of a returning handler whose lock ended meanwhile.
   * The acknowledgement still goes out: only starting a handler is lock-gated.
   * @private
   * @param {object} cohort The acquisition.
   * @param {ShareHandlerCall} call The returning call.
   * @returns {void}
   */
  observeLateReturn(cohort, call) {
    const expired = call.expiredAt(performance.now())
    if (expired)
      this.runner.emit(events.LOCK_EXPIRED, {
        topic: cohort.topic,
        partition: cohort.partition,
        offsets: expired,
        stage: 'after-handler',
      })
  }

  /**
   * A throw: report it, flush the released work at once and pace the node's next request with
   * the retry backoff. Consecutive failures grow the delay; a normal return resets it.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {ShareHandlerCall} call The failed call.
   * @param {*} thrown What the handler threw, an Error or not.
   * @returns {Promise<void>}
   */
  async afterFailure(node, cohort, call, thrown) {
    const error =
      thrown instanceof Error
        ? thrown
        : new Error('Handler threw a non-Error value', { cause: thrown })
    this.runner.emit(events.HANDLER_ERROR, {
      topic: cohort.topic,
      partition: cohort.partition,
      offsets: call.offsets(),
      error,
    })
    node.loop.acknowledge({ cohorts: [cohort] }).catch((failure) => {
      this.options.logger.error('Share handler release flush failed', { error: failure.message })
    })
    node.retryTime =
      node.retryTime === null ? node.schedule.initial() : node.schedule.next(node.retryTime)
    await node.pacing.sleep(node.retryTime).catch(() => {})
  }

  /**
   * Explicit-mode omission: the omitted records are already released; flush every decision, then
   * the verdict ends the run.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {Error} verdict The typed unacknowledged error.
   * @returns {Promise<void>}
   * @throws {Error} The verdict.
   */
  async afterOmission(node, cohort, verdict) {
    await node.loop.acknowledge({ cohorts: [cohort] }).catch((error) => {
      this.options.logger.error('Share omission flush failed', { error: error.message })
    })
    throw verdict
  }

  /**
   * Renews the selected offsets through the node's FIFO. A refusal rejects with the ledger's
   * compact error; success resolves with the broker's duration only while this acquisition is
   * still current. An answer that keeps the duration reports the one this acquisition last had:
   * the fetch's, or that of its last successful renewal. The promise is observed here, so an
   * unawaited renewal never escapes.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The original acquisition.
   * @param {string[]} offsets The validated held offsets.
   * @returns {Promise<number>} The lock duration the broker applied, in milliseconds.
   */
  renew(node, cohort, offsets) {
    const promise = (async () => {
      const result = await node.loop.acknowledge({ renew: [{ cohort, offsets }] })
      const refused = this.ordinaryRefusal(node.issued)
      if (refused) throw refused
      if (result.lockDuration !== undefined) {
        cohort.lockDuration = result.lockDuration
        this.runner.latestLockDuration = result.lockDuration
      }
      return cohort.lockDuration
    })()
    promise.catch(() => {})
    return promise
  }

  /**
   * Resolves once every captured expired call finished.
   * @private
   * @param {object[]} captured Call identities from `SharePollView#captureExpired`.
   * @returns {Promise<void>}
   */
  drained(captured) {
    if (this.poll.drained(captured)) return Promise.resolve()
    return new Promise((resolve) => {
      const waiter = () => {
        if (!this.poll.drained(captured)) return
        this.waiters.delete(waiter)
        resolve()
      }
      this.waiters.add(waiter)
    })
  }

  /**
   * @private
   * @returns {void}
   */
  notifyDrained() {
    for (const waiter of [...this.waiters]) waiter()
  }

  /**
   * Reacts to a membership retirement. The hook runs before anything else can observe it, so the
   * old identity still authorizes the capture: a fence or fatal error freezes eligible terminal
   * work now, a join only adopts the new identity, and our own leave has already captured.
   * @private
   * @param {symbol} oldToken The retired membership identity.
   * @param {'joined'|'fenced'|'fatal'|'stopped'|'poll-expired'} reason Why it retired.
   * @returns {void}
   */
  onInvalidate(oldToken, reason) {
    if (reason === 'joined') {
      this.authorityToken = this.membership.token
      return
    }
    this.retireLookups()
    if (oldToken !== this.authorityToken || this.closing) return
    this.beginClose(reason === 'fenced' ? 'fence' : 'crash')
  }

  /**
   * Starts or joins this run's one close. Forced kinds (`crash`, `fence`, `poll`) freeze at once,
   * also when they escalate a natural close in progress; `stop` and `disconnect` latch the stop
   * request synchronously, so no handler starts after them, and wait naturally for running
   * handlers until the handler boundary. A stop or disconnect that arrives while a fence or poll
   * recovery is closing ends the run once that recovery cleanup is done. Later calls reuse the
   * episode, so a disconnect never recaptures a replacement run. A zero timeout on a close in
   * progress revokes the episode, which retires its traffic operation in the same tick: a final
   * that was written and is unanswered settles lost and its exact socket closes at once.
   * @param {'stop'|'disconnect'|'crash'|'fence'|'poll'} kind Why the run closes.
   * @param {number} [timeout] Validated disconnect timeout, when this call supplies one.
   * @param {number} [startedAt] Original public entry time of that call.
   * @returns {ShareCloseEpisode} The run's episode; `episode.done` settles when the close ended.
   */
  beginClose(kind, timeout, startedAt) {
    const forced = kind === 'crash' || kind === 'fence' || kind === 'poll'
    if (!forced) this.stopRequested = true
    let episode = this.closing
    if (episode) {
      if (kind === 'disconnect') {
        episode.disconnect(timeout, startedAt)
        episode.closeSockets = true
        this.armBoundary(episode)
        if (episode.revoked) this.escalate(episode)
      }
      if (forced) this.escalate(episode)
      return episode
    }
    episode = new ShareCloseEpisode({ run: this, requestTimeout: this.options.requestTimeout })
    episode.kind = kind
    episode.forced = forced
    episode.closeSockets = kind === 'disconnect' || kind === 'crash'
    const done = deferred()
    episode.done = done.promise
    this.closing = episode
    this.admitting = false
    for (const node of this.nodes.values())
      node.pacing.retire(new KafkaJSNonRetriableError('Share run is closing'))
    if (kind === 'disconnect') episode.disconnect(timeout, startedAt)
    this.close(episode).then(done.resolve, done.resolve)
    return episode
  }

  /**
   * Moves a natural close to its forced boundary: eligible terminal work is frozen and ordinary
   * authority retired in this very tick, so an old callback that runs before the close's own
   * continuation can no longer decide or renew. The close then proceeds to its traffic.
   * @private
   * @param {ShareCloseEpisode} episode The run's natural episode.
   * @returns {void}
   */
  escalate(episode) {
    this.freeze(episode)
    episode.boundary?.resolve()
  }

  /**
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {Promise<void>} Settles after cleanup, whatever the outcome of the traffic.
   */
  async close(episode) {
    const { logger } = this.options
    try {
      if (episode.forced || episode.revoked) this.freeze(episode)
      else {
        await this.waitHandlers(episode)
        this.freeze(episode)
      }
      await this.closeTraffic(episode)
    } catch (error) {
      logger.error('Share close failed', { error: error.message })
    } finally {
      this.teardown(episode)
      this.afterClose(episode)
    }
  }

  /**
   * Waits for every node to finish its round, until the handler boundary arrives. The boundary is
   * read from the actual clock after every timer wake, and it is re-armed when a later disconnect
   * supplies a deadline.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {Promise<void>}
   */
  async waitHandlers(episode) {
    episode.boundary = deferred()
    this.armBoundary(episode)
    const idle = Promise.all([...this.nodes.values()].map((node) => node.drive))
    await Promise.race([idle, episode.boundary.promise])
    episode.boundaryOperation?.retire()
  }

  /**
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  armBoundary(episode) {
    episode.boundaryOperation?.retire()
    if (!episode.boundary || episode.handlerBoundary === Infinity) return
    const operation = new ConsumerOperation({ now: episode.now })
    episode.boundaryOperation = operation
    ;(async () => {
      while (episode.now() < episode.handlerBoundary) {
        await operation.sleep(episode.handlerBoundary - episode.now())
      }
      episode.boundary.resolve()
    })().catch(() => {})
  }

  /**
   * The synchronous capture boundary: still-running handlers keep only their explicit choices and
   * unstarted records are released, then every idle established node captures one terminal
   * ticket per partition, and only then is ordinary authority retired and every node disposed,
   * which settles the uncaptured remainder membership-lost. Busy and unopened nodes capture
   * nothing. Every session discarded this way publishes one `session_reset` with reason
   * `membership`. The heartbeat chain is suspended for a poll or fence recovery, so no join can
   * overlap its traffic, and ended for good by every other kind.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  freeze(episode) {
    if (episode.frozen) return
    this.captureCloseCoordinator(episode)
    const bindings = []
    for (const node of this.nodes.values()) {
      if (!node.loop) continue
      this.finalizeActive(node)
      node.captured = node.loop.captureFinal()
      if (node.captured) {
        const { binding } = node.captured
        bindings.push({ nodeId: node.id, current: () => !ShareSession.bindingLost(binding) })
      }
    }
    episode.freeze(bindings)
    this.authorityToken = null
    this.heartbeatRevision = null
    this.retireLookups()
    this.admitting = false
    if (episode.kind === 'poll' || episode.kind === 'fence') this.manager.stop()
    else {
      this.manager.shutdown()
      this.poll.stop()
    }
    for (const node of this.nodes.values()) {
      const discarded = node.loop !== null && !node.loop.disposed
      node.loop?.dispose(new KafkaJSNonRetriableError('Share run closed'), {
        reason: 'membership-lost',
        preserve: node.captured,
      })
      node.operation.retire(new KafkaJSNonRetriableError('Share run closed'))
      node.pacing.retire(new KafkaJSNonRetriableError('Share run closed'))
      if (discarded && (node.session.epoch !== 0 || node.dropped > 0))
        this.onSessionReset(node, 'membership')
    }
  }

  /**
   * Recaptures an already reconnected coordinator at the close boundary, only while the same
   * established membership still owns the same Broker, pool and connection slot. No connection
   * is opened here; once frozen, the captured generation cannot be replaced again.
   * @private
   * @param {ShareCloseEpisode} episode The episode whose authority is about to freeze.
   * @returns {void}
   */
  captureCloseCoordinator(episode) {
    const coordinator = this.coordinator
    const { membership } = this
    if (
      !coordinator ||
      coordinator.current() ||
      episode.guard() ||
      membership.token !== this.authorityToken ||
      membership.closed ||
      membership.state !== MEMBER_STATES.STABLE ||
      membership.memberEpoch <= 0 ||
      membership.coordinator !== this.facade ||
      this.facade?.broker !== coordinator.broker ||
      coordinator.broker.connectionPool !== coordinator.pool ||
      coordinator.pool.pool[0] !== coordinator.connection
    )
      return
    const current = this.bindCoordinator(coordinator.broker)
    if (current.current()) this.coordinator = current
  }

  /**
   * Reports a leave refusal once, distinguishing an omission from a written, unknown outcome.
   * @private
   * @param {ShareCloseEpisode} episode The closing episode.
   * @param {string} reason Why the leave could not complete.
   * @returns {void}
   */
  leaveSkipped(episode, reason) {
    if (episode.leaveReported) return
    episode.leaveReported = true
    this.options.logger.warn(
      episode.leaveWritten ? 'Share leave outcome unknown' : 'Share leave skipped',
      {
        groupId: this.options.groupId,
        reason,
        outcome: episode.leaveWritten ? 'sent-unknown' : 'not-sent',
      }
    )
  }

  /**
   * @private
   * @param {object} node The node record.
   * @returns {void} Promotes the explicit choices of the running call and releases the records
   *   of the acquisition, and of the acquisitions after it, that no handler started.
   */
  finalizeActive(node) {
    const release = (cohort, from) => {
      if (!node.loop.isLive(cohort)) return
      try {
        const { records } = cohort.walk.deliver
        cohort.ledger.completeRange(records, from, records.length, 'release')
      } catch (error) {
        this.options.logger.debug('Share unstarted release skipped', { error: error.message })
      }
    }
    const active = node.active
    if (active) {
      active.call?.freeze()
      if (active.next < active.records.length) release(active.cohort, active.next)
    }
    if (node.queue) {
      for (const cohort of node.queue.cohorts.slice(node.queue.index + 1)) release(cohort, 0)
    }
  }

  /**
   * Sends the captured finals and the leave under one shared cap, each at most once. Reads the
   * clock again on completion so expiry revokes the episode before its captured sockets detach.
   * @private
   * @param {ShareCloseEpisode} episode The frozen episode.
   * @returns {Promise<void>}
   */
  async closeTraffic(episode) {
    const operation = new ConsumerOperation({
      deadlineAt: episode.finalDeadline,
      guard: () => episode.guard(),
      now: episode.now,
    })
    episode.attach(operation)
    let sent = null
    const finals = () => (sent ??= this.sendFinals(episode, operation))
    try {
      if (episode.kind === 'fence') {
        this.leaveSkipped(episode, 'fenced')
        await operation.wait(finals)
        return
      }
      const coordinator = this.coordinator
      let reason = null
      if (episode.guard()) reason = 'close-deadline'
      else if (this.membership.memberEpoch <= 0) reason = 'membership-unestablished'
      else if (
        !coordinator ||
        (!this.membership.coordinator && this.membership.state !== MEMBER_STATES.FATAL)
      )
        reason = 'coordinator-unknown'
      else if (!episode.claimLeave(coordinator)) reason = 'coordinator-stale'
      if (reason) {
        this.leaveSkipped(episode, reason)
        if (episode.kind === 'poll')
          this.membership.leave({
            stale: true,
            guard: () => new KafkaJSNonRetriableError('Share leave was omitted'),
          })
        await operation.wait(finals)
      } else await this.leave(episode, operation, coordinator, finals)
    } catch (error) {
      if (episode.leaveWritten)
        this.leaveSkipped(episode, episode.guard() ? 'close-deadline' : 'leave-failed')
      this.options.logger.warn('Share close traffic ended early', { error: error.message })
    } finally {
      await (sent ?? finals()).catch(() => {})
      episode.guard()
      operation.finish()
    }
  }

  /**
   * The one leave: the membership's own leave, which retires its token before any frame, or the
   * single direct frame through the captured coordinator when the membership cannot issue one.
   * @private
   * @param {ShareCloseEpisode} episode The frozen episode.
   * @param {ConsumerOperation} operation The episode's bounded operation.
   * @param {object} coordinator The captured coordinator binding.
   * @param {() => Promise<void>} finals Lazy at-most-once finals.
   * @returns {Promise<void>}
   */
  async leave(episode, operation, coordinator, finals) {
    const guard = (frameLength) => {
      const refused = episode.refusal(coordinator)
      if (refused)
        this.leaveSkipped(episode, episode.guard() ? 'close-deadline' : 'coordinator-stale')
      else if (frameLength !== undefined) episode.leaveWritten = true
      return refused
    }
    const { membership } = this
    if (membership.state !== MEMBER_STATES.FATAL && !(membership.closed && membership.terminal)) {
      await operation.wait(() =>
        membership.leave({ stale: episode.kind === 'poll', beforeLeave: finals, guard })
      )
      return
    }
    await operation.wait(finals)
    await operation.wait(async () => {
      const refused = guard()
      if (refused) return
      await coordinator.broker.shareGroupHeartbeat(
        {
          groupId: this.options.groupId,
          memberId: membership.memberId,
          memberEpoch: -1,
          rackId: null,
          subscribedTopicNames: null,
        },
        { guard }
      )
    })
  }

  /**
   * Sends every captured node's final through the loop's closing branch, which settles its
   * preserved tickets once. A refused claim still goes through, with a refusing guard, so its
   * tickets settle as not sent.
   * @private
   * @param {ShareCloseEpisode} episode The frozen episode.
   * @param {ConsumerOperation} operation The episode's bounded operation.
   * @returns {Promise<void>}
   */
  async sendFinals(episode, operation) {
    const jobs = []
    for (const node of this.nodes.values()) {
      if (!node.captured) continue
      const binding = episode.nodes.get(node.id)?.binding
      episode.claimFinal(node.id)
      const guard = () =>
        binding
          ? episode.refusal(binding)
          : (episode.guard() ?? new KafkaJSNonRetriableError('No captured binding'))
      jobs.push(node.loop.finishNode({ closing: { guard, captured: node.captured, operation } }))
    }
    await Promise.allSettled(jobs)
  }

  /**
   * Detaches only the exact captured connections that are still current, so a replacement
   * socket is never closed. A clean stop whose traffic completed keeps its healthy connections.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  teardown(episode) {
    const dirty = episode.closeSockets || episode.revoked
    const close = (connection) =>
      connection.disconnect({ drainTimeout: 0 }).catch((error) => {
        this.options.logger.debug('Share captured connection close failed', {
          error: error.message,
        })
      })
    if (dirty) {
      for (const node of this.nodes.values()) {
        const binding = node.captured?.binding
        if (binding && !ShareSession.bindingLost(binding)) close(binding.connection)
      }
      if (this.coordinator?.current()) close(this.coordinator.connection)
    }
    episode.boundaryOperation?.retire()
  }

  /**
   * Completes the episode for its kind: a stop or disconnect ends the run, and so does a fence or
   * poll recovery that a stop or disconnect arrived in. Otherwise a fence resumes the run for the
   * rejoin, restarting the suspended heartbeat chain, a poll recovery waits for its own rejoin,
   * and a crash is finished by its supervisor.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  afterClose(episode) {
    this.closed = true
    const { kind } = episode
    const recovery = kind === 'fence' || kind === 'poll'
    if (kind === 'stop' || kind === 'disconnect' || (recovery && this.stopRequested)) {
      this.runner.emit(events.STOP)
      this.finishRun()
    } else if (recovery) {
      this.nodes.clear()
      this.closing = null
      this.closed = false
      this.admitting = true
      this.heartbeatRevision = Symbol('heartbeat')
      episode.finish()
      if (kind === 'fence') {
        this.manager.ensureStarted()
        this.syncNodes()
      }
      return
    }
    episode.finish()
  }

  /**
   * Ends the run for good: heartbeat, poll view, listeners, nodes and the first-join promise.
   * The assignment and every public snapshot of the client are cleared; the subscription stays.
   * @param {boolean} [release=true] Whether the client forgets this run as its current one.
   * @returns {void}
   */
  finishRun(release = true) {
    if (this.finished) return
    this.finished = true
    this.retired = true
    this.admitting = false
    this.manager.shutdown()
    this.poll.stop()
    this.notifyDrained()
    this.retireLookups()
    this.membership.stop()
    this.unrouted()
    this.operation.finish()
    this.assignment = new Map()
    this.nodes.clear()
    this.firstJoin.settled || this.firstJoin.resolve()
    if (release && this.runner.current === this) this.runner.current = null
  }

  /**
   * Requests a clean stop: running handlers finish naturally, then captured finals and one leave.
   * @returns {Promise<void>} Settles when the close ended.
   */
  stop() {
    this.firstJoin.settled || this.firstJoin.resolve()
    return this.beginClose('stop').done
  }

  /**
   * Once-only terminal supervision: immediate captured close and finite cleanup, then the
   * client's restart verdict.
   * @param {Error} error The exact terminal error.
   * @returns {Promise<void>}
   */
  async crash(error) {
    if (this.crashed) return
    this.crashed = true
    this.firstJoin.settled || this.firstJoin.reject(error)
    const episode = this.beginClose('crash')
    await episode.done.catch(() => {})
    try {
      await this.runner.superviseCrash(this, error)
    } catch (failure) {
      this.options.logger.error('Share crash supervision failed', { error: failure.message })
    }
  }

  /**
   * Sequence of a poll-interval overrun: capture the expired calls, leave, wait until they finish
   * or the run stops, then rejoin at epoch zero with fresh ordinary authority.
   * @private
   * @returns {Promise<void>}
   */
  async onPollExpired() {
    if (this.closing || this.retired || this.stopRequested) return
    const captured = this.poll.captureExpired()
    const episode = this.beginClose('poll')
    await episode.done.catch(() => {})
    if (this.stopRequested || this.retired || this.crashed) return
    await this.membership.leavePromise
    await this.drained(captured)
    if (this.stopRequested || this.retired || this.crashed) return
    this.membership.rejoinAfterPoll(this.poll, captured)
    this.manager.requestTick()
  }
}

module.exports = { ShareRun }
