const { AsyncLocalStorage } = require('node:async_hooks')
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
const describeThrown = require('../../network/describeThrown')

const INITIAL_HEARTBEAT_MS = 1000
/** Holds only the identity of the run whose handler is executing, never a record or batch. */
const handlerScope = new AsyncLocalStorage()

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
    this.detached = new Set()
    this.cleanupOwners = new WeakMap()
    this.handlers = new Map()
    this.assignment = new Map()
    this.applying = 0
    this.committed = []
    this.tap = null
    this.heartbeatRevision = Symbol('heartbeat')
    this.identity = Symbol('share run')
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
    const { message, name, type, code } = error
    this.options.logger.warn('Share heartbeat failed', { error: message, name, type, code })
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
      if (node?.ended) this.detach(node)
      if (!node || node.ended) this.startNode(nodeId, rows)
    }
  }

  /**
   * Takes an ended node out of the map. A close still owns its drive until it settles, and
   * afterward still owns any socket on which the broker may have kept its session.
   * @private
   * @param {object} node The ended node record.
   * @returns {void}
   */
  detach(node) {
    this.nodes.delete(node.id)
    this.detached.add(node)
    node.drive.then(() => this.pruneDetached(node))
  }

  /**
   * Keeps at most one settled cleanup record per data socket. The episode already owns
   * its snapshot if a close started while the drive was settling.
   * @private
   * @param {object} node The detached node whose drive settled.
   * @returns {void}
   */
  pruneDetached(node) {
    if (this.closing) return
    for (const record of this.detached)
      if ((record === node || record.loop?.disposed) && !record.session?.cleanupBinding)
        this.detached.delete(record)
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
   * until the node ends or loses authority. An ended record with remaining socket cleanup
   * passes to the detached set. A failure that still has authority is terminal.
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
      node.session = new ShareSession({
        operation: node.operation,
        cleanupOwners: this.cleanupOwners,
      })
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
      if (!this.closing && this.nodes.get(node.id) === node) this.detach(node)
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
        node.dropped += countWithin(cohort.ledger.offsets, outcome.firstOffset, outcome.lastOffset)
    }
    this.runner.publishResults(cohort, results)
  }

  /**
   * Publishes the reset and stops counting the handler calls whose acquisition it retired, so a
   * stop does not wait for them.
   * @private
   * @param {object} node The node record.
   * @param {string} reason Why the session reset.
   * @returns {void}
   */
  onSessionReset(node, reason) {
    for (const call of this.handlers.keys())
      if (!call.authority.current()) this.handlers.delete(call)
    this.notifyDrained()
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
   * Hands one answer's acquisitions to the handlers, one cohort after another, inside one entry of
   * this run's handler context, so a handler's own calls and the async work it starts carry the
   * run's identity. Resolves only after every record was handled, dropped or released.
   * @private
   * @param {object} node The node record.
   * @param {object[]} cohorts The adopted acquisitions of the answer.
   * @returns {Promise<void>}
   * @throws {Error} The explicit-omission verdict, after its releases were flushed.
   */
  dispatch(node, cohorts) {
    return handlerScope.run(this.identity, () => this.dispatchAnswer(node, cohorts))
  }

  /**
   * @private
   * @param {object} node The node record.
   * @param {object[]} cohorts The adopted acquisitions of the answer.
   * @returns {Promise<void>}
   */
  async dispatchAnswer(node, cohorts) {
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
      offsets: cohort.ledger.offsets,
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
    if (!this.admitHandler(node, cohort, from)) return 'stop'
    this.flushIfDue(node, cohort)
    const now = performance.now()
    const { offsets } = cohort.ledger
    let expired = null
    for (let i = from; i < to; i++) {
      if (!cohort.ledger.canStart(offsets[i], now)) (expired ??= []).push(offsets[i])
    }
    if (expired !== null) {
      cohort.ledger.dropExpired(expired, now)
      this.runner.emit(events.LOCK_EXPIRED, {
        topic: cohort.topic,
        partition: cohort.partition,
        offsets: expired,
        stage: 'before-handler',
      })
      if (expired.length === to - from) return 'drop'
      cohort.expired = new Set(expired)
    }
    // A synchronous instrumentation listener can stop the run before the call is registered.
    return this.admitHandler(node, cohort, from) ? 'start' : 'stop'
  }

  /**
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {number} from First unstarted record.
   * @returns {boolean} Whether the handler may start. Releases unstarted work when admission closes.
   */
  admitHandler(node, cohort, from) {
    if (this.ordinaryRefusal(node.issued) !== null || !node.loop.isLive(cohort)) return false
    if (node.keys.has(cohort.key) && !this.stopRequested) return true
    this.releaseFrom(node, cohort, from)
    return false
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
   * Hands one acquisition to `eachBatch`. The handler gets its own copy of the records array, so
   * reordering or changing `batch.messages` cannot move a choice onto another record: the call,
   * the ledger and the close still read the acquisition's ordered array.
   * @private
   * @param {object} node The node record.
   * @param {object} cohort The acquisition.
   * @param {object} frame The acquisition's call context.
   * @returns {Promise<void>}
   */
  async dispatchBatch(node, cohort, frame) {
    const { deliveryCounts } = cohort.walk.deliver
    let { records, offsets } = frame
    const verdict = this.startGate(node, cohort, 0, records.length)
    if (verdict !== 'start') return
    if (cohort.expired) {
      const live = (_, i) => !cohort.expired.has(frame.offsets[i])
      records = records.filter(live)
      offsets = offsets.filter(live)
      cohort.expired = null
    }
    const delivered = cohort.walk.deliver.records
    for (let i = 0; i < delivered.length; i++) delivered[i].deliveryCount = deliveryCounts[i]
    node.active.next = delivered.length
    const call = new ShareHandlerCall(
      records === frame.records ? frame : { ...frame, records, offsets }
    )
    node.active.call = call
    const batch = {
      topic: cohort.topic,
      partition: cohort.partition,
      messages: records.slice(),
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
    const { offsets } = cohort.ledger
    cohort.ledger.completeRange(offsets, from, offsets.length, 'release')
    if (node.active?.cohort === cohort) node.active.next = offsets.length
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
    outsideHandler(() =>
      node.loop.acknowledge({ cohorts: [cohort] }).catch((error) => {
        this.options.logger.debug('Share boundary flush failed', { error: error.message })
      })
    )
  }

  /**
   * Runs one handler call: poll accounting, the application call, lock-expiry observation, the
   * policy completion, and the handler pacing or the omission verdict. The call is registered
   * under the membership that issued its node for as long as it runs, and removes its own
   * registration once, after its completion, whether it returned or threw; a stop waits only for
   * registered calls. While a stop is requested no flush is sent here: the close final carries
   * the decisions.
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
    this.handlers.set(call, node.issued)
    let failed = false
    let thrown
    let verdict = null
    try {
      try {
        if (pollCall === null) throw new KafkaJSNonRetriableError('Share run is no longer polling')
        await this.handler(payload)
      } catch (error) {
        failed = true
        thrown = error
      } finally {
        this.poll.finish(pollCall)
      }
      if (!call.open || !call.authority.current()) {
        call.complete(failed)
        return
      }
      this.observeLateReturn(cohort, call)
      verdict = call.complete(failed)
    } finally {
      this.handlers.delete(call)
      this.notifyDrained()
    }
    if (failed) await this.afterFailure(node, cohort, call, thrown)
    else {
      node.retryTime = null
      if (!this.stopRequested) this.flushIfDue(node, cohort)
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
   * the retry backoff. Consecutive failures grow the delay; a normal return resets it. During a
   * stop the release is not flushed: it goes into the close final.
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
    if (!this.stopRequested)
      outsideHandler(() =>
        node.loop.acknowledge({ cohorts: [cohort] }).catch((failure) => {
          this.options.logger.error('Share handler release flush failed', {
            error: failure.message,
          })
        })
      )
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
    await outsideHandler(() =>
      node.loop.acknowledge({ cohorts: [cohort] }).catch((error) => {
        this.options.logger.error('Share omission flush failed', { error: error.message })
      })
    )
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
      const result = await outsideHandler(() =>
        node.loop.acknowledge({ renew: [{ cohort, offsets }] })
      )
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
    return this.settled(() => this.poll.drained(captured))
  }

  /**
   * Resolves once `done` holds, checked again whenever a handler call returns, a session reset
   * retires calls, or the run freezes or finishes.
   * @private
   * @param {() => boolean} done The condition.
   * @returns {Promise<void>}
   */
  settled(done) {
    if (done()) return Promise.resolve()
    return new Promise((resolve) => {
      const waiter = () => {
        if (!done()) return
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
   * old identity still authorizes the capture. A broker fence or fatal error escalates an
   * in-progress stop; the run's own leave and stop never escalate it.
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
    if (oldToken !== this.authorityToken) return
    if (reason === 'fenced') this.beginClose('fence')
    else if (reason === 'fatal') this.beginClose('crash')
  }

  /**
   * Starts or joins this run's one close. Forced kinds (`crash`, `fence`, `poll`) freeze at once,
   * also when they escalate a natural close in progress; `stop` and `disconnect` latch the stop
   * request synchronously, so no handler starts after them, wait without limit (or until the
   * handler boundary of a disconnect deadline) for running handlers, then drain in-flight node
   * requests until the drain boundary. A stop or disconnect that arrives while a fence or poll
   * recovery is closing ends the run once that recovery cleanup is done. Later calls reuse the
   * episode, so a disconnect never recaptures a replacement run, and the first disconnect's budget
   * is set before anything that can call application code; a later deadline re-arms the handler
   * wait and the drain. A zero timeout on a close in progress revokes the episode, which retires
   * its drain and traffic operations in the same tick: a final that was written and is unanswered
   * settles lost and its exact socket closes at once. The episode works on a snapshot of every
   * current node record plus detached drives and their unresolved socket cleanup, taken here.
   * @param {'stop'|'disconnect'|'crash'|'fence'|'poll'} kind Why the run closes.
   * @param {number} [timeout] Validated disconnect timeout, when this call supplies one.
   * @param {number} [startedAt] Original public entry time of that call.
   * @returns {ShareCloseEpisode} The run's episode; `episode.done` settles when the close ended,
   *   and `episode.expired` lists the expired calls a poll close captured.
   */
  beginClose(kind, timeout, startedAt) {
    const forced = kind === 'crash' || kind === 'fence' || kind === 'poll'
    if (!forced) this.stopRequested = true
    let episode = this.closing
    if (episode) {
      if (this.finished) {
        if (!forced) this.finishRun()
        return episode
      }
      if (kind === 'disconnect') {
        episode.disconnect(timeout, startedAt)
        episode.closeSockets = true
        this.armBoundary(episode)
        this.armDrain(episode)
        if (episode.revoked) this.escalate(episode)
      }
      if (forced) {
        if (!episode.forced && !episode.frozen) {
          episode.kind = kind
          episode.forced = true
        }
        if (kind === 'crash') episode.closeSockets = true
        this.escalate(episode)
      }
      return episode
    }
    episode = new ShareCloseEpisode({
      run: this,
      requestTimeout: this.options.requestTimeout,
      records: [...this.nodes.values(), ...this.detached],
      logger: this.options.logger,
    })
    episode.kind = kind
    episode.forced = forced
    episode.closeSockets = kind === 'disconnect' || kind === 'crash'
    episode.expired = kind === 'poll' ? this.poll.captureExpired() : []
    const done = deferred()
    episode.done = done.promise
    this.closing = episode
    this.admitting = false
    if (kind === 'disconnect') episode.disconnect(timeout, startedAt)
    for (const node of episode.records)
      node.pacing.retire(new KafkaJSNonRetriableError('Share run is closing'))
    this.close(episode).then(done.resolve, done.resolve)
    return episode
  }

  /**
   * Moves a natural close to its forced boundary: eligible terminal work is frozen and ordinary
   * authority retired in this very tick, so an old callback that runs before the close's own
   * continuation can no longer decide or renew. The handler wait and the drain end in the same
   * tick, and the close then proceeds to its traffic.
   * @private
   * @param {ShareCloseEpisode} episode The run's natural episode.
   * @returns {void}
   */
  escalate(episode) {
    this.freeze(episode)
    episode.boundary?.resolve()
    episode.drainOperation?.retire()
    episode.drainGate?.resolve()
  }

  /**
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {Promise<void>} Settles after cleanup, whatever the outcome of the traffic.
   */
  async close(episode) {
    const { logger } = this.options
    try {
      if (!episode.forced && !episode.revoked) {
        await this.waitHandlers(episode)
        if (!episode.frozen && !episode.revoked) await this.drain(episode)
      }
      this.freeze(episode)
      await this.closeTraffic(episode)
    } catch (error) {
      logger.error('Share close failed', { error: error.message })
    } finally {
      this.teardown(episode)
      this.afterClose(episode)
    }
  }

  /**
   * Waits for the open handler calls of the current membership, until the handler boundary
   * arrives. Calls a freeze or a session reset retired no longer count, and nothing else is
   * awaited here: in-flight requests are the drain's. The boundary is read from the actual clock
   * after every timer wake, and it is re-armed when a later disconnect supplies a deadline.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {Promise<void>}
   */
  async waitHandlers(episode) {
    episode.boundary = deferred()
    this.armBoundary(episode)
    await Promise.race([this.settled(() => !this.handlersOpen()), episode.boundary.promise])
    episode.boundaryOperation?.retire()
  }

  /**
   * @private
   * @returns {boolean} Whether a registered handler call of the current membership still runs.
   */
  handlersOpen() {
    for (const [call, issued] of this.handlers)
      if (issued === this.authorityToken && call.authority.current()) return true
    return false
  }

  /**
   * Starts the shared cap at the end of the handler wait, then waits until no node of the episode
   * has a request running or queued in its session, or until the drain boundary. A node that is
   * only pacing, looking up its broker or has no session is idle. An escalation or a zero timeout
   * ends the drain in the same tick; a later disconnect deadline re-arms its boundary.
   * @private
   * @param {ShareCloseEpisode} episode The run's natural episode.
   * @returns {Promise<void>}
   */
  async drain(episode) {
    episode.startCap(episode.now())
    episode.drainGate = deferred()
    this.armDrain(episode)
    const gate = episode.drainGate
    const busy = () => episode.records.filter((node) => node.session?.busy)
    const idle = (async () => {
      for (let nodes = busy(); nodes.length && !gate.settled; nodes = busy())
        await Promise.race([
          Promise.all(nodes.map((node) => node.session.whenIdle())),
          gate.promise,
        ])
    })()
    await Promise.race([idle, gate.promise])
    episode.drainOperation?.retire()
    episode.drainOperation = null
  }

  /**
   * Arms the drain's own wake at the episode's current drain boundary, replacing an earlier one.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  armDrain(episode) {
    episode.drainOperation?.retire()
    episode.drainOperation = null
    const gate = episode.drainGate
    if (!gate || gate.settled) return
    episode.drainOperation = this.wake(episode, gate, episode.drainBoundary())
  }

  /**
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  armBoundary(episode) {
    episode.boundaryOperation?.retire()
    if (!episode.boundary || episode.handlerBoundary === Infinity) return
    episode.boundaryOperation = this.wake(episode, episode.boundary, episode.handlerBoundary)
  }

  /**
   * Resolves `gate` once the episode clock reaches `at`, reading the clock again after every timer
   * wake, so a timer delivered early never ends a wait. At once when `at` already passed.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @param {{resolve: () => void}} gate The wait to end.
   * @param {number} at Episode time.
   * @returns {ConsumerOperation} The operation that owns the timer; retiring it cancels the wake.
   */
  wake(episode, gate, at) {
    const operation = new ConsumerOperation({ now: episode.now })
    ;(async () => {
      while (episode.now() < at) await operation.sleep(at - episode.now())
      gate.resolve()
    })().catch(() => {})
    return operation
  }

  /**
   * The synchronous capture boundary: still-running handlers keep only their explicit choices and
   * unstarted records are released, then every idle established node captures one terminal
   * ticket per partition, and only then is ordinary authority retired and every node disposed,
   * which settles the uncaptured remainder membership-lost. Busy and unopened nodes capture
   * nothing. Every session discarded this way publishes one `session_reset` with reason
   * `membership`. The heartbeat chain is suspended for a poll or fence recovery, so no join can
   * overlap its traffic, and ended for good by every other kind. A node whose capture fails
   * captures nothing; every other node still captures, and every node is retired and disposed.
   * The boundary runs once: an application callback reached from inside it (a logger, an event
   * listener) that closes again joins it instead of capturing a second time. A capture or disposal
   * failure is kept as the raw thrown value, and read and logged only after every node retired, so
   * an error that throws when read cannot stop another node's capture or disposal.
   *
   * It works on the episode's records. Only a record still current in the map captures a final;
   * a detached record is disposed like the others. The data binding of each record that may
   * still own a broker session moves to the episode, including after a reset or refused final.
   * Disposal closes no socket and each connection is then decided once:
   * one carrying a captured final waits for that final; otherwise one a record could not capture
   * is closed now, unless only a detached record holds it while a current record is bound to it.
   * The calls the freeze closes leave the poll view, except the expired calls a poll close
   * captured, and no longer count for a stop.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  freeze(episode) {
    if (episode.frozen || episode.freezing) return
    episode.freezing = true
    const failures = []
    this.captureCloseCoordinator(episode)
    const { records } = episode
    const bindings = []
    for (const node of records) {
      if (!node.loop) continue
      try {
        this.finalizeActive(node)
        node.captured = this.nodes.get(node.id) === node ? node.loop.captureFinal() : null
      } catch (error) {
        node.captured = null
        failures.push(['Share final capture failed', node.id, error])
      }
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
    this.handlers.clear()
    if (episode.kind === 'poll' || episode.kind === 'fence') {
      this.manager.stop()
      this.poll.retain(episode.expired)
    } else {
      this.manager.shutdown()
      this.poll.stop()
    }
    for (const node of records) {
      const binding = node.session?.cleanupBinding
      episode.bindings.set(node, binding ? [binding] : [])
      node.session?.releaseCleanup()
    }
    for (const node of records) {
      const discarded = node.loop !== null && !node.loop.disposed
      try {
        node.loop?.dispose(new KafkaJSNonRetriableError('Share run closed'), {
          reason: 'membership-lost',
          preserve: node.captured,
          closeSockets: false,
        })
      } catch (error) {
        failures.push(['Share node disposal failed', node.id, error])
      }
      node.operation.retire(new KafkaJSNonRetriableError('Share run closed'))
      node.pacing.retire(new KafkaJSNonRetriableError('Share run closed'))
      if (discarded && (node.session.epoch !== 0 || node.dropped > 0))
        this.onSessionReset(node, 'membership')
    }
    this.closeUncaptured(episode)
    this.notifyDrained()
    for (const [message, nodeId, error] of failures)
      this.options.logger.error(message, { nodeId, error: describeThrown(error) })
  }

  /**
   * @private
   * @param {ShareCloseEpisode} episode The frozen episode.
   * @returns {Map<object, object>} Each connection that carries a captured final, with its record.
   */
  finalConnections(episode) {
    const held = new Map()
    for (const node of episode.records)
      if (node.captured) held.set(node.captured.binding.connection, node)
    return held
  }

  /**
   * Closes, at the freeze, the connection of every record that holds a session or a written
   * request and captured no final, so the broker drops that session. A connection that carries a
   * captured final is left to the cleanup, and a detached record never closes a connection a
   * current record is bound to.
   * @private
   * @param {ShareCloseEpisode} episode The frozen episode.
   * @returns {void}
   */
  closeUncaptured(episode) {
    const held = this.finalConnections(episode)
    const current = new Set()
    for (const node of episode.records)
      if (this.nodes.get(node.id) === node)
        for (const { connection } of episode.bindings.get(node)) current.add(connection)
    for (const node of episode.records) {
      if (node.captured) continue
      const detached = this.nodes.get(node.id) !== node
      for (const binding of episode.bindings.get(node)) {
        const { connection } = binding
        if (held.has(connection) || (detached && current.has(connection))) continue
        if (!ShareSession.bindingLost(binding)) episode.closeConnection(connection)
      }
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
        const { offsets } = cohort.ledger
        cohort.ledger.completeRange(offsets, from, offsets.length, 'release')
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
      deadlineAt: Math.min(episode.deadlineAt, episode.finalDeadline),
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
    const close = (connection) => episode.closeConnection(connection)
    for (const node of episode.records) {
      if (!node.captured) continue
      const binding = episode.nodes.get(node.id)?.binding
      episode.claimFinal(node.id)
      const guard = () =>
        binding
          ? episode.refusal(binding)
          : (episode.guard() ?? new KafkaJSNonRetriableError('No captured binding'))
      jobs.push(
        node.loop.finishNode({ closing: { guard, captured: node.captured, operation, close } })
      )
    }
    await Promise.allSettled(jobs)
  }

  /**
   * Closes, once per connection, every data connection of the episode's records whose session
   * has no confirmed final, so no connection to an unfinished share session stays open. A
   * connection that carries a captured final follows only that final's record. A clean stop keeps
   * the connections of confirmed finals and the coordinator's; a disconnect, crash or revoked
   * close closes every captured connection and the coordinator's. Only the exact captured
   * generations that are still current are touched, so a replacement socket is never closed.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  teardown(episode) {
    const dirty = episode.closeSockets || episode.revoked
    const held = this.finalConnections(episode)
    for (const node of episode.records) {
      if (!dirty && node.captured?.confirmed) continue
      for (const binding of episode.bindings.get(node) ?? []) {
        const owner = held.get(binding.connection)
        if (owner && owner !== node) continue
        if (!ShareSession.bindingLost(binding)) episode.closeConnection(binding.connection)
      }
    }
    for (const node of episode.records) node.captured = null
    if (dirty && this.coordinator?.current()) episode.closeConnection(this.coordinator.connection)
    episode.boundaryOperation?.retire()
  }

  /**
   * Completes the episode for its kind: a stop or disconnect ends the run, and so does a fence or
   * poll recovery that a stop or disconnect arrived in. Otherwise a fence resumes the run for the
   * rejoin, restarting the suspended heartbeat chain, and a poll recovery waits for its own
   * rejoin. A crash releases its resources before the supervisor asks for a restart verdict.
   * @private
   * @param {ShareCloseEpisode} episode The run's episode.
   * @returns {void}
   */
  afterClose(episode) {
    this.closed = true
    const { kind } = episode
    const recovery = kind === 'fence' || kind === 'poll'
    if (kind === 'crash' || this.crashed) {
      if (this.stopRequested) this.runner.emit(events.STOP)
      this.finishRun(this.stopRequested)
    } else if (kind === 'stop' || kind === 'disconnect' || (recovery && this.stopRequested)) {
      this.runner.emit(events.STOP)
      this.finishRun()
    } else if (recovery) {
      this.nodes.clear()
      this.detached.clear()
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
   * @param {boolean} [release=true] Whether the client forgets this run as its current one,
   *   including if it has already been cleaned up before a restart verdict.
   * @returns {void}
   */
  finishRun(release = true) {
    if (this.finished) {
      if (release && this.runner.current === this) this.runner.current = null
      return
    }
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
    this.detached.clear()
    this.handlers.clear()
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
    this.options.telemetry?.suspend()
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
    const episode = this.beginClose('poll')
    const captured = episode.expired
    await episode.done.catch(() => {})
    if (this.stopRequested || this.retired || this.crashed) return
    await this.membership.leavePromise
    await this.drained(captured)
    if (this.stopRequested || this.retired || this.crashed) return
    this.membership.rejoinAfterPoll(this.poll, captured)
    this.manager.requestTick()
  }
}

/**
 * @returns {symbol|undefined} The identity of the run whose handler, or async work it started, is
 *   calling; undefined outside a share handler.
 */
const handlerRunIdentity = () => handlerScope.getStore()

/**
 * Runs client work outside any handler's identity: queued acknowledgements, and the FIFO drain,
 * connections, crash supervision and listeners they reach, must never pass as a handler's own call.
 * @template T
 * @param {() => T} work The work to start.
 * @returns {T} What `work` returned.
 */
const outsideHandler = (work) => handlerScope.run(undefined, work)

module.exports = { ShareRun, handlerRunIdentity, outsideHandler }
