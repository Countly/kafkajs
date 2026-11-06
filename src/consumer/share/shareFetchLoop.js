const createRetry = require('../../retry')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { withConsumerOperation } = require('../../admin/deadline')
const {
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
  KafkaJSShareGroupsNotSupportedError,
} = require('../../errors')
const { ShareAckLedger, validateAcknowledgements } = require('./shareAckLedger')
const { ShareRecordWalk } = require('./shareRecordWalk')
const { ShareSession } = require('./shareSession')
const shareCapability = require('./shareCapability')
const {
  sessionAction,
  fetchAction,
  acknowledgementAction,
  transportAction,
} = require('./shareErrorContract')

/** An empty round is followed by at least this long between two fetch writes. */
const EMPTY_ROUND_MS = 50
/** A session-limit reset (133) is paced at least this long, which keeps it under 20 per 5 s. */
const SESSION_LIMIT_DELAY_MS = 250
/** Floor for every other paced error, so a zero retry configuration cannot spin. */
const ERROR_DELAY_MS = 50
const NOOP_LOGGER = { error() {}, warn() {}, info() {}, debug() {} }
const DUPLICATE = Symbol('duplicate-partition')
const IMPLEMENTED = {
  [apiKeys.ShareFetch]: requests.ShareFetch,
  [apiKeys.ShareAcknowledge]: requests.ShareAcknowledge,
}

/**
 * @param {string} topicId Topic UUID.
 * @param {number} partition Partition index.
 * @returns {string} The map key of a partition; names never index it, so `__proto__` is harmless.
 */
const keyOf = (topicId, partition) => `${topicId}:${partition}`

/**
 * One acquisition of one partition, as this loop retains it. A partition may have several while
 * older terminal results wait for their turn; they are never merged into one request.
 * @typedef {object} Cohort
 * @property {string} topic Topic name, used only in outcomes.
 * @property {string} topicId Topic UUID.
 * @property {number} partition Partition index.
 * @property {object|symbol} token The acquisition's opaque identity, kept as the ledger's token.
 * @property {ShareAckLedger} ledger The acquisition's compact acknowledgement state.
 * @property {object} walk The classified walk of the answer, `walk.deliver` first.
 * @property {boolean} pendingHandlers True until every delivery was handed to a handler or dropped.
 * @property {boolean} dispatched True once the handler callback was given this acquisition.
 */

/**
 * One request's captured identity, applied once by one answer.
 * @typedef {object} Exchange
 * @property {symbol} open The session open the request was queued under.
 * @property {78|79} apiKey Request API.
 * @property {'fetch'|'flush'|'renew'|'final'} kind What the request is for.
 * @property {Array<{ledger: ShareAckLedger, ticket: object, row: Cohort}>} prepared Tickets sent.
 * @property {object|null} descriptor Frozen `{token, apiKey, epoch, topics, forgottenTopicsData}`.
 * @property {object|null} fields The wire fields.
 * @property {number|undefined} sentAt Monotonic time of the last guard check that saw a frame length.
 * @property {Error|null} refused The last refusal the guard returned.
 * @property {boolean} stale True once a reset or disposal took the request over.
 * @property {(frameLength?: number) => Error|null} guard Original-authority pre-write check.
 * @property {{connection: object}|null} binding The data socket the request was bound to.
 * @property {boolean} [settled] Final only: true once the transport call itself ended, whatever
 *   its owner's wait did, so only the socket of a request still unanswered is ever closed.
 */

/**
 * The fence of metadata work that one session open started. It is the node operation's own fence
 * plus the open itself, so a provider that asks `check`, `closed` or `guard` after a session reset
 * installs nothing, even though the node operation survives the reset.
 */
class OpenFence {
  /**
   * @param {ShareSession} session The node's session.
   * @param {symbol} open The open identity the work started under.
   */
  constructor(session, open) {
    this.session = session
    this.guard = () => session.refusal(open)
  }

  /** @returns {boolean} Whether the open or the node operation can no longer authorize work. */
  get closed() {
    return this.guard() !== null
  }

  /**
   * @returns {void}
   * @throws {Error} The refusal of the open or of the node operation.
   */
  check() {
    const refused = this.guard()
    if (refused) throw refused
  }

  /**
   * @param {(error: Error) => void} cancel Local cleanup, run when the node operation retires.
   * @returns {() => void} Unregisters it without running it.
   */
  register(cancel) {
    return this.session.operation.register(cancel)
  }
}

/**
 * Plans, sends and settles the share traffic of one leader node. Request order is the node
 * session's FIFO; this class decides what each request carries, applies each answer once and keeps
 * the node's acquisitions. It starts no timer of its own: every wait is a wait of the node
 * operation.
 *
 * One round is one fetching request followed by every handler of what it delivered, and no second
 * fetching request starts before that ends, so nothing is prefetched. The FIFO ends before the
 * handlers, so a renewal or flush issued by a handler can use it while the round is still open.
 *
 * Forget model: with `P` the session's previous model, `T` every partition listed in a request,
 * `W` the wanted set and `H` the partitions still holding undelivered work, a fetch forgets
 * `{p in P∪T | p not in W and p not in H}` and the answer applies `(P∪T) − forgotten`.
 */
class ShareFetchLoop {
  /**
   * @param {object} options Node-local dependencies and validated consumer options.
   * @param {ShareSession} options.session The node's session; owns the model, epoch and FIFO. The
   *   loop installs itself as its reset observer, so a session serves one loop.
   * @param {string} options.groupId Share group id.
   * @param {string} options.memberId Stable member id of this run.
   * @param {number} [options.maxWaitTimeInMs=500] Long-poll limit; zero is valid.
   * @param {number} options.minBytes Minimum bytes; zero is valid.
   * @param {number} options.maxBytes Positive maximum bytes.
   * @param {number} options.maxRecords Positive per-node record limit, also the batch size.
   * @param {'record_limit'|'batch_optimized'} options.acquireMode Acquisition mode.
   * @param {object} [options.broker] The node's Broker; required to transmit.
   * @param {object} [options.cluster] Topic names, leader hints and guarded metadata refresh.
   * @param {object} [options.membership] Share membership; judges a membership error (code 25).
   * @param {() => number} [options.now] Monotonic clock in milliseconds.
   * @param {object} [options.logger] Logger with `error`, `warn` and `debug`.
   * @param {() => Array<{topicId: string, partition: number, topic?: string}>} [options.wanted]
   *   The node's current wanted partitions, read inside the queue so a stale request reads nothing.
   * @param {(results: object[], cohort: Cohort) => void} [options.onResults] Compact ledger
   *   results, each once, with the acquisition they belong to.
   * @param {(move: {topic: string, topicId: string, partition: number, leaderId: number, reason: 'leader-moved'}) => void} [options.onMove]
   *   A partition now has another leader and the old node's acquisitions of it are retired.
   * @param {(error: Error) => void} [options.onFatal] A terminal failure the run supervisor owns.
   * @param {(oldOpen: symbol, reason: string, cause?: Error) => void} [options.onReset] Called once
   *   per session reset, after every old acquisition was invalidated.
   * @param {(cohorts: Cohort[]) => Promise<void>} [options.dispatch] Hands the acquisitions of one
   *   answer to the handlers; resolves when every delivery was handled, dropped or released.
   * @param {{membershipToken?: symbol, acquire?: Function}} [options.authority] The captured run
   *   authority: the issued membership token and the factory of acquisition tokens.
   * @param {object} [options.retry] Retry options whose backoff schedule paces errors.
   * @param {boolean} [options.checkCrcs] Passed to the record decoder when defined.
   * @param {number} [options.maxDecompressedBatchBytes] Passed to the record decoder when defined.
   */
  constructor({
    session,
    groupId,
    memberId,
    maxWaitTimeInMs = 500,
    minBytes,
    maxBytes,
    maxRecords,
    acquireMode,
    broker,
    cluster,
    membership,
    now = () => performance.now(),
    logger = NOOP_LOGGER,
    wanted = () => [],
    onResults = () => {},
    onMove = () => {},
    onFatal = () => {},
    onReset = () => {},
    dispatch = async () => {},
    authority = {},
    retry,
    checkCrcs,
    maxDecompressedBatchBytes,
  }) {
    this.session = session
    this.operation = session.operation
    this.groupId = groupId
    this.memberId = memberId
    this.maxWaitTimeInMs = maxWaitTimeInMs
    this.minBytes = minBytes
    this.maxBytes = maxBytes
    this.maxRecords = maxRecords
    this.acquireMode = acquireMode
    this.broker = broker
    this.cluster = cluster
    this.membership = membership
    this.now = now
    this.logger = logger
    this.wanted = wanted
    this.onResults = onResults
    this.onMove = onMove
    this.onFatal = onFatal
    this.onReset = onReset
    this.dispatch = dispatch
    this.authority = authority
    this.decode = { checkCrcs, maxDecompressedBatchBytes }
    this.schedule = createRetry.backoff(retry)
    this.retryTime = null
    this.cohorts = new Map()
    this.exchanges = new Set()
    this.collected = null
    this.running = null
    this.finishing = false
    this.finishPromise = null
    this.disposed = false
    session.onReset = (oldOpen, reason, cause) => this.onSessionReset(oldOpen, reason, cause)
  }

  /**
   * Builds a positive-limit fetch. Every added or acknowledgement-only partition joins the forget
   * candidate union, so a revoked partition is forgotten in the very frame that carries its last
   * acknowledgement. The model changes only when the answer is applied.
   * @param {Array<{topicId: string, partition: number}>} wanted The node's wanted partitions.
   * @param {Array<{ledger: ShareAckLedger, ticket: object}>} prepared Terminal tickets prepared
   *   inside the queue, at most one per partition.
   * @param {Set<string>} holding Keys that still hold undelivered work after ticket preparation.
   * @returns {object} The ShareFetch fields: an opening request lists every wanted partition and
   *   carries no acknowledgements; an established one lists only new and acknowledged partitions.
   * @throws {KafkaJSNonRetriableError} For zero limits, acknowledgements on an opening request,
   *   renewal tickets, or a ticket group `ShareAckLedger.requestTopics` refuses.
   */
  buildFetch(wanted, prepared, holding) {
    if (this.maxBytes <= 0 || this.maxRecords <= 0)
      throw new KafkaJSNonRetriableError('Fetching limits must be positive')
    if (this.session.epoch === 0 && prepared.length)
      throw new KafkaJSNonRetriableError('An opening share fetch cannot carry acknowledgements')
    for (const { ticket } of prepared) {
      if (ticket.isRenewAck) throw new KafkaJSNonRetriableError('Renewal requires ShareAcknowledge')
      validateAcknowledgements(ticket.acknowledgementBatches, { isRenewAck: false })
    }
    const topics = new Map()
    const listed = new Map()
    const wantedKeys = new Set()
    const add = (topicId, partitionIndex, acknowledgementBatches) => {
      const key = keyOf(topicId, partitionIndex)
      if (!topics.has(topicId)) topics.set(topicId, { topicId, partitions: new Map() })
      const partitions = topics.get(topicId).partitions
      if (!partitions.has(partitionIndex))
        partitions.set(partitionIndex, { partitionIndex, acknowledgementBatches })
      else if (acknowledgementBatches.length)
        partitions.get(partitionIndex).acknowledgementBatches = acknowledgementBatches
      listed.set(key, { topicId, partition: partitionIndex })
    }
    for (const topic of ShareAckLedger.requestTopics(prepared)) {
      for (const partition of topic.partitions)
        add(topic.topicId, partition.partitionIndex, partition.acknowledgementBatches)
    }
    for (const row of wanted) {
      const key = keyOf(row.topicId, row.partition)
      wantedKeys.add(key)
      if (!this.session.model.has(key)) add(row.topicId, row.partition, [])
    }
    const candidates = new Map(this.session.model)
    for (const [key, row] of listed) candidates.set(key, row)
    const forgotten = new Map()
    for (const [key, row] of candidates) {
      if (wantedKeys.has(key) || holding.has(key)) continue
      if (!forgotten.has(row.topicId))
        forgotten.set(row.topicId, { topicId: row.topicId, partitions: [] })
      forgotten.get(row.topicId).partitions.push(row.partition)
    }
    const wireTopics = []
    for (const row of topics.values())
      wireTopics.push({ topicId: row.topicId, partitions: [...row.partitions.values()] })
    return {
      groupId: this.groupId,
      memberId: this.memberId,
      shareSessionEpoch: this.session.epoch,
      maxWaitMs: this.maxWaitTimeInMs,
      minBytes: this.minBytes,
      maxBytes: this.maxBytes,
      maxRecords: this.maxRecords,
      batchSize: this.maxRecords,
      shareAcquireMode: this.acquireMode === 'record_limit' ? 1 : 0,
      isRenewAck: false,
      topics: wireTopics,
      forgottenTopicsData: [...forgotten.values()],
    }
  }

  /**
   * Builds a flush, a renewal or an established final; never a zero-limit fetch.
   * @param {Array<{ledger: ShareAckLedger, ticket: object}>} prepared Single-kind tickets.
   * @param {boolean} [final=false] Use epoch -1, terminal tickets only.
   * @returns {object|null} The ShareAcknowledge fields (no limits, no forgotten list), or null for
   *   a final on an unopened session, which sends nothing.
   * @throws {KafkaJSNonRetriableError} For an acknowledgement on an unopened session or a renewing final.
   */
  buildAcknowledge(prepared, final = false) {
    if (this.session.epoch === 0) {
      if (final) return null
      throw new KafkaJSNonRetriableError('Share acknowledgements require an established session')
    }
    const isRenewAck = prepared.length > 0 && prepared[0].ticket.isRenewAck
    if (final && isRenewAck) throw new KafkaJSNonRetriableError('A final cannot renew records')
    const topics = ShareAckLedger.requestTopics(prepared)
    for (const topic of topics) {
      for (const partition of topic.partitions)
        validateAcknowledgements(partition.acknowledgementBatches, { isRenewAck })
    }
    return {
      groupId: this.groupId,
      memberId: this.memberId,
      shareSessionEpoch: final ? -1 : this.session.epoch,
      isRenewAck,
      topics,
    }
  }

  /**
   * Completes one request-and-handlers round; concurrent callers share it, which is what keeps
   * the loop from prefetching. A renewal may use the session queue while handlers run.
   * @param {(open: symbol) => Promise<object>} fetch Lazy exchange, entered through the session
   *   queue; resolves with `{reply, sentAt, answeredAt, empty, backoff, reset, closed, effects}`
   *   after the acknowledgements it carried were settled.
   * @param {(reply: Cohort[]) => Promise<void>} handlers Resolves after every delivered or dropped record.
   * @returns {Promise<object>} The exchange result, after the handlers, the answer's metadata
   *   effects and the owned pacing delay. A reset or final answer skips the handlers. When a data
   *   session reset or its socket was lost while the handlers ran, the old open's effects are
   *   discarded and the round still ends with its pacing delay, so the next round reopens.
   * @throws {Error} The original request, handler or operation failure; a node whose operation was
   *   retired, a closed or retired session, or an open that ended before the handlers started
   *   rejects with its "no longer current" refusal.
   */
  round(fetch, handlers) {
    if (this.running) return this.running
    const token = this.session.token
    const task = async () => {
      const result = await this.session.enqueue(fetch)
      if (!result.reset && !result.closed) {
        const refused = this.session.refusal(token)
        if (refused) throw refused
        await handlers(result.reply)
        const afterHandlers = this.session.refusal(token)
        if (!afterHandlers) await this.runEffects(result, token)
        else if (!this.reopenable(token)) throw afterHandlers
      }
      await this.pace(result)
      return result
    }
    this.running = task().finally(() => {
      this.running = null
    })
    this.running.catch(() => {})
    return this.running
  }

  /**
   * Tells a recoverable end of a round's open from a terminal one. A reset replaced the open, or
   * only its data socket was lost, while the node operation and the session are intact: the next
   * exchange reopens or resets under backoff. Anything else (a retired operation, a closed or
   * retired session) is a terminal failure and is never recovered here.
   * @private
   * @param {symbol} open The open identity the round captured.
   * @returns {boolean} Whether the node can simply continue with a fresh round.
   */
  reopenable(open) {
    const { session } = this
    if (session.closed || session.operation.guard() !== null) return false
    return open !== session.token || session.connectionLost(open)
  }

  /**
   * Runs one fetching round for this node: the exchange, then the handlers via `dispatch`.
   * @returns {Promise<object>} The round result.
   * @throws {KafkaJSNonRetriableError} When the node is finishing or disposed.
   * @throws {Error} What `round` throws.
   */
  poll() {
    if (this.finishing) return Promise.reject(new KafkaJSNonRetriableError('Share node is closing'))
    return this.round(
      (open) => this.exchange({ apiKey: apiKeys.ShareFetch, kind: 'fetch' }, open),
      (cohorts) => this.dispatchCohorts(cohorts)
    )
  }

  /**
   * Reads pacing from the actual last pre-write time, not from request creation or decode time.
   * @param {number} sentAt Last transmission-guard monotonic time.
   * @param {number} answeredAt Monotonic arrival time of the answer.
   * @param {boolean} empty True only for no records, no gaps and no error work.
   * @returns {number} Delay before another fetching frame, at most 50 milliseconds.
   */
  emptyDelay(sentAt, answeredAt, empty) {
    return empty ? Math.max(0, EMPTY_ROUND_MS - (answeredAt - sentAt)) : 0
  }

  /**
   * @private
   * @param {number} minimum Floor for this error.
   * @returns {number} The next delay of the retry backoff schedule, at least `minimum`.
   */
  errorDelay(minimum) {
    this.retryTime =
      this.retryTime === null ? this.schedule.initial() : this.schedule.next(this.retryTime)
    return Math.max(minimum, this.retryTime)
  }

  /**
   * Waits the delay the round owes, on the node operation's own timer. The clock is read again
   * after each wait, so a timer that fires early never authorizes traffic.
   * @private
   * @param {object} result The exchange result.
   * @returns {Promise<void>}
   * @throws {Error} The operation's refusal if it retires meanwhile.
   */
  async pace(result) {
    if (!result.backoff) this.retryTime = null
    let delay = result.backoff
      ? this.errorDelay(result.minDelay)
      : this.emptyDelay(result.sentAt, result.answeredAt, result.empty)
    while (delay > 0) {
      await this.operation.sleep(delay)
      delay = result.backoff ? 0 : this.emptyDelay(result.sentAt, this.now(), result.empty)
    }
  }

  /**
   * Hands the acquisitions of one answer to the handlers, then marks each current one ready.
   * @private
   * @param {Cohort[]} cohorts Acquisitions the answer created.
   * @returns {Promise<void>}
   * @throws {Error} The dispatch failure; the acquisitions stay held, so nothing is forgotten early.
   */
  async dispatchCohorts(cohorts) {
    if (!cohorts.length || this.finishing) return
    for (const row of cohorts) row.dispatched = true
    await this.dispatch(cohorts)
    for (const row of cohorts) if (this.isLive(row)) row.pendingHandlers = false
    this.prune()
  }

  /**
   * One request, entered through the session queue; it never enqueues itself. Reads the wanted
   * set and the acquisitions here, so a stale queued request prepares no ticket, then acquires the
   * connection under the original guard, builds, sends once and applies the answer once.
   * @param {object} options What to send.
   * @param {78|79} options.apiKey ShareFetch or ShareAcknowledge.
   * @param {'fetch'|'flush'|'renew'|'final'} options.kind Request purpose.
   * @param {Array<{topicId: string, partition: number}>} [options.wanted] Overrides the wanted provider.
   * @param {Cohort[]} [options.cohorts] Restricts a flush to these acquisitions.
   * @param {Array<{cohort: Cohort, offsets: string[]}>} [options.renew] Held offsets to renew.
   * @param {symbol} open The open identity captured when the request was queued.
   * @returns {Promise<object>} `{reply, sentAt, answeredAt, empty, backoff, minDelay, reset, closed,
   *   results, lockDuration, effects, error}`. A transport failure or a reset answer resolves with
   *   `reset` and a backoff; a stale, retired or terminal one rejects.
   * @throws {Error} The refusal of a retired or stale session, or a terminal failure.
   */
  async exchange(options, open) {
    const { apiKey, kind } = options
    const session = this.session
    if (kind === 'fetch' && this.finishing)
      throw new KafkaJSNonRetriableError('Share node is closing')
    const refused = session.refusal(open)
    if (refused) {
      if (!session.connectionLost(open)) throw refused
      if (kind === 'final')
        return this.resultOf(undefined, this.now(), { closed: true, error: refused })
      return this.loseConnection(undefined, refused)
    }
    if (apiKey === apiKeys.ShareAcknowledge && session.epoch === 0) {
      if (kind === 'final') return this.idleResult()
      throw new KafkaJSNonRetriableError('Share acknowledgements require an established session')
    }
    const exchange = this.newExchange(options, open)
    this.exchanges.add(exchange)
    try {
      if (!session.binding) await this.establish(exchange)
      exchange.binding = session.binding
      this.build(exchange, options)
    } catch (error) {
      return this.failExchange(exchange, error)
    }
    if (!exchange.fields) {
      this.exchanges.delete(exchange)
      return this.idleResult()
    }
    let response
    try {
      response = await this.operation.wait(() => this.send(exchange))
    } catch (error) {
      return this.failExchange(exchange, error)
    }
    return this.complete(exchange, response)
  }

  /**
   * @private
   * @param {{apiKey: 78|79, kind: string}} options Request options.
   * @param {symbol} open Captured open identity.
   * @returns {Exchange} A request record with its original-authority guard.
   */
  newExchange({ apiKey, kind }, open) {
    const exchange = {
      open,
      apiKey,
      kind,
      prepared: [],
      descriptor: null,
      fields: null,
      sentAt: undefined,
      refused: null,
      stale: false,
      binding: null,
    }
    exchange.guard = (frameLength) => {
      const refused = this.session.refusal(open)
      if (refused) {
        exchange.refused = refused
        return refused
      }
      if (frameLength !== undefined) exchange.sentAt = this.now()
      return null
    }
    return exchange
  }

  /**
   * @private
   * @returns {object} A result for a request that sent nothing; it still paces like an empty round.
   */
  idleResult() {
    const at = this.now()
    return this.resultOf(undefined, at, { empty: true })
  }

  /**
   * @private
   * @param {Exchange|undefined} exchange The request, when one was sent.
   * @param {number} answeredAt Monotonic time of the answer.
   * @param {object} [extra] Fields that override the defaults.
   * @returns {object} The exchange result with every field present.
   */
  resultOf(exchange, answeredAt, extra = {}) {
    return {
      reply: [],
      sentAt: exchange ? exchange.sentAt : answeredAt,
      answeredAt,
      empty: false,
      backoff: false,
      minDelay: ERROR_DELAY_MS,
      reset: false,
      closed: false,
      results: [],
      lockDuration: undefined,
      effects: [],
      error: undefined,
      ...extra,
    }
  }

  /**
   * Opens the data connection for an unbound session under the original guard, rechecking the
   * original pool and authority after every await, then binds only the originating open to the
   * selected socket and its generation. An established session never comes here, so it cannot
   * silently reconnect and keep its epoch.
   * @private
   * @param {Exchange} exchange The request.
   * @returns {Promise<void>}
   * @throws {KafkaJSShareGroupsNotSupportedError} When the broker cannot run the share APIs.
   * @throws {Error} The original refusal, or a connection failure.
   */
  async establish(exchange) {
    const { broker, operation } = this
    const { open, apiKey, guard } = exchange
    const pool = broker.connectionPool
    const recheck = () => {
      const refused = guard() || (broker.connectionPool !== pool && this.replaced(pool))
      if (refused) throw refused
    }
    if (!broker.isConnected()) {
      await operation.wait(() => broker.connect({ guard }))
      recheck()
    }
    this.assertCapable(broker)
    const connection = await operation.wait(() =>
      pool.getConnectionByRequest({ request: { apiKey } }, { guard })
    )
    recheck()
    this.session.bind(open, { broker, pool, connection, generation: pool.generationFor(apiKey) })
  }

  /**
   * @private
   * @param {object} pool The pool the request started on.
   * @returns {Error} A refusal for a broker whose pool was replaced while connecting.
   */
  replaced(pool) {
    return new KafkaJSNonRetriableError(`Broker pool of ${pool.brokerAddress} was replaced`)
  }

  /**
   * @private
   * @param {object} broker The connected broker.
   * @returns {void}
   * @throws {KafkaJSShareGroupsNotSupportedError} When an API range or the finalized feature refuses.
   */
  assertCapable(broker) {
    const verdict = shareCapability({
      versions: broker.versions,
      finalizedFeatures: broker.finalizedFeatures,
      finalizedFeaturesEpoch: broker.finalizedFeaturesEpoch,
      implemented: IMPLEMENTED,
      required: [apiKeys.ShareFetch, apiKeys.ShareAcknowledge],
    })
    if (verdict.ok) return
    throw new KafkaJSShareGroupsNotSupportedError(
      `Share groups refused: ${verdict.reason}`,
      verdict
    )
  }

  /**
   * Prepares the tickets and builds the frame inside the queue. A failure settles every ticket
   * already prepared as not sent before it propagates. Leaves `exchange.fields` null when there is
   * nothing to send.
   * @private
   * @param {Exchange} exchange The request.
   * @param {object} options Request options, see {@link ShareFetchLoop#exchange}.
   * @returns {void}
   * @throws {Error} The builder or ticket failure.
   */
  build(exchange, options) {
    const { apiKey, kind, prepared } = exchange
    const { session } = this
    try {
      let fields = null
      if (kind === 'fetch') {
        const source = options.wanted ?? this.wanted
        const rows = typeof source === 'function' ? source() : source
        if (session.epoch !== 0) prepared.push(...this.prepareTerminal())
        fields = this.buildFetch(rows, prepared, this.holding())
        const nothing = !rows.length && !prepared.length && !fields.forgottenTopicsData.length
        if (nothing) fields = null
      } else {
        if (kind === 'final') this.releaseUndelivered()
        if (kind === 'renew') prepared.push(...this.prepareRenewals(options.renew))
        else prepared.push(...this.prepareTerminal(options.cohorts))
        if (prepared.length || kind === 'final')
          fields = this.buildAcknowledge(prepared, kind === 'final')
      }
      if (!fields) return
      exchange.fields = fields
      exchange.descriptor = Object.freeze({
        token: exchange.open,
        apiKey,
        epoch: fields.shareSessionEpoch,
        topics: fields.topics,
        forgottenTopicsData: fields.forgottenTopicsData,
      })
    } catch (error) {
      this.settleAll(prepared, 'not-sent', error)
      prepared.length = 0
      throw error
    }
  }

  /**
   * Prepares at most one terminal ticket per partition, from the oldest acquisition that has
   * decisions; a stale-token preparation settles through `onResults` and sends nothing.
   * @private
   * @param {Cohort[]} [only] Restricts preparation to these acquisitions.
   * @returns {Array<{ledger: ShareAckLedger, ticket: object, row: Cohort}>} Prepared tickets.
   */
  prepareTerminal(only) {
    const allowed = only ? new Set(only) : null
    const prepared = []
    for (const rows of this.cohorts.values()) {
      for (const row of rows) {
        if (allowed && !allowed.has(row)) continue
        const { ticket, results } = row.ledger.prepare({ token: row.token })
        this.emit(results, row)
        if (ticket) {
          prepared.push({ ledger: row.ledger, ticket, row })
          break
        }
      }
    }
    return prepared
  }

  /**
   * @private
   * @param {Array<{cohort: Cohort, offsets: string[]}>} [renew] Held offsets per acquisition.
   * @returns {Array<{ledger: ShareAckLedger, ticket: object, row: Cohort}>} Prepared renewal tickets.
   * @throws {KafkaJSNonRetriableError} For an acquisition this node no longer holds, or what the
   *   ledger refuses (a renewal already pending, a non-held offset).
   */
  prepareRenewals(renew = []) {
    const prepared = []
    try {
      for (const { cohort, offsets } of renew) {
        if (!this.isLive(cohort))
          throw new KafkaJSNonRetriableError('Renewal targets a retired acquisition')
        const { ticket, results } = cohort.ledger.prepare({
          token: cohort.token,
          renewOffsets: offsets,
        })
        this.emit(results, cohort)
        if (ticket) prepared.push({ ledger: cohort.ledger, ticket, row: cohort })
      }
    } catch (error) {
      this.settleAll(prepared, 'not-sent', error)
      throw error
    }
    return prepared
  }

  /**
   * @private
   * @returns {Set<string>} Keys of partitions with an acquisition that still holds undelivered
   *   work. Never derived from `ledger.size`, which counts decided and sent entries too.
   */
  holding() {
    const keys = new Set()
    for (const [key, rows] of this.cohorts)
      if (rows.some((row) => row.pendingHandlers)) keys.add(key)
    return keys
  }

  /**
   * @private
   * @param {Cohort} row An acquisition.
   * @returns {boolean} Whether this node still retains it.
   */
  isLive(row) {
    return this.cohorts.get(keyOf(row.topicId, row.partition))?.includes(row) === true
  }

  /**
   * Sends only through the Broker's guarded share methods, inside the node operation's scope, so
   * the network request queue registers its cancellation there: retiring the node removes a
   * request that is still queued (throttled or behind the in-flight limit) with its timer. The
   * guard returns the original refusal and, when it sees a frame length, records the transmission
   * time; it never supplies current tokens or changes the epoch or the model.
   * @private
   * @param {Exchange} exchange The request.
   * @returns {Promise<object>} The decoded answer.
   */
  send(exchange) {
    const { guard, fields } = exchange
    return withConsumerOperation(this.operation, () => {
      if (exchange.apiKey === apiKeys.ShareAcknowledge)
        return this.broker.shareAcknowledge(fields, { guard })
      const { checkCrcs, maxDecompressedBatchBytes } = this.decode
      return this.broker.shareFetch(
        {
          ...fields,
          ...(checkCrcs !== undefined && { checkCrcs }),
          ...(maxDecompressedBatchBytes !== undefined && { maxDecompressedBatchBytes }),
        },
        { guard }
      )
    })
  }

  /**
   * Classifies a request that threw. A retired or stale session discards it without a reset. A
   * request the guard saw written, or whose bound socket was lost, is an unknown outcome: the
   * session resets and its acquisitions settle session-lost, never accepted or resent. A request
   * that never reached the wire settles its tickets not sent.
   * @private
   * @param {Exchange} exchange The request.
   * @param {Error} error What the build or the transport threw.
   * @returns {object} A paced reset result for a transport loss or a retriable pre-write failure.
   * @throws {Error} For retirement, a stale session or any non-retriable failure.
   */
  failExchange(exchange, error) {
    this.exchanges.delete(exchange)
    const { session } = this
    if (
      exchange.stale ||
      session.operation.guard() ||
      session.closed ||
      session.token !== exchange.open
    )
      throw error
    const written = exchange.sentAt !== undefined
    if (exchange.kind === 'final') {
      this.settleAll(exchange.prepared, written ? 'session-lost' : 'not-sent', error)
      return this.resultOf(exchange, this.now(), { closed: true, error })
    }
    if (written || session.connectionLost(exchange.open))
      return this.loseConnection(exchange, error)
    this.settleAll(exchange.prepared, 'not-sent', error)
    if (error !== exchange.refused && error.retriable === true) {
      this.logger.warn('Share request failed before it was written', { error: error.message })
      return this.resultOf(exchange, this.now(), { backoff: true, error })
    }
    throw error
  }

  /**
   * @private
   * @param {Exchange|undefined} exchange The request that found the loss, if any.
   * @param {Error} error The transport failure or the binding refusal.
   * @returns {object} A reset result; the session's reset callback already settled every
   *   acquisition session-lost.
   */
  loseConnection(exchange, error) {
    const policy = transportAction(error)
    this.logger.warn('Share session lost its connection', { error: error.message })
    this.session.reset(policy.reason, error)
    return this.resultOf(exchange, this.now(), { reset: true, backoff: true, error })
  }

  /**
   * Applies an answer once, in this order: the session (epoch, model, reset policy), then the
   * acknowledgements the request carried, then the records of a fetch. A stale answer throws
   * before any of it, so an old answer cannot touch a replacement.
   * @private
   * @param {Exchange} exchange The request.
   * @param {object} response The decoded answer.
   * @returns {object} The exchange result.
   * @throws {Error} The refusal of a stale answer, or a terminal failure after its settlement.
   */
  complete(exchange, response) {
    const answeredAt = this.now()
    if (!this.exchanges.delete(exchange) || exchange.stale)
      throw new KafkaJSNonRetriableError('Share exchange is no longer current')
    const policy = this.session.apply(exchange.descriptor, response)
    const result = this.resultOf(exchange, answeredAt)
    if (policy.action === 'reset') {
      this.logger.warn('Share session was reset by the broker', { error: policy.error.message })
      return {
        ...result,
        reset: true,
        backoff: policy.backoff,
        minDelay: response.errorCode === 133 ? SESSION_LIMIT_DELAY_MS : ERROR_DELAY_MS,
        error: policy.error,
      }
    }
    if (policy.action === 'closed') return this.completeFinal(exchange, response, policy, result)
    if (policy.action === 'ok') return this.completeOk(exchange, response, result)
    this.settleAll(exchange.prepared, 'rejected-by-broker', policy.error)
    result.error = policy.error
    if (policy.action === 'fatal') {
      this.notify(this.onFatal, policy.error)
      throw policy.error
    }
    if (policy.action === 'fence') this.fence(policy.error)
    else this.logger.error('Share request failed', { error: policy.error.message })
    result.backoff = true
    return result
  }

  /**
   * @private
   * @param {Exchange} exchange The request.
   * @param {object} response A successful top-level answer.
   * @param {object} result The result being built.
   * @returns {object} The result with settled acknowledgements and adopted acquisitions.
   * @throws {Error} A fatal partition failure, after every sibling acquisition was registered.
   */
  completeOk(exchange, response, result) {
    const settled = this.settlePrepared(exchange, response)
    result.results = settled
    const lock = response.acquisitionLockTimeoutMs
    result.lockDuration = lock > 0 ? lock : undefined
    if (exchange.apiKey === apiKeys.ShareFetch) this.applyPartitions(exchange, response, result)
    else result.empty = settled.every((outcome) => !outcome.error)
    return result
  }

  /**
   * Ends the node on a final's answer: the original tickets receive only their outcome, and the
   * session stays closed with no reset, replacement open or backoff.
   * @private
   * @param {Exchange} exchange The final request.
   * @param {object} response The decoded answer.
   * @param {{error: Error|null}} policy The closed policy.
   * @param {object} result The result being built.
   * @returns {object} The terminal result.
   */
  completeFinal(exchange, response, policy, result) {
    result.closed = true
    if (policy.error === null) {
      result.results = this.settlePrepared(exchange, response)
      return result
    }
    const { errorCode } = response
    const lost = errorCode === 122 || errorCode === 123
    this.settleAll(exchange.prepared, lost ? 'session-lost' : 'rejected-by-broker', policy.error)
    result.error = policy.error
    if (!exchange.captured) this.deliverVerdict(errorCode)
    return result
  }

  /**
   * Hands a final's membership or fatal verdict to its owners; a failed final changes nothing else.
   * @private
   * @param {number} errorCode The final's top-level code.
   * @returns {void}
   */
  deliverVerdict(errorCode) {
    const verdict = sessionAction(apiKeys.ShareAcknowledge, errorCode)
    if (verdict.action === 'fence') this.fence(verdict.error)
    else if (verdict.action === 'fatal') this.notify(this.onFatal, verdict.error)
  }

  /**
   * Passes a membership error to the share membership under the token issued to this node.
   * @private
   * @param {Error} error The original protocol error.
   * @returns {void}
   */
  fence(error) {
    if (this.membership) this.membership.onMembershipError(error, this.authority.membershipToken)
  }

  /**
   * Settles every prepared ticket against its own partition of the answer, whether or not the
   * partition was forgotten meanwhile: ShareFetch reads `acknowledgeErrorCode`, ShareAcknowledge
   * `errorCode`, and a partition the answer omitted or repeated fails its ticket as invalid.
   * A consumed ticket is never rebuilt or resent.
   * @param {Exchange} exchange The request that carried the tickets.
   * @param {object} response The decoded answer.
   * @returns {object[]} Every compact result, also emitted once to `onResults`.
   */
  settlePrepared(exchange, response) {
    if (!exchange.prepared.length) return []
    const byKey = this.indexResponse(response)
    const fetching = exchange.apiKey === apiKeys.ShareFetch
    const all = []
    for (const { ledger, ticket, row } of exchange.prepared) {
      const entry = byKey.get(keyOf(row.topicId, row.partition))
      let outcome
      if (entry === undefined || entry === DUPLICATE) {
        const cause =
          entry === DUPLICATE
            ? new KafkaJSNonRetriableError('Acknowledgement response repeated a partition')
            : undefined
        outcome = { present: false, cause }
      } else {
        outcome = acknowledgementAction(fetching ? entry.acknowledgeErrorCode : entry.errorCode)
        const lock = response.acquisitionLockTimeoutMs
        if (outcome.result === 'ok' && ticket.isRenewAck)
          outcome = {
            ...outcome,
            sentAt: exchange.sentAt,
            lockDuration: lock > 0 ? lock : undefined,
          }
      }
      const results = ledger.settle(ticket, { token: row.token, ...outcome })
      this.emit(results, row)
      all.push(...results)
    }
    this.prune()
    return all
  }

  /**
   * @private
   * @param {object} response The decoded answer.
   * @returns {Map<string, object|symbol>} Partitions by topic id and index; a repeated identity
   *   maps to a marker, so the last row cannot hide the failure.
   */
  indexResponse(response) {
    const byKey = new Map()
    for (const topic of response.responses || []) {
      for (const partition of topic.partitions || []) {
        const key = keyOf(topic.topicId, partition.partitionIndex)
        byKey.set(key, byKey.has(key) ? DUPLICATE : partition)
      }
    }
    return byKey
  }

  /**
   * Applies the partitions of a successful fetch answer, in two steps. First, synchronously: every
   * successful partition becomes an acquisition (walked once, one ledger), a leader move retires
   * the old node's acquisitions of that partition, and a fatal failure is only recorded. Then
   * metadata work is returned as `result.effects` for the round to run, so a slow refresh never
   * delays a sibling's handlers or holds the queue.
   * @param {Exchange} exchange The fetch request.
   * @param {object} response The decoded answer.
   * @param {object} result The result being built; receives `reply`, `effects`, `empty`, `backoff`.
   * @returns {void}
   * @throws {Error} A fatal partition failure, after the siblings were registered and the
   *   undelivered ones released; `onFatal` is told first.
   */
  applyPartitions(exchange, response, result) {
    const byKey = this.indexResponse(response)
    const lockDuration = response.acquisitionLockTimeoutMs
    const open = exchange.open
    let fatal = null
    let work = result.results.some((outcome) => outcome.error)
    for (const topic of response.responses || []) {
      for (const partition of topic.partitions || []) {
        const index = partition.partitionIndex
        if (byKey.get(keyOf(topic.topicId, index)) !== partition) {
          this.logger.error('Share fetch answer repeated a partition', { partition: index })
          work = true
          continue
        }
        const { action, error } = fetchAction(partition.errorCode)
        if (action === 'ok') {
          const acquired =
            (partition.acquiredRecords?.length ?? 0) + (partition.messages?.length ?? 0)
          if (acquired > 0 || partition.corruption) {
            work = true
            result.reply.push(this.adopt(exchange, topic.topicId, partition, lockDuration))
          }
          if (partition.acknowledgeErrorCode) work = true
          continue
        }
        work = true
        result.backoff = true
        if (action === 'fatal') fatal = fatal || error
        else if (action === 'move') this.planMove(result, open, topic.topicId, partition, response)
        else if (action === 'refresh') this.planRefresh(result, open, topic.topicId, index)
        else
          this.logger.error('Share fetch partition failed', {
            partition: index,
            error: error.message,
          })
      }
    }
    result.empty = !work
    if (fatal) {
      for (const row of result.reply) this.releaseUndelivered(row)
      result.reply = []
      this.notify(this.onFatal, fatal)
      throw fatal
    }
  }

  /**
   * Walks one partition's answer exactly once and registers its acquisition: one ledger, built
   * from the answer's own walk and the pre-write time, with no per-record copy.
   * @private
   * @param {Exchange} exchange The fetch request.
   * @param {string} topicId Topic UUID.
   * @param {object} partition The decoded partition.
   * @param {number} lockDuration The answer's acquisition lock timeout.
   * @returns {Cohort} The registered acquisition.
   * @throws {KafkaJSNonRetriableError} For a malformed acquisition or lock.
   */
  adopt(exchange, topicId, partition, lockDuration) {
    const index = partition.partitionIndex
    const topic = this.cluster?.topicName(topicId) ?? topicId
    const walk = ShareRecordWalk.walk(
      {
        acquiredRecords: partition.acquiredRecords ?? [],
        messages: partition.messages ?? [],
        corruption: partition.corruption,
      },
      this.logger
    )
    const token = this.authority.acquire
      ? this.authority.acquire({
          topic,
          topicId,
          partition: index,
          open: exchange.open,
          generation: this.session.binding?.generation,
        })
      : Symbol('share-acquisition')
    const ledger = new ShareAckLedger({
      topic,
      topicId,
      partition: index,
      token,
      sentAt: exchange.sentAt,
      lockDuration,
      walk,
    })
    const row = {
      topic,
      topicId,
      partition: index,
      token,
      ledger,
      walk,
      pendingHandlers: walk.deliver.records.length > 0,
      dispatched: false,
    }
    const key = keyOf(topicId, index)
    if (!this.cohorts.has(key)) this.cohorts.set(key, [])
    this.cohorts.get(key).push(row)
    return row
  }

  /**
   * A leader move: the old node's acquisitions of the partition are retired now, and the routing
   * work (hint, refresh, notification) is left for the round.
   * @private
   * @param {object} result The result being built.
   * @param {symbol} open The request's open identity.
   * @param {string} topicId Topic UUID.
   * @param {object} partition The decoded partition with its `currentLeader`.
   * @param {object} response The answer, for its node endpoints.
   * @returns {void}
   */
  planMove(result, open, topicId, partition, response) {
    const index = partition.partitionIndex
    this.retireKey(keyOf(topicId, index), 'session-lost')
    const leader = partition.currentLeader
    const fence = this.fenceFor(result, open)
    result.effects.push(() => this.relocate(fence, topicId, index, leader, response.nodeEndpoints))
  }

  /**
   * @private
   * @param {object} result The result being built.
   * @param {symbol} open The request's open identity.
   * @param {string} topicId Topic UUID the answer named.
   * @param {number} index Partition index.
   * @returns {void}
   */
  planRefresh(result, open, topicId, index) {
    const fence = this.fenceFor(result, open)
    result.effects.push(() => this.refreshTopic(fence, topicId))
  }

  /**
   * @private
   * @param {object} result The result being built.
   * @param {symbol} open The request's open identity.
   * @returns {OpenFence} The one fence every metadata effect of this answer carries.
   */
  fenceFor(result, open) {
    result.fence ??= new OpenFence(this.session, open)
    return result.fence
  }

  /**
   * Applies a usable leader hint, else refreshes metadata, then reports the move. The fence of
   * the original open goes into the provider, so a reset that happens while it awaits keeps it
   * from installing anything, and every await is followed by a check of that same fence.
   * @private
   * @param {OpenFence} fence The original open's fence.
   * @param {string} topicId Topic UUID.
   * @param {number} index Partition index.
   * @param {{leaderId: number, leaderEpoch: number}|undefined} leader The answer's hint.
   * @param {object[]|undefined} nodeEndpoints The answer's endpoints.
   * @returns {Promise<void>}
   * @throws {Error} The refusal of the open or of the node operation.
   */
  async relocate(fence, topicId, index, leader, nodeEndpoints) {
    const { cluster, operation } = this
    const topic = cluster.topicName(topicId)
    const known =
      leader &&
      leader.leaderId >= 0 &&
      ((nodeEndpoints || []).some(({ nodeId }) => nodeId === leader.leaderId) ||
        cluster.hasNode(leader.leaderId))
    let applied = false
    if (known) {
      applied = await operation.wait(() =>
        cluster.applyLeaderHint(
          {
            topic,
            partition: index,
            leaderId: leader.leaderId,
            leaderEpoch: leader.leaderEpoch,
            nodeEndpoints: nodeEndpoints?.length ? nodeEndpoints : undefined,
          },
          { fence }
        )
      )
      fence.check()
    }
    if (!applied) {
      await this.refreshMetadata(fence)
      fence.check()
    }
    const leaderId = cluster.cachedLeader(topic, index)
    this.notify(this.onMove, { topic, topicId, partition: index, leaderId, reason: 'leader-moved' })
  }

  /**
   * Reloads metadata after a retriable partition error. A topic that now has another id was
   * recreated: acquisitions under the old id are retired and never reused under the new one.
   * @private
   * @param {OpenFence} fence The original open's fence.
   * @param {string} topicId The topic id the broker answered about.
   * @returns {Promise<void>}
   * @throws {Error} The refusal of the open or of the node operation.
   */
  async refreshTopic(fence, topicId) {
    const name = this.cluster.topicName(topicId)
    await this.refreshMetadata(fence)
    fence.check()
    const current = name === undefined ? undefined : this.cluster.topicId(name)
    if (current !== undefined && current !== topicId) {
      for (const key of [...this.cohorts.keys()])
        if (key.startsWith(`${topicId}:`)) this.retireKey(key, 'session-lost')
    }
  }

  /**
   * @private
   * @param {OpenFence} fence The original open's fence.
   * @returns {Promise<void>} One metadata refresh fenced by the original open and bound to the
   *   node operation; a failure is logged, because the round's backoff already paces the retry.
   * @throws {Error} The refusal of the open or of the node operation.
   */
  async refreshMetadata(fence) {
    try {
      await this.operation.wait(() =>
        this.cluster.refreshMetadata({ fence, allowLeaderless: true })
      )
    } catch (error) {
      fence.check()
      this.logger.error('Share metadata refresh failed', { error: error.message })
    }
  }

  /**
   * Runs the metadata work an answer asked for, after its handlers and before the next fetch.
   * @private
   * @param {object} result The exchange result.
   * @param {symbol} open The round's open identity.
   * @returns {Promise<void>}
   * @throws {Error} The original refusal if the node retired meanwhile.
   */
  async runEffects(result, open) {
    const effects = result.effects || []
    for (const effect of effects) await effect()
    if (effects.length) this.recheck(open)
  }

  /**
   * @private
   * @param {symbol} open Original open identity.
   * @returns {void}
   * @throws {Error} The current refusal of that open.
   */
  recheck(open) {
    const refused = this.session.refusal(open)
    if (refused) throw refused
  }

  /**
   * Flushes terminal decisions or renews held offsets through one queued ShareAcknowledge. A
   * renewal consumes a normal wire epoch and settles before any later terminal preparation,
   * because the FIFO keeps its order even when its caller does not await it. An answer that owes
   * a backoff (code 133, an unknown error, a delayed reset, a lost connection) is paced here, still
   * inside the FIFO, so the next request of the node cannot be written before the delay ends; the
   * caller, or its failure, is released after that delay.
   * @param {object} [options] What to send.
   * @param {Cohort[]} [options.cohorts] Restricts a flush to these acquisitions; all by default.
   * @param {Array<{cohort: Cohort, offsets: string[]}>} [options.renew] Renews these held offsets.
   * @returns {Promise<object>} The exchange result; for a renewal `lockDuration` is the new
   *   duration, or undefined when the answer kept the previous one.
   * @throws {KafkaJSShareAcknowledgementError} A failed renewal, from the ledger's compact result.
   *   A retirement returns that same error even when it first rejects the queue's outer wait;
   *   a renewal retired before preparation keeps the original queue refusal.
   * @throws {Error} Any other failure of the request, or the node operation's refusal while it waits.
   */
  acknowledge({ cohorts, renew } = {}) {
    const collected = []
    const request = this.session.enqueue(async (open) => {
      this.collected = collected
      try {
        const result = await this.exchange(
          { apiKey: apiKeys.ShareAcknowledge, kind: renew ? 'renew' : 'flush', cohorts, renew },
          open
        )
        if (result.backoff) await this.pace(result)
        return result
      } finally {
        if (this.collected === collected) this.collected = null
      }
    })
    const failure = () =>
      renew && collected.find((outcome) => outcome.type === 'renew' && outcome.error)?.error
    const outcome = request.then(
      (result) => {
        const error = failure()
        if (error) throw error
        return result
      },
      (error) => {
        throw failure() || error
      }
    )
    outcome.catch(() => {})
    return outcome
  }

  /**
   * Ends the node after its last wanted partition left or its run closes. New fetching starts
   * stop at once and never-handled acquisitions are released. An established idle node then sends
   * one final (epoch -1) behind any pending renewal; an unopened node sends nothing; a captured
   * closing sends only its preserved terminal subset under its own predicate. Repeated calls share
   * the first.
   * @param {object} [options] Closing context.
   * @param {{guard: (frameLength?: number) => Error|null, captured: object, operation: import('../groupProtocol/interface').ConsumerOperation}} [options.closing]
   *   A captured close: `captured` comes from {@link ShareFetchLoop#captureFinal}, `guard` is the
   *   immutable closing predicate of the closing episode and `operation` is the episode's own
   *   operation, which is required: the final's wait and its queue registration belong to it, so
   *   its expiry settles an unanswered final, and a predicate alone cannot wake a waiting request.
   *   A close without an operation settles its captured tickets not sent and rejects.
   * @returns {Promise<void>} Resolves when the node is disposed.
   * @throws {KafkaJSInvariantViolation} When a captured close has no operation.
   * @throws {Error} The final's failure; the node is disposed regardless.
   */
  finishNode({ closing } = {}) {
    if (this.finishPromise) return this.finishPromise
    this.finishing = true
    this.finishPromise = this.finish(closing)
    this.finishPromise.catch(() => {})
    return this.finishPromise
  }

  /**
   * @private
   * @param {{guard: Function, captured: object, operation: object}} [closing] A captured close.
   * @returns {Promise<void>}
   * @throws {KafkaJSInvariantViolation} When a captured close has no operation.
   */
  async finish(closing) {
    try {
      if (closing) await this.sendFinal(closing)
      else if (this.session.epoch !== 0 && !this.session.closed)
        await this.session.enqueue((open) =>
          this.exchange({ apiKey: apiKeys.ShareAcknowledge, kind: 'final' }, open)
        )
    } finally {
      this.dispose()
    }
  }

  /**
   * Captures, synchronously, the terminal subset an idle established node may still close with:
   * prepares one eligible ticket per partition and the final's frame. A busy node, an unopened
   * node and a retired session yield null, so a forced close never queues behind a busy request.
   * @returns {{binding: object, prepared: object[], fields: object, descriptor: object}|null} The
   *   exact tickets, the established binding and the frame, or null.
   */
  captureFinal() {
    const { session } = this
    if (
      !session.binding ||
      session.epoch === 0 ||
      session.closed ||
      session.busy ||
      this.exchanges.size ||
      session.refusal(session.token)
    )
      return null
    this.releaseUndelivered()
    const prepared = this.prepareTerminal()
    let fields
    try {
      fields = this.buildAcknowledge(prepared, true)
    } catch (error) {
      this.settleAll(prepared, 'not-sent', error)
      return null
    }
    const descriptor = Object.freeze({
      token: session.token,
      apiKey: apiKeys.ShareAcknowledge,
      epoch: -1,
      topics: fields.topics,
    })
    return { binding: session.binding, prepared, fields, descriptor }
  }

  /**
   * Writes a captured final once, after ordinary retirement, under the closing predicate and the
   * captured binding alone. The final's wait belongs to the closing operation, so the episode's
   * expiry settles an unanswered final session-lost, and the final tears down its own exchange and
   * transport whether or not the node was disposed first. The predicate is checked again after the
   * answer, and an answer that arrives after it refused is not published as acceptance.
   * @private
   * @param {{guard: (frameLength?: number) => Error|null, captured: object, operation?: import('../groupProtocol/interface').ConsumerOperation}} closing
   *   The closing context; its operation is required.
   * @returns {Promise<void>}
   * @throws {KafkaJSInvariantViolation} When there is no operation; the captured tickets are first
   *   settled not sent.
   */
  async sendFinal({ guard, captured, operation }) {
    if (!operation) {
      const error = new KafkaJSInvariantViolation('A captured close requires its closing operation')
      this.settleAll(captured.prepared, 'not-sent', error)
      throw error
    }
    await this.writeFinal(operation, guard, captured)
  }

  /**
   * Sends the final and settles its tickets exactly once. An unanswered final, written and not
   * answered when the wait ends, is removed from the active exchanges and its exact data socket is
   * closed here, independently of the node's disposed flag; an unwritten or already ended one only
   * leaves the active exchanges.
   * @private
   * @param {import('../groupProtocol/interface').ConsumerOperation} operation Owns the final's
   *   wait and its queue registration.
   * @param {(frameLength?: number) => Error|null} guard The closing predicate.
   * @param {{binding: object, prepared: object[], fields: object, descriptor: object}} captured
   *   What {@link ShareFetchLoop#captureFinal} froze.
   * @returns {Promise<void>}
   */
  async writeFinal(operation, guard, captured) {
    const { binding, prepared, fields, descriptor } = captured
    const exchange = {
      apiKey: 79,
      kind: 'final',
      prepared,
      captured: true,
      sentAt: undefined,
      stale: false,
      binding,
      settled: false,
    }
    const check = (frameLength) => {
      const refused =
        guard(frameLength) ||
        (ShareSession.bindingLost(binding) &&
          new KafkaJSNonRetriableError('Share session connection was lost'))
      if (refused) return refused
      if (frameLength !== undefined) exchange.sentAt = this.now()
      return null
    }
    const early = check()
    if (early) {
      this.settleAll(prepared, 'not-sent', early)
      return
    }
    this.exchanges.add(exchange)
    let response
    try {
      response = await operation.wait(() =>
        withConsumerOperation(operation, () => {
          const call = binding.broker.shareAcknowledge(fields, { guard: check })
          const done = () => {
            exchange.settled = true
          }
          call.then(done, done)
          return call
        })
      )
    } catch (error) {
      const written = exchange.sentAt !== undefined
      this.exchanges.delete(exchange)
      if (written && !exchange.settled && !ShareSession.bindingLost(binding))
        this.closeSocket(binding.connection)
      this.settleAll(prepared, written ? 'session-lost' : 'not-sent', error)
      return
    }
    this.exchanges.delete(exchange)
    const late = guard()
    if (late) {
      this.settleAll(prepared, 'session-lost', late)
      return
    }
    const policy = this.session.applyFinal(descriptor, response)
    this.completeFinal(exchange, response, policy, this.resultOf(exchange, this.now()))
  }

  /**
   * Releases acquisitions whose deliveries were never handed to a handler, so a close or a fatal
   * answer settles them as released instead of stranding them.
   * @private
   * @param {Cohort} [row] One acquisition; every acquisition this node holds when omitted.
   * @returns {void}
   */
  releaseUndelivered(row) {
    if (!row) {
      for (const rows of this.cohorts.values())
        for (const each of rows) this.releaseUndelivered(each)
      return
    }
    if (!row.pendingHandlers || row.dispatched) return
    const { records } = row.walk.deliver
    row.ledger.complete(
      records.map((record) => record.offset),
      'release'
    )
    row.pendingHandlers = false
  }

  /**
   * Disposes the node once: revokes admissions, settles every acquisition, retires the session
   * (queued requests reject, the operation's waits, timers and still-queued network requests are
   * removed) and closes the data socket of every request this node still tracks as written and
   * unanswered, but only while that socket is still the one it was written on. A replacement
   * socket or the membership socket is never touched. A captured final that is written after
   * this call is not tracked here: {@link ShareFetchLoop#finishNode} tears it down itself.
   * @param {Error} [cause] Why the node ends.
   * @param {object} [options] Disposal options.
   * @param {'session-lost'|'membership-lost'|'not-sent'} [options.reason='session-lost'] The
   *   outcome of acquisitions that are not settled.
   * @param {{prepared: object[]}} [options.preserve] A captured final whose exact tickets survive.
   * @param {number} [options.drainTimeout=0] Milliseconds the data socket may drain before it closes.
   * @returns {void} Idempotent and synchronous; a socket close is observed in the background.
   */
  dispose(cause, { reason = 'session-lost', preserve = null, drainTimeout = 0 } = {}) {
    if (this.disposed) return
    this.disposed = true
    this.finishing = true
    const unanswered = new Set()
    for (const { sentAt, binding, settled } of this.exchanges)
      if (sentAt !== undefined && !settled && binding && !ShareSession.bindingLost(binding))
        unanswered.add(binding.connection)
    this.invalidateAll(reason, preserve)
    this.session.retire(cause || new KafkaJSNonRetriableError('Share node was disposed'))
    for (const connection of unanswered) this.closeSocket(connection, drainTimeout)
  }

  /**
   * @private
   * @param {{disconnect: (options: {drainTimeout: number}) => Promise<*>}} connection A data socket.
   * @param {number} [drainTimeout=0] Milliseconds it may drain before it closes.
   * @returns {void} Closes it in the background; a failure is logged, never thrown.
   */
  closeSocket(connection, drainTimeout = 0) {
    connection.disconnect({ drainTimeout }).catch((error) => {
      this.logger.error('Share data connection close failed', { error: error.message })
    })
  }

  /**
   * Runs when the session resets: every acquisition of the old open settles session-lost and the
   * run supervisor is told once. A running handler is merely stale; nothing here stops it.
   * @private
   * @param {symbol} oldOpen The open that ended.
   * @param {string} reason Reset reason.
   * @param {Error} [cause] Original failure.
   * @returns {void}
   */
  onSessionReset(oldOpen, reason, cause) {
    this.invalidateAll('session-lost')
    this.notify(this.onReset, oldOpen, reason, cause)
  }

  /**
   * @private
   * @param {'session-lost'|'membership-lost'|'not-sent'} reason Outcome for unsettled work.
   * @param {{prepared: object[]}|null} [preserve] A captured final whose tickets stay settleable.
   * @returns {void}
   */
  invalidateAll(reason, preserve = null) {
    for (const exchange of this.exchanges) exchange.stale = true
    this.exchanges.clear()
    const rows = [...this.cohorts.values()].flat()
    this.cohorts.clear()
    for (const row of rows) {
      const kept = preserve?.prepared.find((entry) => entry.ledger === row.ledger)
      this.emit(row.ledger.invalidate(reason, kept ? kept.ticket : null), row)
    }
  }

  /**
   * @private
   * @param {string} key A partition key.
   * @param {'session-lost'|'membership-lost'|'not-sent'} reason Outcome for its acquisitions.
   * @returns {void} Retires and forgets every acquisition this node holds for that partition.
   */
  retireKey(key, reason) {
    const rows = this.cohorts.get(key) || []
    this.cohorts.delete(key)
    for (const row of rows) this.emit(row.ledger.invalidate(reason), row)
  }

  /**
   * Drops acquisitions with nothing left to hand over or settle, so an idle node retains none.
   * @private
   * @returns {void}
   */
  prune() {
    for (const [key, rows] of this.cohorts) {
      if (rows.every((row) => row.pendingHandlers || row.ledger.size > 0)) continue
      const kept = rows.filter((row) => row.pendingHandlers || row.ledger.size > 0)
      if (kept.length) this.cohorts.set(key, kept)
      else this.cohorts.delete(key)
    }
  }

  /**
   * Settles every ticket of a request with one failure outcome.
   * @private
   * @param {Array<{ledger: ShareAckLedger, ticket: object, row: Cohort}>} prepared Tickets.
   * @param {'not-sent'|'session-lost'|'rejected-by-broker'} result The failure.
   * @param {Error} cause Original failure.
   * @returns {void}
   */
  settleAll(prepared, result, cause) {
    for (const { ledger, ticket, row } of prepared) {
      this.emit(ledger.settle(ticket, { token: row.token, result, cause }), row)
    }
    this.prune()
  }

  /**
   * Gives each compact result to `onResults` once and to an active renewal's collector.
   * @private
   * @param {object[]} results Ledger results.
   * @param {Cohort} row Their acquisition.
   * @returns {void}
   */
  emit(results, row) {
    if (!results.length) return
    if (this.collected) this.collected.push(...results)
    this.notify(this.onResults, results, row)
  }

  /**
   * Runs a callback of the run supervisor so its failure cannot corrupt this node's state.
   * @private
   * @param {Function} callback The callback.
   * @param {...*} args Its arguments.
   * @returns {void}
   */
  notify(callback, ...args) {
    try {
      callback(...args)
    } catch (error) {
      this.logger.error('Share node observer failed', { error: error.message })
    }
  }
}

module.exports = { ShareFetchLoop }
