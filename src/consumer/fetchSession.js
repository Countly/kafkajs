const { ConsumerOperation } = require('./groupProtocol/interface')

/**
 * One node's run-owned Fetch session. Request identity is independent of wire epoch.
 * The fetcher serializes sends; each response or error consumes its token once.
 */
class FetchSession {
  /** @returns {FetchSession} an initial session @throws Never. */
  constructor() {
    this.sessionId = 0
    this.epoch = 0
    this.partitions = new Map()
    this.usesTopicIds = undefined
    this.seq = 0
    this.closed = false
    this.pending = null
  }

  /**
   * Copies the desired map and computes added, changed and forgotten entries.
   * Full requests replace broker state; building never advances the wire epoch.
   * @param {object[]} requests full desired topic/partition entries
   * @param {object} options snapshot addressing mode
   * @param {Map<string,string>} [options.topicIds] full desired name/id mapping
   * @param {boolean} options.usesTopicIds negotiated version uses topic IDs
   * @returns {{wire:object,token:{session:FetchSession,seq:number}}} fields and identity
   * @throws {Error} retired session or missing ID in ID mode
   */
  build(requests, { topicIds, usesTopicIds }) {
    if (this.closed) throw new Error('Fetch session is closed')
    if (this.usesTopicIds !== undefined && this.usesTopicIds !== usesTopicIds) this.reset(false)
    this.usesTopicIds = usesTopicIds
    const desired = new Map()
    for (const { topic, partitions } of requests) {
      const topicId = usesTopicIds ? topicIds?.get(topic) : undefined
      if (usesTopicIds && topicId === undefined) throw new Error('Fetch topic ID is missing')
      for (const data of partitions) {
        const key = JSON.stringify([topic, data.partition])
        if (desired.has(key)) throw new Error('Duplicate Fetch partition')
        desired.set(key, { topic, topicId, ...data })
      }
    }
    const full = this.epoch === 0
    const changed = []
    const forgotten = []
    for (const [key, old] of this.partitions) {
      const next = desired.get(key)
      if (!full && (!next || (usesTopicIds && old.topicId !== next.topicId))) forgotten.push(old)
    }
    for (const [key, next] of desired) {
      const old = this.partitions.get(key)
      if (
        full ||
        !old ||
        old.fetchOffset !== next.fetchOffset ||
        old.maxBytes !== next.maxBytes ||
        old.currentLeaderEpoch !== next.currentLeaderEpoch ||
        old.topicId !== next.topicId ||
        old.logStartOffset !== next.logStartOffset
      )
        changed.push(next)
    }
    const token = { session: this, seq: ++this.seq }
    const wire = {
      sessionId: this.sessionId,
      sessionEpoch: this.epoch,
      topics: group(changed, false),
      forgottenTopics: group(forgotten, true),
      topicIds: usesTopicIds ? new Map(topicIds) : undefined,
    }
    this.partitions = desired
    this.pending = { token, full }
    return { wire, token }
  }

  /**
   * Tests the current unconsumed request identity without changing state.
   * @param {{session:FetchSession,seq:number}} token request identity
   * @returns {boolean} true only for the latest live request
   * @throws Never.
   */
  current(token) {
    return (
      !this.closed &&
      token?.session === this &&
      token.seq === this.seq &&
      this.pending?.token === token
    )
  }

  /**
   * Accepts Java's full/incremental response table, including delivery with ID zero.
   * Unknown/duplicate partitions invalidate the entire response before delivery.
   * @param {{session:FetchSession,seq:number}} token original request identity
   * @param {object} response normalized broker response with full-map topic names
   * @returns {boolean} whether the response may be processed
   * @throws Never for decoded responses.
   */
  handleResponse(token, response) {
    if (!this.current(token)) return false
    const { full } = this.pending
    this.pending = null
    if (response.errorCode) {
      this.reset(response.errorCode === 70)
      return false
    }
    const seen = new Set()
    let invalid = false
    for (const { topicName, partitions } of response.responses) {
      if (topicName == null) invalid = true
      for (const { partition } of partitions) {
        const key = JSON.stringify([topicName, partition])
        if (!this.partitions.has(key) || seen.has(key)) invalid = true
        seen.add(key)
      }
    }
    const throttled = response.throttleTime > 0 || response.clientSideThrottleTime > 0
    if (full && (seen.size !== this.partitions.size || (!seen.size && throttled))) invalid = true
    if (invalid) {
      this.reset(full)
      return false
    }
    if (response.sessionId === 0) this.reset(true)
    else {
      if (full) this.sessionId = response.sessionId
      this.epoch = full ? 1 : this.epoch === 2147483647 ? 1 : this.epoch + 1
    }
    return true
  }

  /**
   * Fences the next request after any send exception; replica cleanup is caller-owned.
   * @param {{session:FetchSession,seq:number}} token original send identity
   * @returns {void} stale errors have no effect
   * @throws Never.
   */
  handleError(token) {
    if (this.current(token)) this.reset(false)
  }

  /**
   * Resets protocol state and invalidates pending callbacks without reusing seq.
   * @param {boolean} initial discard the broker's unknown session ID
   * @returns {void} next build is full at epoch zero
   * @throws Never.
   */
  reset(initial) {
    if (initial) this.sessionId = 0
    this.epoch = 0
    this.pending = null
    this.seq++
  }

  /**
   * Retires local state synchronously before a best-effort close can await anything.
   * @returns {{sessionId:number}|null} one positive broker session ID, once
   * @throws Never.
   */
  close() {
    if (this.closed) return null
    const result = this.sessionId > 0 ? { sessionId: this.sessionId } : null
    this.closed = true
    this.seq++
    this.pending = null
    this.partitions.clear()
    return result
  }
}

/**
 * Groups a diff while retaining the recorded identity of replaced/deleted topics.
 * @param {object[]} cells partition snapshots
 * @param {boolean} forgotten encode partition numbers and recorded IDs
 * @returns {object[]} ordered topic runs, safe for arbitrary topic names
 * @throws Never for normalized cells.
 */
function group(cells, forgotten) {
  const topics = new Map()
  for (const { topic, topicId, ...data } of cells) {
    const key = JSON.stringify([topic, topicId])
    if (!topics.has(key)) topics.set(key, { topic, topicId, partitions: [] })
    topics.get(key).partitions.push(forgotten ? data.partition : data)
  }
  return [...topics.values()]
}

/**
 * Owns only current node sessions for one run. Closing shares one promise and budget.
 */
class FetchSessions {
  /**
   * @param {object} options run-owned sender and diagnostics
   * @param {Function} options.sendClose sends with original ConsumerOperation
   * @param {number} options.requestTimeout ordinary client request timeout
   * @param {object} options.logger debug logger
   * @returns {FetchSessions} an empty run registry
   * @throws Never for valid internal options.
   */
  constructor({ sendClose, requestTimeout, logger }) {
    this.sendClose = sendClose
    this.requestTimeout = requestTimeout
    this.logger = logger
    this.sessions = new Map()
    this.closing = null
  }

  /**
   * @param {number} nodeId current broker node
   * @returns {FetchSession} one session for this node
   * @throws {Error} registry already closing
   */
  forNode(nodeId) {
    if (this.closing) throw new Error('Fetch sessions are closing')
    if (!this.sessions.has(nodeId)) this.sessions.set(nodeId, new FetchSession())
    return this.sessions.get(nodeId)
  }

  /**
   * @param {number} nodeId a broker node
   * @returns {FetchSession|undefined} the node's session when it holds partitions, so an idle node
   *   never allocates one
   * @throws Never.
   */
  active(nodeId) {
    const session = this.sessions.get(nodeId)
    return session?.partitions.size ? session : undefined
  }

  /**
   * Retires one node's session without a network close, for a broker that cannot serve sessions.
   * @param {number} nodeId a broker node
   * @returns {void}
   * @throws Never.
   */
  release(nodeId) {
    this.sessions.get(nodeId)?.close()
    this.sessions.delete(nodeId)
  }

  /**
   * Retires every session locally and sends nothing; the run is being forced closed and its
   * fetchers may still hold a request. The registry then refuses new sessions and a later
   * `closeAll` has nothing to send.
   * @returns {void}
   * @throws Never.
   */
  retireAll() {
    this.closing ??= Promise.resolve()
    for (const session of this.sessions.values()) session.close()
    this.sessions.clear()
  }

  /**
   * Retires nodes absent from the current broker set; late responses are fenced.
   * @param {number[]} nodeIds current eligible fetcher nodes
   * @returns {void} no historical node/session map
   * @throws Never.
   */
  retain(nodeIds) {
    const current = new Set(nodeIds)
    for (const [nodeId, session] of this.sessions) {
      if (!current.has(nodeId)) {
        session.close()
        this.sessions.delete(nodeId)
      }
    }
  }

  /**
   * Called after fetchers stop, before leave, once per run. Each node closes in parallel.
   * A finite supplied deadline is never renewed; an omitted deadline is capped at 2s.
   * @param {object} [options] captured close context
   * @param {number} [options.deadlineAt] original monotonic close deadline
   * @param {Function} [options.guard] original run refusal, Error or null
   * @returns {Promise<void>} failures are debug-only and all client waits are bounded
   * @throws Never for operational close failures.
   */
  closeAll({ deadlineAt, guard = () => null } = {}) {
    if (this.closing) return this.closing
    const deadline = Number.isFinite(deadlineAt)
      ? deadlineAt
      : performance.now() + Math.min(this.requestTimeout, 2000)
    const entries = [...this.sessions].map(([nodeId, session]) => [nodeId, session.close()])
    this.sessions.clear()
    this.closing = Promise.resolve().then(async () => {
      const operation = new ConsumerOperation({ deadlineAt: deadline, guard })
      try {
        await Promise.all(
          entries.map(async ([nodeId, close]) => {
            if (!close) return
            try {
              await operation.wait(() => {
                operation.check()
                return this.sendClose(
                  nodeId,
                  {
                    sessionId: close.sessionId,
                    sessionEpoch: -1,
                    topics: [],
                    forgottenTopics: [],
                  },
                  operation
                )
              })
            } catch (error) {
              this.logger.debug('Fetch session close failed', { nodeId, error: error.message })
            }
          })
        )
      } finally {
        operation.finish()
      }
    })
    return this.closing
  }
}

module.exports = { FetchSession, FetchSessions }
