const { KafkaJSNonRetriableError } = require('../../errors')
const { sessionAction } = require('./shareErrorContract')

const MAX_EPOCH = 2147483647
const RESET_REASONS = { 122: 'session-not-found', 123: 'invalid-epoch' }

/**
 * Serializes one node's ShareFetch and ShareAcknowledge requests and owns the node's wire epoch,
 * its opaque open identity and its partition model.
 *
 * Epochs are wire counters and repeat; the open `token` is a fresh symbol per open and never
 * repeats, so equal epochs or offsets never give an old request authority over a replacement.
 * The queue holds lazy work: nothing is prepared before a callback is entered, and the epoch is
 * read when a request is built, never reserved when it is queued.
 */
class ShareSession {
  /**
   * @param {object} options Session dependencies.
   * @param {import('../groupProtocol/interface').ConsumerOperation} options.operation The node's
   *   operation. It owns every wait of this session, and its guard carries the original run and
   *   membership identity; this session retires it only on {@link ShareSession#retire}.
   * @param {(oldToken: symbol, reason: string, cause?: Error) => void} [options.onReset]
   *   Synchronous: called once per reset, after the old open is invalid and before any
   *   replacement work can use the new one.
   */
  constructor({ operation, onReset = () => {} }) {
    this.operation = operation
    this.onReset = onReset
    this.epoch = 0
    this.token = Symbol('share-open')
    this.model = new Map()
    this.queue = []
    this.active = false
    this.closed = false
    this.resetStreak = 0
    this.binding = null
  }

  /** @returns {boolean} Whether a request runs or waits; a captured close skips a busy node. */
  get busy() {
    return this.active || this.queue.length > 0
  }

  /**
   * Queues a lazy request builder. No ledger ticket or transport promise exists before entry.
   * @param {(token: symbol) => Promise<*>|*} work Receives the open identity captured now.
   * @returns {Promise<*>} The callback result or its original failure; a rejection nobody awaits
   *   is still observed here.
   * @throws {Error} Through the returned promise: the refusal of a retired operation, or a
   *   "no longer current" error when the session reset, closed or retired before entry.
   */
  enqueue(work) {
    const token = this.token
    const result = new Promise((resolve, reject) => {
      this.queue.push({ work, token, resolve, reject })
      this.drain()
    })
    result.catch(() => {})
    return result
  }

  /**
   * Runs exactly one queued callback at a time; a failed callback cannot block the entries behind it.
   * @private
   * @returns {void}
   */
  drain() {
    if (this.active) return
    const entry = this.queue.shift()
    if (!entry) return
    if (this.closed || entry.token !== this.token) {
      entry.reject(new KafkaJSNonRetriableError('Share session is no longer current'))
      this.drain()
      return
    }
    this.active = true
    this.operation
      .wait(() => entry.work(entry.token))
      .then(entry.resolve, entry.reject)
      .finally(() => {
        this.active = false
        this.drain()
      })
  }

  /**
   * @param {{broker: object, pool: object, connection: object, generation: number}} binding A binding.
   * @returns {boolean} Whether its data socket is gone: the broker replaced its pool, the
   *   connection closed, or the selected generation moved. Compares numbers and references only.
   */
  static bindingLost(binding) {
    return (
      binding.broker.connectionPool !== binding.pool ||
      !binding.connection.isConnected() ||
      binding.pool.generationFor(78) !== binding.generation
    )
  }

  /**
   * Reads the current refusal. Connection loss is a refusal before any generation counter moves.
   * It never connects, resets or adopts newer authority.
   * @param {symbol} token Captured open identity.
   * @returns {Error|null} The operation's refusal, a "no longer current" error, a "connection was
   *   lost" error, or null.
   */
  refusal(token) {
    const refused = this.operation.guard()
    if (refused) return refused
    if (this.closed || token !== this.token)
      return new KafkaJSNonRetriableError('Share session is no longer current')
    if (this.binding && ShareSession.bindingLost(this.binding))
      return new KafkaJSNonRetriableError('Share session connection was lost')
    return null
  }

  /**
   * @param {symbol} token Captured open identity.
   * @returns {boolean} True when the open is current and only its bound data socket is gone, so
   *   the caller resets the session instead of treating the refusal as retirement.
   */
  connectionLost(token) {
    return (
      this.binding !== null &&
      token === this.token &&
      !this.closed &&
      this.operation.guard() === null &&
      ShareSession.bindingLost(this.binding)
    )
  }

  /**
   * Binds only the originating open to its established API 78/79 socket.
   * @param {symbol} token Open identity captured before connecting.
   * @param {{broker: object, pool: object, connection: object, generation: number}} binding The
   *   broker, its pool, the selected connection and that connection's generation.
   * @returns {void}
   * @throws {Error} The current refusal, or when the session is already bound to another socket.
   */
  bind(token, binding) {
    const error = this.refusal(token)
    if (error) throw error
    if (this.binding && this.binding.connection !== binding.connection)
      throw new KafkaJSNonRetriableError('Share session connection was replaced')
    this.binding = binding
  }

  /**
   * Applies a current top-level answer exactly once: advances or wraps the epoch, updates the
   * model, or resets on a session error. Acknowledgements and records are the caller's job.
   * A final (epoch -1) answer is terminal and checked before any reset policy, so it never reopens.
   * @param {{token: symbol, apiKey: 78|79, epoch: number, topics: object[], forgottenTopicsData?: object[]}} request
   *   The descriptor captured when the request was built.
   * @param {{errorCode: number}} response Decoded top-level answer.
   * @returns {{action: string, backoff: boolean, error: Error|null}} The classified policy. A
   *   reset runs `onReset` synchronously, and the first consecutive 122/123 is immediate while
   *   every later reset and every 133 asks for backoff.
   * @throws {Error} The refusal of a stale answer; callers discard it before any other effect.
   */
  apply(request, response) {
    const refused = this.refusal(request.token)
    if (refused) throw refused
    if (request.epoch === -1) return this.applyFinal(request, response)
    const policy = sessionAction(request.apiKey, response.errorCode)
    if (policy.action === 'reset') {
      const backoff = response.errorCode === 133 || this.resetStreak > 0
      this.resetStreak += 1
      this.reset(RESET_REASONS[response.errorCode] || 'session-limit', policy.error)
      return { ...policy, backoff }
    }
    for (const topic of request.topics) {
      for (const partition of topic.partitions) {
        const key = `${topic.topicId}:${partition.partitionIndex}`
        this.model.set(key, { topicId: topic.topicId, partition: partition.partitionIndex })
      }
    }
    for (const topic of request.forgottenTopicsData || []) {
      for (const partition of topic.partitions) this.model.delete(`${topic.topicId}:${partition}`)
    }
    this.epoch = request.epoch === MAX_EPOCH ? 1 : request.epoch + 1
    if (response.errorCode === 0) this.resetStreak = 0
    return policy
  }

  /**
   * Ends the session on a final answer. It judges the answer by the final's own code but never
   * checks ordinary authority, so a captured closing request, written after ordinary retirement,
   * ends the session the same way.
   * @param {{apiKey: 78|79}} request The final's descriptor.
   * @param {{errorCode: number}} response Decoded top-level answer.
   * @returns {{action: 'closed', backoff: false, error: Error|null}} The original failure, if any.
   *   No reset, replacement open or backoff follows, and the model is cleared for good.
   */
  applyFinal(request, response) {
    const policy = sessionAction(request.apiKey, response.errorCode)
    this.model.clear()
    this.binding = null
    this.closed = true
    return { action: 'closed', backoff: false, error: policy.error }
  }

  /**
   * Retires the old open before minting a replacement, so an old request, answer or ticket cannot
   * act on the new one. Queued old work is rejected here, synchronously, before `onReset` can
   * enqueue fresh work; the active callback finishes on its own. The node operation stays usable
   * for reopening.
   * @param {'session-not-found'|'invalid-epoch'|'session-limit'|'connection'|'leader-moved'|'membership'} reason
   *   Why the session reset.
   * @param {Error} [cause] Original broker or transport failure.
   * @returns {void}
   */
  reset(reason, cause) {
    const oldToken = this.token
    this.token = Symbol('share-open')
    this.epoch = 0
    this.model.clear()
    this.binding = null
    const error = cause || new KafkaJSNonRetriableError('Share session was reset')
    for (const entry of this.queue.splice(0)) entry.reject(error)
    this.onReset(oldToken, reason, cause)
  }

  /**
   * Retires the session for good: rejects all queued work and retires the owned operation. It
   * sends no final and closes no shared broker; the active wait settles through the operation.
   * @param {Error} error Original retirement cause.
   * @returns {void} Idempotent.
   */
  retire(error) {
    this.closed = true
    this.model.clear()
    this.binding = null
    this.operation.retire(error)
    for (const entry of this.queue.splice(0)) entry.reject(error)
  }
}

module.exports = { ShareSession }
