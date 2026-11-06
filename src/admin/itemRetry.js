const { KafkaJSError, KafkaJSInvariantViolation } = require('../errors')
const { createErrorFromCode } = require('../protocol/error')
const { retrierOf, currentDeadline, checked } = require('./deadline')

/** One new item-result call; successful and terminal cells leave its retry set permanently. */
class ItemRetry {
  /**
   * Copies unique caller cells in request order. Legacy aggregate methods keep AggregateOutcome.
   * @param {object} options
   * @param {object[]} options.items validated caller cells
   * @param {(item: object) => string} options.keyOf canonical identity
   * @param {object} options.retry unchanged retry settings
   * @param {(row: object, item: object) => boolean} options.retryable family/route numeric classifier; may throw call-level failures
   * @param {(error: Error) => boolean} options.callFailure family capability/validation/call-level classifier
   * @param {(message: string|null) => string|null} options.sanitize family diagnostic sanitizer
   * @returns {ItemRetry} one call-local result holder
   * @throws {Error} duplicate keys are an internal contract violation
   */
  constructor({ items, keyOf, retry, retryable, callFailure, sanitize }) {
    this.items = items.slice()
    this.keyOf = keyOf
    this.retry = retry
    this.retryable = retryable
    this.callFailure = callFailure
    this.sanitize = sanitize
    this.last = new Map()
    this.pending = new Map(this.items.map((item) => [keyOf(item), item]))
    if (this.pending.size !== this.items.length) throw new Error('Duplicate item retry keys')
    this.results = new Map()
    this.deadline = currentDeadline()
    this.unown = this.deadline?.own(() => this.dispose())
  }

  /** Clears call-local holders on completion or deadline, without touching a transport.
   * @returns {void}
   */
  dispose() {
    this.unown?.()
    this.items = []
    this.pending.clear()
    this.results.clear()
    this.last.clear()
  }

  /**
   * Saves a broker or internal local-failure row once, retaining its typed error.
   * Code -1 plus an Error is reserved for local unresolved routing/RPC failure; wire -1 without
   * that Error still becomes the protocol UNKNOWN_SERVER_ERROR.
   * @param {object} item original requested cell
   * @param {object} row correlated broker row, with numeric errorCode
   * @returns {Error|null} retriable child to request another attempt, or null
   * @throws {Error} invocation refusal before publication
   */
  accept(item, row) {
    this.deadline?.check()
    const error =
      row.errorCode === 0
        ? null
        : row.errorCode === -1 && row.error instanceof Error
          ? row.error
          : createErrorFromCode(row.errorCode)
    const result = { ...item, ...row, error, errorMessage: this.sanitize(row.errorMessage ?? null) }
    if (error && this.retryable(row, item)) {
      this.last.set(this.keyOf(item), result)
      return new KafkaJSError(error, { retriable: true, cause: error })
    }
    const key = this.keyOf(item)
    this.results.set(key, result)
    this.pending.delete(key)
    return null
  }

  /**
   * Runs only pending cells. Route invalidation stays in send(), owned by each family.
   * A malformed/missing/foreign/duplicate row is unresolved locally, never inferred success.
   * @param {(items: object[]) => Promise<object[]>} send complete per-item rows from this attempt
   * @returns {Promise<object[]>} results in caller order, errorCode -1 for unresolved local failure
   * @throws {Error} API deadline always rejects the whole call, preserving unknown mutation outcome
   */
  async run(send) {
    let outstanding
    let localFailure = null
    try {
      await retrierOf(this.retry)(async (bail) => {
        this.deadline?.check()
        const attempted = new Map(this.pending)
        let rows
        try {
          rows = checked(await send([...attempted.values()]))
          localFailure = null
        } catch (error) {
          if (this.callFailure(error)) return bail(error)
          localFailure = error
          throw error
        }
        if (!Array.isArray(rows)) throw new KafkaJSInvariantViolation('Missing item results')
        const byKey = new Map()
        for (const row of rows) {
          const key = this.keyOf(row)
          if (!attempted.has(key) || byKey.has(key) || !Number.isInteger(row.errorCode)) {
            throw new KafkaJSInvariantViolation('Invalid item result identity')
          }
          byKey.set(key, row)
        }
        if (byKey.size !== attempted.size)
          throw new KafkaJSInvariantViolation('Missing item result')
        let retryError
        for (const [key, item] of attempted) {
          retryError = this.accept(item, byKey.get(key)) || retryError
        }
        if (retryError) throw retryError
      })
    } catch (error) {
      this.deadline?.check()
      if (this.callFailure(error) || error instanceof KafkaJSInvariantViolation) throw error
      if (
        (error.cause && this.callFailure(error.cause)) ||
        error.cause instanceof KafkaJSInvariantViolation
      )
        throw error.cause
      outstanding = error
    }
    this.deadline?.check()
    for (const [key, item] of this.pending) {
      this.results.set(
        key,
        localFailure || !this.last.has(key)
          ? {
              ...item,
              errorCode: -1,
              error: localFailure || outstanding,
              errorMessage: this.sanitize((localFailure || outstanding)?.message ?? null),
            }
          : this.last.get(key)
      )
    }
    this.pending.clear()
    return this.items.map((item) => this.results.get(this.keyOf(item)))
  }
}

/**
 * Creates one item-result retry call; no new factory, protocol export or public framework.
 * @param {object} options ItemRetry constructor arguments
 * @param {Function} send attempt sender, operating only on its supplied pending subset
 * @returns {Promise<object[]>} caller-ordered item results
 * @throws {Error} API deadline or invalid duplicate caller cells
 */
const itemRetry = async (options, send) => {
  const call = new ItemRetry(options)
  try {
    return await call.run(send)
  } finally {
    call.dispose()
  }
}

module.exports = { itemRetry }
