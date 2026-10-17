const {
  KafkaJSAggregateError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSDeliveryTimeout,
} = require('../errors')

/**
 * Whether a failure names the items it is about: an aggregate with a list of children, at least
 * one. Anything else, including an aggregate with no list, says nothing about which items went
 * through, so it ends the call as it is.
 * @param {Error} error what a CreateTopics or AlterPartitionReassignments request threw
 * @returns {boolean}
 */
const isPerItemFailure = (error) =>
  error instanceof KafkaJSAggregateError && Array.isArray(error.errors) && error.errors.length > 0

/**
 * Splits the children of an aggregate by whether sending the request again can change them.
 * A child is retryable only when it says `retriable === true`, the rule the aggregate itself
 * uses: a child with no flag, a string, a null or a hole cannot be classified, so it is terminal,
 * which is stricter than the retrier's "absent means retriable". Reads by index, so it never
 * throws and never copies the list, whatever its length.
 * @param {KafkaJSAggregateError} error what a CreateTopics or AlterPartitionReassignments answer
 *   threw, one child per failed topic or partition; a missing list counts as no children
 * @param {(child: Error) => string} keyOf the key of the item a child answers for
 * @returns {{ retryable: Error[], terminal: Error[], failed: Set<string> }} the two groups, and
 *   the keys of every child that has one (the items that did not go through in this answer)
 */
const settleAggregate = (error, keyOf) => {
  const children = Array.isArray(error.errors) ? error.errors : []
  const settled = { retryable: [], terminal: [], failed: new Set() }

  for (let index = 0; index < children.length; index++) {
    const child = children[index]
    if (child?.retriable === true) {
      settled.retryable.push(child)
    } else {
      settled.terminal.push(child)
    }
    if (child != null) {
      settled.failed.add(keyOf(child))
    }
  }

  return settled
}

/**
 * What the retrier ended with, as a list of children: the aggregate of the last attempt when the
 * retrier gave up on it (retries or deadline), else the error itself.
 * @param {Error} outstanding
 * @returns {Error[]}
 */
const childrenOf = (outstanding) => {
  const gaveUp =
    outstanding instanceof KafkaJSNumberOfRetriesExceeded ||
    outstanding instanceof KafkaJSDeliveryTimeout
  const source = gaveUp ? outstanding.cause : outstanding

  return isPerItemFailure(source) ? source.errors : [source]
}

/**
 * The outcome of one call that sends many items and is answered per item (`createTopics`,
 * `alterPartitionReassignments`), kept across the attempts of its retrier.
 *
 * The original request is never changed: each attempt sends `pending`, the part of it that is
 * still retryable, so a mutation the broker confirmed is never sent again. Terminal outcomes are
 * kept whatever a later attempt does, and `finish` turns the whole call into one result.
 */
class AggregateOutcome {
  /**
   * @param {object} options
   * @param {string[]} options.keys the key of every item of the original request
   * @param {(child: Error) => string} options.keyOf the key of the item a child answers for
   * @param {(keys: Set<string>) => object[]} options.select the part of the original request that
   *   holds exactly those keys, in request order; must not change the original
   * @param {string} options.message the message of the aggregate `finish` rejects with
   * @param {(child: Error) => boolean} [options.isBenign] a terminal child that does not fail the
   *   call (a topic that already exists): it only turns the result of `finish` to `false`
   */
  constructor({ keys, keyOf, select, message, isBenign = () => false }) {
    this.keyOf = keyOf
    this.select = select
    this.message = message
    this.isBenign = isBenign
    this.pendingKeys = new Set(keys)
    /** @type {Set<string>} the keys a response confirmed */
    this.created = new Set()
    /** @type {Error[]} the children that will not be retried, in the order they were seen */
    this.terminal = []
  }

  /** @returns {object[]} the part of the original request the next attempt sends */
  get pending() {
    return this.select(this.pendingKeys)
  }

  /**
   * An attempt went through: every item it sent is confirmed.
   * @returns {void}
   */
  succeeded() {
    for (const key of this.pendingKeys) {
      this.created.add(key)
    }
    this.pendingKeys = new Set()
  }

  /**
   * An attempt threw an aggregate. Items it does not list are confirmed, terminal children are
   * kept for good, and the retryable children become the next `pending`.
   * @param {KafkaJSAggregateError} error
   * @returns {KafkaJSAggregateError|null} an aggregate of only the retryable children, for the
   *   attempt to throw so the retrier backs off; null when none is left to retry
   */
  settle(error) {
    const { retryable, terminal, failed } = settleAggregate(error, this.keyOf)
    for (const key of this.pendingKeys) {
      if (!failed.has(key)) {
        this.created.add(key)
      }
    }
    for (const child of terminal) {
      this.terminal.push(child)
    }
    this.pendingKeys = new Set(retryable.map(this.keyOf))

    return retryable.length > 0 ? new KafkaJSAggregateError(error.message, retryable) : null
  }

  /**
   * Ends the call. Call it once the retrier has settled.
   * @param {Error} [outstanding] what the retrier rejected with (retries spent, deadline, a bail);
   *   omitted when it resolved
   * @returns {boolean} true when every item went through; false when the only terminal children
   *   were benign
   * @throws {Error} `outstanding` itself when no terminal child was kept; otherwise a
   *   non-retriable `KafkaJSAggregateError` of the terminal children, followed by the children
   *   `outstanding` left behind, so no terminal outcome is lost
   */
  finish(outstanding) {
    if (outstanding) {
      if (this.terminal.length === 0) {
        throw outstanding
      }
      throw new KafkaJSAggregateError(this.message, [...this.terminal, ...childrenOf(outstanding)])
    }

    if (this.terminal.some((child) => !this.isBenign(child))) {
      throw new KafkaJSAggregateError(this.message, this.terminal)
    }

    return this.terminal.length === 0
  }
}

module.exports = { isPerItemFailure, settleAggregate, AggregateOutcome }
