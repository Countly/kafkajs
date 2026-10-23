const {
  KafkaJSRefreshRetriableError,
  KafkaJSTransactionAbortableError,
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSDeliveryTimeout,
  KafkaJSProducerClosedError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSInvariantViolation,
} = require('../../errors')
const { categoryOf, RETRIABLE_CATEGORIES } = require('../../protocol/errorCategories')

/** @typedef {import('../../protocol/errorCategories').Category} Category */
/** @typedef {import('../../protocol/errorCategories').Surface} Surface */

const SURFACES = ['produce', 'txn', 'offsets']
const DUPLICATE_SEQUENCE_NUMBER = 46

/**
 * CLUSTER_AUTHORIZATION_FAILED, UNSUPPORTED_VERSION and TRANSACTIONAL_ID_AUTHORIZATION_FAILED: the
 * configuration errors the Java producer makes fatal instead of abortable, because no abort lets
 * the producer carry on until the ACL or the broker changes.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 */
const FATAL_CONFIGURATION_CODES = new Set([31, 35, 53])

/** The classes whose category is what they are; `classify` never restamps them. */
const CATEGORY_CLASSES = [
  KafkaJSRefreshRetriableError,
  KafkaJSTransactionAbortableError,
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSDeliveryTimeout,
  KafkaJSProducerClosedError,
]

/**
 * @param {Error} error
 * @returns {boolean} whether `error` is an instance of one of the five category classes
 */
const isCategoryClass = (error) =>
  CATEGORY_CLASSES.some((ErrorClass) => error instanceof ErrorClass)

/**
 * @param {Error} error
 * @returns {Error[]} the error and its `cause` links, outermost first; a loop ends the chain
 */
const causeChain = (error) => {
  const chain = []
  for (let link = error; link != null && !chain.includes(link); link = link.cause) {
    chain.push(link)
  }
  return chain
}

/**
 * @param {Error} error
 * @returns {Error|undefined} the first link carrying a Kafka error code; a socket error's `code`
 *   is a string and is skipped
 */
const codedLink = (error) => causeChain(error).find(({ code }) => typeof code === 'number')

/**
 * @param {Error} error
 * @returns {Category} the category the error was built with, or the one its `retriable` flag
 *   implies for an error from outside this package
 */
const ownCategory = (error) =>
  error.category ?? (error.retriable === true ? 'retriable' : 'applicationRecoverable')

/**
 * Classifies an error for the request it arrived on, and stamps the category on the error itself:
 * the caller sees the same object, with the same `name`, prototype and fields, plus the category
 * the request implies. Nothing is re-wrapped.
 *
 * - The five category classes keep their own category; in particular a `KafkaJSDeliveryTimeout`
 *   is never restamped from the error it gave up on.
 * - The Kafka code is read from the error, or from the first `cause` that carries one.
 * - DUPLICATE_SEQUENCE_NUMBER on Produce answers `{ category: null, duplicate: true }`: the broker
 *   already has the batch, so the send succeeded and nothing is stamped.
 * - A category that would contradict the error's own `retriable` flag (a retries-exhausted wrap
 *   around a retriable code) is not stamped; the error keeps its own.
 *
 * Classify the error as it was thrown, before any retrier wrap. The retrier re-wraps a
 * non-retriable error as a `KafkaJSNonRetriableError` whose cause is the wrapped error's own cause
 * (`cause: e.cause || e`), which hides the wrapped error's class: a wrapped category class has
 * lost the protection above and is answered from its cause, so a `KafkaJSDeliveryTimeout` over a
 * CLUSTER_AUTHORIZATION_FAILED becomes `invalidConfiguration`.
 *
 * @param {Error} error the error as thrown, not as the retrier re-wrapped it
 * @param {Surface} surface
 * @param {{ transactional?: boolean }} [options] whether the producer is transactional
 * @returns {{ category: Category|null, duplicate?: true }}
 * @throws {KafkaJSInvariantViolation} for a surface other than `produce`, `txn` or `offsets`
 */
const classify = (error, surface, { transactional = false } = {}) => {
  if (!SURFACES.includes(surface)) {
    throw new KafkaJSInvariantViolation(`Unknown error surface "${surface}"`)
  }

  if (isCategoryClass(error)) {
    return { category: error.category }
  }

  const link = codedLink(error)
  if (link === undefined) {
    return { category: ownCategory(error) }
  }

  if (link.code === DUPLICATE_SEQUENCE_NUMBER && surface === 'produce') {
    return { category: null, duplicate: true }
  }

  const category = categoryOf(link.code, { retriable: link.retriable, surface, transactional })
  if (RETRIABLE_CATEGORIES.has(category) !== (error.retriable === true)) {
    return { category: ownCategory(error) }
  }

  error.category = category
  return { category }
}

/**
 * Whether an error stands for a retriable one whose retry budget ran out: the retrier's
 * `KafkaJSNumberOfRetriesExceeded` is somewhere in its cause chain.
 * @param {Error} error
 * @returns {boolean}
 */
const spentRetries = (error) =>
  causeChain(error).some((link) => link instanceof KafkaJSNumberOfRetriesExceeded)

/**
 * The state a transaction moves to when a send or a transactional request fails with `error`,
 * decided by its category, as the Java producer decides it: an abortable error, and a retriable
 * one whose budget is spent, leave a transaction that can only be aborted; an
 * application-recoverable error leaves a producer that has to be replaced. A configuration error
 * is fatal for CLUSTER_AUTHORIZATION_FAILED, UNSUPPORTED_VERSION and
 * TRANSACTIONAL_ID_AUTHORIZATION_FAILED, and abortable otherwise: the transaction is missing the
 * failed request's records either way. The caller still sees the error's own category.
 *
 * An instance of one of the five category classes is decided by what it is, so a
 * `KafkaJSApplicationRecoverableError` is fatal whatever its causes say. The retrier wraps a
 * non-retriable error in a `KafkaJSNonRetriableError` whose cause is the wrapped error's own cause
 * (`cause: e.cause || e`): that hides the wrapped error's class, and on a second wrap the
 * `KafkaJSNumberOfRetriesExceeded` too, and a wrapper whose chain still holds a spent budget is
 * read as spent (ABORTABLE_ERROR), while an application-recoverable error as thrown stays
 * FATAL_ERROR whatever its cause. Arm the error as thrown, before any retrier wrap, when its
 * class has to count.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 * @param {Error} error an error `classify` has seen, or one of the five category classes
 * @returns {'ABORTABLE_ERROR'|'FATAL_ERROR'}
 */
const transactionStateFor = (error) => {
  switch (error.category) {
    case 'abortable':
    case 'retriable':
    case 'refreshRetriable':
      return 'ABORTABLE_ERROR'
    case 'invalidConfiguration': {
      const link = codedLink(error)
      return link !== undefined && FATAL_CONFIGURATION_CODES.has(link.code)
        ? 'FATAL_ERROR'
        : 'ABORTABLE_ERROR'
    }
    default:
      return !isCategoryClass(error) && spentRetries(error) ? 'ABORTABLE_ERROR' : 'FATAL_ERROR'
  }
}

module.exports = { classify, transactionStateFor, categoryOf }
