const {
  KafkaJSDeliveryTimeout,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../../errors')

/**
 * Whether `error` is a failure of the request itself that clears with time: a retriable error, the
 * retriable one a spent retry budget or a delivery deadline gave up on, or a coordinator lookup that found no broker.
 * @param {Error} error
 * @returns {boolean}
 */
const isTransient = (error) =>
  error.retriable === true ||
  ((error instanceof KafkaJSNumberOfRetriesExceeded || error instanceof KafkaJSDeliveryTimeout) &&
    error.cause?.retriable === true) ||
  error instanceof KafkaJSGroupCoordinatorNotFound

/**
 * What the exactly-once copier does with a chunk that failed, as Java's exactly-once loop does:
 * the same producer goes on after every error that is not fatal to it, and only a fatal error, or
 * an outcome nothing can settle, builds a new producer. A new producer is a new group member, which
 * is a rebalance, so it is never the answer to an outage.
 *
 * - `retry`: no transaction was begun, because `transaction()` itself failed on a request that
 *   clears with time (the coordinator lookup it makes, a connection reset). The producer stays
 *   ready, so the next fetch repeats the chunk.
 * - `abort`: an abortable error left a transaction that can only be aborted; the chunk is repeated
 *   once it is.
 * - `replace`: anything else, a fatal error (a fenced producer, a configuration error) and a
 *   failure of a transaction that was begun and is not abortable, which includes a commit whose
 *   outcome stays unknown.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java
 * @param {Error} error what the chunk's transaction rejected with
 * @param {{ begun: boolean }} where whether `transaction()` had resolved
 * @returns {'retry'|'abort'|'replace'}
 */
const recoveryFor = (error, { begun }) => {
  if (!begun) return isTransient(error) ? 'retry' : 'replace'
  return error.category === 'abortable' ? 'abort' : 'replace'
}

module.exports = { recoveryFor }
