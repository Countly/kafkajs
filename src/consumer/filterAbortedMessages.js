const Long = require('../utils/long')
const ABORTED_MESSAGE_KEY = Buffer.from([0, 0, 0, 0])

/**
 * Whether a record is the control record that ends an aborted transaction. The key alone does not
 * say so: four zero bytes are legal data in the key of an ordinary record, and a producer's abort
 * filter must not end there. Only a control record, as the batch's control attribute marks it, is
 * a marker (as in Java).
 * @param {{ key: Buffer|null, isControlRecord: boolean }} message
 * @returns {boolean}
 */
const isAbortMarker = ({ key, isControlRecord }) => {
  // Handle null/undefined keys.
  if (!isControlRecord || !key) return false
  // Cast key to buffer defensively
  return Buffer.from(key).equals(ABORTED_MESSAGE_KEY)
}

/**
 * Remove messages marked as aborted according to the aborted transactions list.
 *
 * Start of an aborted transaction is determined by message offset: every listed transaction whose
 * first offset is at or before a record is active for that record, so transactions that began
 * before the first record of the fetch are all active from it. The list is sorted by first offset
 * here, as in Java: the protocol does not promise that order.
 * End of an aborted transaction is determined by control messages.
 * @param {Message[]} messages
 * @param {Transaction[]} [abortedTransactions]
 * @returns {Message[]} Messages which did not participate in an aborted transaction
 *
 * @typedef {object} Message
 * @param {Buffer} key
 * @param {boolean} isControlRecord
 * @param {lastOffset} key  Int64
 * @param {RecordBatch}  batchContext
 *
 * @typedef {object} Transaction
 * @param {string} firstOffset  Int64
 * @param {string} producerId  Int64
 *
 * @typedef {object} RecordBatch
 * @param {string}  producerId  Int64
 * @param {boolean}  inTransaction
 */
module.exports = ({ messages, abortedTransactions }) => {
  if (!abortedTransactions || !abortedTransactions.length) {
    return messages
  }

  const pending = [...abortedTransactions].sort((a, b) =>
    Long.fromValue(a.firstOffset).compare(b.firstOffset)
  )
  let next = 0
  const currentAbortedTransactions = new Set()

  return messages.filter((message) => {
    if (next < pending.length) {
      const offset = Long.fromValue(message.offset)
      while (next < pending.length && offset.gte(pending[next].firstOffset)) {
        currentAbortedTransactions.add(pending[next++].producerId)
      }
    }

    const { producerId, inTransaction } = message.batchContext

    if (isAbortMarker(message)) {
      // Transaction is over, we no longer need to ignore messages from this producer
      currentAbortedTransactions.delete(producerId)
    } else if (currentAbortedTransactions.has(producerId) && inTransaction) {
      return false
    }

    return true
  })
}
