const Long = require('../utils/long')
const filterAbortedMessages = require('./filterAbortedMessages')

/**
 * The leader epoch a decoded record carries, or -1 when it carries none (a legacy message set).
 * @param {{ batchContext?: { partitionLeaderEpoch?: number } }} message
 * @returns {number}
 */
const epochOf = (message) => {
  const epoch = message.batchContext && message.batchContext.partitionLeaderEpoch
  return typeof epoch === 'number' ? epoch : -1
}

/** The strings `Long.fromString` reads as 0 although they are not offsets. */
const NOT_FINITE = new Set(['NaN', 'Infinity', '+Infinity', '-Infinity'])

/**
 * A batch collects messages returned from a single fetch call.
 *
 * A batch could contain _multiple_ Kafka RecordBatches.
 */
module.exports = class Batch {
  #nextBatchOffset
  #nextBatchLeaderEpoch
  #advances
  #fetchEpoch
  #admittedUnder

  /**
   * @param {string} topic
   * @param {string|number} fetchedOffset the offset the Fetch asked for
   * @param {object} partitionData a decoded Fetch partition. Its `nextBatchOffset` and
   *   `nextBatchLeaderEpoch` (the offset after the last complete record batch and that batch's
   *   leader epoch) let a batch with no record at or after `fetchedOffset` move the position to the
   *   end of that record batch
   * @param {number} [fetchEpoch] the leader epoch the Fetch was built at; absent for a batch built
   *   outside a fetch
   * @param {object} [admittedUnder] the validation entry the Fetch was admitted under
   *   (`ConsumerGroup#admissionEntry`)
   */
  constructor(topic, fetchedOffset, partitionData, fetchEpoch, admittedUnder) {
    this.#fetchEpoch = fetchEpoch
    this.#admittedUnder = admittedUnder
    this.fetchedOffset = fetchedOffset
    const longFetchedOffset = Long.fromValue(this.fetchedOffset)
    const { abortedTransactions, messages } = partitionData

    this.topic = topic
    this.partition = partitionData.partition
    this.highWatermark = partitionData.highWatermark

    /**
     * The assignment this batch was fetched under, stamped by `ConsumerGroup#fetch`. `null` on a
     * batch built outside a fetch, which is never stale by construction.
     * @type {number|null}
     */
    this.assignmentEpoch = null

    this.rawMessages = messages
    // Apparently fetch can return different offsets than the target offset provided to the fetch API.
    // Discard messages that are not in the requested offset
    // https://github.com/apache/kafka/blob/bf237fa7c576bd141d78fdea9f17f65ea269c290/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L912
    this.messagesWithinOffset = this.rawMessages.filter((message) =>
      Long.fromValue(message.offset).gte(longFetchedOffset)
    )

    // 1. Don't expose aborted messages
    // 2. Don't expose control records
    // @see https://kafka.apache.org/documentation/#controlbatch
    this.messages = filterAbortedMessages({
      messages: this.messagesWithinOffset,
      abortedTransactions,
    }).filter((message) => !message.isControlRecord)

    this.leaderEpochIndex = null

    this.#nextBatchOffset = partitionData.nextBatchOffset
    this.#nextBatchLeaderEpoch = partitionData.nextBatchLeaderEpoch
    this.#advances =
      this.#nextBatchOffset !== undefined &&
      this.messagesWithinOffset.length === 0 &&
      Long.fromValue(this.#nextBatchOffset).compare(longFetchedOffset) > 0
  }

  /**
   * The admission the Fetch that produced `batch` was built under: its leader epoch and the
   * validation entry it was admitted under. Kept off the batch's own properties, so the
   * `eachBatch` payload does not show it.
   * @param {object} batch
   * @returns {{epoch: number, entry: object|undefined}|undefined} `undefined` for a batch built
   *   outside a fetch, including an object that is not a `Batch`
   */
  static admissionOf(batch) {
    if (!(#fetchEpoch in batch) || batch.#fetchEpoch === undefined) return undefined
    return { epoch: batch.#fetchEpoch, entry: batch.#admittedUnder }
  }

  /**
   * The leader epoch of the record at `offset`, or `-1` when no record in this batch carries it.
   *
   * Searches `messagesWithinOffset` rather than `messages`, because `messages` excludes control
   * and aborted records while `lastOffset()` and the control-record fast-forward can both resolve
   * to exactly those offsets - and answering -1 for an offset whose epoch is known would
   * understate the tracked epoch, which is what turns a healthy position into a false truncation.
   *
   * The last entry is checked first (the overwhelmingly common case: resolving the end of a
   * batch), and an index is built lazily only for a lookup that is not the last entry, so a
   * handler resolving many offsets is linear overall rather than quadratic.
   *
   * @param {string|number|import('../utils/long')} offset compared by value: the decimal string
   *   the decoder produces is looked up as it is, and any other spelling (a number, a `Long`, a
   *   string with leading zeros) is turned into that string once when the first lookup misses. A
   *   value that is not an offset answers -1, including `NaN`, `Infinity` and their strings, which
   *   `Long.fromValue` would read as 0.
   * @returns {number}
   */
  leaderEpochAt(offset) {
    const epoch = this.#epochAt(offset)
    if (epoch !== undefined) return epoch

    if (typeof offset === 'number' ? !Number.isFinite(offset) : NOT_FINITE.has(offset)) return -1

    let canonical
    try {
      canonical = Long.fromValue(offset).toString()
    } catch {
      return -1
    }
    return canonical === offset ? -1 : (this.#epochAt(canonical) ?? -1)
  }

  /**
   * @param {unknown} offset an offset in the decoder's decimal spelling, matched exactly
   * @returns {number|undefined} the epoch of that record (-1 when it carries none), or `undefined`
   *   when no record of this batch has that spelling
   */
  #epochAt(offset) {
    const messages = this.messagesWithinOffset

    if (messages.length === 0) {
      return this.#advances && offset === this.lastOffset()
        ? (this.#nextBatchLeaderEpoch ?? -1)
        : undefined
    }

    const last = messages[messages.length - 1]
    if (last.offset === offset) {
      return epochOf(last)
    }

    if (!this.leaderEpochIndex) {
      this.leaderEpochIndex = new Map()
      for (const message of messages) {
        this.leaderEpochIndex.set(message.offset, epochOf(message))
      }
    }

    return this.leaderEpochIndex.get(offset)
  }

  isEmpty() {
    return this.messages.length === 0
  }

  isEmptyIncludingFiltered() {
    return this.messagesWithinOffset.length === 0
  }

  /**
   * If the batch contained raw messages (i.e was not truly empty) but all messages were filtered out due to
   * log compaction, control records or other reasons. Also true for a batch with no record at all
   * that still moves the position to the end of an emptied record batch.
   */
  isEmptyDueToFiltering() {
    return this.isEmpty() && (this.rawMessages.length > 0 || this.#advances)
  }

  isEmptyControlRecord() {
    return (
      this.isEmpty() && this.messagesWithinOffset.some(({ isControlRecord }) => isControlRecord)
    )
  }

  /**
   * With compressed messages, it's possible for the returned messages to have offsets smaller than the starting offset.
   * These messages will be filtered out (i.e. they are not even included in this.messagesWithinOffset)
   * If these are the only messages, the batch will appear as an empty batch.
   *
   * isEmpty() and isEmptyIncludingFiltered() will always return true if the batch is empty,
   * but this method will only return true if the batch is empty due to log compacted messages.
   *
   * @returns boolean True if the batch is empty, because of log compacted messages in the partition.
   */
  isEmptyDueToLogCompactedMessages() {
    const hasMessages = this.rawMessages.length > 0
    return hasMessages && this.isEmptyIncludingFiltered()
  }

  firstOffset() {
    return this.isEmptyIncludingFiltered() ? null : this.messagesWithinOffset[0].offset
  }

  /**
   * The offset to resolve after this batch. A batch with no record at or after the fetch offset
   * whose last complete record batch ends beyond it gives the last offset of that record batch;
   * otherwise it is the last record, the fetch offset when every record is below it, or the high
   * watermark - 1 for a batch with no record at all.
   * @returns {string}
   */
  lastOffset() {
    if (this.#advances) {
      return Long.fromValue(this.#nextBatchOffset).add(-1).toString()
    }

    if (this.isEmptyDueToLogCompactedMessages()) {
      return this.fetchedOffset
    }

    if (this.isEmptyIncludingFiltered()) {
      return Long.fromValue(this.highWatermark).add(-1).toString()
    }

    return this.messagesWithinOffset[this.messagesWithinOffset.length - 1].offset
  }

  /**
   * Returns the lag based on the last offset in the batch (also known as "high")
   */
  offsetLag() {
    const lastOffsetOfPartition = Long.fromValue(this.highWatermark).add(-1)
    const lastConsumedOffset = Long.fromValue(this.lastOffset())
    return lastOffsetOfPartition.add(lastConsumedOffset.multiply(-1)).toString()
  }

  /**
   * Returns the lag based on the first offset in the batch
   */
  offsetLagLow() {
    if (this.isEmptyIncludingFiltered()) {
      return '0'
    }

    const lastOffsetOfPartition = Long.fromValue(this.highWatermark).add(-1)
    const firstConsumedOffset = Long.fromValue(this.firstOffset())
    return lastOffsetOfPartition.add(firstConsumedOffset.multiply(-1)).toString()
  }
}
