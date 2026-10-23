const { INT_32_MAX_VALUE } = require('../../constants')
const { RETRIABLE_CATEGORIES } = require('../../protocol/errorCategories')
const { KafkaJSInvariantViolation } = require('../../errors')
const Fifo = require('../../utils/fifo')

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const OUT_OF_ORDER_SEQUENCE_NUMBER = 45
const DUPLICATE_SEQUENCE_NUMBER = 46
const INVALID_PRODUCER_ID_MAPPING = 49
const UNKNOWN_PRODUCER_ID = 59

const NO_SEQUENCE = -1
const NO_OFFSET = -1

/** What a partition with no state reads as. */
const NO_STATE = Object.freeze({
  lastAckedSequence: NO_SEQUENCE,
  lastAckedOffset: NO_OFFSET,
  unresolvedNextSequence: null,
})

/** Returned by `resolveUnresolved()` when no partition is marked; never mutated. */
const NOTHING_RESOLVED = Object.freeze([])

/**
 * A sequence plus an increment, wrapping past INT_32_MAX_VALUE to 0 the way the broker computes a
 * batch's last sequence.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecordBatch.java
 * @param {number} sequence
 * @param {number} increment
 * @returns {number}
 */
const addSequence = (sequence, increment) =>
  sequence > INT_32_MAX_VALUE - increment
    ? increment - (INT_32_MAX_VALUE - sequence) - 1
    : sequence + increment

/**
 * The inverse of `addSequence`.
 * @param {number} sequence
 * @param {number} decrement
 * @returns {number}
 */
const subtractSequence = (sequence, decrement) =>
  sequence < decrement ? INT_32_MAX_VALUE - (decrement - sequence) + 1 : sequence - decrement

/** 2^31: the int32 sequences a partition counts through before it wraps. */
const SEQUENCE_SPACE = INT_32_MAX_VALUE + 1

/**
 * Whether `sequence` is at or ahead of `current` on the wrapping sequence: less than half the
 * space ahead of it, so a sequence just past the wrap is ahead of one just below
 * INT_32_MAX_VALUE.
 * @param {number} sequence
 * @param {number} current a sequence, or -1 for none - every sequence is ahead of none
 * @returns {boolean}
 */
const isForward = (sequence, current) =>
  current === NO_SEQUENCE ||
  (sequence - current + SEQUENCE_SPACE) % SEQUENCE_SPACE < SEQUENCE_SPACE / 2

/**
 * Reads the immutable child membership even after its payload has been released.
 * @param {PartitionBatch} batch
 * @returns {number} its record count, or the live array's count for an older direct caller
 */
const recordCountOf = (batch) => batch.recordCount ?? batch.messages.length

/**
 * @param {PartitionBatch} batch
 * @returns {number} last sequence of its immutable record membership
 */
const lastSequenceOf = (batch) => addSequence(batch.firstSequence, recordCountOf(batch) - 1)

/**
 * Removes `item` from `list` when it is there.
 * @param {Array} list
 * @param {*} item
 * @returns {void}
 */
const removeFrom = (list, item) => {
  const index = list.indexOf(item)
  if (index !== -1) {
    list.splice(index, 1)
  }
}

/**
 * Reads an offset from a Produce response as a Number.
 * @param {string|number|undefined|null} offset an int64 as a Produce response decodes it
 * @returns {number} the offset, or -1 when it is absent
 */
const offsetOf = (offset) => (offset == null ? NO_OFFSET : Number(offset))

/**
 * Creates the state of a partition that has never been used.
 * @param {string} topic
 * @param {number} partition
 * @param {number} incarnation sequencer-wide creation or reset stamp
 * @returns {PartitionEntry}
 */
const createEntry = (topic, partition, incarnation) => ({
  incarnation,
  topic,
  partition,
  nextSequence: 0,
  lastAckedSequence: NO_SEQUENCE,
  lastAckedOffset: NO_OFFSET,
  stale: false,
  unresolvedNextSequence: null,
  inFlight: [],
  queued: new Fifo(),
})

/**
 * Whether every batch a partition has in flight was restarted since it was sent: each carries no
 * producer stamp, so the next drain re-sends them from 0 under the pair in force then. Such a
 * partition already numbers under the next pair, and marking it stale would restart it at 0
 * again once those re-sends are acknowledged. False for a partition with nothing in flight.
 * @param {PartitionEntry} entry
 * @returns {boolean}
 */
const restartedSinceSent = (entry) =>
  entry.inFlight.length > 0 && entry.inFlight.every((batch) => batch.producerEpoch == null)

/**
 * @typedef {Object} PartitionBatch
 * @property {number} id
 * @property {string} topic
 * @property {number} partition
 * @property {object[]} messages
 * @property {number} acks
 * @property {number} timeout
 * @property {number} compression
 * @property {number|null} firstSequence assigned when the batch is first drained, kept across a
 *   retry, rewritten only by a restart or a rebase
 * @property {string|number|null} producerId the producer id the batch's sequence belongs to;
 *   stamped when the batch is drained without one, cleared by a restart
 * @property {number|null} producerEpoch the epoch that goes with `producerId`
 * @property {PartitionEntry} [entry] captured when numbered; disposal never creates a replacement
 * @property {number} [incarnation] entry stamp captured when numbered
 * @property {number} attempt 0 before the first re-send; one more per `requeue()`
 * @property {Array<{ resolve: Function, reject: Function }>} settlers
 * @property {number} backoffUntil 0, or the time before which the batch may not be re-sent
 * @property {number} createdAt
 * @property {boolean} sequenceReset whether the batch was renumbered since it was last sent
 *   (Java's `ProducerBatch.sequenceHasBeenReset`)
 * @property {number} [recordCount] immutable count for state changes after payload detachment
 * @property {object|null} [child] the part of a refused request this batch belongs to until a
 *   request carrying it is written: the drain puts it only with the other batches of that part
 */

/**
 * @typedef {{ [topic: string]: { [leaderNodeId: string]: number[] } }} LeaderMap
 * @typedef {'COMPLETE_AS_DUPLICATE'|'RETRY_IN_ORDER'|'BUMP_EPOCH'|'FAIL_BATCH'|'FAIL_BATCH_AND_REBASE'|'FAIL_BATCH_AND_BUMP'} Verdict
 * @typedef {Object} DrainGate what the drain needs to know about the producer at this moment
 * @property {number} now
 * @property {boolean} producerIdValid
 * @property {string|number} producerId stamped on every batch the drain numbers
 * @property {number} producerEpoch
 * @property {(topic: string, partition: number) => boolean} isPartitionInTransaction always true
 *   for a producer that is not transactional
 * @property {import('./recordSize').RequestBudget} [budget] the producer's request-size plan: a
 *   request takes batches while their planned bytes stay within its `maxRequestBytes`
 *   (`maxRequestSize` less the request's framing); none means no limit
 * @typedef {Object} PartitionEntry
 * @property {string} topic
 * @property {number} partition
 * @property {number} nextSequence
 * @property {number} lastAckedSequence
 * @property {number} lastAckedOffset an int64 kept as a Number, exact below 2^53
 * @property {boolean} stale
 * @property {number|null} unresolvedNextSequence
 * @property {PartitionBatch[]} inFlight numbered and not settled - on the wire or waiting for a
 *   re-send - in the order they were numbered, which the wrap keeps from being numeric order
 * @property {Fifo<PartitionBatch>} queued waiting to be drained: re-sends first, in the order they
 *   were numbered, then batches never sent, by arrival
 */

/**
 * Per-partition sequencing for the idempotent producer: one queue of batches per partition, the
 * rules that decide which partitions may be drained, and the verdict on a failed batch.
 *
 * Synchronous and passive: it never awaits, sends or changes the producer id or epoch. A batch is
 * numbered when it is drained, and the drain skips - never waits on - a partition that must not
 * run ahead. The caller applies each verdict's consequence through `requeue`, `fail`, `rebase`,
 * `restartPartition`, `markStaleExcept`, `markAllStale` and `resetPartition`, and settles the
 * batch's `settlers` itself; only `abortQueued` rejects settlers.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java
 */
class PartitionSequencer {
  #logger
  #incarnation = 0
  #maxInFlightPerPartition
  /** @type {Map<string, Map<number, PartitionEntry>>} */
  #topics = new Map()
  /** @type {Set<PartitionEntry>} */
  #unresolved = new Set()

  /**
   * Creates a sequencer that holds no partitions.
   * @param {object} options
   * @param {import('../../../types').Logger} options.logger
   * @param {number} [options.maxInFlightPerPartition=1] how many batches of one partition may be
   *   numbered and unacknowledged at once. One keeps a partition in order whatever happens to a
   *   request on its way to the broker: a later batch is numbered only once the one before it was
   *   answered, so it can never reach the broker first.
   */
  constructor({ logger, maxInFlightPerPartition = 1 }) {
    this.#logger = logger
    this.#maxInFlightPerPartition = maxInFlightPerPartition
  }

  /**
   * Queues batches behind whatever their partition already holds. Assigns no sequence.
   * @param {PartitionBatch[]} batches
   * @returns {void}
   */
  enqueue(batches) {
    for (const batch of batches) {
      this.#entryFor(batch.topic, batch.partition).queued.push(batch)
    }
  }

  /**
   * The batches that may go to one broker now: at most one per partition it leads, all sharing
   * the first batch's `acks`, `timeout`, `compression` and producer id and epoch, so they fit one
   * Produce request. A batch drained for the first time is numbered and stamped with the gate's
   * producer id and epoch; a re-sent batch keeps its sequence and its stamp.
   *
   * A partition is skipped - and the rest of the pass goes on - while its head batch waits out a
   * backoff, while it is not yet in the transaction, and while one of these holds:
   * - no producer id is held (no partition is drained at all);
   * - the head batch was never sent, and the partition already has `maxInFlightPerPartition`
   *   batches in flight, has an unresolved sequence, or is stale with batches still in flight. A
   *   stale partition with nothing in flight restarts at 0 first;
   * - the head batch is a re-send that is not the partition's first batch in flight, the
   *   earliest numbered.
   * A request also skips a head batch that would take its planned size past the gate's budget,
   * once it holds a batch, so it always holds one; and one whose `child` differs from its first
   * batch's, so a part of a refused request goes out alone until it is written.
   * Java breaks out of the node's pass at a blocked partition, and at `max.request.size`;
   * skipping lets unrelated partitions go out on the same request.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java
   * @param {number|string} nodeId the broker the request is for
   * @param {LeaderMap} leaders the partitions each broker leads, for the topics being sent
   * @param {DrainGate} gate
   * @returns {PartitionBatch[]} removed from their queues and tracked as in flight
   */
  drain(nodeId, leaders, gate) {
    const ready = []
    if (!gate.producerIdValid) {
      return ready
    }

    const node = String(nodeId)
    const { budget } = gate
    let first = null
    let planned = 0
    for (const topic of Object.keys(leaders)) {
      const partitions = leaders[topic][node]
      const entries = this.#topics.get(topic)
      if (partitions === undefined || entries === undefined) {
        continue
      }
      let topicBytes = budget === undefined ? 0 : budget.topicBytes(topic)

      for (const partition of partitions) {
        const entry = entries.get(Number(partition))
        if (entry === undefined || entry.queued.size === 0) {
          continue
        }

        const head = entry.queued.peek()
        if (
          head.backoffUntil > gate.now ||
          !gate.isPartitionInTransaction(topic, entry.partition)
        ) {
          continue
        }

        if (head.firstSequence === null) {
          if (
            entry.inFlight.length >= this.#maxInFlightPerPartition ||
            entry.unresolvedNextSequence !== null ||
            (entry.stale && entry.inFlight.length > 0)
          ) {
            continue
          }
        } else if (entry.inFlight[0] !== head) {
          continue
        }

        if (first !== null && !fitsRequest(first, head, gate)) {
          continue
        }
        if (budget !== undefined) {
          const bytes = topicBytes + budget.batchBytes(head)
          if (first !== null && planned + bytes > budget.maxRequestBytes) {
            continue
          }
          planned += bytes
          topicBytes = 0
        }

        entry.queued.shift()
        if (head.firstSequence === null) {
          this.#number(entry, head)
        }

        if (head.producerEpoch == null) {
          head.entry = entry
          head.incarnation = entry.incarnation
          head.producerId = gate.producerId
          head.producerEpoch = gate.producerEpoch
        }

        head.sequenceReset = false
        first = first || head
        ready.push(head)
      }
    }

    return ready
  }

  /**
   * Settles a batch the broker acknowledged: it leaves the partition's in-flight list and
   * `lastAckedSequence` moves to its last sequence. `lastAckedOffset` moves to the batch's last
   * offset, `baseOffset + record count - 1`, unless the response carries no offset - a
   * `DUPLICATE_SEQUENCE_NUMBER` success - which leaves it alone. Both only ever move forward; for
   * `lastAckedSequence` forward is read across the wrap, so a batch ending past
   * INT_32_MAX_VALUE moves it on.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TxnPartitionMap.java
   * @param {PartitionBatch} batch a batch on the wire; an acknowledged batch is never queued
   * @param {{ baseOffset: string|number, logStartOffset?: string|number } | { duplicate: true }} result
   * @returns {void}
   */
  complete(batch, result) {
    const entry = batch.entry
    if (entry === undefined) return
    this.#remove(entry, batch)
    if (
      this.#lookup(batch.topic, batch.partition) !== entry ||
      batch.incarnation !== entry.incarnation
    ) {
      this.#dropIfIdle(entry)
      return
    }
    const lastSequence = lastSequenceOf(batch)
    if (isForward(lastSequence, entry.lastAckedSequence)) entry.lastAckedSequence = lastSequence
    const baseOffset = result.duplicate ? NO_OFFSET : offsetOf(result.baseOffset)
    if (baseOffset >= 0)
      entry.lastAckedOffset = Math.max(entry.lastAckedOffset, baseOffset + recordCountOf(batch) - 1)
    this.#dropIfIdle(entry)
  }

  /**
   * Settles a batch that failed for good, wherever it is - in flight or still queued - and gives
   * nothing back: the batch's sequence range stays used. Call `markUnresolved` before it for a
   * batch that expired.
   * @param {PartitionBatch} batch
   * @returns {void}
   */
  fail(batch) {
    const entry = batch.entry || this.#lookup(batch.topic, batch.partition)
    if (entry === undefined) return
    this.#remove(entry, batch)
    this.#dropIfIdle(entry)
  }

  /**
   * Puts a failed batch back for a re-send with the same sequence: one more attempt, a backoff,
   * and a place in the queue ahead of every batch never sent, after any re-send numbered before
   * it. It stays in flight, so the drain holds it until it is the partition's first batch in
   * flight. Re-sends are ordered by their place in the in-flight list, never by `firstSequence`:
   * across the wrap a batch at 0 was numbered after one at INT_32_MAX_VALUE - 4, and sorting it
   * first would hold both for ever.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java
   * @param {PartitionBatch} batch a batch in flight
   * @param {number} backoffUntil the time before which it may not be drained
   * @param {{ counted?: boolean }} [options] false only for an unwritten private refusal
   * @returns {void}
   */
  requeue(batch, backoffUntil, { counted = true } = {}) {
    const { queued, inFlight } = this.#entryFor(batch.topic, batch.partition)
    if (counted) batch.attempt += 1
    batch.backoffUntil = backoffUntil

    const rank = inFlight.indexOf(batch)
    let index = 0
    while (
      index < queued.size &&
      queued.at(index).firstSequence !== null &&
      inFlight.indexOf(queued.at(index)) < rank
    ) {
      index += 1
    }

    queued.insertAt(index, batch)
  }

  /**
   * What to do with a batch whose Produce failed, from its error, the partition's state and the
   * retry budget. Pure: the caller applies the consequence.
   *
   * - `DUPLICATE_SEQUENCE_NUMBER` (46): the broker already has the batch - `COMPLETE_AS_DUPLICATE`.
   * - An expired batch: `FAIL_BATCH`, whatever attempts remain.
   * - `UNKNOWN_PRODUCER_ID` (59), with attempts left: a re-send when the log start offset is unknown
   *   or the batch was renumbered since it was sent; `RETRY_IN_ORDER` (transactional, after a
   *   restart at 0) or `BUMP_EPOCH` (idempotent-only) when the log was truncated past the last
   *   acknowledged offset; otherwise `FAIL_BATCH` (transactional) or `BUMP_EPOCH`.
   * - `OUT_OF_ORDER_SEQUENCE_NUMBER` (45), with attempts left: `RETRY_IN_ORDER` when the batch is
   *   not the partition's next sequence; when it is - or while a sequence is unresolved -
   *   `FAIL_BATCH` (transactional) or `BUMP_EPOCH` (idempotent-only, only for the batch at the
   *   unresolved sequence, otherwise a re-send).
   * - 45 or 59 with no attempts left: `FAIL_BATCH` (transactional) or `FAIL_BATCH_AND_BUMP`. Java's
   *   `UnknownProducerIdException` is an `OutOfOrderSequenceException`, and an idempotent-only
   *   producer bumps its epoch for either.
   * - `INVALID_PRODUCER_ID_MAPPING` (49): `FAIL_BATCH`.
   * - A retriable category: `RETRY_IN_ORDER` while attempts remain.
   * - Anything else: `FAIL_BATCH_AND_REBASE` while attempts remain - the failed batch's sequences
   *   are given back - and `FAIL_BATCH` once they are spent, because the broker may have the batch.
   * Every other exhausted case is `FAIL_BATCH`.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @param {PartitionBatch} batch
   * @param {object} failure
   * @param {string|null} failure.category the classified category; null for code 46
   * @param {number} [failure.code] the Kafka error code, when there is one
   * @param {string|number} [failure.logStartOffset] the partition's log start offset from the
   *   Produce response; absent or -1 when unknown
   * @param {boolean} failure.transactional
   * @param {number} failure.attemptsLeft re-sends the retry budget still allows this batch
   * @param {boolean} failure.expired the batch ran out of its delivery timeout
   * @returns {Verdict}
   */
  verdict(batch, { category, code, logStartOffset, transactional, attemptsLeft, expired }) {
    if (code === DUPLICATE_SEQUENCE_NUMBER) {
      return 'COMPLETE_AS_DUPLICATE'
    }

    if (expired) {
      return 'FAIL_BATCH'
    }

    const canRetry = attemptsLeft > 0
    if (code === UNKNOWN_PRODUCER_ID || code === OUT_OF_ORDER_SEQUENCE_NUMBER) {
      if (!canRetry) {
        return transactional ? 'FAIL_BATCH' : 'FAIL_BATCH_AND_BUMP'
      }

      return code === UNKNOWN_PRODUCER_ID
        ? this.#unknownProducerId(batch, logStartOffset, transactional)
        : this.#outOfOrder(batch, transactional)
    }

    if (code === INVALID_PRODUCER_ID_MAPPING || !canRetry) {
      return 'FAIL_BATCH'
    }

    return RETRIABLE_CATEGORIES.has(category) ? 'RETRY_IN_ORDER' : 'FAIL_BATCH_AND_REBASE'
  }

  /**
   * Whether an `UNKNOWN_PRODUCER_ID` for this batch is the log-truncation case: the log start
   * offset is known, the batch was not renumbered since it was sent, and the partition's last
   * acknowledged offset is below the log start - retention or `DeleteRecords` removed the head of
   * the log, and the producer's state with it. A transactional producer restarts the partition at
   * 0 and re-sends; an idempotent-only one bumps its epoch.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {PartitionBatch} batch
   * @param {string|number} [logStartOffset]
   * @returns {boolean}
   */
  isTruncated(batch, logStartOffset) {
    const start = offsetOf(logStartOffset)
    if (start < 0 || batch.sequenceReset) {
      return false
    }

    return this.#stateOf(batch).lastAckedOffset < start
  }

  /**
   * Gives a failed batch's sequences back (a transactional producer's permanent failure with
   * attempts left): the batch is settled, `nextSequence` drops by its record count, and every
   * batch numbered after it that is still in flight moves down by the same amount and counts as
   * renumbered.
   * Queued batches never sent need nothing - they are numbered from `nextSequence` when drained.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TxnPartitionEntry.java
   * @param {PartitionBatch} batch a batch that was sent
   * @returns {void}
   */
  rebase(batch) {
    const entry = batch.entry || this.#lookup(batch.topic, batch.partition)
    if (
      entry === undefined ||
      this.#lookup(batch.topic, batch.partition) !== entry ||
      batch.incarnation !== entry.incarnation
    )
      return
    const position = entry.inFlight.indexOf(batch)
    if (position < 0) return
    this.#giveBack(entry, batch, position)

    this.#logger.debug('Sequences given back after a failed batch', {
      topic: entry.topic,
      partition: entry.partition,
      firstSequence: batch.firstSequence,
      recordCount: recordCountOf(batch),
      nextSequence: entry.nextSequence,
    })
    this.#dropIfIdle(entry)
  }

  /**
   * Takes a batch out of its partition and gives its sequences back: `nextSequence` drops by its
   * record count, and every batch numbered after it moves down by as much and counts as renumbered.
   * @param {PartitionEntry} entry
   * @param {PartitionBatch} batch
   * @param {number} position its place in the partition's in-flight list
   * @returns {void}
   */
  #giveBack(entry, batch, position) {
    const count = recordCountOf(batch)
    this.#remove(entry, batch)
    entry.nextSequence = subtractSequence(entry.nextSequence, count)
    for (let index = position; index < entry.inFlight.length; index += 1) {
      const later = entry.inFlight[index]
      later.firstSequence = subtractSequence(later.firstSequence, count)
      later.sequenceReset = true
    }
  }

  /**
   * Puts a batch whose current attempt was refused before anything of it was written back at the
   * head of its partition's queue, unnumbered, for the drain to number afresh: a numbered batch
   * gives its range back first, as `rebase` does (one batch in flight per partition leaves none
   * numbered after it), so no sequence is skipped or used twice. A batch the sequencer does not
   * hold yet - the second half of a cut batch - is only queued.
   * @param {PartitionBatch} batch a batch never written
   * @returns {void}
   */
  restoreUnsent(batch) {
    const entry = this.#entryFor(batch.topic, batch.partition)
    const position = entry.inFlight.indexOf(batch)
    if (position >= 0) {
      this.#giveBack(entry, batch, position)
    } else {
      this.#remove(entry, batch)
    }
    batch.firstSequence = null
    batch.producerId = null
    batch.producerEpoch = null
    batch.entry = undefined
    batch.incarnation = undefined
    entry.queued.insertAt(0, batch)
  }

  /**
   * Restarts a partition's sequences at 0: its batches in flight are renumbered from 0 in order,
   * count as renumbered and lose their producer stamp (the next drain stamps the current pair),
   * `nextSequence` becomes their total record count and `lastAckedSequence` -1. Queued batches
   * never sent are untouched. Clears the partition's stale flag and unresolved sequence.
   *
   * A partition left with nothing in flight, nothing queued and nothing unresolved is dropped, and
   * its `lastAckedOffset` with it; the Java client keeps that offset. Two paths reach the drop: a
   * restart of a partition with nothing in flight, and `rebase()` of the sole batch in flight
   * right after a restart.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TxnPartitionEntry.java
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  restartPartition(topic, partition) {
    const entry = this.#lookup(topic, partition)
    if (entry === undefined) {
      return
    }

    this.#restart(entry)
    this.#logger.debug('Sequences restarted at 0', {
      topic,
      partition: entry.partition,
      nextSequence: entry.nextSequence,
    })
    this.#dropIfIdle(entry)
  }

  /**
   * Marks every partition but one stale after a local epoch bump: a stale partition keeps the
   * sequences of its batches in flight, numbers nothing new until they have settled, and then
   * restarts at 0 under the new epoch.
   *
   * A partition whose batches in flight were all restarted since they were sent is left alone:
   * its re-sends already go out from 0 under the newest pair, so a second bump in the same Produce
   * response must not restart it again. A partition with nothing in flight, or with any batch
   * still stamped, is marked. So is a partly restarted one: its unstamped re-sends then draw one
   * more `OUT_OF_ORDER_SEQUENCE_NUMBER` and one more bump, and no record is lost.
   * @param {string} topic the partition that triggered the bump, restarted separately
   * @param {number} partition
   * @returns {void}
   */
  markStaleExcept(topic, partition) {
    const trigger = this.#lookup(topic, partition)
    for (const entry of this.#entries()) {
      if (entry !== trigger && !restartedSinceSent(entry)) {
        entry.stale = true
      }
    }
  }

  /**
   * Marks every partition stale, for a new producer id or epoch no partition has restarted under
   * yet. A stale partition behaves as `markStaleExcept` describes, which also names the
   * partitions left alone: those whose batches in flight were all restarted since they were sent.
   * @returns {void}
   */
  markAllStale() {
    for (const entry of this.#entries()) {
      if (!restartedSinceSent(entry)) {
        entry.stale = true
      }
    }
  }

  /**
   * Forgets what the partition knows about the broker's state for this producer - after an
   * `UNKNOWN_PRODUCER_ID` the broker has none: no acknowledged sequence or offset, no unresolved
   * sequence, numbering from 0. Batches in flight keep their sequences and settle normally; until
   * they have, the partition is stale and numbers nothing new.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  resetPartition(topic, partition) {
    const entry = this.#lookup(topic, partition)
    if (entry === undefined) {
      return
    }

    entry.lastAckedOffset = NO_OFFSET
    this.#clearUnresolved(entry)
    if (entry.inFlight.length > 0) {
      entry.incarnation = ++this.#incarnation
      entry.stale = true
      return
    }

    this.#restart(entry)
    this.#dropIfIdle(entry)
  }

  /**
   * Resets every partition for a new producer id or epoch (`initialize`, `reinitialize`, an
   * EndTxn adoption): only partitions holding batches never sent survive, with fresh state.
   * @returns {void}
   * @throws {KafkaJSInvariantViolation} when a batch is in flight; the state is then unchanged
   */
  resetAll() {
    const kept = new Map()
    for (const entry of this.#entries()) {
      if (entry.inFlight.length > 0) {
        throw new KafkaJSInvariantViolation(
          `Sequences reset with a batch in flight on ${entry.topic} partition ${entry.partition}`
        )
      }

      if (entry.queued.size > 0) {
        const fresh = createEntry(entry.topic, entry.partition, ++this.#incarnation)
        fresh.queued = entry.queued
        if (!kept.has(entry.topic)) {
          kept.set(entry.topic, new Map())
        }
        kept.get(entry.topic).set(entry.partition, fresh)
      }
    }

    this.#topics = kept
    this.#unresolved.clear()
  }

  /**
   * Records that a numbered batch with transmission history expired without a definitive answer, so the broker may hold sequences the client cannot account for. Until the
   * partition has nothing in flight and `resolveUnresolved()` settles it, the drain numbers
   * nothing new there, and a 45 for the batch at the unresolved sequence counts as the next one.
   * The caller establishes transmission history; an unstamped pending restart can mark its live entry despite its old incarnation, but a stamped obsolete attempt cannot.
   * The mark only moves forward, across the wrap too.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @param {PartitionBatch} batch
   * @returns {void}
   */
  markUnresolved(batch) {
    const entry = batch.entry || this.#lookup(batch.topic, batch.partition)
    if (entry === undefined || batch.firstSequence === null) return
    if (
      this.#lookup(batch.topic, batch.partition) !== entry ||
      (batch.incarnation !== entry.incarnation && batch.producerEpoch != null)
    )
      return
    const next = addSequence(batch.firstSequence, recordCountOf(batch))
    const current = entry.unresolvedNextSequence
    entry.unresolvedNextSequence = current === null || isForward(next, current) ? next : current
    this.#unresolved.add(entry)
  }

  /**
   * Settles every unresolved sequence whose partition has nothing in flight: `'cleared'` when the
   * acknowledged sequences reach `nextSequence` (the expired batch's successors landed), `'gap'`
   * otherwise - the caller bumps the epoch (idempotent-only) or arms the abortable error
   * (transactional). Either way the mark is gone. Run before every drain.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @returns {ReadonlyArray<{ topic: string, partition: number, outcome: 'cleared'|'gap' }>}
   */
  resolveUnresolved() {
    if (this.#unresolved.size === 0) {
      return NOTHING_RESOLVED
    }

    const resolved = []
    for (const entry of this.#unresolved) {
      if (entry.inFlight.length > 0) {
        continue
      }

      const reached = entry.nextSequence === addSequence(entry.lastAckedSequence, 1)
      this.#clearUnresolved(entry)
      resolved.push({
        topic: entry.topic,
        partition: entry.partition,
        outcome: reached ? 'cleared' : 'gap',
      })
      this.#dropIfIdle(entry)
    }

    return resolved
  }

  /**
   * Tells whether any partition holds a batch that is numbered and not yet settled. A batch
   * waiting for a re-send counts; a batch never sent does not.
   * @returns {boolean}
   */
  hasInFlight() {
    for (const entry of this.#entries()) {
      if (entry.inFlight.length > 0) {
        return true
      }
    }

    return false
  }

  /**
   * Reads the sequence a partition's next new batch gets.
   * @param {string} topic
   * @param {number} partition
   * @returns {number} the next sequence; 0 for a partition that holds no state
   */
  nextSequence(topic, partition) {
    const entry = this.#lookup(topic, partition)
    return entry === undefined ? 0 : entry.nextSequence
  }

  /**
   * Moves a partition's next sequence by `delta`, negative to move it back, and returns it. The
   * result rotates to 0 once it reaches INT_32_MAX_VALUE, as the former `updateSequence` did -
   * unlike `drain()`, which numbers a batch at INT_32_MAX_VALUE and wraps after it. A partition
   * left at 0 with nothing else to hold is dropped.
   * @param {string} topic
   * @param {number} partition
   * @param {number} delta
   * @returns {number} the new next sequence
   */
  advance(topic, partition, delta) {
    const entry = this.#entryFor(topic, partition)
    let sequence = entry.nextSequence + delta
    if (sequence >= INT_32_MAX_VALUE) {
      this.#logger.debug(
        `Sequence for ${topic} ${partition} exceeds max value (${sequence}). Rotating to 0.`
      )
      sequence = 0
    }

    entry.nextSequence = sequence
    this.#dropIfIdle(entry)
    return sequence
  }

  /**
   * Rejects every batch waiting in a queue - never sent, or waiting for a re-send - with `error`,
   * after taking it out of its queue and the in-flight list. Batches on the wire are untouched;
   * they settle on their responses.
   * @param {Error} error the same object for every batch
   * @returns {PartitionBatch[]} the batches rejected
   */
  abortQueued(error) {
    const aborted = []
    for (const entry of this.#entries()) {
      if (entry.queued.size === 0) {
        continue
      }

      for (const batch of entry.queued) {
        removeFrom(entry.inFlight, batch)
        batch.sequenceReset = false
        aborted.push(batch)
      }

      entry.queued.clear()
      this.#dropIfIdle(entry)
    }

    for (const batch of aborted) {
      for (const settler of batch.settlers) {
        settler.reject(error)
      }
    }

    return aborted
  }

  /**
   * A copy of one partition's state.
   * @param {string} topic
   * @param {number} partition
   * @returns {{ nextSequence: number, lastAckedSequence: number, lastAckedOffset: number,
   *   stale: boolean, unresolvedNextSequence: number|null, inFlight: PartitionBatch[],
   *   queued: PartitionBatch[] } | null} null when the partition holds no state - never used, or
   *   dropped once idle after a reset
   */
  entry(topic, partition) {
    const entry = this.#lookup(topic, partition)
    if (entry === undefined) {
      return null
    }

    return {
      nextSequence: entry.nextSequence,
      lastAckedSequence: entry.lastAckedSequence,
      lastAckedOffset: entry.lastAckedOffset,
      stale: entry.stale,
      unresolvedNextSequence: entry.unresolvedNextSequence,
      inFlight: [...entry.inFlight],
      queued: [...entry.queued],
    }
  }

  /**
   * Numbers a batch never sent from the partition's next sequence and puts it in flight.
   * @param {PartitionEntry} entry
   * @param {PartitionBatch} batch a batch never sent; a stale partition restarts first
   * @returns {void}
   */
  #number(entry, batch) {
    if (entry.stale) {
      this.#restart(entry)
    }

    batch.entry = entry
    batch.incarnation = entry.incarnation
    batch.firstSequence = entry.nextSequence
    entry.nextSequence = addSequence(entry.nextSequence, recordCountOf(batch))
    entry.inFlight.push(batch)
  }

  /**
   * Restarts a partition's sequences at 0: renumbers its batches in flight in order and clears its
   * acknowledged sequence, stale flag and unresolved sequence. Leaves the entry in the map.
   * @param {PartitionEntry} entry
   * @returns {void}
   */
  #restart(entry) {
    entry.incarnation = ++this.#incarnation
    let sequence = 0
    for (const batch of entry.inFlight) {
      batch.firstSequence = sequence
      batch.producerId = null
      batch.producerEpoch = null
      batch.sequenceReset = true
      sequence = addSequence(sequence, recordCountOf(batch))
    }

    entry.nextSequence = sequence
    entry.lastAckedSequence = NO_SEQUENCE
    entry.stale = false
    this.#clearUnresolved(entry)
  }

  /**
   * Forgets a partition's unresolved sequence, in the entry and in the set `resolveUnresolved()`
   * walks.
   * @param {PartitionEntry} entry
   * @returns {void}
   */
  #clearUnresolved(entry) {
    entry.unresolvedNextSequence = null
    this.#unresolved.delete(entry)
  }

  /**
   * Settles a batch without an acknowledgement: takes it out of the partition's in-flight list and
   * queue, whichever holds it, and clears its renumbered flag.
   * @param {PartitionEntry} entry
   * @param {PartitionBatch} batch
   * @returns {void}
   */
  #remove(entry, batch) {
    removeFrom(entry.inFlight, batch)
    entry.queued.remove(batch)
    batch.sequenceReset = false
  }

  /**
   * Drops a partition that holds nothing and would number from 0 with no acknowledged sequence -
   * what every reset leaves - so the map does not grow with every partition a long-lived producer
   * ever wrote to. A partition whose last batch ended at INT_32_MAX_VALUE also numbers from 0
   * next, but keeps its acknowledged sequence and offset.
   *
   * Dropping also forgets `lastAckedOffset`, which the Java client keeps. Two paths reach it: a
   * restart of a partition with nothing in flight, and `rebase()` of the sole batch in flight
   * right after a restart.
   * @param {PartitionEntry} entry
   * @returns {void}
   */
  #dropIfIdle(entry) {
    if (
      entry.nextSequence !== 0 ||
      entry.lastAckedSequence !== NO_SEQUENCE ||
      entry.inFlight.length > 0 ||
      entry.queued.size > 0 ||
      entry.unresolvedNextSequence !== null
    ) {
      return
    }

    const partitions = this.#topics.get(entry.topic)
    if (partitions?.get(entry.partition) !== entry) return
    partitions.delete(entry.partition)
    if (partitions.size === 0) {
      this.#topics.delete(entry.topic)
    }
  }

  /**
   * Reads the acknowledged and unresolved state of a batch's partition without creating an entry.
   * @param {PartitionBatch} batch
   * @returns {{ lastAckedSequence: number, lastAckedOffset: number,
   *   unresolvedNextSequence: number|null }} the batch's partition, or what one with no state reads as
   */
  #stateOf(batch) {
    return this.#lookup(batch.topic, batch.partition) || NO_STATE
  }

  /**
   * Finds a partition's entry without creating one. The partition may be a number or a numeric
   * string, as a Produce response gives it.
   * @param {string} topic
   * @param {number|string} partition
   * @returns {PartitionEntry|undefined}
   */
  #lookup(topic, partition) {
    const partitions = this.#topics.get(topic)
    return partitions === undefined ? undefined : partitions.get(Number(partition))
  }

  /**
   * Finds a partition's entry, creating it when the partition holds no state.
   * @param {string} topic
   * @param {number|string} partition
   * @returns {PartitionEntry}
   */
  #entryFor(topic, partition) {
    const id = Number(partition)
    let partitions = this.#topics.get(topic)
    if (partitions === undefined) {
      partitions = new Map()
      this.#topics.set(topic, partitions)
    }

    let entry = partitions.get(id)
    if (entry === undefined) {
      entry = createEntry(topic, id, ++this.#incarnation)
      partitions.set(id, entry)
    }

    return entry
  }

  /**
   * Walks the entry of every partition, across topics, without building a list.
   * @returns {IterableIterator<PartitionEntry>}
   */
  *#entries() {
    for (const partitions of this.#topics.values()) {
      yield* partitions.values()
    }
  }

  /**
   * Picks the verdict for an `UNKNOWN_PRODUCER_ID` (59) with attempts left.
   * @param {PartitionBatch} batch
   * @param {string|number} logStartOffset
   * @param {boolean} transactional
   * @returns {Verdict}
   */
  #unknownProducerId(batch, logStartOffset, transactional) {
    if (offsetOf(logStartOffset) < 0 || batch.sequenceReset) {
      return 'RETRY_IN_ORDER'
    }

    if (this.isTruncated(batch, logStartOffset)) {
      return transactional ? 'RETRY_IN_ORDER' : 'BUMP_EPOCH'
    }

    return transactional ? 'FAIL_BATCH' : 'BUMP_EPOCH'
  }

  /**
   * Picks the verdict for an `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) with attempts left.
   * @param {PartitionBatch} batch
   * @param {boolean} transactional
   * @returns {Verdict}
   */
  #outOfOrder(batch, transactional) {
    const { unresolvedNextSequence: unresolved, lastAckedSequence } = this.#stateOf(batch)
    if (unresolved === null) {
      const next = !batch.sequenceReset && batch.firstSequence === addSequence(lastAckedSequence, 1)
      if (!next) {
        return 'RETRY_IN_ORDER'
      }

      return transactional ? 'FAIL_BATCH' : 'BUMP_EPOCH'
    }

    if (transactional) {
      return 'FAIL_BATCH'
    }

    return batch.firstSequence === unresolved ? 'BUMP_EPOCH' : 'RETRY_IN_ORDER'
  }
}

/**
 * Whether a batch can join a Produce request whose first batch is `first`: the same request-level
 * options, the same producer id and epoch (a batch never sent takes the gate's), and the same
 * part of a refused request, or none.
 * @param {PartitionBatch} first
 * @param {PartitionBatch} batch
 * @param {DrainGate} gate
 * @returns {boolean}
 */
const fitsRequest = (first, batch, gate) => {
  const stamp = batch.producerEpoch == null ? gate : batch
  return (
    batch.acks === first.acks &&
    batch.timeout === first.timeout &&
    batch.compression === first.compression &&
    stamp.producerId === first.producerId &&
    stamp.producerEpoch === first.producerEpoch &&
    (batch.child ?? null) === (first.child ?? null)
  )
}

module.exports = PartitionSequencer
