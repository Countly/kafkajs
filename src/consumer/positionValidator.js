const Long = require('../utils/long')
const { validationPartitionAction } = require('./fetchErrorContract')

/**
 * The wire value for "unknown", used by an OffsetForLeaderEpoch response for both the end offset
 * of an epoch and the epoch itself, and by this class for a tracked epoch that is not known.
 */
const UNKNOWN_EPOCH = -1

/** The first retry backoff of a held partition, and its cap (Java's `retry.backoff` defaults). */
const RETRY_BACKOFF_MS = 100
const MAX_RETRY_BACKOFF_MS = 1000
const APPLYING = Symbol('applying')

/**
 * One state per assigned partition for KIP-320 position validation, and the outcome rules for a
 * reply to the OffsetForLeaderEpoch request that state produced.
 *
 * Pure: no I/O, no timers, no knowledge of brokers or offsets beyond the numbers it is handed.
 * The consumer group performs the requests, the seeks and the event emission; this class only
 * decides what should happen and remembers what is in flight.
 *
 * A partition moves between these states, keyed by `` `${topic}|${partition}` ``:
 *
 * - **unvalidated** — no entry. The next `request()` examines it; it may be fetched.
 * - **awaiting** — a request is in flight; the position it was built from is captured so a reply
 *   that arrives after the position moved can be recognised and dropped. It must never stay here
 *   without a request in flight: every exit is either performed by `complete()` or by the caller
 *   through `confirm()`, `fail()`, `release()`, `markValidated()`, `clear()` or `forgetTopic()`.
 * - **applying** — a truncation or reset owns the entry until the operation settles.
 * - **held** — the request or reposition failed (`fail()`), or the reply was discarded. A failure
 *   carries `failures`, `retryAt`, `failedAgainst` (the leader epoch it failed under),
 *   `heldSince` and `warnedAt`; it is asked again once `retryAt` passes, at once under a new
 *   leader epoch, and leaves the state on a seek (`forgetTopic()`) or a rebalance.
 * - **validated** — `validatedAgainst` holds the leader epoch the position was checked against.
 *   The partition is re-examined only when the cached leader epoch differs from it.
 *
 * Awaiting, applying and held partitions are left out of every Fetch request that is built, and a
 * Fetch answer or fetched batch for one is dropped (`holdsFetch()`). A request built before the
 * hold can still reach the broker; only its answer is dropped.
 *
 * Every mutation is idempotent per key and last-writer-wins: fetchers run one per node and a
 * partition normally belongs to one of them per round, but that is best-effort rather than an
 * invariant, so no lock is taken and a duplicate reply is a no-op through the guards in
 * `complete()`.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation
 */
module.exports = class PositionValidator {
  /**
   * @param {object} options
   * @param {import('../../types').Logger} options.logger
   */
  constructor({ logger }) {
    this.logger = logger.namespace('PositionValidator')
    this.entries = new Map()
    this.assignmentGeneration = 0
  }

  /**
   * Examines one assigned partition and, when its position needs checking, records the request
   * that is about to be made.
   *
   * The single entry point for both the "cached leader epoch changed" trigger and the transition
   * into `awaiting`: a partition whose `validatedAgainst` differs from `currentLeaderEpoch` — a
   * partition that was never validated counts as different — needs a request, unless either epoch
   * is unknown. A held or awaiting entry stays as it is while the cached leader epoch is unknown
   * and a tracked epoch exists; only a partition with no entry, or one validated before, is
   * recorded as validated against an unknown epoch straight away. That recording matters: without
   * it the partition would be re-examined, and re-logged, on every round. A held entry it replaces
   * passes on its failure count, `heldSince` and `warnedAt`.
   *
   * Anything other than `'unchanged'` means the leader epoch moved, which is the caller's signal to
   * clear the partition's preferred read replica. There is no separate "mark unvalidated" call.
   *
   * @param {string} topic
   * @param {number} partition
   * @param {object} position
   * @param {string} position.offset the offset this round is about to fetch
   * @param {number} position.trackedEpoch the leader epoch of the last consumed record, -1 when unknown
   * @param {number} position.currentLeaderEpoch the cached leader epoch, -1 when unknown
   * @returns {{ action: 'unchanged'|'awaiting'|'validated', generation: number }} the assignment
   *   generation observed here, which `complete()` has to be given back so that a reply from a
   *   previous assignment cannot be applied to this one
   */
  request(topic, partition, { offset, trackedEpoch, currentLeaderEpoch }) {
    const generation = this.assignmentGeneration
    const key = this.getKey(topic, partition)
    const entry = this.entries.get(key)

    if (entry && entry.validatedAgainst === currentLeaderEpoch) {
      return { action: 'unchanged', generation }
    }

    if (
      currentLeaderEpoch === UNKNOWN_EPOCH &&
      trackedEpoch !== UNKNOWN_EPOCH &&
      entry &&
      entry.validatedAgainst === undefined
    ) {
      return { action: 'unchanged', generation }
    }

    if (trackedEpoch === UNKNOWN_EPOCH || currentLeaderEpoch === UNKNOWN_EPOCH) {
      this.markValidated(topic, partition, currentLeaderEpoch)
      return { action: 'validated', generation }
    }

    const held = entry?.validatedAgainst === undefined ? entry : undefined
    this.entries.set(key, {
      validatedAgainst: undefined,
      pending: { offset, trackedEpoch, currentLeaderEpoch },
      failures: held?.failures ?? 0,
      heldSince: held?.heldSince,
      warnedAt: held?.warnedAt,
    })

    return { action: 'awaiting', generation }
  }

  /**
   * Records the partition as validated against `currentLeaderEpoch` without a reply, and takes it
   * out of `awaiting`.
   *
   * This is the exit the caller uses when no request can be made at all: a leader that cannot
   * serve the API. A request that failed holds the partition instead (`fail()`).
   *
   * Generation-guarded like `complete()`: an exit that belongs to a previous assignment writes
   * nothing, so a fetcher finishing its round after a rebalance cannot record another fetcher's
   * fresh entry as validated against the epoch it observed.
   *
   * @param {string} topic
   * @param {number} partition
   * @param {number} currentLeaderEpoch
   * @param {number} [generation] the generation `request()` returned for this partition; defaults
   *   to the current one, which is the caller that has no round to be stale against
   * @returns {boolean} whether it was recorded; `false` means the generation was stale and nothing
   *   was written, which is also the caller's signal to leave the partition's tracked epoch alone
   */
  markValidated(topic, partition, currentLeaderEpoch, generation = this.assignmentGeneration) {
    if (generation !== this.assignmentGeneration) {
      return false
    }

    this.entries.set(this.getKey(topic, partition), {
      validatedAgainst: currentLeaderEpoch,
      pending: null,
    })

    return true
  }

  /**
   * Whether this partition's position has to be examined this round, without allocating anything
   * beyond the map key.
   *
   * The steady state of a fetch round is that nothing needs validating, and this is the check that
   * keeps that round free of the offset read, the position literal and `request()`'s return
   * object. `request()` remains the only path into `awaiting`.
   *
   * @param {string} topic
   * @param {number} partition
   * @param {number} currentLeaderEpoch the cached leader epoch
   * @param {number} [now] the monotonic time (`performance.now()`) a held partition's backoff is
   *   compared with
   * @returns {boolean} true when the partition is unvalidated, held, or was validated against a
   *   different epoch; false when it is already validated against this epoch, a request is in
   *   flight for it (whoever put it there owns its exit), or it failed under this same leader
   *   epoch and its backoff has not passed
   */
  needsValidation(topic, partition, currentLeaderEpoch, now) {
    const entry = this.entries.get(this.getKey(topic, partition))

    if (!entry) {
      return true
    }

    if (entry.pending) {
      return false
    }

    if (entry.validatedAgainst !== undefined) {
      return entry.validatedAgainst !== currentLeaderEpoch
    }

    return entry.failedAgainst !== currentLeaderEpoch || !(entry.retryAt > now)
  }

  /**
   * Applies the reply to one partition of an OffsetForLeaderEpoch request.
   *
   * Five guards decide whether the reply is still relevant, and a reply that fails any of them is
   * `'discarded'`: the assignment generation is stale, the partition is no longer awaiting, the
   * position moved under the request, a user seek has been queued for it meanwhile, or metadata
   * names another leader or leader epoch than the request asked. A discard ends the request here,
   * in this call — skipping the call instead would leave it awaiting with no request in flight, and
   * it would never be fetched again. The two kinds of discard differ in what they write: a moved
   * position, a queued seek or a moved leader leaves *that* key held with no backoff, so the next
   * round asks again, while a stale generation or an absent key writes nothing at all, so a late
   * reply cannot resurrect an entry the new assignment may not own, nor undo the validation of a
   * partition the new assignment has since validated itself.
   *
   * `'retry'` and `'fatal'` leave the state alone; the caller holds the partition with `fail()`
   * and asks again in a later round. `'truncated'` and `'reset'` leave the partition held, not
   * validated: the caller confirms it with `confirm()` once the reposition has been applied.
   *
   * @param {string} topic
   * @param {number} partition
   * @param {object} currentPosition the `{ offset, trackedEpoch }` pair read *again now*, not the
   *   pair captured when the request was made — comparing the captured pair with itself would
   *   never detect a move
   * @param {string} currentPosition.offset
   * @param {number} currentPosition.trackedEpoch
   * @param {object} reply
   * @param {number} reply.errorCode the partition's error code, 0 when it succeeded
   * @param {string|number|import('../utils/long')} reply.endOffset the end offset of the tracked
   *   epoch, -1 when undefined; compared as a 64-bit value, never as a string, so the decoded form
   *   of the int64 cannot change the outcome
   * @param {number} reply.leaderEpoch the epoch the broker answered with, -1 when undefined
   * @param {boolean} reply.seekQueued whether a user seek is queued for this partition; a guard
   *   the validator cannot evaluate itself
   * @param {boolean} [reply.leaderMoved] whether metadata now names another leader or leader epoch
   *   than the request was sent to; the other guard the validator cannot evaluate itself
   * @param {number} reply.generation the generation `request()` returned for this partition
   * @param {object} [reply.requestEntry] the entry captured before sending the request
   * @returns {{ action: 'discarded'|'validated'|'truncated'|'reset'|'retry'|'fatal', divergingOffset: string|null, leaderEpoch: number }}
   *   `divergingOffset` is the offset to resume from on `'truncated'`, `null` otherwise
   */
  complete(
    topic,
    partition,
    currentPosition,
    { errorCode, endOffset, leaderEpoch, seekQueued, leaderMoved, generation, requestEntry }
  ) {
    const key = this.getKey(topic, partition)
    const entry = this.entries.get(key)

    if (generation !== this.assignmentGeneration) {
      return this.discard(topic, partition, 'stale generation')
    }

    if (!entry?.pending || entry.pending === APPLYING || (requestEntry && requestEntry !== entry)) {
      return this.discard(topic, partition, 'not awaiting')
    }

    const { pending } = entry
    const positionMoved =
      currentPosition.offset !== pending.offset ||
      currentPosition.trackedEpoch !== pending.trackedEpoch

    if (seekQueued || positionMoved || leaderMoved) {
      entry.pending = null
      const reason = seekQueued ? 'seek queued' : positionMoved ? 'position moved' : 'leader moved'
      return this.discard(topic, partition, reason)
    }

    const action = validationPartitionAction(errorCode)
    if (action !== 'ok') {
      return {
        action: action === 'retry' ? 'retry' : 'fatal',
        divergingOffset: null,
        leaderEpoch: UNKNOWN_EPOCH,
      }
    }

    const end = Long.fromValue(endOffset)

    if (end.equals(UNKNOWN_EPOCH) || Long.fromValue(leaderEpoch).equals(UNKNOWN_EPOCH)) {
      entry.pending = APPLYING
      return { action: 'reset', divergingOffset: null, leaderEpoch: UNKNOWN_EPOCH }
    }

    if (end.lessThan(Long.fromValue(currentPosition.offset))) {
      entry.pending = APPLYING
      return { action: 'truncated', divergingOffset: end.toString(), leaderEpoch }
    }

    this.markValidated(topic, partition, pending.currentLeaderEpoch)
    return { action: 'validated', divergingOffset: null, leaderEpoch }
  }

  /**
   * Marks a held partition validated once its truncation or reset outcome has been applied.
   * Identity-guarded like `release()`: a partition replaced, forgotten or cleared since is left
   * alone.
   * @param {{topic: string, partition: number, entry: object, currentLeaderEpoch: number}} item
   *   the awaiting item, its entry captured with `entryOf`
   * @returns {void}
   */
  confirm({ topic, partition, entry, currentLeaderEpoch }) {
    if (entry !== undefined && this.entries.get(this.getKey(topic, partition)) === entry) {
      this.markValidated(topic, partition, currentLeaderEpoch)
    }
  }

  /**
   * Holds the given partitions after their validation failed, so they are not fetched, and sets
   * each one's retry backoff: 100 ms after the first failure, doubling up to 1000 ms. Identity-
   * guarded like `release()`: an entry validated, replaced or forgotten since is left alone, and so
   * is one whose attempt already ended (held, or discarded by `complete()` or `supersede()`).
   * @param {{topic: string, partition: number, entry: object, currentLeaderEpoch: number}[]} items
   *   awaiting items, each entry captured with `entryOf`
   * @param {number} now the monotonic time (`performance.now()`) the failure is recorded at
   * @returns {object[]} the items whose entry this call held, for the caller's warning
   */
  fail(items, now) {
    const held = []
    for (const item of items) {
      const entry = this.entries.get(this.getKey(item.topic, item.partition))
      if (entry === undefined || entry !== item.entry || !entry.pending) continue

      entry.pending = null
      entry.failures += 1
      entry.retryAt =
        now + Math.min(RETRY_BACKOFF_MS * 2 ** (entry.failures - 1), MAX_RETRY_BACKOFF_MS)
      entry.failedAgainst = item.currentLeaderEpoch
      entry.heldSince ??= now
      held.push(item)
    }
    return held
  }

  /**
   * Ends an attempt whose leader or leader epoch was replaced before it could be used: the entry
   * stays held with no backoff, so the next round asks under the new metadata. Identity-guarded
   * like `confirm()`. Never deletes the entry, which would let the partition be fetched unchecked.
   * @param {{topic: string, partition: number, entry: object}} item the awaiting item
   * @returns {void}
   */
  supersede({ topic, partition, entry }) {
    if (entry !== undefined && this.entries.get(this.getKey(topic, partition)) === entry) {
      entry.pending = null
    }
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean} whether the partition is held after a failed validation (`fail()`) and not
   *   asked again since; such a partition waits for the metadata refresh its failure owes
   */
  isHeldAfterFailure(topic, partition) {
    const entry = this.entries.get(this.getKey(topic, partition))
    return (
      entry !== undefined &&
      entry.validatedAgainst === undefined &&
      !entry.pending &&
      entry.retryAt !== undefined
    )
  }

  /**
   * Whether this partition must be kept out of a Fetch request, or its answer dropped.
   * @param {string} topic
   * @param {number} partition
   * @param {number} [currentLeaderEpoch] the epoch the Fetch carries; without it only awaiting,
   *   applying and held entries hold
   * @returns {boolean} whether the partition is awaiting, applying, held or validated for a
   *   different epoch. An absent entry does not hold Fetch.
   */
  holdsFetch(topic, partition, currentLeaderEpoch) {
    const entry = this.entries.get(this.getKey(topic, partition))
    if (entry === undefined) return false
    const { validatedAgainst } = entry
    return (
      validatedAgainst === undefined ||
      (currentLeaderEpoch !== undefined && validatedAgainst !== currentLeaderEpoch)
    )
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {object|undefined} the entry object `request()` just created or kept; its identity
   *   is what `release()` compares, so a replacement entry is never mistaken for it
   */
  entryOf(topic, partition) {
    return this.entries.get(this.getKey(topic, partition))
  }

  /**
   * Returns the given entries to unvalidated, by identity, so a held or abandoned reply is not
   * needed to remove orphan awaiting state. An entry that was validated, replaced or forgotten
   * since is a different object and is left alone: a late call cannot delete a replacement entry.
   * @param {{topic: string, partition: number, entry: object}[]} entries entries captured with
   *   `entryOf`
   * @returns {void}
   */
  release(entries) {
    for (const { topic, partition, entry } of entries) {
      const key = this.getKey(topic, partition)
      if (this.entries.get(key) === entry) this.entries.delete(key)
    }
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean} whether a request for this partition is in flight
   */
  isAwaiting(topic, partition) {
    const pending = this.entries.get(this.getKey(topic, partition))?.pending
    return Boolean(pending && pending !== APPLYING)
  }

  /**
   * Diagnostic accessor: nothing in the consumer reads it, and the decision it would support is
   * `needsValidation()`'s. It is kept because the state it reports is the class's whole contract
   * and is otherwise only observable through a request.
   *
   * @param {string} topic
   * @param {number} partition
   * @returns {number|undefined} the leader epoch the position was last validated against, or
   *   `undefined` for a partition that is unvalidated or awaiting a reply
   */
  validatedAgainst(topic, partition) {
    const entry = this.entries.get(this.getKey(topic, partition))
    return entry ? entry.validatedAgainst : undefined
  }

  /**
   * Drops the state of the given partitions of one topic, so they are validated again from
   * scratch. Used when a topic is recreated: what a partition was validated against belonged to
   * the previous incarnation.
   * @param {string} topic
   * @param {number[]} [partitions]
   * @returns {void}
   */
  forgetTopic(topic, partitions) {
    for (const partition of partitions || []) {
      this.entries.delete(this.getKey(topic, partition))
    }
  }

  /**
   * Drops the state of partitions this member no longer holds, WITHOUT bumping the assignment
   * generation.
   *
   * A bump would strand every retained partition that has a validation in flight: `complete()`
   * discards on a generation mismatch and writes nothing, `needsValidation()` returns false while
   * a request is pending, and an awaiting partition is excluded from the fetch - so it would never
   * be fetched again. Deleting only the revoked keys gives the fence that is actually needed: a
   * late reply for a deleted entry hits the absent-key guard in `complete()` and is discarded.
   *
   * @param {Array<{topic: string, partitions: number[]}>} topicPartitions
   * @returns {void}
   */
  forgetPartitions(topicPartitions) {
    for (const { topic, partitions } of topicPartitions) {
      for (const partition of partitions) {
        this.entries.delete(this.getKey(topic, partition))
      }
    }
  }

  /**
   * Drops every partition and stamps the next assignment generation. Called when the group syncs:
   * a partition that was revoked and reassigned must not keep counting as validated against an
   * epoch it no longer holds, and a reply to a request the previous assignment made must no longer
   * be applied.
   * @returns {void}
   */
  clear() {
    this.entries.clear()
    this.assignmentGeneration++
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {string}
   */
  getKey(topic, partition) {
    return `${topic}|${partition}`
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @param {string} reason
   * @returns {{ action: 'discarded', divergingOffset: null, leaderEpoch: number }}
   */
  discard(topic, partition, reason) {
    this.logger.debug('Discarding position validation reply', { topic, partition, reason })
    return { action: 'discarded', divergingOffset: null, leaderEpoch: UNKNOWN_EPOCH }
  }
}
