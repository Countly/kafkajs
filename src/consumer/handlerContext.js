const { AsyncLocalStorage } = require('node:async_hooks')

const storage = new AsyncLocalStorage()

/**
 * One partition acquisition of one run: the consumer group keeps one per owned partition and
 * replaces it, never reuses it, when the partition is acquired again. `retired` latches once the
 * acquisition ends (revoke, rebuild, end of the run). Holds numbers only, never a batch or a client.
 * @typedef {{ acquiredAt: number, changedAt: number, recreatedAt: number, soughtAt: number, retired: boolean }} Acquisition
 */

/**
 * The fields `Runner#isBatchStale` reads, plus the acquisition the batch was fetched under.
 * @typedef {{ topic: string, partition: number, assignmentEpoch: number|null, acquireClock: number, ownershipKey: string, acquisition?: Acquisition }} BatchIdentity
 */

/**
 * Carries one batch's ownership identity through its handlers and any asynchronous work they
 * start. Only the identity is stored, never the batch: every async resource created inside keeps
 * its store reachable for its whole lifetime.
 * @param {BatchIdentity} identity the batch's ownership identity
 * @param {() => unknown} work the entire batch handler loop
 * @returns {unknown} the exact return value or promise from work
 * @throws {Error} a synchronous error from work, unchanged
 */
const run = (identity, work) => storage.run(identity, work)

/** @returns {BatchIdentity|undefined} the calling handler's batch ownership identity, absent in detached work */
const current = () => storage.getStore()

/**
 * Whether a seek of the identity's partition was admitted after its batch was fetched.
 * @param {BatchIdentity} identity
 * @returns {boolean} false for an identity without an acquisition
 */
const isSought = (identity) =>
  identity.acquisition !== undefined && identity.acquisition.soughtAt > identity.acquireClock

/**
 * The seek authority fence, decided from the identity alone: its batch was superseded by a seek
 * or a topic recreation after it was fetched, or the acquisition it was fetched under ended.
 * Once true it stays true. Such a batch may neither resolve nor commit an offset.
 * @param {BatchIdentity} identity
 * @returns {boolean} false for an identity without an acquisition
 */
const isSuperseded = (identity) => {
  const acquisition = identity.acquisition
  return (
    acquisition !== undefined &&
    (acquisition.retired ||
      acquisition.soughtAt > identity.acquireClock ||
      acquisition.recreatedAt > identity.acquireClock)
  )
}

const STALE_BATCH = Symbol('handlerContext:staleBatch')

/**
 * Marks a commit refusal that says only that the calling batch lost its authority to commit (a
 * seek, a recreation or the end of its acquisition superseded it): what the broker already
 * accepted for such a commit is still recorded, while the caller gets the refusal.
 * @template {Error} T
 * @param {T} error the refusal
 * @returns {T} the same error, marked
 */
const markStaleBatch = (error) => {
  error[STALE_BATCH] = true
  return error
}

/**
 * @param {unknown} error a commit refusal
 * @returns {boolean} whether it only refuses a superseded batch (`markStaleBatch`)
 */
const isStaleBatch = (error) => error?.[STALE_BATCH] === true

module.exports = { run, current, isSought, isSuperseded, markStaleBatch, isStaleBatch }
