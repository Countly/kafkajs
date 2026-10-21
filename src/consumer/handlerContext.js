const { AsyncLocalStorage } = require('node:async_hooks')

const storage = new AsyncLocalStorage()

/**
 * The fields `Runner#isBatchStale` reads.
 * @typedef {{ topic: string, partition: number, assignmentEpoch: number|null, acquireClock: number, ownershipKey: string }} BatchIdentity
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

module.exports = { run, current }
