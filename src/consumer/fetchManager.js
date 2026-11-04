const seq = require('../utils/seq')
const createFetcher = require('./fetcher')
const createWorker = require('./worker')
const createWorkerQueue = require('./workerQueue')
const { KafkaJSNoBrokerAvailableError } = require('../errors')

/** @typedef {ReturnType<typeof createFetchManager>} FetchManager */

/**
 * One fetcher and the lifetime of its `start()` promise.
 * @typedef {object} FetcherRun
 * @property {ReturnType<typeof createFetcher>} fetcher
 * @property {boolean} retiring set once the fetcher was asked to stop because its node left
 * @property {Promise<void>} done settles, never rejecting, once the fetcher has fully stopped
 */

/**
 * @param {Set<number>} wanted the node ids the manager fetches from
 * @param {number[]} current the node ids the cluster advertises now
 * @returns {boolean} whether the two differ; a plain comparison, so a round pays no allocation
 */
const differs = (wanted, current) => {
  if (current.length !== wanted.size) return true
  for (const nodeId of current) if (!wanted.has(nodeId)) return true
  return false
}

/**
 * Runs one fetcher per broker node and keeps that set equal to the cluster's node set.
 *
 * When the node set changes the manager reconciles instead of restarting: the fetchers of nodes
 * that stay are never interrupted, a node that joined gets a fetcher at once, and the fetcher of a
 * node that left is stopped on its own while the others carry on. At most one fetcher exists per
 * node: a node that returns while its retiring fetcher still drains gets its new fetcher only when
 * the old one has fully stopped, which keeps one Fetch in flight per node and the fetch-session
 * epochs serial. Kafkajs 2.2.4 stopped and awaited every fetcher before recreating them all.
 *
 * @param {object} options
 * @param {import('../../types').Logger} options.logger
 * @param {() => number[]} options.getNodeIds the node ids the cluster advertises now
 * @param {(nodeId: number) => Promise<import('../../types').Batch[]>} options.fetch
 * @param {import('./worker').Handler<T>} options.handler
 * @param {number} [options.concurrency]
 * @template T
 */
const createFetchManager = ({
  logger: rootLogger,
  getNodeIds,
  fetch,
  handler,
  concurrency = 1,
}) => {
  const logger = rootLogger.namespace('FetchManager')
  const workers = seq(concurrency, (workerId) => createWorker({ handler, workerId }))
  const workerQueue = createWorkerQueue({ workers })

  /** Shared by every fetcher, across reconciliations, so no two process one partition at once. */
  const partitionAssignments = new Map()
  /** @type {Map<number, FetcherRun>} every fetcher not yet fully stopped, at most one per node */
  const runs = new Map()
  /** @type {Set<number>} the nodes the manager fetches from, as of the last reconciliation */
  let wanted = new Set()
  /**
   * Latched by the public `stop()` only. A stop must also end the reconciliation, which would
   * otherwise start the fetchers of a node that returns while the manager is stopping.
   */
  let stopped = false
  /** @type {{ error: unknown }|null} the first error a fetcher ended with in this run */
  let failure = null
  /** True while `sync` runs, so the first round of a fetcher it starts cannot reconcile re-entrantly. */
  let reconciling = false
  /** @type {(() => void)|null} settles the running `start()` once no fetcher is left */
  let settleStart = null

  /**
   * @returns {Array<ReturnType<typeof createFetcher>>} the fetchers of the current node set; a
   *   fetcher that is retiring is not listed
   */
  const getFetchers = () => {
    const fetchers = []
    for (const run of runs.values()) if (!run.retiring) fetchers.push(run.fetcher)
    return fetchers
  }

  /**
   * Runs once per fetcher round, before its Fetch. When the cluster's node set differs from the
   * one in use it reconciles; an empty node list is not a change, so the Fetch itself reports a
   * cluster with no broker, as before.
   * @param {number} nodeId the node the calling fetcher serves
   * @returns {Promise<import('../../types').Batch[]>} the round's batches; none when the node has
   *   left, so a fetcher of a departed node issues no further Fetch
   */
  const round = async (nodeId) => {
    if (!reconciling && !stopped && !failure) {
      const current = getNodeIds()
      if (current.length !== 0 && differs(wanted, current)) reconcile(current)
    }

    return wanted.has(nodeId) ? fetch(nodeId) : []
  }

  /**
   * Adopts a new node set and brings the fetchers to it. Synchronous, so reconciliations are
   * serialized by construction and a second detection of the same change finds nothing to do.
   * @param {number[]} current the cluster's non-empty node set
   * @returns {void}
   */
  const reconcile = (current) => {
    const next = new Set(current)
    const added = current.filter((nodeId) => !wanted.has(nodeId))
    const removed = [...wanted].filter((nodeId) => !next.has(nodeId))

    logger.debug('Reconciling fetchers', { added, removed })
    wanted = next
    sync()
  }

  /**
   * Makes the fetchers match `wanted`: asks the fetcher of a node that left to stop, and starts a
   * fetcher for each wanted node that has none. A node whose retiring fetcher still drains is
   * started by that fetcher's release. Starts nothing once the manager is stopped or has failed.
   * @returns {void}
   */
  const sync = () => {
    reconciling = true
    try {
      for (const [nodeId, run] of runs) {
        if (!wanted.has(nodeId) && !run.retiring) {
          run.retiring = true
          run.fetcher.stop()
        }
      }

      if (stopped || failure) return

      for (const nodeId of wanted) {
        if (!runs.has(nodeId)) startRun(nodeId)
      }
    } finally {
      reconciling = false
    }
  }

  /**
   * Starts the fetcher of a node. The run is registered before the fetcher starts, because the
   * fetcher's first round runs synchronously.
   * @param {number} nodeId
   * @returns {void}
   */
  const startRun = (nodeId) => {
    const fetcher = createFetcher({
      nodeId,
      workerQueue,
      partitionAssignments,
      fetch: round,
      logger,
    })
    /** @type {FetcherRun} */
    const run = { fetcher, retiring: false, done: null }
    runs.set(nodeId, run)
    logger.debug('Fetcher started', { nodeId })
    run.done = fetcher
      .start()
      .then(undefined, fail)
      .then(() => release(nodeId, run))
  }

  /**
   * Runs once a fetcher has fully stopped: forgets it, starts the fetcher of a node that returned
   * while it drained, and settles `start()` when no fetcher is left.
   * @param {number} nodeId
   * @param {FetcherRun} run
   * @returns {void}
   */
  const release = (nodeId, run) => {
    if (runs.get(nodeId) === run) runs.delete(nodeId)
    logger.debug('Fetcher stopped', { nodeId })
    sync()

    if (runs.size === 0 && settleStart) {
      settleStart()
      settleStart = null
    }
  }

  /**
   * Records the first error a fetcher ended with and stops every other fetcher, which `start()`
   * then waits for before it rethrows.
   * @param {unknown} error
   * @returns {void}
   */
  const fail = (error) => {
    if (failure) return
    failure = { error }
    stopFetchers().catch((stopError) =>
      logger.debug('Error while stopping fetchers', { error: stopError.message })
    )
  }

  /**
   * Stops every fetcher that exists, retiring ones included, and waits until each has fully
   * stopped. Does not latch.
   * @returns {Promise<void>}
   */
  const stopFetchers = async () => {
    logger.debug('Stopping fetchers...')
    const all = [...runs.values()]
    await Promise.all(all.map((run) => run.fetcher.stop()))
    await Promise.all(all.map((run) => run.done))
    logger.debug('Stopped fetchers')
  }

  /**
   * Runs one fetcher per broker node until stopped or until a fetcher throws. A change of the
   * node set reconciles the fetchers in place and never ends this call. Clears the stop latch on
   * entry, so a manager can be restarted.
   * @returns {Promise<void>} resolves once `stop()` has drained every fetcher
   * @throws {KafkaJSNoBrokerAvailableError} when the cluster advertises no node at the start
   * @throws {Error} the first error a fetcher ended with, after every fetcher has stopped
   */
  const start = async () => {
    logger.debug('Starting...')
    stopped = false
    failure = null

    const nodeIds = getNodeIds()
    if (nodeIds.length === 0) {
      throw new KafkaJSNoBrokerAvailableError()
    }

    wanted = new Set(nodeIds)
    const drained = new Promise((resolve) => {
      settleStart = resolve
    })
    sync()
    await drained

    if (failure) {
      throw failure.error
    }
  }

  /**
   * Stops the manager for good, until the next `start()`, and drains every fetcher. Errors never
   * propagate: callers fire this without awaiting it from the rebalance path, so a rejection here
   * would be unobserved.
   * @returns {Promise<void>}
   */
  const stop = async () => {
    stopped = true

    try {
      await stopFetchers()
    } catch (error) {
      logger.debug('Error while stopping fetchers', { error: error.message })
    }
  }

  return { start, stop, getFetchers }
}

module.exports = createFetchManager
