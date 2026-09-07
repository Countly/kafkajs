const seq = require('../utils/seq')
const createFetcher = require('./fetcher')
const createWorker = require('./worker')
const createWorkerQueue = require('./workerQueue')
const { KafkaJSFetcherRebalanceError, KafkaJSNoBrokerAvailableError } = require('../errors')

/** @typedef {ReturnType<typeof createFetchManager>} FetchManager */

/**
 * @param {object} options
 * @param {import('../../types').Logger} options.logger
 * @param {() => number[]} options.getNodeIds
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

  let fetchers = []
  /**
   * Latched by the public `stop()` only. `start()` re-creates `fetchers` inside its own loop on a
   * broker-pool change, so without this flag a stop fired during that window awaits an array that
   * has already been replaced and `start()` never settles - and the whole rebalance verdict path
   * waits for it to settle.
   */
  let stopped = false

  const getFetchers = () => fetchers

  const createFetchers = () => {
    const nodeIds = getNodeIds()
    const partitionAssignments = new Map()

    if (nodeIds.length === 0) {
      throw new KafkaJSNoBrokerAvailableError()
    }

    const validateShouldRebalance = () => {
      const current = getNodeIds()
      const hasChanged =
        nodeIds.length !== current.length || nodeIds.some((nodeId) => !current.includes(nodeId))
      if (hasChanged && current.length !== 0) {
        throw new KafkaJSFetcherRebalanceError()
      }
    }

    const fetchers = nodeIds.map((nodeId) =>
      createFetcher({
        nodeId,
        workerQueue,
        partitionAssignments,
        fetch: async (nodeId) => {
          validateShouldRebalance()
          return fetch(nodeId)
        },
        logger,
      })
    )

    logger.debug(`Created ${fetchers.length} fetchers`, { nodeIds, concurrency })
    return fetchers
  }

  /**
   * Stops the fetchers that exist right now, without latching. The internal re-create path uses
   * this: an ordinary broker-pool change must tear the current set down and build a new one, not
   * end the manager.
   * @returns {Promise<void>}
   */
  const stopFetchers = async () => {
    logger.debug('Stopping fetchers...')
    await Promise.all(fetchers.map((fetcher) => fetcher.stop()))
    logger.debug('Stopped fetchers')
  }

  /**
   * Runs one fetcher per broker node until stopped or until a fetcher throws something other than
   * a broker-pool change. Clears the stop latch on entry, so a manager can be restarted.
   * @returns {Promise<void>}
   */
  const start = async () => {
    logger.debug('Starting...')
    stopped = false

    while (true) {
      if (stopped) {
        break
      }

      fetchers = createFetchers()

      try {
        await Promise.all(fetchers.map((fetcher) => fetcher.start()))
      } catch (error) {
        await stopFetchers()

        if (error instanceof KafkaJSFetcherRebalanceError) {
          if (stopped) {
            break
          }

          logger.debug('Rebalancing fetchers...')
          continue
        }

        throw error
      }

      break
    }
  }

  /**
   * Stops the manager for good, until the next `start()`. Errors never propagate: callers fire
   * this without awaiting it from the rebalance path, so a rejection here would be unobserved.
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
