const EventEmitter = require('events')

/**
 * Fetches data from all assigned nodes, waits for workerQueue to drain and repeats.
 *
 * A failed `fetch` the caller's `retryDelay` accepts is retried by this fetcher after that delay,
 * so the failure of one node never reaches the fetchers of the others. `stop()` cancels the wait.
 * Any other `fetch` error, and every worker error, ends the fetcher and rejects `start()`.
 *
 * @param {object} options
 * @param {number} options.nodeId
 * @param {import('./workerQueue').WorkerQueue} options.workerQueue
 * @param {Map<string, string[]>} options.partitionAssignments
 * @param {(nodeId: number) => Promise<T[]>} options.fetch
 * @param {import('../../types').Logger} options.logger
 * @param {(error: Error) => number|undefined} [options.retryDelay] called with a failed `fetch`'s
 *   error: the milliseconds to wait before fetching again, or undefined to fail the fetcher
 * @template T
 */
const createFetcher = ({
  nodeId,
  workerQueue,
  partitionAssignments,
  fetch,
  retryDelay,
  logger: rootLogger,
}) => {
  const logger = rootLogger.namespace(`Fetcher ${nodeId}`)
  const emitter = new EventEmitter()
  let isRunning = false
  /** @type {(() => void)|null} ends the retry wait early; set only while one is pending */
  let wakeBackoff = null

  const getWorkerQueue = () => workerQueue
  const assignmentKey = ({ topic, partition }) => `${topic}|${partition}`
  const getAssignedFetcher = (batch) => partitionAssignments.get(assignmentKey(batch))
  const assignTopicPartition = (batch) => partitionAssignments.set(assignmentKey(batch), nodeId)
  const unassignTopicPartition = (batch) => partitionAssignments.delete(assignmentKey(batch))
  const filterUnassignedBatches = (batches) =>
    batches.filter((batch) => {
      const assignedFetcher = getAssignedFetcher(batch)
      if (assignedFetcher != null && assignedFetcher !== nodeId) {
        logger.info(
          'Filtering out batch due to partition already being processed by another fetcher',
          {
            topic: batch.topic,
            partition: batch.partition,
            assignedFetcher,
            fetcher: nodeId,
          }
        )
        return false
      }

      return true
    })

  /**
   * One round's Fetch. A failure `retryDelay` accepts is waited out here, unless the fetcher was
   * stopped meanwhile, and the round yields no batches.
   * @returns {Promise<T[]>}
   * @throws {Error} the Fetch's error when `retryDelay` does not accept it
   */
  const fetchOrBackOff = async () => {
    try {
      return await fetch(nodeId)
    } catch (error) {
      const delay = retryDelay?.(error)
      if (delay === undefined) throw error

      if (isRunning) {
        await new Promise((resolve) => {
          const timer = setTimeout(resolve, delay)
          wakeBackoff = () => {
            clearTimeout(timer)
            resolve()
          }
        })
        wakeBackoff = null
      }

      return []
    }
  }

  const start = async () => {
    if (isRunning) return
    isRunning = true

    while (isRunning) {
      try {
        const batches = await fetchOrBackOff()

        // Yield floor. A normal empty round has already long-polled `maxWaitTime` on the wire, so
        // this costs nothing; it turns an iteration that resolved in microtasks only - a fetch
        // short-circuited while the runner is stopping - into a live loop instead of a hang.
        if (batches.length === 0) {
          await new Promise((resolve) => setImmediate(resolve))
        }

        if (isRunning) {
          const availableBatches = filterUnassignedBatches(batches)

          if (availableBatches.length > 0) {
            availableBatches.forEach(assignTopicPartition)
            try {
              await workerQueue.push(...availableBatches)
            } finally {
              availableBatches.forEach(unassignTopicPartition)
            }
          }
        }
      } catch (error) {
        isRunning = false
        emitter.emit('end')
        throw error
      }
    }
    emitter.emit('end')
  }

  const stop = async () => {
    if (!isRunning) return
    isRunning = false
    wakeBackoff?.()
    await new Promise((resolve) => emitter.once('end', () => resolve()))
  }

  return { start, stop, getWorkerQueue }
}

module.exports = createFetcher
