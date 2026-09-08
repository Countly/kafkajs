const EventEmitter = require('events')
const Long = require('../utils/long')

/**
 * Fetches data from all assigned nodes, waits for workerQueue to drain and repeats.
 *
 * With `maxPrefetchBatches: 1` the fetcher may keep one Fetch in flight while the previous
 * response's batches are with the workers, but only for a dispatch whose every partition fully
 * resolved on its PREVIOUS iteration. The run-ahead offsets travel as an ephemeral per-request
 * override and are stored nowhere, so nothing a handler does or does not resolve can move the
 * client's idea of where to read next; a prefetched batch that no longer matches the resolved
 * offset when it is dispatched is dropped by the runner and re-read by the next ordinary Fetch.
 *
 * @param {object} options
 * @param {number} options.nodeId
 * @param {import('./workerQueue').WorkerQueue} options.workerQueue
 * @param {Map<string, string[]>} options.partitionAssignments
 * @param {(nodeId: number, options?: { offsetOverrides: Map<string, string>, prefetch: boolean }) => Promise<T[]>} options.fetch
 * @param {import('../../types').Logger} options.logger
 * @param {0|1} [options.maxPrefetchBatches=0] how many prefetched batches may be in flight ahead
 *        of the workers. `0` disables the feature and reaches no new code path.
 * @param {(batch: T) => boolean} [options.isFullyResolved] whether the handler resolved a batch
 *        to its end. Injected because a fetcher cannot reach an offset; absent means never
 *        eligible, so the fetcher behaves as if the knob were `0`.
 * @template T
 */
const createFetcher = ({
  nodeId,
  workerQueue,
  partitionAssignments,
  fetch,
  logger: rootLogger,
  maxPrefetchBatches = 0,
  isFullyResolved,
}) => {
  const logger = rootLogger.namespace(`Fetcher ${nodeId}`)
  const emitter = new EventEmitter()
  let isRunning = false

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

  const prefetchEnabled = maxPrefetchBatches > 0 && typeof isFullyResolved === 'function'

  /** @param {object} batch @returns {string} the offset one past the batch's last record */
  const nextAfter = (batch) => Long.fromValue(batch.lastOffset()).add(1).toString()

  /**
   * Whether one Fetch may run ahead of this dispatch. Eligibility is read from the PREVIOUS
   * iteration, which is both the question that matters ("did the last batch on this partition get
   * fully handled?") and the only one answerable here: at dispatch time the batch in hand has not
   * been handled yet.
   * @param {object[]} batches the batches just handed to the workers
   * @param {Map<string, boolean>|null} fullyResolved the previous dispatch's verdicts, `null` on
   *        the first iteration after a start or a fetcher rebuild
   * @returns {boolean}
   */
  const canPrefetch = (batches, fullyResolved) =>
    prefetchEnabled &&
    fullyResolved !== null &&
    batches.every((batch) => fullyResolved.get(assignmentKey(batch)) === true)

  /** @param {object[]} batches @returns {Map<string, string>} run-ahead offset per partition */
  const buildOffsetOverrides = (batches) =>
    new Map(batches.map((batch) => [assignmentKey(batch), nextAfter(batch)]))

  const start = async () => {
    if (isRunning) return
    isRunning = true

    /** The in-flight prefetch, consumed by the next iteration's head. */
    let pending = null
    /** The previous dispatch's per-partition resolution verdicts; `null` before the first one. */
    let fullyResolved = null

    while (isRunning) {
      try {
        const batches = pending ? await pending : await fetch(nodeId)
        pending = null

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
            const pushed = workerQueue.push(...availableBatches)

            if (canPrefetch(availableBatches, fullyResolved)) {
              pending = fetch(nodeId, {
                offsetOverrides: buildOffsetOverrides(availableBatches),
                prefetch: true,
              })
              // Attached in the same turn the promise is created: a rejection observed later than
              // this - a dropped connection, or a fetcher rebalance - terminates the process on
              // Node 22+ before the loop head could reach it.
              pending.catch(() => {})
            }

            try {
              await pushed
            } finally {
              if (prefetchEnabled) {
                fullyResolved = new Map(
                  availableBatches.map((batch) => [assignmentKey(batch), isFullyResolved(batch)])
                )
              }
              availableBatches.forEach(unassignTopicPartition)
            }
          }
        }
      } catch (error) {
        isRunning = false

        if (pending) {
          await pending.catch(() => {})
          pending = null
        }

        emitter.emit('end')
        throw error
      }
    }

    if (pending) {
      await pending.catch(() => {})
      pending = null
    }

    emitter.emit('end')
  }

  const stop = async () => {
    if (!isRunning) return
    isRunning = false
    await new Promise((resolve) => emitter.once('end', () => resolve()))
  }

  return { start, stop, getWorkerQueue }
}

module.exports = createFetcher
