'use strict'

/**
 * Subscribes to a named fixture topic after creation. Leader readiness on the controller does not
 * imply that every broker has learned the topic yet. Only UNKNOWN_TOPIC_OR_PARTITION (including
 * a wrapped cause) is retried, every 100 ms for at most five seconds; auto-creation is unchanged.
 * @param {{ subscribe: (options: object) => Promise<void> }} consumer connected public consumer
 * @param {{ topic: string, fromBeginning: boolean }} options reused unchanged on every attempt
 * @param {() => boolean} [isStopped] whether the owner has begun stopping
 * @returns {Promise<boolean>} true after subscription succeeds, false when stopped
 * @throws {Error} the last topic error at the deadline, or any other error immediately
 */
const subscribeNamedTopic = async (consumer, options, isStopped = () => false) => {
  const deadline = Date.now() + 5000
  let lastError
  while (!isStopped()) {
    if (lastError && Date.now() >= deadline) throw lastError
    try {
      await consumer.subscribe(options)
      return !isStopped()
    } catch (error) {
      if (
        error.type !== 'UNKNOWN_TOPIC_OR_PARTITION' &&
        error.cause?.type !== 'UNKNOWN_TOPIC_OR_PARTITION'
      )
        throw error
      lastError = error
      const remaining = deadline - Date.now()
      if (remaining <= 0) throw lastError
      if (isStopped()) return false
      await new Promise((resolve) => setTimeout(resolve, Math.min(100, remaining)))
    }
  }
  return false
}

module.exports = { subscribeNamedTopic }
