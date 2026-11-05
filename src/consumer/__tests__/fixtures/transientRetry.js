const { bounded, delay } = require('../../../producer/__tests__/fixtures/bounded')

/** The pause between two attempts, ms. */
const RETRY_PAUSE_MS = 250

/**
 * @param {unknown} error
 * @returns {boolean} whether the client marks the failure retriable, as it does a reset connection
 */
const isRetriable = (error) => error?.retriable === true

/**
 * Runs one lookup against a cluster that may still be settling, and runs it again, on the same
 * input, while it fails with a retriable error. Any other failure is thrown at once.
 * @template T
 * @param {AbortSignal} scope the owner's; no lookup starts once it has aborted
 * @param {number} ms the budget of the lookup with all its retries
 * @param {() => Promise<T>} op the lookup
 * @param {object} [options]
 * @param {string} [options.label] names the lookup in its time-out message
 * @param {number} [options.delayMs] the pause between attempts
 * @returns {Promise<T>} what `op` returned
 * @throws {Error} a failure that is not retriable; `<label> did not settle within <ms> ms`; or the
 *   reason the scope aborted with
 */
const retryTransient = (scope, ms, op, { label, delayMs = RETRY_PAUSE_MS } = {}) =>
  bounded(
    scope,
    ms,
    async (inner) => {
      for (;;) {
        try {
          return await bounded(inner, Infinity, op)
        } catch (error) {
          if (inner.aborted || !isRetriable(error)) throw error
          await delay(inner, delayMs)
        }
      }
    },
    { label }
  )

module.exports = { retryTransient }
