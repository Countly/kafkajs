const { inspect } = require('util')
const FencedRetry = require('./fenced')
const { MAX_DELAY } = require('../producer/deadline')
const {
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSNonRetriableError,
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
} = require('../errors')

const isTestMode = process.env.NODE_ENV === 'test'
const RETRY_DEFAULT = isTestMode ? require('./defaults.test') : require('./defaults')

const random = (min, max) => {
  return Math.random() * (max - min) + min
}

const randomFromRetryTime = (factor, retryTime) => {
  const delta = factor * retryTime
  return Math.ceil(random(retryTime - delta, retryTime + delta))
}

const UNRECOVERABLE_ERRORS = ['RangeError', 'ReferenceError', 'SyntaxError', 'TypeError']
const isErrorUnrecoverable = (e) => UNRECOVERABLE_ERRORS.includes(e.name)
const isErrorRetriable = (error) =>
  (error.retriable || error.retriable !== false) && !isErrorUnrecoverable(error)

/**
 * The delay schedule over already-merged options: a first delay jittered by `factor` around
 * `initialRetryTime`, then each delay jittered around the previous one, multiplied by `multiplier`
 * and capped at `maxRetryTime`. Every delay is clamped to >= 0, so a `factor` above 1 or a
 * negative `multiplier` can never hand `setTimeout` a negative delay (Node warns and waits 1 ms).
 * Each call draws `Math.random` once.
 * @param {{ initialRetryTime: number, factor: number, multiplier: number, maxRetryTime: number }} merged
 * @returns {{ initial: () => number, next: (previous: number) => number }}
 */
const backoffOf = ({ initialRetryTime, factor, multiplier, maxRetryTime }) => ({
  initial: () => Math.max(0, randomFromRetryTime(factor, initialRetryTime)),
  next: (previous) =>
    Math.max(0, Math.min(randomFromRetryTime(factor, previous) * multiplier, maxRetryTime)),
})

/**
 * The retrier's own delay schedule, for callers that pace work the way the retrier does.
 * @param {import("../../types").RetryOptions} [opts] unset keys take the retrier's defaults
 * @returns {{ initial: () => number, next: (previous: number) => number }}
 */
const createBackoff = (opts = {}) => backoffOf(Object.assign({}, RETRY_DEFAULT, opts))

/**
 * Refuses a `maxTotalTime` that is set but is not an integer from 1 to 2147483647: `0` or a
 * negative number would reject at the first failure, a fraction or a numeric string is not a
 * millisecond count, a larger value arms timers Node runs after 1 ms (the producer's wake-up at a
 * queued batch's deadline), and silently ignoring it would hide a deadline the caller meant to
 * set.
 * @param {unknown} maxTotalTime `undefined` and `null` mean no deadline
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError}
 */
const assertMaxTotalTime = (maxTotalTime) => {
  if (maxTotalTime == null) {
    return
  }

  if (!(Number.isInteger(maxTotalTime) && maxTotalTime >= 1)) {
    throw new KafkaJSInvalidConfigurationError(
      `retry.maxTotalTime must be an integer of at least 1, got ${inspect(maxTotalTime)}`
    )
  }

  if (maxTotalTime > MAX_DELAY) {
    throw new KafkaJSInvalidConfigurationError(
      `retry.maxTotalTime must be an integer of at least 1 and at most ${MAX_DELAY}, got ${maxTotalTime}`
    )
  }
}

/**
 * Builds the `retry(retryTime, retryCount)` step: it runs `fn` and turns a failed attempt into one
 * outcome, checked in this order. A non-retriable error rejects with `KafkaJSNonRetriableError`.
 * A retriable one with no retries left rejects with `KafkaJSNumberOfRetriesExceeded`. With retries
 * left, if the wait about to be slept (`retryTime`) would end past the `maxTotalTime` deadline it
 * rejects with `KafkaJSDeliveryTimeout`, whose `cause` is the thrown error as is. Otherwise the
 * next attempt is scheduled after `retryTime`. The deadline is taken once, when this is built.
 * @param {{ retries: number, maxTotalTime?: number | null }} configs the merged retrier options
 * @param {{ next: (previous: number) => number }} backoff computes the delay after `retryTime`
 * @param {(value: any) => void} resolve
 * @param {(error: Error) => void} reject
 * @param {(bail: (err: Error) => void, retryCount: number, retryTime: number) => Promise<any>} fn
 * @returns {(retryTime: number, retryCount?: number) => void}
 */
const createRetriable = (configs, backoff, resolve, reject, fn) => {
  let aborted = false
  const { retries, maxTotalTime } = configs
  const deadline = maxTotalTime == null ? Infinity : Date.now() + maxTotalTime

  const bail = (error) => {
    aborted = true
    reject(error || new Error('Aborted'))
  }

  const retry = (retryTime, retryCount = 0) => {
    if (aborted) return

    const nextRetryTime = backoff.next(retryTime)
    const shouldRetry = retryCount < retries

    const scheduleRetry = () => {
      setTimeout(() => retry(nextRetryTime, retryCount + 1), retryTime)
    }

    fn(bail, retryCount, retryTime)
      .then(resolve)
      .catch((e) => {
        if (isErrorRetriable(e)) {
          if (!shouldRetry) {
            reject(
              new KafkaJSNumberOfRetriesExceeded(e, { retryCount, retryTime, cause: e.cause || e })
            )
          } else if (Date.now() + retryTime > deadline) {
            reject(
              new KafkaJSDeliveryTimeout(
                `Gave up after ${retryCount + 1} attempts: the next one would start after the ${maxTotalTime} ms deadline`,
                { cause: e }
              )
            )
          } else {
            scheduleRetry()
          }
        } else {
          reject(new KafkaJSNonRetriableError(e, { cause: e.cause || e }))
        }
      })
  }

  return retry
}

/**
 * @typedef {(fn: (bail: (err: Error) => void, retryCount: number, retryTime: number) => any) => Promise<ReturnType<fn>>} Retrier
 */

/**
 * Builds a retrier: it runs `fn`, and on a retriable failure runs it again after the backoff
 * schedule above, up to `retries` more times.
 *
 * `maxTotalTime` (ms) adds a deadline measured from the first attempt: when the wait before the
 * next attempt would end past it, the retrier rejects with `KafkaJSDeliveryTimeout` - its `cause`
 * the error the last attempt threw - instead of waiting. Running out of `retries` first still
 * rejects with `KafkaJSNumberOfRetriesExceeded`, and a non-retriable error is not affected. Unset,
 * `undefined` or `null`, the retrier behaves exactly as without it.
 * @param {import("../../types").RetryOptions & { maxTotalTime?: number, fence?: { register: Function } }} [opts] optional producer cancellation; absent, the original retrier runs unchanged
 * @returns {Retrier}
 * @throws {KafkaJSInvalidConfigurationError} when `maxTotalTime` is set and is not an integer from
 *   1 to 2147483647; thrown here, before any retrier is returned
 */
const createRetry = (opts = {}) => {
  assertMaxTotalTime(opts?.maxTotalTime)

  return (fn) => {
    if (opts?.fence) {
      const configs = Object.assign({}, RETRY_DEFAULT, opts)
      return new FencedRetry({
        configs,
        backoff: backoffOf(configs),
        fn,
        isRetriable: isErrorRetriable,
      }).promise
    }
    return new Promise((resolve, reject) => {
      const configs = Object.assign({}, RETRY_DEFAULT, opts)
      const backoff = backoffOf(configs)
      const start = createRetriable(configs, backoff, resolve, reject, fn)
      start(backoff.initial())
    })
  }
}

createRetry.backoff = createBackoff

/**
 * The values the retrier gives an option the caller leaves unset - the test defaults under
 * `NODE_ENV=test`, as for the retrier itself. Read-only.
 * @type {Readonly<{ maxRetryTime: number, initialRetryTime: number, factor: number,
 *   multiplier: number, retries: number }>}
 */
createRetry.defaults = Object.freeze({ ...RETRY_DEFAULT })

module.exports = createRetry
