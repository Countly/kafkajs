const {
  KafkaJSDeliveryTimeout,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
} = require('../errors')

/** One producer retry operation, with a cancellable attempt and backoff timer. */
class FencedRetry {
  /**
   * @param {object} options merged retry settings, including fence and backoff
   * @param {object} options.configs retry counts, deadline and producer fence
   * @param {{initial: Function, next: Function}} options.backoff shared delay schedule
   * @param {Function} options.fn callback run for each attempt
   * @param {Function} options.isRetriable the shared retry classifier
   */
  constructor({ configs, backoff, fn, isRetriable }) {
    this.configs = configs
    this.backoff = backoff
    this.fn = fn
    this.isRetriable = isRetriable
    this.deadline = configs.maxTotalTime == null ? Infinity : Date.now() + configs.maxTotalTime
    this.done = false
    this.timer = null
    this.unregister = () => {}
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
    this.run(backoff.initial(), 0)
  }

  /** @param {Error|null} error @param {unknown} value @returns {void} */
  finish(error, value) {
    if (this.done) return
    this.done = true
    clearTimeout(this.timer)
    this.timer = null
    this.unregister()
    if (error) this.reject(error)
    else this.resolve(value)
  }

  /**
   * Holds one registration while an attempt or its timer is outstanding.
   * @returns {void}
   */
  listen() {
    this.unregister()
    this.unregister = this.configs.fence.register((error) => this.finish(error))
  }

  /** @param {number} retryTime @param {number} retryCount @returns {void} */
  run(retryTime, retryCount) {
    this.listen()
    if (this.done) return
    const nextRetryTime = this.backoff.next(retryTime)
    Promise.resolve()
      .then(() => {
        if (this.done) return
        return this.fn((error) => this.finish(error || new Error('Aborted')), retryCount, retryTime)
      })
      .then(
        (value) => this.finish(null, value),
        (error) => {
          if (this.done) return
          if (!this.isRetriable(error))
            return this.finish(new KafkaJSNonRetriableError(error, { cause: error.cause || error }))
          if (retryCount >= this.configs.retries)
            return this.finish(
              new KafkaJSNumberOfRetriesExceeded(error, {
                retryCount,
                retryTime,
                cause: error.cause || error,
              })
            )
          if (Date.now() + retryTime > this.deadline)
            return this.finish(
              new KafkaJSDeliveryTimeout(
                `Gave up after ${retryCount + 1} attempts: the next one would start after the ${this.configs.maxTotalTime} ms deadline`,
                { cause: error }
              )
            )
          this.listen()
          if (this.done) return
          this.timer = setTimeout(() => {
            this.timer = null
            this.unregister()
            this.run(nextRetryTime, retryCount + 1)
          }, retryTime)
        }
      )
  }
}

module.exports = FencedRetry
