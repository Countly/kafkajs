const { inspect } = require('node:util')
const Generation = require('../producer/generation')
const { KafkaJSInvalidConfigurationError, KafkaJSNonRetriableError } = require('../errors')

/**
 * Reads the producer close contract without changing the client. Non-object signal arguments
 * mean natural draining; an options object's timeout must be a valid timer duration.
 * @param {unknown} options legacy callback argument or close options
 * @returns {number|undefined} integer milliseconds from 0 through 2147483647
 * @throws {KafkaJSInvalidConfigurationError} invalid timeout in an object
 */
const timeoutOf = (options) => {
  if (options === undefined || options === null || typeof options !== 'object') return undefined
  const { timeout } = options
  if (timeout === undefined) return undefined
  if (!Number.isInteger(timeout) || timeout < 0 || timeout > 2147483647) {
    throw new KafkaJSInvalidConfigurationError(
      `timeout must be an integer from 0 to 2147483647, got ${inspect(timeout)}`
    )
  }
  return timeout
}

/**
 * Owns one admin's admitted calls and shared close episode. Transport cleanup completes before
 * explicit connect may replace a Generation. An old generation is never reopened.
 */
class AdminLifecycle {
  /**
   * @param {object} options lifecycle collaborators
   * @param {object} options.cluster shared admin transport with bounded disconnect
   * @param {object} options.logger admin logger
   * @param {() => void} options.onDisconnect emits the existing disconnect event
   * @param {import('../telemetry/factory').TelemetryFactory|null} [options.telemetry=null] private
   *   telemetry binding whose one close joins the first valid disconnect
   * @returns {AdminLifecycle} initially usable lifecycle, retaining implicit first connection
   * @throws Never.
   */
  constructor({ cluster, logger, onDisconnect, telemetry = null }) {
    this.cluster = cluster
    this.logger = logger
    this.onDisconnect = onDisconnect
    this.telemetry = telemetry
    this.active = new Set()
    this.closing = null
    this.closed = false
    this.episode = null
    this.generation = this.newGeneration()
  }

  /**
   * Makes a fence whose pre-write guard checks the actual monotonic close deadline as well.
   * @returns {Generation} replacement used only after the old cleanup ends
   * @throws Never.
   */
  newGeneration() {
    const generation = new Generation()
    const guard = generation.guard
    generation.guard = () => {
      const episode = this.episode
      if (episode?.generation === generation && performance.now() >= episode.due) episode.force()
      return guard()
    }
    return generation
  }

  /**
   * Admits one lazy call and observes its late outcome. Forced close settles the client waiter;
   * the call body must also use the captured fence at every continuation and private write.
   * @template T
   * @param {(generation: Generation) => Promise<T>|T} work unstarted public call
   * @returns {Promise<T>} result while this captured generation remains usable
   * @throws {KafkaJSNonRetriableError|Error} closing/closed refusal or original call failure
   */
  run(work) {
    if (this.closing || this.closed) {
      return Promise.reject(
        new KafkaJSNonRetriableError(
          'The admin is disconnected or disconnecting; call connect() to resume'
        )
      )
    }
    const generation = this.generation
    const task = generation.wait(
      Promise.resolve().then(() => {
        const error = generation.guard()
        if (error) throw error
        return work(generation)
      })
    )
    this.active.add(task)
    task.then(
      () => this.active.delete(task),
      () => this.active.delete(task)
    )
    return task
  }

  /**
   * Joins the full cleanup before admitting an explicit connection on a replacement lifecycle.
   * @param {(generation: Generation) => Promise<void>} work fenced cluster connect and event
   * @returns {Promise<void>}
   * @throws {Error} original setup failure or captured lifecycle refusal
   */
  async connect(work) {
    while (this.closing) await this.closing
    if (this.closed) {
      this.generation = this.newGeneration()
      this.closed = false
    }
    return this.run(work)
  }

  /**
   * Validates before mutation and captures the first valid call's single absolute deadline.
   * Repeated calls share the same Promise. Natural cleanup retains its original rejection;
   * bounded cleanup errors are logged as in producer close.
   * @param {{ timeout?: number }|unknown} [options] omitted timeout drains admitted work
   * @returns {Promise<void>} shared episode, or immediate success after completed cleanup
   * @throws {KafkaJSInvalidConfigurationError} as a rejection for invalid object options
   * @throws {Error} original transport cleanup failure when draining naturally
   */
  disconnect(options) {
    const startedAt = performance.now()
    let timeout
    try {
      timeout = timeoutOf(options)
    } catch (error) {
      return Promise.reject(error)
    }
    if (this.closing) return this.closing
    if (this.closed) return Promise.resolve()
    this.cluster.cancelPendingBootstrapResolution?.()
    const generation = this.generation
    const due = timeout === undefined ? Infinity : startedAt + timeout
    // Runs concurrently with the core close and never shortens or extends its budget.
    const finalTelemetry =
      this.telemetry?.close({
        deadline: due,
        remaining: () => (due === Infinity ? Infinity : Math.max(0, due - performance.now())),
        zero: timeout === 0,
      }) ?? null
    const episode = { generation, due, forced: false, cleanup: null }
    const disconnectCluster = () => {
      if (!episode.cleanup) {
        const args =
          timeout === undefined ? [] : [{ drainTimeout: Math.max(0, due - performance.now()) }]
        episode.cleanup = Promise.resolve()
          .then(() => this.cluster.disconnect(...args))
          .catch((error) => {
            this.logger.warn('Disconnecting the cluster failed', { error: error?.message })
            if (timeout === undefined) throw error
          })
      }
      return episode.cleanup
    }
    let forcedDone
    const forced = new Promise((resolve) => {
      forcedDone = resolve
    })
    episode.force = () => {
      if (episode.forced) return
      episode.forced = true
      generation.close(
        new KafkaJSNonRetriableError(
          'The admin was closed; outcome is unknown; a mutation may have been applied'
        )
      )
      disconnectCluster().then(forcedDone)
    }
    this.episode = episode
    const admitted = [...this.active]
    let timer
    this.closing = Promise.resolve().then(async () => {
      try {
        if (timeout === 0 || performance.now() >= due) episode.force()
        else if (timeout !== undefined)
          timer = setTimeout(episode.force, Math.max(0, due - performance.now()))
        if (!episode.forced) await Promise.race([Promise.allSettled(admitted), forced])
        await disconnectCluster()
      } finally {
        clearTimeout(timer)
        if (finalTelemetry) await finalTelemetry
        generation.close(
          new KafkaJSNonRetriableError('The admin was disconnected; outcome is unknown')
        )
        this.closed = true
        this.episode = null
        this.closing = null
        this.onDisconnect()
      }
    })
    if (timeout === 0) episode.force()
    return this.closing
  }
}

module.exports = AdminLifecycle
