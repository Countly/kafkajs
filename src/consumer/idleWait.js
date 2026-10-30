const Generation = require('../producer/generation')
const { consumerStoppingError } = require('./rebalanceErrorContract')

const alwaysCurrent = () => true

/**
 * A run-owned collection of independently timed idle fetchers. It also owns the run's terminal
 * `fence`: closing it releases pending preparation waits and every registration made on it, such
 * as the routing subscription. An ordinary `wake()` closes nothing.
 */
module.exports = class IdleWait {
  /** @returns {IdleWait} an open collection with no remembered wake */
  constructor() {
    this.waiters = new Set()
    this.disposed = false
    /** @type {Generation} closed once, by `dispose()`; a new run gets a new fence */
    this.fence = new Generation()
  }

  /**
   * Waits for this timer or the next wake. A retired run, or a caller whose evidence is already
   * stale, settles immediately. Currentness is checked again after registration, with no await in
   * between, so a change before registration prevents the wait and a change after it finds the
   * waiter through `wake()`. Nothing about the evidence is remembered.
   * @param {number} ms idle duration in milliseconds
   * @param {() => boolean} [isCurrent] whether the evidence the caller sleeps on still holds
   * @returns {Promise<void>} settles once, with its timer and entry removed
   */
  wait(ms, isCurrent = alwaysCurrent) {
    if (this.disposed || !isCurrent()) return Promise.resolve()
    return new Promise((resolve) => {
      const waiter = { timer: null, finish: null }
      waiter.finish = () => {
        if (!this.waiters.delete(waiter)) return
        clearTimeout(waiter.timer)
        resolve()
      }
      waiter.timer = setTimeout(waiter.finish, ms)
      this.waiters.add(waiter)
      if (this.disposed || !isCurrent()) waiter.finish()
    })
  }

  /** Wakes current fetchers once; an empty wake retains no state. @returns {void} */
  wake() {
    for (const waiter of this.waiters) waiter.finish()
  }

  /**
   * Retires this run permanently: closes its fence with the stopping error, which runs every
   * registered cancellation, and settles the outstanding waits. Idempotent.
   * @returns {void}
   */
  dispose() {
    this.disposed = true
    this.fence.close(consumerStoppingError())
    this.wake()
  }
}
