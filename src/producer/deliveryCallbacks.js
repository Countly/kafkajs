const { AsyncLocalStorage } = require('node:async_hooks')
const Generation = require('./generation')

/** Fences caller callbacks that run after a send has been admitted. */
class DeliveryCallbacks {
  #active = new Set()
  #execution = new AsyncLocalStorage()
  #blocked = null

  /**
   * Revokes one reentrant callback or all active ones. A graceful close still drains work
   * admitted before it, including callbacks that did not start the close.
   * @param {Error} error
   * @param {{ reentrant?: boolean, block?: boolean }} [options]
   * @returns {void}
   */
  interrupt(error, { reentrant = false, block = false } = {}) {
    if (block) this.#blocked = error
    if (reentrant) {
      const execution = this.#execution.getStore()
      if (this.#active.has(execution)) execution.close(error)
      return
    }
    for (const execution of this.#active) execution.close(error)
  }

  /** Reopens extension delivery for a new transaction. @returns {void} */
  reset() {
    this.#blocked = null
  }

  /** @returns {Error|null} the refusal shared by delivery attempts in an aborting transaction */
  refusal() {
    return this.#blocked
  }

  /**
   * Checks the delivery's authority after caller code runs, even if it threw. A pending Promise
   * must also be interruptible because a compressor can await the abort or close it started.
   * @param {() => *} invoke
   * @param {() => ?Error} guard
   * @param {boolean} [asynchronous]
   * @returns {*}
   */
  run(invoke, guard, asynchronous = false) {
    const execution = new Generation()
    const check = () => {
      const error = guard() ?? execution.guard() ?? this.#blocked
      if (error) throw error
    }
    check()
    this.#active.add(execution)
    let value
    try {
      value = this.#execution.run(execution, invoke)
    } catch (error) {
      this.#active.delete(execution)
      check()
      throw error
    }
    if (!asynchronous) {
      this.#active.delete(execution)
      check()
      return value
    }
    return execution.wait(value).finally(() => {
      this.#active.delete(execution)
      check()
    })
  }
}

module.exports = DeliveryCallbacks
