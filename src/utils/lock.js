const { format } = require('util')
const { KafkaJSLockTimeout } = require('../errors')
const { NETWORK_LOCK_TIMEOUT } = require('../network/instrumentationEvents')

const PRIVATE = {
  LOCKED: Symbol('private:Lock:locked'),
  TIMEOUT: Symbol('private:Lock:timeout'),
  WAITING: Symbol('private:Lock:waiting'),
  TIMEOUT_ERROR_MESSAGE: Symbol('private:Lock:timeoutErrorMessage'),
  EMITTER: Symbol('private:Lock:instrumentationEmitter'),
  DESCRIPTION: Symbol('private:Lock:description'),
}

const TIMEOUT_MESSAGE = 'Timeout while acquiring lock (%d waiting locks)'

/**
 * A simple FIFO mutex. `acquire()` resolves immediately when the lock is free, otherwise
 * queues the caller and resolves callers in the order they queued, as `release()` is called.
 * A caller that waits longer than `timeout` rejects with `KafkaJSLockTimeout` instead of
 * waiting forever.
 */
module.exports = class Lock {
  /**
   * @param {Object} options
   * @param {number} options.timeout Milliseconds a caller may wait in `acquire()` before it
   *   rejects with `KafkaJSLockTimeout`.
   * @param {string} [options.description] Identifies what this lock guards. Included in the
   *   timeout error message and, when `instrumentationEmitter` is set, in the
   *   `NETWORK_LOCK_TIMEOUT` event payload.
   * @param {import('../instrumentation/emitter')} [options.instrumentationEmitter] When set, a
   *   waiter timing out emits `NETWORK_LOCK_TIMEOUT` with `{ description, waiting, timeout }`
   *   before rejecting. Omit to keep the lock silent, as every caller did before this option
   *   existed.
   * @throws {TypeError} If `timeout` is not a number.
   */
  constructor({ timeout, description = null, instrumentationEmitter = null } = {}) {
    if (typeof timeout !== 'number') {
      throw new TypeError(`'timeout' is not a number, received '${typeof timeout}'`)
    }

    this[PRIVATE.LOCKED] = false
    this[PRIVATE.TIMEOUT] = timeout
    this[PRIVATE.WAITING] = new Set()
    this[PRIVATE.EMITTER] = instrumentationEmitter
    this[PRIVATE.DESCRIPTION] = description
    this[PRIVATE.TIMEOUT_ERROR_MESSAGE] = () => {
      const timeoutMessage = format(TIMEOUT_MESSAGE, this[PRIVATE.WAITING].size)
      return description ? `${timeoutMessage}: "${description}"` : timeoutMessage
    }
  }

  /**
   * Acquire the lock, queueing behind any current holder and earlier waiters.
   * @returns {Promise<void>} Resolves once the lock is held by the caller.
   * @throws {KafkaJSLockTimeout} If the wait exceeds `timeout`. When an `instrumentationEmitter`
   *   was configured, `NETWORK_LOCK_TIMEOUT` is emitted first.
   */
  async acquire() {
    return new Promise((resolve, reject) => {
      if (!this[PRIVATE.LOCKED]) {
        this[PRIVATE.LOCKED] = true
        return resolve()
      }

      let timeoutId = null
      const tryToAcquire = async () => {
        if (!this[PRIVATE.LOCKED]) {
          this[PRIVATE.LOCKED] = true
          clearTimeout(timeoutId)
          this[PRIVATE.WAITING].delete(tryToAcquire)
          return resolve()
        }
      }

      this[PRIVATE.WAITING].add(tryToAcquire)
      timeoutId = setTimeout(() => {
        // The message should contain the number of waiters _including_ this one
        const error = new KafkaJSLockTimeout(this[PRIVATE.TIMEOUT_ERROR_MESSAGE]())
        this[PRIVATE.WAITING].delete(tryToAcquire)

        if (this[PRIVATE.EMITTER]) {
          this[PRIVATE.EMITTER].emit(NETWORK_LOCK_TIMEOUT, {
            description: this[PRIVATE.DESCRIPTION],
            waiting: this[PRIVATE.WAITING].size + 1,
            timeout: this[PRIVATE.TIMEOUT],
          })
        }

        reject(error)
      }, this[PRIVATE.TIMEOUT])
    })
  }

  /**
   * Release the lock, immediately handing it to the longest-waiting caller, if any.
   * @returns {Promise<void>}
   */
  async release() {
    this[PRIVATE.LOCKED] = false
    const waitingLock = this[PRIVATE.WAITING].values().next().value

    if (waitingLock) {
      return waitingLock()
    }
  }
}
