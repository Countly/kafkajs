/**
 * One producer lifecycle. Closing it cancels local waiters and fences every old continuation.
 * A later connect creates another instance; an old instance is never reopened.
 */
class Generation {
  #error = null
  #cancels = new Set()

  /** @returns {boolean} whether this lifecycle has been closed */
  get closed() {
    return this.#error !== null
  }

  /** @returns {Error|null} the error that closed this lifecycle */
  get error() {
    return this.#error
  }

  /**
   * Registers local cancellation until the work ends. A closed lifecycle cancels immediately.
   * @param {(error: Error) => void} cancel local cleanup, without remote outcome claims
   * @returns {() => void} idempotent unregister
   */
  register(cancel) {
    if (this.closed) {
      cancel(this.#error)
      return () => {}
    }
    this.#cancels.add(cancel)
    return () => this.#cancels.delete(cancel)
  }

  /**
   * Closes once and invokes every cancellation even if another cancellation throws.
   * @param {Error} error the local forced-close error
   * @returns {void} never throws
   */
  close(error) {
    if (this.closed) return
    this.#error = error
    const cancels = [...this.#cancels]
    this.#cancels.clear()
    for (const cancel of cancels) {
      try {
        cancel(error)
      } catch {}
    }
  }

  /** @returns {void} @throws {Error} the forced-close error when closed */
  check() {
    if (this.closed) throw this.#error
  }

  /** @returns {Error|null} pre-write refusal, suitable for the connection guard */
  guard = () => this.#error

  /**
   * Waits for a local operation until it ends or this lifecycle closes. Its underlying work
   * still checks this captured instance after every await and at transmission.
   * @param {Promise<unknown>} operation already-started work with its own continuation fence
   * @returns {Promise<unknown>} the value, or the close error without a remote claim
   * @throws {Error} the operation's error or the close error
   */
  wait(operation) {
    return new Promise((resolve, reject) => {
      const unregister = this.register(reject)
      Promise.resolve(operation).then(
        (value) => {
          unregister()
          if (!this.closed) resolve(value)
        },
        (error) => {
          unregister()
          reject(error)
        }
      )
    })
  }
}

module.exports = Generation
