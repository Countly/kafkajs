/** The longest delay `setTimeout` honours, in ms; a longer one runs after 1 ms. */
const MAX_DELAY = 2147483647

/** One delivery budget, represented as an absolute wall-clock timestamp. */
class Deadline {
  #dueAt
  /**
   * @param {number} startedAt epoch milliseconds captured by the caller
   * @param {number|null} budgetMs null or undefined means no deadline
   */
  constructor(startedAt, budgetMs) {
    this.startedAt = startedAt
    this.budgetMs = budgetMs
    this.#dueAt = budgetMs == null ? Infinity : startedAt + budgetMs
  }

  /** @returns {number} absolute epoch milliseconds, or Infinity */
  get dueAt() {
    return this.#dueAt
  }

  /**
   * @param {number} [now=Date.now()] epoch milliseconds
   * @returns {number} remaining milliseconds, never negative
   */
  remaining(now = Date.now()) {
    return Math.max(0, this.dueAt - now)
  }
}

/** One detachable timer for an owner's earliest unsettled deadline. */
class ExpiryTimer {
  #onDue
  #handle = null
  #dueAt = Infinity

  /**
   * @param {(now: number) => number} onDue synchronous; settles all due deliveries and returns the next deadline
   */
  constructor(onDue) {
    this.#onDue = onDue
  }

  /**
   * Arms a new deadline only when it is earlier than the existing arm.
   * @param {number} dueAt absolute epoch milliseconds, or Infinity
   * @returns {void}
   */
  arm(dueAt) {
    if (dueAt === Infinity || (this.#handle !== null && dueAt >= this.#dueAt)) {
      return
    }
    this.clear()
    this.#dueAt = dueAt
    this.#handle = setTimeout(this.#fire, Math.min(MAX_DELAY, Math.max(0, dueAt - Date.now())))
  }

  /** Clears the owned handle and forgets its deadline. @returns {void} */
  clear() {
    if (this.#handle !== null) {
      clearTimeout(this.#handle)
    }
    this.#handle = null
    this.#dueAt = Infinity
  }

  /** Recomputes all due work at each scheduled wake. @returns {void} */
  #fire = () => {
    this.#handle = null
    this.#dueAt = Infinity
    this.arm(this.#onDue(Date.now()))
  }
}

module.exports = { Deadline, ExpiryTimer, MAX_DELAY }
