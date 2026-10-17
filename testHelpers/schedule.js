/**
 * A call into a stub or a scripted broker, as a test sees it: `api` plus whatever the stub adds
 * (`version`, `node`, `topic`, `partition`, its own arguments).
 * @typedef {{ api: string, version?: number, node?: number|string, topic?: string,
 *   partition?: number, [key: string]: unknown }} Call
 */

/**
 * Picks calls. Every field present must equal the call's own; `match` is a last resort for what
 * the fields cannot say.
 * @typedef {{ api?: string, version?: number, node?: number|string, topic?: string,
 *   partition?: number, match?: (call: Call) => boolean }} Selector
 */

/**
 * @param {Selector} selector
 * @param {Call} call
 * @returns {boolean} whether `call` is one the selector picks
 */
const selects = ({ match, ...fields }, call) =>
  Object.entries(fields).every(([key, value]) => call[key] === value) && (!match || match(call))

/**
 * One call held until the test lets it go: the test sees when the call arrives and decides how it
 * ends, which is how a test puts two operations in a chosen order.
 */
class Gate {
  constructor() {
    this.call = null
    this.settle = null
    this.arrival = new Promise((resolve) => {
      this.announce = resolve
    })
  }

  /**
   * @returns {Promise<Call>} resolves with the held call once it arrives, at once if it already did
   */
  arrived() {
    return this.arrival
  }

  /**
   * Lets the held call return `value`.
   * @param {*} [value]
   * @returns {void}
   * @throws {Error} when no call has arrived yet or the gate was already released
   */
  release(value) {
    this.finish(({ resolve }) => resolve(value))
  }

  /**
   * Lets the held call reject.
   * @param {Error} error
   * @returns {void}
   * @throws {Error} when no call has arrived yet or the gate was already released
   */
  fail(error) {
    this.finish(({ reject }) => reject(error))
  }

  /**
   * Takes the call in. Only `Schedule` calls it, when the held call arrives; a gate holds one call
   * in its lifetime.
   * @internal
   * @param {Call} call
   * @returns {Promise<*>} what `release` or `fail` ends the call with
   * @throws {Error} when the gate already holds, or held, a call
   */
  hold(call) {
    if (this.call) {
      throw new Error('Gate already holds a call')
    }

    this.call = call
    const held = new Promise((resolve, reject) => {
      this.settle = { resolve, reject }
    })
    this.announce(call)
    return held
  }

  /**
   * Ends the held call the way `how` says, once.
   * @private
   * @param {(settle: { resolve: Function, reject: Function }) => void} how settles the held call
   * @returns {void}
   * @throws {Error} when no call has arrived yet or the gate was already released
   */
  finish(how) {
    if (!this.settle) {
      throw new Error(this.call ? 'Gate already released' : 'Gate released before a call arrived')
    }

    const { settle } = this
    this.settle = null
    how(settle)
  }
}

/**
 * Routes the calls of a stub. A stub hands each call to `run`; the schedule logs it, then answers
 * it from the oldest rule that picks it: a `hold` (the call waits on a `Gate`), a `once` (a fixed
 * answer), or else the stub's own default.
 */
class Schedule {
  constructor() {
    /** @type {Call[]} every call `run` has seen, in arrival order */
    this.calls = []
    this.rules = []
  }

  /**
   * Holds the next call the selector picks.
   * @param {Selector} selector
   * @returns {Gate} the gate that call will wait on
   */
  hold(selector) {
    const gate = new Gate()
    this.rules.push({ selector, answer: (call) => gate.hold(call) })
    return gate
  }

  /**
   * Answers the next call the selector picks with `answer` instead of the default.
   * @param {Selector} selector
   * @param {*|((call: Call) => *)} answer a value, an `Error` (the call rejects with it) or a
   *   function of the call returning either
   * @returns {void}
   */
  once(selector, answer) {
    this.rules.push({
      selector,
      answer: (call) => {
        const result = typeof answer === 'function' ? answer(call) : answer
        return result instanceof Error ? Promise.reject(result) : result
      },
    })
  }

  /**
   * Logs a call and answers it.
   * @param {Call} call
   * @param {(call: Call) => *} fallback the stub's own answer when no rule picks the call
   * @returns {Promise<*>}
   */
  async run(call, fallback) {
    this.calls.push(call)
    const index = this.rules.findIndex(({ selector }) => selects(selector, call))
    if (index === -1) {
      return fallback(call)
    }

    const [{ answer }] = this.rules.splice(index, 1)
    return answer(call)
  }
}

/**
 * A small deterministic generator, so a failing schedule can be replayed from its seed.
 * @see https://github.com/bryc/code/blob/master/jshash/PRNGs.md
 * @param {number} seed any integer
 * @returns {() => number} a function returning a float in [0, 1)
 */
const mulberry32 = (seed) => {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

/** The most steps whose every ordering is listed: 6! = 720. */
const EXHAUSTIVE_STEPS = 6

/**
 * The orders in which `steps` can be released, each a permutation of the step indices.
 * Up to six steps every ordering is returned (lexicographic, so the count is `steps.length!`, 720
 * at six). Above that a seeded sample of `limit` distinct orderings is returned, the same ones for
 * the same seed. `limit` caps the exhaustive list too.
 * @param {unknown[]} steps only its length is used, so holes in it count as steps
 * @param {{ seed?: number, limit?: number }} [options]
 * @returns {number[][]}
 */
const interleavings = (steps, { seed = 1, limit = 720 } = {}) => {
  const indices = Array.from({ length: steps.length }, (_, index) => index)

  if (indices.length <= EXHAUSTIVE_STEPS) {
    return permutations(indices).slice(0, limit)
  }

  const random = mulberry32(seed)
  const seen = new Map()
  const available = indices.reduce((product, _, index) => product * (index + 1), 1)
  while (seen.size < Math.min(limit, available)) {
    const order = indices.slice()
    for (let i = order.length - 1; i > 0; i--) {
      const j = Math.floor(random() * (i + 1))
      ;[order[i], order[j]] = [order[j], order[i]]
    }
    seen.set(order.join(','), order)
  }

  return [...seen.values()]
}

/**
 * @param {number[]} items
 * @returns {number[][]} every ordering of `items`, lexicographic when `items` is ascending
 */
const permutations = (items) =>
  items.length <= 1
    ? [items]
    : items.flatMap((item, index) =>
        permutations([...items.slice(0, index), ...items.slice(index + 1)]).map((rest) => [
          item,
          ...rest,
        ])
      )

module.exports = { Gate, Schedule, interleavings, mulberry32 }
