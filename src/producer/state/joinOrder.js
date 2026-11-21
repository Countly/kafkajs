const Fifo = require('../../utils/fifo')

/**
 * @typedef {object} OrderedSend
 * @property {boolean} settled whether it has joined its batches or ended without joining
 * @property {?object[]} entries its records assigned to partitions, `null` until assigned
 * @property {?(string|string[])} lanes the topic, or topics, it carries records for
 */

/**
 * Keeps the call order of one producer's sends where they join their partition batches: a send
 * whose records have their partitions joins only once every earlier send that shares a topic with
 * it has joined or ended. Partitioning can finish out of call order (each send retries a failed
 * metadata refresh on its own jittered backoff), but records join batches, and so take their
 * sequence numbers, in call order per topic. Sends of other topics are never held.
 *
 * A lane is the queue of one topic's sends, in call order. A send that ended stays in a lane only
 * while an earlier send of that topic has not ended, so every lane is bounded by the sends not yet
 * ended, and a lane is deleted once it is empty.
 */
class JoinOrder {
  /** @type {Map<string, OrderedSend|Fifo<OrderedSend>>} a lone send is kept as itself, not in a Fifo */
  #lanes = new Map()
  #join

  /**
   * @param {(send: OrderedSend) => void} join joins a send's `entries` to their batches; the send
   *   is `settled` when it returns
   */
  constructor(join) {
    this.#join = join
  }

  /**
   * Queues a send behind every earlier send of its topics, at its public call order.
   * @param {OrderedSend} send with `lanes` set
   * @returns {void}
   */
  enter(send) {
    const { lanes } = send
    if (typeof lanes === 'string') {
      this.#push(lanes, send)
      return
    }
    for (const topic of lanes) {
      this.#push(topic, send)
    }
  }

  /**
   * A send has its `entries`: it joins now when no earlier send of its topics is still waiting,
   * then so does every later send it was holding.
   * @param {OrderedSend} send
   * @returns {void}
   */
  ready(send) {
    const { lanes } = send
    if (typeof lanes === 'string' && this.#lanes.get(lanes) === send) {
      this.#lanes.delete(lanes)
      this.#join(send)
      return
    }
    if (this.#isFirst(send)) {
      this.#join(send)
      this.passed(send)
    }
  }

  /**
   * A send has joined or ended: every later send that waited only for it, and is ready, joins, in
   * call order per topic. Iterative, so a long run of waiting sends cannot overflow the stack.
   * @param {OrderedSend} send `settled`
   * @returns {void}
   */
  passed(send) {
    let work = null
    for (let done = send; done !== undefined; done = work?.pop()) {
      const { lanes } = done
      if (typeof lanes === 'string') {
        const next = this.#joinable(lanes)
        if (next !== undefined) {
          this.#join(next)
          ;(work ??= []).push(next)
        }
        continue
      }
      for (const topic of lanes) {
        const next = this.#joinable(topic)
        if (next !== undefined) {
          this.#join(next)
          ;(work ??= []).push(next)
        }
      }
    }
  }

  /**
   * Forgets every lane, for a forced close that ends every waiting send itself: no send joins
   * while they end.
   * @returns {void}
   */
  clear() {
    this.#lanes.clear()
  }

  /**
   * @param {string} topic
   * @param {OrderedSend} send
   * @returns {void}
   */
  #push(topic, send) {
    const lane = this.#lanes.get(topic)
    if (lane === undefined) {
      this.#lanes.set(topic, send)
    } else if (lane instanceof Fifo) {
      lane.push(send)
    } else {
      const fifo = new Fifo()
      fifo.push(lane)
      fifo.push(send)
      this.#lanes.set(topic, fifo)
    }
  }

  /**
   * @param {string} topic
   * @returns {OrderedSend|undefined} the first send of the topic's lane, when it is ready and
   *   first in each of its other lanes too
   */
  #joinable(topic) {
    const next = this.#head(topic)
    return next !== undefined && next.entries !== null && this.#isFirst(next) ? next : undefined
  }

  /**
   * @param {OrderedSend} send
   * @returns {boolean} whether it is the first send not ended in each of its lanes
   */
  #isFirst(send) {
    const { lanes } = send
    if (typeof lanes === 'string') {
      return this.#head(lanes) === send
    }
    for (const topic of lanes) {
      if (this.#head(topic) !== send) {
        return false
      }
    }
    return true
  }

  /**
   * Drops the ended sends at the front of a topic's lane, and the lane once it is empty.
   * @param {string} topic
   * @returns {OrderedSend|undefined} the first send of the lane that has not ended
   */
  #head(topic) {
    const lane = this.#lanes.get(topic)
    if (lane === undefined) {
      return undefined
    }
    if (!(lane instanceof Fifo)) {
      if (lane.settled) {
        this.#lanes.delete(topic)
        return undefined
      }
      return lane
    }
    while (lane.size > 0 && lane.peek().settled) {
      lane.shift()
    }
    if (lane.size === 0) {
      this.#lanes.delete(topic)
      return undefined
    }
    return lane.peek()
  }

  /** @returns {number} how many topics have a lane, for tests */
  get size() {
    return this.#lanes.size
  }
}

module.exports = JoinOrder
