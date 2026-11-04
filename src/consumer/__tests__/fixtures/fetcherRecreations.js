/** The most recreation times one observation keeps; later ones are counted, never stored. */
const MAX_TIMES = 100

/**
 * Observes one consumer client's completed fetcher drains through its public log creator.
 * Each instance belongs to one Kafka client; other clients must use their own creator.
 * Only the recreation times are retained, so DEBUG traffic cannot inflate evidence. The times are
 * a diagnostic: no bound of the routing oracle reads them.
 */
class FetcherRecreations {
  /**
   * Creates an empty observation on the wire transcript's monotonic clock.
   * @param {() => number} [now] clock, replaceable for offline replay
   */
  constructor(now = () => performance.now()) {
    this.now = now
    /** @type {number[]} when each recreation was logged, oldest first, at most `MAX_TIMES` */
    this.times = []
    this.dropped = 0
    this.logCreator = this.logCreator.bind(this)
  }

  /**
   * Creates the logger for this consumer only; discards unrelated namespaces and messages.
   * @param {number} level configured public log level
   * @returns {Function} synchronous log sink; never writes to console
   */
  logCreator(level) {
    return ({ namespace, log }) => {
      if (namespace !== 'FetchManager' || log.message !== 'Rebalancing fetchers...') return
      if (this.times.length < MAX_TIMES) this.times.push(this.now())
      else this.dropped += 1
    }
  }

  /**
   * @param {number} at `performance.now()` of the earliest recreation wanted
   * @returns {{ times: number[], dropped: number }} every recreation from `at` on, and how many
   *   later ones the cap discarded
   */
  since(at) {
    return { times: this.times.filter((time) => time >= at), dropped: this.dropped }
  }
}

module.exports = { FetcherRecreations, MAX_TIMES }
