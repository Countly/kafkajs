/** The most entries of each kind one observation keeps; later ones are counted, never stored. */
const MAX_TIMES = 100

/**
 * Observes one consumer client's fetcher reconciliations and each fetcher's start and stop through
 * its public log creator. Each instance belongs to one Kafka client; other clients must use their
 * own creator. Only bounded, small records are retained, so DEBUG traffic cannot inflate evidence.
 * They are a diagnostic: no bound of the routing oracle reads them.
 */
class FetcherRecreations {
  /**
   * Creates an empty observation on the wire transcript's monotonic clock.
   * @param {() => number} [now] clock, replaceable for offline replay
   */
  constructor(now = () => performance.now()) {
    this.now = now
    /** @type {Array<{ at: number, added: number[], removed: number[] }>} oldest first, capped */
    this.reconciliations = []
    /** @type {Array<{ at: number, nodeId: number, event: 'started'|'stopped' }>} capped */
    this.fetchers = []
    /** How many entries of either kind the cap discarded. */
    this.dropped = 0
    this.logCreator = this.logCreator.bind(this)
  }

  /** @returns {number[]} when each reconciliation was logged, oldest first */
  get times() {
    return this.reconciliations.map(({ at }) => at)
  }

  /**
   * Creates the logger for this consumer only; discards unrelated namespaces and messages.
   * @param {number} level configured public log level
   * @returns {Function} synchronous log sink; never writes to console
   */
  logCreator(level) {
    return ({ namespace, log }) => {
      if (namespace !== 'FetchManager') return
      if (log.message === 'Reconciling fetchers') {
        this.keep(this.reconciliations, { at: this.now(), added: log.added, removed: log.removed })
      } else if (log.message === 'Fetcher started' || log.message === 'Fetcher stopped') {
        const event = log.message === 'Fetcher started' ? 'started' : 'stopped'
        this.keep(this.fetchers, { at: this.now(), nodeId: log.nodeId, event })
      }
    }
  }

  /**
   * @param {object[]} list the list to append to
   * @param {object} entry
   * @returns {void}
   */
  keep(list, entry) {
    if (list.length < MAX_TIMES) list.push(entry)
    else this.dropped += 1
  }

  /**
   * @param {number} at `performance.now()` of the earliest entry wanted
   * @returns {{ reconciliations: object[], fetchers: object[], dropped: number }} every entry from
   *   `at` on, and how many later ones the cap discarded
   */
  since(at) {
    return {
      reconciliations: this.reconciliations.filter((entry) => entry.at >= at),
      fetchers: this.fetchers.filter((entry) => entry.at >= at),
      dropped: this.dropped,
    }
  }
}

module.exports = { FetcherRecreations, MAX_TIMES }
