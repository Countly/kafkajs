/**
 * Observes one consumer client's completed fetcher drains through its public log creator.
 * Each instance belongs to one Kafka client; other clients must use their own creator.
 * Only the recreation boundary is retained, so DEBUG traffic cannot inflate evidence.
 */
class FetcherRecreations {
  /**
   * Creates an empty observation on the wire transcript's monotonic clock.
   * @param {() => number} [now] clock, replaceable for offline replay
   */
  constructor(now = () => performance.now()) {
    this.now = now
    this.times = []
    this.logCreator = this.logCreator.bind(this)
  }

  /**
   * Creates the logger for this consumer only; discards unrelated namespaces and messages.
   * @param {number} level configured public log level
   * @returns {Function} synchronous log sink; never writes to console
   */
  logCreator(level) {
    return ({ namespace, log }) => {
      if (namespace === 'FetchManager' && log.message === 'Rebalancing fetchers...') {
        this.times.push(this.now())
      }
    }
  }

  /**
   * Selects one shared recreation boundary after the loss and before every destination starts.
   * Multiple destinations in one episode use this same value. No log keeps strict oracle bounds.
   * @param {Array<{errorAt: number, nextAt: number}>} moves destinations of one move episode
   * @returns {{nodeSetChanged: boolean, drainEndAt: number|null}} explicit observation
   */
  boundary(moves) {
    const loss = Math.max(...moves.map(({ errorAt }) => errorAt))
    const first = Math.min(...moves.map(({ nextAt }) => nextAt))
    const drainEndAt = this.times.find((at) => at > loss && at < first) ?? null
    return { nodeSetChanged: drainEndAt !== null, drainEndAt }
  }
}

module.exports = { FetcherRecreations }
