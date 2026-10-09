const { KafkaJSInvariantViolation } = require('../errors')

/**
 * Whether a producer sends idempotently, whether its creator asked for that explicitly, and the
 * one change either can undergo: a configuration idempotence does not allow turns it off for the
 * rest of the producer's life, with a warning. Only a producer whose creator did not ask for
 * idempotence can be turned off; for one that did - `idempotent: true`, or a `transactionalId` -
 * the same conflict is a configuration error the caller throws instead.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default
 */
class Idempotence {
  #enabled
  #explicit
  #logger

  /**
   * @param {object} options
   * @param {boolean} options.enabled
   * @param {boolean} options.explicit whether the producer's creator asked for idempotence
   * @param {import('../../types').Logger} options.logger
   */
  constructor({ enabled, explicit, logger }) {
    this.#enabled = enabled
    this.#explicit = explicit
    this.#logger = logger
  }

  /** @returns {boolean} */
  isEnabled() {
    return this.#enabled
  }

  /** @returns {boolean} whether the producer's creator asked for idempotence */
  isExplicit() {
    return this.#explicit
  }

  /**
   * Turns idempotence off for good and logs `warning`, the first time only.
   * @param {'acks'|'maxInFlightRequests'|'retries'} rule the setting that does not allow it
   * @param {string} warning names the setting, its value and both remedies
   * @returns {void}
   * @throws {KafkaJSInvariantViolation} for a producer whose creator asked for idempotence
   */
  disable(rule, warning) {
    if (this.#explicit) {
      throw new KafkaJSInvariantViolation(
        `Idempotence asked for explicitly cannot be disabled by ${rule}`
      )
    }

    if (this.#enabled) {
      this.#enabled = false
      this.#logger.warn(warning, { rule })
    }
  }
}

module.exports = Idempotence
