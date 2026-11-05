const { inspect } = require('util')
const { KafkaJSInvalidConfigurationError } = require('../../errors')

/**
 * KIP-1123's rack preference for the built-in partitioners: which partitions a record without a
 * key may go to. It is off until the producer opts in with `rackAware: true` and a non-empty
 * `rackId`; off, it only filters out the partitions that have no leader, and reads no rack.
 */
class RackPreference {
  constructor() {
    /** @type {((nodeId: number) => string|undefined)|null} null while the preference is off */
    this.rackOf = null
    this.rackId = ''
  }

  /**
   * Checks the producer's two rack options.
   * @param {{ rackId?: string, rackAware?: boolean }} options `undefined` means the default
   * @returns {void}
   * @throws {KafkaJSInvalidConfigurationError} when `rackId` is not a string, `rackAware` is not a
   *   boolean, or `rackAware` is true with an empty `rackId`
   */
  static validate({ rackId = '', rackAware = false }) {
    if (typeof rackId !== 'string') {
      throw new KafkaJSInvalidConfigurationError(`rackId must be a string, got ${inspect(rackId)}`)
    }
    if (typeof rackAware !== 'boolean') {
      throw new KafkaJSInvalidConfigurationError(
        `rackAware must be a boolean, got ${inspect(rackAware)}`
      )
    }
    if (rackAware && rackId === '') {
      throw new KafkaJSInvalidConfigurationError('rackAware requires a non-empty rackId')
    }
  }

  /**
   * Takes the producer's private context. Only a context that opted in, with a rack, turns the
   * preference on; a `rackId` alone does not.
   * @param {{ rackOf: (nodeId: number) => string|undefined, rackId?: string,
   *   rackAware?: boolean }} context
   * @returns {void}
   */
  attach({ rackOf, rackId = '', rackAware = false }) {
    this.rackId = rackId
    this.rackOf = rackAware === true && rackId !== '' ? rackOf : null
  }

  /**
   * The partitions a record without a key may go to: those with a leader in the producer's rack
   * when it has opted in and any has, else every partition with a leader. A leader whose rack is
   * unknown is no match. Empty only when no partition has a leader, which the caller handles as it
   * always has.
   * @param {{ partitionId: number, leader: number }[]} partitionMetadata the topic's partitions,
   *   whatever their ids
   * @returns {{ partitionId: number, leader: number }[]} entries of `partitionMetadata`
   */
  eligible(partitionMetadata) {
    const available = partitionMetadata.filter(({ leader }) => leader >= 0)
    if (this.rackOf === null) return available
    const local = available.filter(({ leader }) => this.rackOf(leader) === this.rackId)
    return local.length > 0 ? local : available
  }
}

module.exports = RackPreference
