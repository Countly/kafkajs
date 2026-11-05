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
   * Whether the preference is on, so a sticky partition must be checked against the racks again as
   * the producer's routing changes in place.
   * @returns {boolean}
   */
  get enabled() {
    return this.rackOf !== null
  }

  /**
   * Whether a keyless record is held to the producer's rack: the preference is on and some
   * partition with a leader is led in that rack. Reads no rack while the preference is off, and
   * allocates nothing.
   * @param {{ partitionId: number, leader: number }[]} partitionMetadata
   * @returns {boolean} the `narrowed` argument of `admits`
   */
  narrows(partitionMetadata) {
    if (this.rackOf === null) return false
    for (const { leader } of partitionMetadata) {
      if (leader >= 0 && this.rackOf(leader) === this.rackId) return true
    }
    return false
  }

  /**
   * Whether a keyless record may go to a partition: it has a leader and, when `narrowed`, that
   * leader is in the producer's rack (a leader whose rack is unknown is no match).
   * @param {{ partitionId: number, leader: number }} partition
   * @param {boolean} narrowed whether `narrows` held for the topic's partitions
   * @returns {boolean}
   */
  admits({ leader }, narrowed) {
    return leader >= 0 && (!narrowed || this.rackOf(leader) === this.rackId)
  }

  /**
   * The partitions a record without a key may go to: those with a leader in the producer's rack
   * when it has opted in and any has, else every partition with a leader. Empty only when no
   * partition has a leader, which the caller handles as it always has.
   * @param {{ partitionId: number, leader: number }[]} partitionMetadata the topic's partitions,
   *   whatever their ids
   * @returns {{ partitionId: number, leader: number }[]} entries of `partitionMetadata`
   */
  eligible(partitionMetadata) {
    const narrowed = this.narrows(partitionMetadata)
    return partitionMetadata.filter((partition) => this.admits(partition, narrowed))
  }

  /**
   * Whether a topic's sticky partition may still be used: the topic has it and `eligible` would
   * offer it, or offers nothing so every partition is a candidate. Allocates nothing, and is
   * constant time while the partition is still led in the producer's rack (or, with the
   * preference off, led at all); otherwise it is linear in the partition count.
   * @param {{ partitionId: number, leader: number }[]} partitionMetadata
   * @param {number} partition the sticky partition's id
   * @returns {boolean}
   */
  allows(partitionMetadata, partition) {
    let entry = partitionMetadata[partition]
    if (entry?.partitionId !== partition) {
      entry = undefined
      for (const candidate of partitionMetadata) {
        if (candidate.partitionId !== partition) continue
        entry = candidate
        break
      }
    }
    if (entry === undefined) return false
    if (this.admits(entry, this.enabled)) return true
    const narrowed = this.narrows(partitionMetadata)
    return this.admits(entry, narrowed) || !partitionMetadata.some((p) => this.admits(p, narrowed))
  }
}

module.exports = RackPreference
