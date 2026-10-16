const { inspect } = require('util')
const murmur2 = require('../default/murmur2')
const { KafkaJSNonRetriableError } = require('../../../errors')

/** Kafka's `Utils.toPositive`: clears the sign bit, so a negative hash still maps to a partition. */
const toPositive = (x) => x & 0x7fffffff

/**
 * @param {Buffer|string|null|undefined} field a message key or value
 * @returns {number} its size in bytes - a string as UTF-8, any other value as its string form - or
 *   0 when absent
 */
const sizeOf = (field) => {
  if (field == null) {
    return 0
  }

  return Buffer.isBuffer(field) ? field.length : Buffer.byteLength(String(field))
}

/**
 * Picks the partition a topic's keyless records stick to next: uniformly at random among the
 * partitions that have a leader, or among all of them when none has one. Like the Java client's
 * built-in partitioner, the pick may land on the partition just left.
 * @param {{ partitionId: number, leader: number }[]} partitionMetadata
 * @returns {number}
 */
const pickPartition = (partitionMetadata) => {
  const available = partitionMetadata.filter(({ leader }) => leader >= 0)
  const candidates = available.length > 0 ? available : partitionMetadata
  return candidates[Math.floor(Math.random() * candidates.length)].partitionId
}

/**
 * KIP-794's strictly uniform sticky partitioner, opt-in. Unlike `DefaultPartitioner` it is a
 * function you call, with its options or with none:
 * `kafka.producer({ createPartitioner: Partitioners.UniformStickyPartitioner({ stickyBytes: 8192 }) })`.
 * Passed uncalled, as `DefaultPartitioner` is, it still works and uses the default options.
 *
 * - A record with an explicit `partition` goes there.
 * - A keyed record goes where `DefaultPartitioner` sends it, `toPositive(murmur2(key)) % partitions`
 *   — the Java client's mapping, so co-partitioned topics stay co-partitioned — unless `ignoreKeys`.
 * - Every other record goes to its topic's sticky partition until `stickyBytes` of key and value
 *   have been assigned to it; the record that reaches the threshold still goes there and the next
 *   one picks again. The state lives as long as the producer's partitioner, so small sends stick
 *   too, and its size is one entry per topic. A sticky partition the topic no longer has is
 *   picked again.
 *
 * Adaptive partitioning (`partitioner.adaptive.partitioning.enable`,
 * `partitioner.availability.timeout.ms`) is not implemented: it needs per-partition queue depths
 * this client does not track.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java
 * @param {object} [options]
 * @param {number} [options.stickyBytes=16384] bytes assigned to a partition before the next pick
 * @param {boolean} [options.ignoreKeys=false] partition keyed records as if they had no key
 * @returns {import('../../../../types').ICustomPartitioner}
 * @throws {KafkaJSNonRetriableError} when `stickyBytes` is not an integer of at least 1
 */
const UniformStickyPartitioner = ({ stickyBytes = 16384, ignoreKeys = false } = {}) => {
  if (!Number.isInteger(stickyBytes) || stickyBytes < 1) {
    throw new KafkaJSNonRetriableError(
      `stickyBytes must be an integer of at least 1, got ${inspect(stickyBytes)}`
    )
  }

  /** @returns {ReturnType<import('../../../../types').ICustomPartitioner>} */
  const createPartitioner = () => {
    /** @type {Map<string, { partition: number, bytes: number }>} */
    const sticky = new Map()

    return ({ topic, partitionMetadata, message }) => {
      if (message.partition !== null && message.partition !== undefined) {
        return message.partition
      }

      if (!ignoreKeys && message.key !== null && message.key !== undefined) {
        return toPositive(murmur2(message.key)) % partitionMetadata.length
      }

      let current = sticky.get(topic)
      if (current === undefined || current.partition >= partitionMetadata.length) {
        current = { partition: pickPartition(partitionMetadata), bytes: 0 }
        sticky.set(topic, current)
      }

      current.bytes += sizeOf(message.key) + sizeOf(message.value)
      if (current.bytes >= stickyBytes) {
        sticky.delete(topic)
      }

      return current.partition
    }
  }

  // Passed uncalled, the producer's `createPartitioner()` returns this function and then calls it
  // with each record, so a record argument means it is standing in for the partitioner itself.
  let uncalled
  return (record) => {
    if (record === undefined) {
      return createPartitioner()
    }

    uncalled ??= createPartitioner()
    return uncalled(record)
  }
}

module.exports = UniformStickyPartitioner
