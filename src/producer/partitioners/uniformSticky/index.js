const { inspect } = require('util')
const murmur2 = require('../default/murmur2')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('../../../errors')
const { wire } = require('../../state/recordSize')
const { register } = require('../builtIn')
const RackPreference = require('../rackPreference')

/** Kafka's `Utils.toPositive`: clears the sign bit, so a negative hash still maps to a partition. */
const toPositive = (x) => x & 0x7fffffff

/** How long a topic's sticky state lives without a keyless record, in ms. */
const IDLE_MS = 300000
/** How many topics may hold sticky state before creating one sweeps the idle ones. */
const SWEEP_AT = 64

/**
 * Picks the partition a topic's keyless records stick to next: uniformly at random among the
 * eligible partitions, or among all of them when none is eligible. Like the Java client's
 * built-in partitioner, the pick may land on the partition just left.
 * @param {RackPreference} racks which partitions are eligible: the ones with a leader, or those of
 *   the producer's rack among them
 * @param {{ partitionId: number, leader: number }[]} partitionMetadata
 * @returns {number}
 */
const pickPartition = (racks, partitionMetadata) => {
  const eligible = racks.eligible(partitionMetadata)
  const candidates = eligible.length > 0 ? eligible : partitionMetadata
  return candidates[Math.floor(Math.random() * candidates.length)].partitionId
}

/**
 * KIP-794's strictly uniform sticky partitioner: the producer's default when it is given no
 * `createPartitioner`, with `stickyBytes` its `batchSize`. Unlike `DefaultPartitioner` it is a
 * function you call, with its options or with none:
 * `kafka.producer({ createPartitioner: Partitioners.UniformStickyPartitioner({ stickyBytes: 8192 }) })`.
 * Passed uncalled, as `DefaultPartitioner` is, it still works and uses the default options.
 *
 * - A record with an explicit `partition` goes there.
 * - A keyed record goes where `DefaultPartitioner` sends it, `toPositive(murmur2(key)) % partitions`
 *   — the Java client's mapping, so co-partitioned topics stay co-partitioned — unless `ignoreKeys`.
 * - Every other record goes to its topic's sticky partition until `stickyBytes` have been assigned
 *   to it, each record counting its wire size (at least 24 bytes, so records with no bytes move on
 *   too); the record that reaches the threshold still goes there and the next one picks again. A
 *   sticky partition the topic no longer has, or that lost its leader while another partition has
 *   one, is picked again - checked once for each partition list the producer's metadata hands it.
 *   The state lives as long as the producer's partitioner, one entry per topic; creating a topic's
 *   entry while 64 or more are held drops those idle for longer than 300000 ms.
 *
 * With `adaptive` (KIP-794's adaptive partitioning), each pick weighs the eligible partitions by
 * the batches waiting for them, as the producer reports them through `attach`: a partition is
 * picked with probability proportional to `maxQueue + 1 - queue(p)`, so a slow broker's partitions
 * get fewer records; equal queues, or fewer than two eligible partitions, pick uniformly.
 * `partitioner.availability.timeout.ms` is not implemented.
 *
 * Eligible means having a leader. A producer that opted in with `rackAware: true` and a `rackId`
 * (KIP-1123) narrows that to the partitions whose leader is in its rack when any is, and checks
 * the sticky partition against that set at every record, since a leader hint moves a leader in
 * place; a partition outside the set is left at once. A record with a partition or, unless
 * `ignoreKeys`, a key never reads a rack.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java
 * @param {object} [options]
 * @param {number} [options.stickyBytes=16384] bytes assigned to a partition before the next pick
 * @param {boolean} [options.ignoreKeys=false] partition keyed records as if they had no key
 * @param {boolean} [options.adaptive=false] weigh each pick by the partitions' queues
 * @returns {import('../../../../types').ICustomPartitioner}
 * @throws {KafkaJSNonRetriableError} when `stickyBytes` is not an integer of at least 1
 * @throws {KafkaJSInvalidConfigurationError} when `adaptive` is not a boolean
 */
const UniformStickyPartitioner = ({
  stickyBytes = 16384,
  ignoreKeys = false,
  adaptive = false,
} = {}) => {
  if (!Number.isInteger(stickyBytes) || stickyBytes < 1) {
    throw new KafkaJSNonRetriableError(
      `stickyBytes must be an integer of at least 1, got ${inspect(stickyBytes)}`
    )
  }
  if (typeof adaptive !== 'boolean') {
    throw new KafkaJSInvalidConfigurationError(
      `adaptive must be a boolean, got ${inspect(adaptive)}`
    )
  }

  /** @returns {ReturnType<import('../../../../types').ICustomPartitioner>} */
  const createPartitioner = () => {
    /** @type {Map<string, { partition: number, bytes: number, usedAt: number, checked: object|null }>} */
    const sticky = new Map()
    /** @type {((topic: string, partition: number) => number)|null} */
    let loadOf = null
    const racks = new RackPreference()
    /** The running sum of the weights of a weighted pick, reused from one pick to the next. */
    let cumulative = new Float64Array(0)

    /**
     * Drops the sticky state of every topic idle for longer than `IDLE_MS`.
     * @param {number} now
     * @returns {void}
     */
    const sweep = (now) => {
      for (const [topic, { usedAt }] of sticky) {
        if (now - usedAt > IDLE_MS) sticky.delete(topic)
      }
    }

    /**
     * The adaptive pick (`BuiltInPartitioner.nextPartition`): a cumulative table of
     * `maxQueue + 1 - queue(p)` over the eligible partitions, one random draw and a binary search
     * for the first entry above it. It scans `partitionMetadata` in place, building no list.
     * @param {string} topic
     * @param {{ partitionId: number, leader: number }[]} partitionMetadata
     * @returns {number}
     */
    const weighted = (topic, partitionMetadata) => {
      if (cumulative.length < partitionMetadata.length) {
        cumulative = new Float64Array(partitionMetadata.length)
      }
      const narrowed = racks.narrows(partitionMetadata)
      let count = 0
      let max = -1
      let equal = true
      for (const partition of partitionMetadata) {
        if (!racks.admits(partition, narrowed)) continue
        const queue = loadOf(topic, partition.partitionId)
        equal = equal && (count === 0 || queue === max)
        max = Math.max(max, queue)
        cumulative[count++] = queue
      }
      if (count < 2 || equal) {
        return pickPartition(racks, partitionMetadata)
      }
      let total = 0
      for (let i = 0; i < count; i++) {
        total += max + 1 - cumulative[i]
        cumulative[i] = total
      }
      const draw = Math.random() * total
      let low = 0
      let high = count - 1
      while (low < high) {
        const middle = (low + high) >> 1
        if (cumulative[middle] > draw) high = middle
        else low = middle + 1
      }
      for (const partition of partitionMetadata) {
        if (racks.admits(partition, narrowed) && low-- === 0) return partition.partitionId
      }
    }

    const partitioner = ({ topic, partitionMetadata, message }) => {
      if (message.partition !== null && message.partition !== undefined) {
        return message.partition
      }

      if (!ignoreKeys && message.key !== null && message.key !== undefined) {
        return toPositive(murmur2(message.key)) % partitionMetadata.length
      }

      const now = Date.now()
      let current = sticky.get(topic)
      if (current === undefined) {
        if (sticky.size >= SWEEP_AT) sweep(now)
        current = { partition: -1, bytes: 0, usedAt: now, checked: null }
        sticky.set(topic, current)
      }
      if (current.partition >= 0 && (racks.enabled || current.checked !== partitionMetadata)) {
        if (!racks.allows(partitionMetadata, current.partition)) current.partition = -1
      }
      current.checked = partitionMetadata
      if (current.partition < 0) {
        current.partition =
          adaptive && loadOf !== null
            ? weighted(topic, partitionMetadata)
            : pickPartition(racks, partitionMetadata)
        current.bytes = 0
      }

      const { partition } = current
      current.usedAt = now
      current.bytes += wire(message)
      if (current.bytes >= stickyBytes) {
        current.partition = -1
      }
      return partition
    }

    /**
     * The producer's seam (KIP-1123's, for a built-in partitioner only): called once, with how
     * many batches wait for a partition, the rack of a node and the producer's rack options. An
     * adaptive pick reads `loadOf`; `rackOf` is read only once `rackAware` and a `rackId` ask.
     * @param {{ loadOf: (topic: string, partition: number) => number,
     *   rackOf: (nodeId: number) => string|undefined, rackId?: string,
     *   rackAware?: boolean }} context
     * @returns {void}
     */
    partitioner.attach = (context) => {
      loadOf = context.loadOf
      racks.attach(context)
    }
    return register(partitioner)
  }

  // Passed uncalled, the producer's `createPartitioner()` returns this function and then calls it
  // with each record, so a record argument means it is standing in for the partitioner itself.
  let uncalled
  const factory = (record) => {
    if (record === undefined) {
      return createPartitioner()
    }

    uncalled ??= createPartitioner()
    return uncalled(record)
  }
  /**
   * Attaches the supported uncalled form to the same lazily created instance used for records.
   * @param {{ loadOf: Function, rackOf: Function, rackId?: string, rackAware?: boolean }} context
   * @returns {void}
   */
  factory.attach = (context) => {
    uncalled ??= createPartitioner()
    uncalled.attach(context)
  }
  return register(factory)
}

module.exports = UniformStickyPartitioner
