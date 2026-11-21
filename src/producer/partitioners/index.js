const DefaultPartitioner = require('./default')
const LegacyPartitioner = require('./legacy')
const UniformStickyPartitioner = require('./uniformSticky')

module.exports = {
  DefaultPartitioner,
  LegacyPartitioner,
  /**
   * @deprecated Use DefaultPartitioner instead
   *
   * The JavaCompatiblePartitioner was renamed DefaultPartitioner
   * and made to be the default in 2.0.0.
   */
  JavaCompatiblePartitioner: DefaultPartitioner,
  /**
   * KIP-794 uniform sticky partitioner, the producer's default partitioner (adaptive, sticking for
   * `batchSize` bytes). Call it with its options to get a partitioner; its own `adaptive` default
   * is `false`. For the previous non-adaptive sticky behavior pass `createPartitioner:
   * Partitioners.UniformStickyPartitioner({ stickyBytes: batchSize, adaptive: false })` with the
   * producer's `batchSize` (16384 by default); KafkaJS 2.2.4's round-robin is `DefaultPartitioner`.
   */
  UniformStickyPartitioner,
}
