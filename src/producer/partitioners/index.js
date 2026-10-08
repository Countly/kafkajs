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
   * KIP-794 uniform sticky partitioner, opt-in. Call it with its options to get a partitioner:
   * `createPartitioner: Partitioners.UniformStickyPartitioner({ stickyBytes: 16384 })`.
   */
  UniformStickyPartitioner,
}
