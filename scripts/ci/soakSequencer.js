// Jest supplies the default sequencer alongside its runner, pinned together in package-lock.json.
// eslint-disable-next-line n/no-extraneous-require
const Sequencer = require('@jest/test-sequencer').default

/** Keep the one soak spec in every shard; soakEnvironment partitions its individual cases. */
module.exports = class SoakSequencer extends Sequencer {
  shard(tests) {
    if (tests.length !== 1) throw new Error('Case sharding requires exactly one soak spec')
    return tests
  }
}
