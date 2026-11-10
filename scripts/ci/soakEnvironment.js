// Jest supplies its default Node environment, pinned with the runner in package-lock.json.
// eslint-disable-next-line n/no-extraneous-require
const { TestEnvironment } = require('jest-environment-node')

/** Split a single spec's cases before hooks run, preserving Jest's ordinary hook lifecycle. */
module.exports = class SoakEnvironment extends TestEnvironment {
  constructor(config, context) {
    super(config, context)
    this.shard = config.globalConfig.shard
    if (!this.shard) throw new Error('The soak environment requires --shard=i/n')
  }

  handleTestEvent(event, state) {
    if (event.name !== 'run_start') return
    const { shardIndex, shardCount } = this.shard
    let index = 0
    const visit = (block) => {
      for (const child of block.children) {
        if (child.type === 'describeBlock') visit(child)
        else {
          const selected = index++ % shardCount === shardIndex - 1
          if (!selected && child.mode !== 'todo') child.mode = 'skip'
        }
      }
    }
    visit(state.rootDescribeBlock)
  }
}
