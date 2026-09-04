const produce = require('./produce')
const fetch = require('./fetch')
const listOffsets = require('./listOffsets')
const offsetCommit = require('./offsetCommit')
const createTopics = require('./createTopics')
const deleteTopics = require('./deleteTopics')
const describeAcls = require('./describeAcls')
const createAcls = require('./createAcls')
const deleteAcls = require('./deleteAcls')
const describeConfigs = require('./describeConfigs')

/**
 * Kafka 4.x request-version floors (KIP-896 / KIP-724), taken from the 4.3 message JSON
 * reflected in docs/KafkaCompatibility.md (npm run compat:generate). Implementing a version below the
 * floor is dead code: a 4.x broker never advertises it.
 */
const FLOORS = {
  Produce: [produce, 3],
  Fetch: [fetch, 4],
  ListOffsets: [listOffsets, 1],
  OffsetCommit: [offsetCommit, 2],
  CreateTopics: [createTopics, 2],
  DeleteTopics: [deleteTopics, 1],
  DescribeAcls: [describeAcls, 1],
  CreateAcls: [createAcls, 1],
  DeleteAcls: [deleteAcls, 1],
  DescribeConfigs: [describeConfigs, 1],
}

describe('Protocol > Requests > Kafka 4.x version floors', () => {
  for (const [apiName, [definition, floor]] of Object.entries(FLOORS)) {
    const belowFloorName = `${apiName} implements no version below v${floor}`
    const stillFloorName = `${apiName} still implements the floor version v${floor}`

    it(belowFloorName, () => {
      const belowFloor = definition.versions.map(Number).filter((v) => v < floor)
      expect(belowFloor).toEqual([])
    })

    it(stillFloorName, () => {
      expect(definition.versions.map(Number)).toContain(floor)
    })
  }
})

describe('Protocol > MessageSet write path', () => {
  it('has no runtime caller for the legacy MessageSet/Message encoders', () => {
    // They survive only as the decoder specs' fixture generators (see the task header).
    // Nothing under src/ may require them once Produce v0-v2 are gone.
    //
    // Implemented with fs rather than grep on purpose: grep exits 1 when it matches nothing,
    // and execFileSync throws on a non-zero exit, so a shelled-out version would ERROR at
    // exactly the moment the deletions land instead of passing.
    const fs = require('fs')
    const path = require('path')

    const walk = (dir) =>
      fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
        const full = path.join(dir, entry.name)
        if (entry.isDirectory()) return walk(full)
        return entry.isFile() && full.endsWith('.js') ? [full] : []
      })

    const src = path.resolve(__dirname, '..', '..')
    const offenders = walk(src).filter((file) => {
      if (file.endsWith('.spec.js')) return false
      // messageSet/index.js legitimately requires ../message: they are one kept pair.
      if (file === path.join(src, 'protocol', 'messageSet', 'index.js')) return false
      const source = fs.readFileSync(file, 'utf8')
      return (
        /require\((['"]).*messageSet\1\)/.test(source) ||
        /require\((['"])\.\.\/message\1\)/.test(source)
      )
    })

    expect(offenders).toEqual([])
  })

  it('still ships the MessageSet decoder (4.x brokers may serve magic 0/1 batches)', () => {
    expect(typeof require('../messageSet/decoder')).toBe('function')
    expect(typeof require('../message/decoder')).toBe('function')
  })
})
