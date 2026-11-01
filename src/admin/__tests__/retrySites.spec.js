const fs = require('node:fs')
const path = require('node:path')

const sites = [
  'createTopics',
  'createPartitions',
  'deleteTopics',
  'fetchTopicOffsets',
  'fetchTopicOffsetsByTimestamp',
  'describeConfigs',
  'alterConfigs',
  'incrementalAlterConfigs',
  'describeByPartitions',
  'listGroups',
  'deleteGroups',
  'removeMembersFromConsumerGroup',
  'deleteTopicRecords',
  'createAcls',
  'describeAcls',
  'deleteAcls',
  'alterPartitionReassignments',
  'listPartitionReassignments',
  'GroupDescriptions.describeBatch',
  'GroupDescriptions.describeClassicBatch',
]

describe('admin retry ownership', () => {
  it('retains all twenty original retry owners', () => {
    const index = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8')
    const groups = fs.readFileSync(path.join(__dirname, '..', 'groupDescriptions.js'), 'utf8')
    expect(sites).toHaveLength(20)
    expect(index.match(/retrierOf\(retry\)/g)).toHaveLength(18)
    expect(groups.match(/retrierOf\(this\.retry\)/g)).toHaveLength(2)
    for (const method of sites.slice(0, 18))
      expect(index).toMatch(new RegExp(`const ${method} = async`))
  })

  it('builds generic retries only at the deadline boundary', () => {
    for (const file of fs.readdirSync(path.join(__dirname, '..'))) {
      if (!file.endsWith('.js') || file.endsWith('.spec.js') || file === 'deadline.js') continue
      const source = fs.readFileSync(path.join(__dirname, '..', file), 'utf8')
      expect(source).not.toMatch(/\bcreateRetry\s*\(/)
    }
  })
})
