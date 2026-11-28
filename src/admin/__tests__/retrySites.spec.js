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
]

describe('admin retry ownership', () => {
  it('retains the eighteen original retry owners of the admin client', () => {
    const index = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8')
    expect(sites).toHaveLength(18)
    const owners = [...index.matchAll(/const (\w+) = async|retrierOf\(retry\)/g)]
    const counts = new Map()
    let owner
    for (const match of owners) {
      if (match[1]) owner = match[1]
      else counts.set(owner, (counts.get(owner) || 0) + 1)
    }
    for (const method of sites) {
      expect([method, counts.get(method)]).toEqual([
        method,
        method === 'deleteTopics' ? 4 : method === 'fetchTopicOffsetsByTimestamp' ? 2 : 1,
      ])
    }
    expect([...counts.values()].reduce((sum, count) => sum + count, 0)).toBe(22)
    for (const method of sites) expect(index).toMatch(new RegExp(`const ${method} = async`))
  })

  it('runs both group describe APIs through the same per-id retry path', () => {
    const groups = fs.readFileSync(path.join(__dirname, '..', 'groupDescriptions.js'), 'utf8')
    const resolver = fs.readFileSync(
      path.join(__dirname, '..', 'groupDescriptionResolver.js'),
      'utf8'
    )
    expect(groups.match(/new GroupDescriptionResolver\(/g)).toHaveLength(1)
    expect(groups).toContain('await resolver.resolve(names.ids)')
    expect(resolver.match(/this\.retrier\(/g)).toHaveLength(1)
    const calls = [...resolver.matchAll(/this\.#resolveApi\(entry, this\.(\w+)\)/g)]
    expect(calls.map(([, api]) => api)).toEqual(['consumer', 'classic'])
  })

  it('builds generic retries only at the deadline boundary', () => {
    for (const file of fs.readdirSync(path.join(__dirname, '..'))) {
      if (!file.endsWith('.js') || file.endsWith('.spec.js') || file === 'deadline.js') continue
      const source = fs.readFileSync(path.join(__dirname, '..', file), 'utf8')
      expect(source).not.toMatch(/\bcreateRetry\s*\(/)
    }
  })
})
