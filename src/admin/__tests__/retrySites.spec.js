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

  it('runs both group describe APIs through the one counted retry loop of the group resolver', () => {
    const groups = fs.readFileSync(path.join(__dirname, '..', 'groupDescriptions.js'), 'utf8')
    const start = groups.indexOf('  async #resolve(')
    const end = groups.indexOf('\n  async #', start + 1)
    expect([start > 0, end > start]).toEqual([true, true])
    const resolver = groups.slice(start, end)

    // The resolver holds the only counted retry of the file...
    expect(groups.match(/this\.#retrier\(context\)\(/g)).toHaveLength(1)
    expect(resolver.match(/this\.#retrier\(context\)\(/g)).toHaveLength(1)
    // ...and each API reaches it from exactly one call site, the consumer API first.
    const calls = [...groups.matchAll(/this\.#resolve\((\w+(?:\.\w+)?), context, this\.#(\w+)\(/g)]
    expect(calls.map(([, ids, api]) => [ids, api])).toEqual([
      ['names.ids', 'consumerApi'],
      ['fallbackIds', 'classicApi'],
    ])
    expect(groups.match(/this\.#resolve\(/g)).toHaveLength(2)
    expect(groups.match(/this\.#askConsumer\(/g)).toHaveLength(1)
    expect(groups.match(/this\.#askClassic\(/g)).toHaveLength(1)
  })

  it('builds generic retries only at the deadline boundary', () => {
    for (const file of fs.readdirSync(path.join(__dirname, '..'))) {
      if (!file.endsWith('.js') || file.endsWith('.spec.js') || file === 'deadline.js') continue
      const source = fs.readFileSync(path.join(__dirname, '..', file), 'utf8')
      expect(source).not.toMatch(/\bcreateRetry\s*\(/)
    }
  })
})
