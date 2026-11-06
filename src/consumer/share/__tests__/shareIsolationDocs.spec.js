const fs = require('fs')
const path = require('path')
const { validateShareConsumerOptions } = require('..')
const { KafkaJSInvalidConfigurationError } = require('../../../errors')
const root = path.resolve(__dirname, '../../../..')
const statements = [
  "Share isolation defaults to read_uncommitted, unlike consumer's read_committed; set share.isolation.level=read_committed on the GROUP resource and wait for describeConfigs readback before joining.",
  'The readUncommitted option is rejected; configure share.isolation.level for the GROUP resource.',
  "An open transaction blocks a read_committed group's delivery.",
]

describe('Share isolation guide contract', () => {
  test.each(statements)('both guides state %s', (sentence) => {
    for (const file of ['docs/ShareConsumer.md', 'docs/MigrationGuide-3-0-0.md']) {
      expect(fs.readFileSync(path.join(root, file), 'utf8')).toContain(sentence)
    }
  })
  test('rejects a per-client isolation option before runtime state', () => {
    expect(() => validateShareConsumerOptions({ groupId: 'g', readUncommitted: false })).toThrow(
      KafkaJSInvalidConfigurationError
    )
    expect(() => validateShareConsumerOptions({ groupId: 'g', readUncommitted: false })).toThrow(
      'share.isolation.level'
    )
  })
  test('documents compact unknown outcomes and delivery-limit archival', () => {
    const guide = fs.readFileSync(path.join(root, 'docs/ShareConsumer.md'), 'utf8')
    for (const text of [
      'firstOffset',
      'lastOffset',
      'ranges',
      'remotely unknown',
      'delivery-count limit',
      'No automatic reject',
      'GROUP',
    ])
      expect(guide).toContain(text)
    expect(guide).not.toMatch(/\/var\/tmp\/|\/home\/[^ /]+\/|\.superpowers\//)
  })
  test('allows only the public share guide through the documentation exclusion', () => {
    const ignore = fs.readFileSync(path.join(root, '.npmignore'), 'utf8').split('\n')
    expect(ignore).toContain('docs/*')
    expect(ignore).toContain('!docs/ShareConsumer.md')
    expect(ignore).not.toContain('!docs/')
  })
})
