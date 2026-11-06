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

/**
 * @param {string} file Repository-relative path of a shipped document.
 * @returns {string} Its text.
 * @throws {Error} When the file cannot be read.
 */
const read = (file) => fs.readFileSync(path.join(root, file), 'utf8')

describe('Share guide API boundaries', () => {
  const guides = ['docs/ShareConsumer.md', 'docs/MigrationGuide-3-0-0.md']

  test.each([
    [
      'documents the absence of a synchronous flush operation',
      'There is no callable synchronous flush-and-wait',
    ],
    ['documents retained names after stop', 'stop retains the subscribed names'],
    ['documents the stopped replacement sequence', 'use stop → subscribe → run to replace them'],
    [
      'documents the limit of the stopped replacement sequence',
      'This sequence does not preserve running-replacement semantics.',
    ],
  ])('%s in both guides', (title, sentence) => {
    for (const file of guides) expect(read(file)).toContain(sentence)
  })

  test('documents exact offset reset composition in both guides', () => {
    for (const file of ['docs/ShareConsumer.md', 'docs/Admin.md']) {
      const text = read(file)
      for (const word of [
        'fetchTopicOffsets',
        'fetchTopicOffsetsByTimestamp',
        'alterShareGroupOffsets',
        'earliest',
        'latest',
        '1700000000000',
        '9223372036854775807n',
        'per-partition errors',
        'not a new admin method',
      ]) {
        expect(text).toContain(word)
      }
    }
  })

  test('keeps the Admin offset-reset recipe identical to the exercised share guide', () => {
    const recipes = ['docs/ShareConsumer.md', 'docs/Admin.md'].map((file) => {
      const match = read(file).match(/^async function resetShareOffsets\([\s\S]*?^\}/m)
      expect(match).not.toBeNull()
      return match[0]
    })
    expect(recipes[1]).toBe(recipes[0])
  })

  test('documents compact terminal outcomes and nonterminal renewal', () => {
    const text = read('docs/ShareConsumer.md')
    for (const phrase of [
      "`type: 'renew'`",
      'is nonterminal',
      'subsets are disjoint',
      'each terminal offset is reported once',
      'remote outcome unknown',
      'missing partition',
    ]) {
      expect(text).toContain(phrase)
    }
    expect(read('docs/MigrationGuide-3-0-0.md')).toContain('renew results are nonterminal')
  })

  test('documents event instrumentation without offering share spans', () => {
    const guide = read('docs/ShareConsumer.md')
    const sentence =
      'ShareConsumer emits instrumentation events and diagnostics channels; this package does not create share-consumer spans.'
    expect(guide).toContain(sentence)
    expect(read('otel/README.md')).toContain(sentence)
    expect(guide).toContain('43 ordinary channels and 18 share channels')
    expect(guide).toContain('no built-in metrics registry')
    expect(guide).toContain(
      'Client instance identity and metric subscription APIs are separate telemetry work'
    )
  })

  test('documents the additive public surface and test doubles', () => {
    const guide = read('docs/ShareConsumer.md')
    for (const name of [
      'KafkaJSShareGroupsNotSupportedError',
      'KafkaJSShareAcknowledgementError',
      'acquisitionLockTimeoutMs',
      'assignment',
      'subscription',
      'events',
    ]) {
      expect(guide).toContain(name)
    }
    for (const file of guides)
      expect(read(file)).toContain('Hand-written Kafka/Admin test doubles need the added members.')
  })

  test('names the share consumer in the migration limits', () => {
    expect(read('docs/MigrationGuide-3-0-0.md')).toContain(
      'The following limits belong to the share consumer. There is no callable synchronous flush-and-wait'
    )
  })

  test('links the public guide with its share broker floor', () => {
    const readme = read('README.md')
    expect(readme).toContain('[Share groups (Kafka 4.2+)](docs/ShareConsumer.md)')
    expect(readme).toContain(
      'compact acknowledgement outcomes, offset-reset recipes and explicit API boundaries'
    )
  })
})
