const {
  NO_TOPIC_ID,
  usableTopicIds,
  requireTopicIds,
  namesById,
  topicIdSchema,
} = require('./topicIds')
const { KafkaJSInvariantViolation } = require('../../errors')

const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'

describe('Protocol > Requests > topic ids', () => {
  describe('requireTopicIds', () => {
    test('returns the id of each topic, in the order given', () => {
      const topicIds = new Map([
        ['a', ID_A],
        ['b', ID_B],
      ])

      expect(requireTopicIds('OffsetCommit', ['b', 'a', 'b'], topicIds)).toEqual([ID_B, ID_A, ID_B])
      expect(requireTopicIds('OffsetCommit', [], topicIds)).toEqual([])
    })

    test.each([
      ['no map at all', undefined],
      ['an empty map', new Map()],
      ['a map without the topic', new Map([['other', ID_A]])],
      ['a null id', new Map([['a', null]])],
      ['the all-zero id', new Map([['a', NO_TOPIC_ID]])],
    ])('names the topic and the API when given %s', (_, topicIds) => {
      expect(() => requireTopicIds('OffsetFetch', ['a'], topicIds)).toThrow(
        KafkaJSInvariantViolation
      )
      expect(() => requireTopicIds('OffsetFetch', ['a'], topicIds)).toThrow(
        'OffsetFetch v10 request is missing a topic id for topic "a"'
      )
    })
  })

  describe('requireTopicIds version', () => {
    test('names the first version keyed by id, 10 unless the caller says otherwise', () => {
      expect(() => requireTopicIds('TxnOffsetCommit', ['a'], undefined, 6)).toThrow(
        'TxnOffsetCommit v6 request is missing a topic id for topic "a"'
      )
      expect(() => requireTopicIds('OffsetCommit', ['a'], undefined)).toThrow(
        'OffsetCommit v10 request is missing a topic id for topic "a"'
      )
    })
  })

  describe('usableTopicIds', () => {
    const ids = { a: ID_A, b: ID_B }

    test('is the id of every topic, by name, when every one has a usable id', () => {
      expect([...usableTopicIds(['a', 'b'], (name) => ids[name])]).toEqual([
        ['a', ID_A],
        ['b', ID_B],
      ])
    })

    test.each([
      ['an id the lookup does not know', 'c'],
      ['a null id', 'null'],
      ['the all-zero id', 'zero'],
    ])('is nothing when one topic has %s, whatever the others have', (_, name) => {
      const lookup = { ...ids, null: null, zero: NO_TOPIC_ID }

      expect(usableTopicIds(['a', name, 'b'], (topic) => lookup[topic])).toBeUndefined()
    })

    test('is nothing for no topic at all, where there is nothing to key by id', () => {
      expect(usableTopicIds([], () => ID_A)).toBeUndefined()
    })

    test('asks the lookup only as far as it needs to', () => {
      const asked = []
      usableTopicIds(['a', 'x', 'b'], (name) => {
        asked.push(name)
        return ids[name]
      })

      expect(asked).toEqual(['a', 'x'])
    })
  })

  describe('namesById', () => {
    test('inverts the name-to-id map', () => {
      const names = namesById(
        new Map([
          ['a', ID_A],
          ['b', ID_B],
        ])
      )

      expect([...names]).toEqual([
        [ID_A, 'a'],
        [ID_B, 'b'],
      ])
    })

    test('is empty for no map, so a response codec bound to it names nothing', () => {
      expect(namesById().size).toEqual(0)
      expect(namesById(undefined).size).toEqual(0)
    })

    test('is empty for a null map too, rather than failing on it', () => {
      expect(namesById(null).size).toEqual(0)
    })
  })

  describe('topicIdSchema', () => {
    test('replaces each Name field by a TopicId and keeps the rest, in place', () => {
      const previous = {
        api: 'OffsetCommit',
        version: 9,
        direction: 'request',
        fields: [
          { name: 'GroupId', type: 'string', versions: '0+' },
          { name: 'Topics.Name', type: 'string', versions: '0-9' },
          { name: 'Topics.Partitions', type: '[]P', versions: '0+' },
        ],
      }

      expect(topicIdSchema(previous)).toEqual({
        api: 'OffsetCommit',
        version: 10,
        direction: 'request',
        fields: [
          { name: 'GroupId', type: 'string', versions: '0+' },
          { name: 'Topics.TopicId', type: 'uuid', versions: '10+' },
          { name: 'Topics.Partitions', type: '[]P', versions: '0+' },
        ],
      })
    })
  })
})
