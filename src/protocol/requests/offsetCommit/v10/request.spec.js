const RequestV10Protocol = require('./request')
const Decoder = require('../../../decoder')
const { protocol, versions } = require('../index')
const { KafkaJSInvariantViolation } = require('../../../../errors')

const vector = require('../fixtures/java/offsetCommitRequest-v10-byTopicId.json')
const v9Vector = require('../fixtures/java/offsetCommitRequest-v9-basic.json')

const TOPIC_ID = vector.fields.topics[0].topicId
const TOPIC_IDS = new Map([['test-topic', TOPIC_ID]])
const OPTIONS = {
  ...vector.fields,
  topics: [{ topic: 'test-topic', partitions: vector.fields.topics[0].partitions }],
  topicIds: TOPIC_IDS,
}

/** A compact string as it sits in a flexible body: the length plus one, then the bytes. */
const compact = (text) => Buffer.concat([Buffer.from([text.length + 1]), Buffer.from(text)])

/**
 * Reads a v10 body back, field by field, so a case can check what an encoder wrote without
 * trusting the encoder for its own verdict.
 * @param {Buffer} buffer
 * @returns {object}
 */
const readBody = (buffer) => {
  const decoder = new Decoder(buffer)
  const body = {
    groupId: decoder.readUVarIntString(),
    epoch: decoder.readInt32(),
    memberId: decoder.readUVarIntString(),
    groupInstanceId: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray((topic) => {
      const read = {
        topicId: topic.readUuid(),
        partitions: topic.readUVarIntArray((partition) => {
          const entry = {
            partition: partition.readInt32(),
            offset: partition.readInt64().toString(),
            leaderEpoch: partition.readInt32(),
            metadata: partition.readUVarIntString(),
          }
          partition.readTaggedFields()
          return entry
        }),
      }
      topic.readTaggedFields()
      return read
    }),
  }

  decoder.readTaggedFields()
  expect(decoder.canReadBytes(1)).toBe(false)
  return body
}

describe('Protocol > Requests > OffsetCommit > v10', () => {
  test('encodes the by-topic-id vector byte-identically to the Apache 4.3.1 codec', async () => {
    const { buffer } = await RequestV10Protocol(OPTIONS).encode()

    expect(buffer.toString('hex')).toEqual(vector.hex)
  })

  test('is the v9 vector with the topic name swapped for the id, and nothing else', async () => {
    const swapped = Buffer.from(v9Vector.hex, 'hex')
    const named = swapped.indexOf(compact('test-topic'))
    const withId = Buffer.concat([
      swapped.subarray(0, named),
      Buffer.from(TOPIC_ID.replace(/-/g, ''), 'hex'),
      swapped.subarray(named + compact('test-topic').length),
    ])

    expect(named).toBeGreaterThan(0)
    expect(withId.toString('hex')).toEqual(vector.hex)
  })

  test('writes the wire defaults for what the caller leaves out', async () => {
    const bare = {
      ...OPTIONS,
      memberId: '',
      groupInstanceId: undefined,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }
    const { buffer } = await RequestV10Protocol(bare).encode()

    expect(readBody(buffer)).toMatchObject({
      memberId: '',
      groupInstanceId: null,
      topics: [{ partitions: [{ leaderEpoch: -1, metadata: null }] }],
    })
  })

  test('writes each topic with its own id, and each partition with its own epoch and metadata', async () => {
    const { buffer } = await RequestV10Protocol({
      ...OPTIONS,
      topics: [
        {
          topic: 'a',
          partitions: [
            { partition: 0, offset: '5', leaderEpoch: 7, metadata: 'm' },
            { partition: 3, offset: '6', metadata: '' },
          ],
        },
        { topic: 'b', partitions: [{ partition: 1, offset: '9', leaderEpoch: 2 }] },
      ],
      topicIds: new Map([
        ['a', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'],
        ['b', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'],
      ]),
    }).encode()

    expect(readBody(buffer).topics).toEqual([
      {
        topicId: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        partitions: [
          { partition: 0, offset: '5', leaderEpoch: 7, metadata: 'm' },
          { partition: 3, offset: '6', leaderEpoch: -1, metadata: '' },
        ],
      },
      {
        topicId: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
        partitions: [{ partition: 1, offset: '9', leaderEpoch: 2, metadata: null }],
      },
    ])
  })

  describe('a request is never built without an id for every topic', () => {
    test.each([
      ['no map', undefined],
      ['a map without the topic', new Map()],
      ['a null id', new Map([['test-topic', null]])],
      ['the all-zero id', new Map([['test-topic', '00000000-0000-0000-0000-000000000000']])],
    ])('throws at build time given %s', (_, topicIds) => {
      expect(() => RequestV10Protocol({ ...OPTIONS, topicIds })).toThrow(KafkaJSInvariantViolation)
    })
  })

  test('builds with no options for a shape probe, and writes an empty topic list', async () => {
    const request = RequestV10Protocol({})

    expect(request.apiVersion).toEqual(10)
    expect(readBody((await request.encode()).buffer).topics).toEqual([])
  })

  test('is reachable through the version map, and is not built there without ids', async () => {
    const build = protocol({ version: 10 })

    expect(versions).toContain('10')
    expect((await build(OPTIONS).request.encode()).buffer.toString('hex')).toEqual(vector.hex)
    expect(() => build({ ...OPTIONS, topicIds: undefined })).toThrow(KafkaJSInvariantViolation)
  })
})
