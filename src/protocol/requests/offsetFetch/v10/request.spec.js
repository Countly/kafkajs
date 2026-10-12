const RequestV10Protocol = require('./request')
const Decoder = require('../../../decoder')
const { protocol, versions } = require('../index')
const { KafkaJSInvariantViolation } = require('../../../../errors')

const byTopicId = require('../fixtures/java/offsetFetchRequest-v10-byTopicId.json')
const allTopics = require('../fixtures/java/offsetFetchRequest-v10-allTopics.json')
const v9RequireStable = require('../fixtures/java/offsetFetchRequest-v9-requireStable.json')

const TOPIC_ID = byTopicId.fields.topics[0].topicId
const TOPIC_IDS = new Map([['test-topic', TOPIC_ID]])
const BY_ID_OPTIONS = {
  ...byTopicId.fields,
  topics: [{ topic: 'test-topic', partitions: byTopicId.fields.topics[0].partitions }],
  topicIds: TOPIC_IDS,
}

/** A compact string as it sits in a flexible body: the length plus one, then the bytes. */
const compact = (text) => Buffer.concat([Buffer.from([text.length + 1]), Buffer.from(text)])

/**
 * Reads a v10 body back: the one group, its topics (null for "every topic") and the request-level
 * `RequireStable` flag.
 * @param {Buffer} buffer
 * @returns {{ group: object, requireStable: boolean }}
 */
const readBody = (buffer) => {
  const decoder = new Decoder(buffer)
  const [group] = decoder.readUVarIntArray((reader) => {
    const entry = {
      groupId: reader.readUVarIntString(),
      memberId: reader.readUVarIntString(),
      memberEpoch: reader.readInt32(),
      topics: reader.readUVarIntArray((topic) => {
        const read = {
          topicId: topic.readUuid(),
          partitions: topic.readUVarIntArray((partition) => partition.readInt32()),
        }
        topic.readTaggedFields()
        return read
      }),
    }
    reader.readTaggedFields()
    return entry
  })
  const requireStable = decoder.readBoolean()
  decoder.readTaggedFields()
  expect(decoder.canReadBytes(1)).toBe(false)
  return { group, requireStable }
}

describe('Protocol > Requests > OffsetFetch > v10', () => {
  test('encodes the by-topic-id vector byte-identically to the Apache 4.3.1 codec', async () => {
    const { buffer } = await RequestV10Protocol(BY_ID_OPTIONS).encode()

    expect(buffer.toString('hex')).toEqual(byTopicId.hex)
  })

  test('encodes the all-topics vector with the null topic list, and needs no ids for it', async () => {
    const { buffer } = await RequestV10Protocol(allTopics.fields).encode()

    expect(buffer.toString('hex')).toEqual(allTopics.hex)
  })

  test.each([[undefined], [null], [[]]])('writes %j topics as the null list', async (topics) => {
    const { buffer } = await RequestV10Protocol({ ...allTopics.fields, topics }).encode()

    expect(buffer.toString('hex')).toEqual(allTopics.hex)
  })

  test('sends requireStable, which the consumer always asks for, at the same position as v9', async () => {
    // No Apache v10 vector sets the flag. The v9 vector that does, with the topic name swapped for
    // the id, is the bytes v10 must write; the swap is the only difference between the versions.
    const v9 = Buffer.from(v9RequireStable.hex, 'hex')
    const named = v9.indexOf(compact('test-topic'))
    const expected = Buffer.concat([
      v9.subarray(0, named),
      Buffer.from(TOPIC_ID.replace(/-/g, ''), 'hex'),
      v9.subarray(named + compact('test-topic').length),
    ])

    const { buffer } = await RequestV10Protocol({
      groupId: 'test-group',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
      topicIds: TOPIC_IDS,
      requireStable: true,
    }).encode()

    expect(named).toBeGreaterThan(0)
    expect(buffer).toEqual(expected)
    expect(readBody(buffer).requireStable).toBe(true)
  })

  test('defaults to no member, the epoch -1 and unstable offsets allowed, and keeps an empty group id', async () => {
    const { buffer } = await RequestV10Protocol({
      groupId: '',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 4 }] }],
      topicIds: TOPIC_IDS,
    }).encode()

    expect(readBody(buffer)).toEqual({
      group: {
        groupId: '',
        memberId: null,
        memberEpoch: -1,
        topics: [{ topicId: TOPIC_ID, partitions: [4] }],
      },
      requireStable: false,
    })
  })

  describe('a request that names a topic is never built without an id for it', () => {
    test.each([
      ['no map', undefined],
      ['a map without the topic', new Map()],
      ['a null id', new Map([['test-topic', null]])],
      ['the all-zero id', new Map([['test-topic', '00000000-0000-0000-0000-000000000000']])],
    ])('throws at build time given %s', (_, topicIds) => {
      expect(() => RequestV10Protocol({ ...BY_ID_OPTIONS, topicIds })).toThrow(
        KafkaJSInvariantViolation
      )
    })
  })

  test('is reachable through the version map', async () => {
    const { request } = protocol({ version: 10 })(BY_ID_OPTIONS)

    expect(versions).toContain('10')
    expect(request).toMatchObject({ apiKey: 9, apiVersion: 10, apiName: 'OffsetFetch' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(byTopicId.hex)
  })
})
