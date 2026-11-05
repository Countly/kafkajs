const RequestV6Protocol = require('./request')
const Decoder = require('../../../decoder')
const { NO_TOPIC_ID } = require('../../topicIds')
const { protocol, versions } = require('../index')
const { KafkaJSInvariantViolation } = require('../../../../errors')

const vector = (caseId) => require(`../fixtures/java/txnOffsetCommitRequest-v6-${caseId}.json`)

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const NAME_OF = new Map([
  [TOPIC_ID, 'test-topic'],
  [OTHER_TOPIC_ID, 'other-topic'],
])

/**
 * The builder's arguments for a vector: the topics the way a caller names them, and the ids the
 * vector carries for them beside, in the private map.
 * @param {{ fields: object }} vectorOf a Java vector
 * @returns {object}
 */
const optionsOf = ({ fields }) => ({
  ...fields,
  topics: fields.topics.map(({ topicId, partitions }) => ({
    topic: NAME_OF.get(topicId),
    partitions,
  })),
  topicIds: new Map(fields.topics.map(({ topicId }) => [NAME_OF.get(topicId), topicId])),
})

/**
 * Reads a v6 body back field by field, so a case can check what the encoder wrote without trusting
 * the encoder for its own verdict.
 * @param {Buffer} buffer
 * @returns {object} the fields in the vectors' vocabulary
 */
const readBody = (buffer) => {
  const decoder = new Decoder(buffer)
  const body = {
    transactionalId: decoder.readUVarIntString(),
    groupId: decoder.readUVarIntString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    generationId: decoder.readInt32(),
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

describe('Protocol > Requests > TxnOffsetCommit > v6', () => {
  test.each(['defaultMetadata', 'groupMetadata'])(
    'encodes the Apache 4.4.0 %s vector byte for byte',
    async (caseId) => {
      const found = vector(caseId)
      const { buffer } = await RequestV6Protocol(optionsOf(found)).encode()

      expect(buffer.toString('hex')).toEqual(found.hex)
    }
  )

  test.each(['defaultMetadata', 'groupMetadata'])(
    'the %s body reads back as the vector fields, every byte consumed',
    async (caseId) => {
      const found = vector(caseId)
      const { buffer } = await RequestV6Protocol(optionsOf(found)).encode()

      expect(readBody(buffer)).toEqual(found.fields)
    }
  )

  test('writes the id where v5 wrote the name, and no topic name reaches the body', async () => {
    const found = vector('groupMetadata')
    const { buffer } = await RequestV6Protocol(optionsOf(found)).encode()

    expect(buffer.includes(Buffer.from('test-topic'))).toBe(false)
    expect(buffer.includes(Buffer.from('other-topic'))).toBe(false)
    expect(buffer.includes(Buffer.from(TOPIC_ID.replace(/-/g, ''), 'hex'))).toBe(true)
    expect(buffer.includes(Buffer.from(OTHER_TOPIC_ID.replace(/-/g, ''), 'hex'))).toBe(true)
  })

  test('writes the wire defaults for the group metadata a caller leaves out', async () => {
    const found = vector('defaultMetadata')
    const { generationId, memberId, groupInstanceId, ...bare } = optionsOf(found)

    expect(generationId).toEqual(-1)
    expect(memberId).toEqual('')
    expect(groupInstanceId).toBeNull()
    expect((await RequestV6Protocol(bare).encode()).buffer.toString('hex')).toEqual(found.hex)
  })

  test("the offsets a consumer's uncommittedOffsets() hands out encode as the numeric ones do", async () => {
    const found = vector('defaultMetadata')
    const options = optionsOf(found)
    const asReported = {
      ...options,
      topics: options.topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition, leaderEpoch, ...rest }) => ({
          ...rest,
          partition: String(partition),
          ...(leaderEpoch === -1 ? {} : { leaderEpoch }),
        })),
      })),
    }

    expect((await RequestV6Protocol(asReported).encode()).buffer.toString('hex')).toEqual(found.hex)
  })

  test.each([
    ['no map at all', undefined],
    ['an empty map', new Map()],
    ['a map without the topic', new Map([['other-topic', OTHER_TOPIC_ID]])],
    ['a null id', new Map([['test-topic', null]])],
    ['the all-zero id', new Map([['test-topic', NO_TOPIC_ID]])],
  ])(
    'refuses a request whose topic id is given as %s, before it writes anything',
    (_, topicIds) => {
      const options = { ...optionsOf(vector('groupMetadata')), topicIds }
      options.topics = options.topics.slice(0, 1)

      expect(() => RequestV6Protocol(options)).toThrow(KafkaJSInvariantViolation)
      expect(() => RequestV6Protocol(options)).toThrow(
        'TxnOffsetCommit v6 request is missing a topic id for topic "test-topic"'
      )
    }
  )

  test('announces version 6 and is reachable through the version map', async () => {
    const found = vector('groupMetadata')
    const { request } = protocol({ version: 6 })(optionsOf(found))

    expect(versions).toContain('6')
    expect(request).toMatchObject({ apiName: 'TxnOffsetCommit', apiVersion: 6 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(found.hex)
  })

  test("the schema is v5's with each topic name replaced by a topic id, and nothing else changed", () => {
    const v5 = require('../v5/request').schema
    const { schema } = RequestV6Protocol

    expect(schema.version).toEqual(6)
    expect(schema.fields.map(({ name }) => name)).not.toContain('Topics.Name')
    expect(schema.fields).toContainEqual({ name: 'Topics.TopicId', type: 'uuid', versions: '6+' })
    expect(schema.fields.filter(({ name }) => name !== 'Topics.TopicId')).toEqual(
      v5.fields.filter(({ name }) => name !== 'Topics.Name')
    )
  })
})
