const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/createTopicsResponse-v5-${caseId}.json`)

/**
 * Writes a v5 response body of topics that carry no configs: name, error code, no message,
 * partition and replication counts, null configs, then the tag buffer the caller supplies.
 * @param {{ name: string, tags?: { tag: number, value: Buffer }[] }[]} topics
 * @returns {Buffer}
 */
const bodyOf = (topics) =>
  new Encoder()
    .writeInt32(25)
    .writeUVarIntArray(
      topics.map(({ name, tags }) =>
        new Encoder()
          .writeUVarIntString(name)
          .writeInt16(0)
          .writeUVarIntString(null)
          .writeInt32(3)
          .writeInt16(2)
          .writeUVarIntArray(null)
          .writeTaggedFields(tags)
      )
    )
    .writeTaggedFields().buffer

describe('Protocol > Requests > CreateTopics > v5 response', () => {
  test.each(['ok', 'error', 'topicConfigError'])(
    'decodes the Apache 4.3.1 %s vector',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topicErrors: fields.topicErrors,
      })
    }
  )

  test('reports TopicConfigErrorCode 0 when the topic carries no tagged field', async () => {
    const { topicErrors } = await decode(Buffer.from(vector('ok').hex, 'hex'))

    expect(topicErrors[0].topicConfigErrorCode).toBe(0)
  })

  test('keeps the 2.2.4 keys and adds the five new ones', async () => {
    const { topicErrors } = await decode(Buffer.from(vector('ok').hex, 'hex'))

    expect(Object.keys(topicErrors[0]).sort()).toEqual(
      [
        'configs',
        'errorCode',
        'errorMessage',
        'numPartitions',
        'replicationFactor',
        'topic',
        'topicConfigErrorCode',
      ].sort()
    )
  })

  test('decodes a TopicConfigErrorCode without throwing it, and parse resolves it', async () => {
    const data = await decode(Buffer.from(vector('topicConfigError').hex, 'hex'))

    expect(data.topicErrors[0]).toMatchObject({
      errorCode: 0,
      topicConfigErrorCode: 29,
      configs: null,
    })
    await expect(parse(data)).resolves.toBe(data)
  })

  test('reads tag 0 beside a tag it does not know', async () => {
    const code = Buffer.from([0x00, 0x1d])
    const body = bodyOf([
      {
        name: 'a',
        tags: [
          { tag: 0, value: code },
          { tag: 9, value: Buffer.from([1, 2, 3]) },
        ],
      },
    ])

    expect((await decode(body)).topicErrors[0].topicConfigErrorCode).toBe(29)
  })

  test('sorts the topics by name as v3 does', async () => {
    const { topicErrors } = await decode(bodyOf([{ name: 'b' }, { name: 'a' }]))

    expect(topicErrors.map(({ topic }) => topic)).toEqual(['a', 'b'])
  })

  test('parse throws an aggregate of the failed topics', async () => {
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(failed)).rejects.toMatchObject({
      name: 'KafkaJSAggregateError',
      errors: [expect.objectContaining({ topic: 'test-topic', type: 'TOPIC_ALREADY_EXISTS' })],
    })
  })

  test('keeps an empty name, message and config as empty strings, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('')
          .writeInt16(0)
          .writeUVarIntString('')
          .writeInt32(3)
          .writeInt16(2)
          .writeUVarIntArray([
            new Encoder()
              .writeUVarIntString('')
              .writeUVarIntString('')
              .writeBoolean(false)
              .writeInt8(5)
              .writeBoolean(false)
              .writeTaggedFields(),
          ])
          .writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      topicErrors: [
        {
          topic: '',
          errorCode: 0,
          errorMessage: '',
          topicConfigErrorCode: 0,
          numPartitions: 3,
          replicationFactor: 2,
          configs: [{ name: '', value: '', readOnly: false, configSource: 5, isSensitive: false }],
        },
      ],
    })
  })

  test('reads a compact null count as no topics, never as a native error', async () => {
    const body = new Encoder().writeInt32(25).writeUVarIntArray(null).writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      topicErrors: [],
    })
  })
})
