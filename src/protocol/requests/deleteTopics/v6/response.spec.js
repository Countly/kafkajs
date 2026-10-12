const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteTopicsResponse-v6-${caseId}.json`)
const byName = (a, b) => a.topic.localeCompare(b.topic)

const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

/**
 * Writes a v6 response body from results written as the schema has them: a nullable name, the
 * topic id, an error code and a null message.
 * @param {{ name: string|null, id: string|null, errorCode: number }[]} results
 * @returns {Buffer}
 */
const bodyOf = (results) =>
  new Encoder()
    .writeInt32(25)
    .writeUVarIntArray(
      results.map(({ name, id, errorCode }) =>
        new Encoder()
          .writeUVarIntString(name)
          .writeUuid(id)
          .writeInt16(errorCode)
          .writeUVarIntString(null)
          .writeTaggedFields()
      )
    )
    .writeTaggedFields().buffer

describe('Protocol > Requests > DeleteTopics > v6 response', () => {
  test.each(['ok', 'error', 'deletionDisabled'])(
    'decodes the Apache 4.3.1 %s vector with its topic id, topics sorted by name',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topicErrors: [...fields.topicErrors].sort(byName),
      })
    }
  )

  test('keys each result by its name; the zero uuid of a failed name reads as null', async () => {
    const { topicErrors } = await decode(Buffer.from(vector('error').hex, 'hex'))

    expect(topicErrors).toEqual([
      expect.objectContaining({ topic: 'test-topic', topicId: null, errorCode: 3 }),
    ])
  })

  test('decodes a result with a null name, keeps its topic id and sorts it without throwing', async () => {
    const data = await decode(
      bodyOf([
        { name: 'b', id: null, errorCode: 0 },
        { name: null, id: ID, errorCode: 0 },
        { name: 'a', id: null, errorCode: 0 },
        { name: null, id: ID, errorCode: 0 },
      ])
    )

    expect(data.topicErrors.map(({ topic }) => topic)).toEqual([null, null, 'a', 'b'])
    expect(data.topicErrors[0]).toEqual({
      topic: null,
      topicId: ID,
      errorCode: 0,
      errorMessage: null,
    })
    await expect(parse(data)).resolves.toBe(data)
  })

  test('parse throws the error of a result with a null name', async () => {
    const data = await decode(bodyOf([{ name: null, id: ID, errorCode: 100 }]))

    await expect(parse(data)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_ID', code: 100 })
  })

  test('parse throws TOPIC_DELETION_DISABLED from the deletionDisabled vector', async () => {
    const data = await decode(Buffer.from(vector('deletionDisabled').hex, 'hex'))

    await expect(parse(data)).rejects.toMatchObject({ type: 'TOPIC_DELETION_DISABLED' })
  })
})
