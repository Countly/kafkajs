const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteTopicsResponse-v5-${caseId}.json`)
const byName = (a, b) => a.topic.localeCompare(b.topic)

describe('Protocol > Requests > DeleteTopics > v5 response', () => {
  test.each(['ok', 'error', 'deletionDisabled'])(
    'decodes the Apache 4.3.1 %s vector with its error message, topics sorted by name',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topicErrors: [...fields.topicErrors].sort(byName),
      })
    }
  )

  test('keeps the v1 keys and adds errorMessage', async () => {
    const { topicErrors } = await decode(Buffer.from(vector('error').hex, 'hex'))

    expect(Object.keys(topicErrors[0]).sort()).toEqual(['errorCode', 'errorMessage', 'topic'])
    expect(topicErrors[0].errorMessage).toEqual('This server does not host this topic-partition.')
  })

  test('parse throws the error of the first failed topic', async () => {
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(failed)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
  })

  test('keeps an empty name and message as empty strings, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('')
          .writeInt16(0)
          .writeUVarIntString('')
          .writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      topicErrors: [{ topic: '', errorCode: 0, errorMessage: '' }],
    })
  })
})
