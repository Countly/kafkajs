const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteTopicsResponse-v4-${caseId}.json`)
const byName = (a, b) => a.topic.localeCompare(b.topic)

describe('Protocol > Requests > DeleteTopics > v4 response', () => {
  test.each(['ok', 'error', 'deletionDisabled'])(
    'decodes the Apache 4.3.1 %s vector, topics sorted by name',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topicErrors: [...fields.topicErrors].sort(byName),
      })
    }
  )

  test('parse resolves a clean response and throws the error of the first failed topic', async () => {
    const clean = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const disabled = await decode(Buffer.from(vector('deletionDisabled').hex, 'hex'))

    await expect(parse(clean)).resolves.toBe(clean)
    await expect(parse(disabled)).rejects.toMatchObject({ type: 'TOPIC_DELETION_DISABLED' })
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
