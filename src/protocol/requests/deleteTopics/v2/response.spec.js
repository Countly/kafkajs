const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteTopicsResponse-v2-${caseId}.json`)
const byName = (a, b) => a.topic.localeCompare(b.topic)

describe('Protocol > Requests > DeleteTopics > v2 response', () => {
  test.each(['ok', 'error'])(
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
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(clean)).resolves.toBe(clean)
    await expect(parse(failed)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
  })
})
