const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteTopicsResponse-v3-${caseId}.json`)
const byName = (a, b) => a.topic.localeCompare(b.topic)

describe('Protocol > Requests > DeleteTopics > v3 response', () => {
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

  test('parse throws TOPIC_DELETION_DISABLED, the code v3 may return', async () => {
    const { hex } = vector('deletionDisabled')

    await expect(parse(await decode(Buffer.from(hex, 'hex')))).rejects.toMatchObject({
      type: 'TOPIC_DELETION_DISABLED',
      code: 73,
    })
  })
})
