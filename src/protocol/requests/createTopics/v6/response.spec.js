const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/createTopicsResponse-v6-${caseId}.json`)

describe('Protocol > Requests > CreateTopics > v6 response', () => {
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

  test('parse reports THROTTLING_QUOTA_EXCEEDED, the code v6 may return, inside the aggregate', async () => {
    const throttled = { topicErrors: [{ topic: 'test-topic', errorCode: 89 }] }

    await expect(parse(throttled)).rejects.toMatchObject({
      name: 'KafkaJSAggregateError',
      errors: [expect.objectContaining({ topic: 'test-topic', type: 'THROTTLING_QUOTA_EXCEEDED' })],
    })
  })
})
