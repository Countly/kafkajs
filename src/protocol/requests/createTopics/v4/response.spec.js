const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/createTopicsResponse-v4-${caseId}.json`)

describe('Protocol > Requests > CreateTopics > v4 response', () => {
  test.each(['ok', 'error'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      topicErrors: fields.topicErrors,
    })
  })

  test('parse resolves a clean response and throws an aggregate of the failed topics', async () => {
    const clean = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(clean)).resolves.toBe(clean)
    await expect(parse(failed)).rejects.toMatchObject({
      name: 'KafkaJSAggregateError',
      errors: [expect.objectContaining({ topic: 'test-topic', type: 'TOPIC_ALREADY_EXISTS' })],
    })
  })
})
