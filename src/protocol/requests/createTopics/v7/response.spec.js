const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/createTopicsResponse-v7-${caseId}.json`)

describe('Protocol > Requests > CreateTopics > v7 response', () => {
  test.each(['ok', 'error', 'topicConfigError'])(
    'decodes the Apache 4.3.1 %s vector, topic id included',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topicErrors: fields.topicErrors,
      })
    }
  )

  test('reads TopicId between Name and ErrorCode: an id for a created topic, null for a failed one', async () => {
    const ok = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    expect(ok.topicErrors[0]).toMatchObject({
      topic: 'test-topic',
      topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
      errorCode: 0,
    })
    expect(failed.topicErrors[0]).toMatchObject({
      topic: 'test-topic',
      topicId: null,
      errorCode: 36,
    })
  })

  test('decodes the tagged config error of a topic that was created', async () => {
    const data = await decode(Buffer.from(vector('topicConfigError').hex, 'hex'))

    expect(data.topicErrors[0]).toMatchObject({ topicConfigErrorCode: 29, numPartitions: 3 })
    await expect(parse(data)).resolves.toBe(data)
  })

  test('parse throws an aggregate of the failed topics', async () => {
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(failed)).rejects.toMatchObject({
      name: 'KafkaJSAggregateError',
      errors: [expect.objectContaining({ topic: 'test-topic', type: 'TOPIC_ALREADY_EXISTS' })],
    })
  })
})
