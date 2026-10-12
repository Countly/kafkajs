const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/createPartitionsResponse-v3-${caseId}.json`)

describe('Protocol > Requests > CreatePartitions > v3 response', () => {
  test.each(['ok', 'error'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      topicErrors: fields.topicErrors,
    })
  })

  test('parse resolves a clean response and throws the error of the first failed topic', async () => {
    const clean = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(clean)).resolves.toBe(clean)
    await expect(parse(failed)).rejects.toMatchObject({ type: 'INVALID_PARTITIONS', code: 37 })
  })

  test('parse throws THROTTLING_QUOTA_EXCEEDED, the code v3 may return', async () => {
    const throttled = { topicErrors: [{ topic: 'test-topic', errorCode: 89 }] }

    await expect(parse(throttled)).rejects.toMatchObject({
      type: 'THROTTLING_QUOTA_EXCEEDED',
      code: 89,
    })
  })
})
