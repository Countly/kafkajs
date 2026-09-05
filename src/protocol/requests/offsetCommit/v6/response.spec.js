const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetCommit > v6', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v6_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      responses: [
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, errorCode: 0 }],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v6_response_java.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      responses: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 0 }] }],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
