const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetCommit > v8', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response.json')))
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
    const data = await decode(Buffer.from(require('../fixtures/v8_response_java.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      responses: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 0 }] }],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test.each([
    ['4.3.1', '../fixtures/v8_response_kafka431.json'],
    ['4.1.2', '../fixtures/v8_response_kafka412.json'],
  ])('decodes a real broker response from Kafka %s', async (_version, fixture) => {
    const data = await decode(Buffer.from(require(fixture)))

    expect(data.responses[0].topic).toEqual('kafkajs-capture-topic')
    expect(data.responses[0].partitions[0].errorCode).toEqual(0)

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
