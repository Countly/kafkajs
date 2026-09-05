const { decode, parse } = require('./response')

describe('Protocol > Requests > ListOffsets > v5', () => {
  test('response is byte-identical to v4', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              timestamp: '1700000000000',
              offset: '100',
              leaderEpoch: 5,
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v5_response_java.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 0, errorCode: 0, timestamp: '-1', offset: '100', leaderEpoch: 5 },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
