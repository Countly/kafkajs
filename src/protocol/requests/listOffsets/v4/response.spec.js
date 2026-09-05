const { decode, parse } = require('./response')

describe('Protocol > Requests > ListOffsets > v4', () => {
  test('response', async () => {
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

  test('every key present in v3 stays byte-identical, with leaderEpoch added', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))
    const partition = data.responses[0].partitions[0]

    expect(Object.keys(partition).sort()).toEqual(
      ['partition', 'errorCode', 'timestamp', 'offset', 'leaderEpoch'].sort()
    )
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response_java.json')))

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
