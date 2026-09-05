const { decode, parse } = require('./response')

describe('Protocol > Requests > ListOffsets > v9', () => {
  test('response is byte-identical to v6', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v6_response.json')))

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
})
