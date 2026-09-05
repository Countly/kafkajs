const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetFetch > v5', () => {
  test('surfaces committedLeaderEpoch additively, leaving every existing key unchanged', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v5_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 0, offset: '10', committedLeaderEpoch: 3, metadata: null, errorCode: 0 },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
