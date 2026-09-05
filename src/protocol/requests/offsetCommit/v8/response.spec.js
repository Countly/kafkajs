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
})
