const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetForLeaderEpoch > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      topics: [
        {
          topic: 'test-topic',
          partitions: [{ errorCode: 0, partition: 0, leaderEpoch: 5, endOffset: '120' }],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('parse does not throw on a per-partition error code', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response_error.json')))

    expect(data.topics[0].partitions[0].errorCode).toEqual(74)
    // The caller classifies these: a retriable code is retried against the leader within a bounded
    // budget and a non-retriable one turns validation off for the assignment. Neither outcome is
    // expressible from inside parse, and an exception here would reach the fetch loop.
    await expect(parse(data)).resolves.toBe(data)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response_java.json')))

    expect(data.topics).toEqual([
      {
        topic: 'test-topic',
        partitions: [{ errorCode: 0, partition: 0, leaderEpoch: 5, endOffset: '120' }],
      },
    ])
  })
})
