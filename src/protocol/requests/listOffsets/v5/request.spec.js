const RequestV5Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > ListOffsets > v5', () => {
  test('request', async () => {
    const timestamp = 1509285569484
    const topics = [
      {
        topic: 'test-topic',
        partitions: [{ partition: 0, timestamp, currentLeaderEpoch: 3 }],
      },
    ]

    const { buffer } = await RequestV5Protocol({
      replicaId: -1,
      isolationLevel: 1,
      topics,
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readInt32()).toEqual(-1)
    expect(decoder.readInt8()).toEqual(1)
    const decoded = decoder.readArray((d) => ({
      name: d.readString(),
      partitions: d.readArray((p) => ({
        partitionIndex: p.readInt32(),
        currentLeaderEpoch: p.readInt32(),
        timestamp: p.readInt64().toString(),
      })),
    }))

    expect(decoded).toEqual([
      {
        name: 'test-topic',
        partitions: [{ partitionIndex: 0, currentLeaderEpoch: 3, timestamp: `${timestamp}` }],
      },
    ])
  })
})
