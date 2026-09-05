const RequestV4Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > ListOffsets > v4', () => {
  test('request', async () => {
    const timestamp = 1509285569484
    const topics = [
      {
        topic: 'test-topic-727705ce68c29fedddf4',
        partitions: [{ partition: 0, timestamp, currentLeaderEpoch: 5 }],
      },
    ]

    const { buffer } = await RequestV4Protocol({
      replicaId: -1,
      isolationLevel: 0,
      topics,
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readInt32()).toEqual(-1)
    expect(decoder.readInt8()).toEqual(0)
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
        name: 'test-topic-727705ce68c29fedddf4',
        partitions: [{ partitionIndex: 0, currentLeaderEpoch: 5, timestamp: `${timestamp}` }],
      },
    ])
  })

  test('sends CurrentLeaderEpoch from the caller, and -1 when it is not supplied', async () => {
    const { buffer } = await RequestV4Protocol({
      replicaId: -1,
      isolationLevel: 1,
      topics: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 0, timestamp: -2, currentLeaderEpoch: 5 },
            { partition: 1, timestamp: -2 },
          ],
        },
      ],
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readInt32()).toEqual(-1)
    expect(decoder.readInt8()).toEqual(1)
    const topics = decoder.readArray((d) => ({
      name: d.readString(),
      partitions: d.readArray((p) => ({
        partitionIndex: p.readInt32(),
        currentLeaderEpoch: p.readInt32(),
        timestamp: p.readInt64().toString(),
      })),
    }))

    expect(topics[0].partitions).toEqual([
      { partitionIndex: 0, currentLeaderEpoch: 5, timestamp: '-2' },
      { partitionIndex: 1, currentLeaderEpoch: -1, timestamp: '-2' },
    ])
  })
})
