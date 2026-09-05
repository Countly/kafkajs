const RequestV6Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > ListOffsets > v6', () => {
  test('request', async () => {
    const timestamp = 1509285569484
    const topics = [
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, timestamp, currentLeaderEpoch: 5 },
          { partition: 1, timestamp: -2 },
        ],
      },
    ]

    const { buffer } = await RequestV6Protocol({
      replicaId: -1,
      isolationLevel: 1,
      topics,
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readInt32()).toEqual(-1)
    expect(decoder.readInt8()).toEqual(1)
    const decoded = decoder.readUVarIntArray((d) => {
      const topic = {
        name: d.readUVarIntString(),
        partitions: d.readUVarIntArray((p) => {
          const partition = {
            partitionIndex: p.readInt32(),
            currentLeaderEpoch: p.readInt32(),
            timestamp: p.readInt64().toString(),
          }
          p.readTaggedFields()
          return partition
        }),
      }
      d.readTaggedFields()
      return topic
    })
    decoder.readTaggedFields()

    expect(decoded).toEqual([
      {
        name: 'test-topic',
        partitions: [
          { partitionIndex: 0, currentLeaderEpoch: 5, timestamp: `${timestamp}` },
          { partitionIndex: 1, currentLeaderEpoch: -1, timestamp: '-2' },
        ],
      },
    ])
  })
})
