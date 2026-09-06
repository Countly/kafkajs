const RequestV3Protocol = require('./request')
const Decoder = require('../../../decoder')

const readTopics = (decoder) =>
  decoder.readArray((d) => ({
    topic: d.readString(),
    partitions: d.readArray((p) => ({
      partition: p.readInt32(),
      currentLeaderEpoch: p.readInt32(),
      leaderEpoch: p.readInt32(),
    })),
  }))

describe('Protocol > Requests > OffsetForLeaderEpoch > v3', () => {
  const build = () =>
    RequestV3Protocol({
      topics: [
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, currentLeaderEpoch: 5, leaderEpoch: 4 }],
        },
      ],
    })

  test('sends the consumer replica id as -1, always written even though the field is ignorable', async () => {
    const { buffer } = await build().encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readInt32()).toEqual(-1)
    expect(build().apiVersion).toEqual(3)
    expect(readTopics(decoder)).toEqual([
      {
        topic: 'test-topic',
        partitions: [{ partition: 0, currentLeaderEpoch: 5, leaderEpoch: 4 }],
      },
    ])
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('an unknown current leader epoch defaults to -1', async () => {
    const { buffer } = await RequestV3Protocol({
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, leaderEpoch: 4 }] }],
    }).encode()

    const decoder = new Decoder(buffer)
    decoder.readInt32()
    expect(readTopics(decoder)[0].partitions[0].currentLeaderEpoch).toEqual(-1)
  })
})
