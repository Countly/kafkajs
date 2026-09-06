const RequestV4Protocol = require('./request')
const RequestV3Protocol = require('../v3/request')
const Decoder = require('../../../decoder')

const args = {
  topics: [
    { topic: 'test-topic', partitions: [{ partition: 0, currentLeaderEpoch: 5, leaderEpoch: 4 }] },
  ],
}

describe('Protocol > Requests > OffsetForLeaderEpoch > v4', () => {
  test('encodes the same values with compact strings, compact arrays and tag buffers', async () => {
    const { buffer } = await RequestV4Protocol(args).encode()
    const v3 = await RequestV3Protocol(args).encode()

    expect(buffer).not.toEqual(v3.buffer)
    expect(RequestV4Protocol(args).apiVersion).toEqual(4)

    const decoder = new Decoder(buffer)
    expect(decoder.readInt32()).toEqual(-1)
    const topics = decoder.readUVarIntArray((d) => {
      const topic = {
        topic: d.readUVarIntString(),
        partitions: d.readUVarIntArray((p) => {
          const partition = {
            partition: p.readInt32(),
            currentLeaderEpoch: p.readInt32(),
            leaderEpoch: p.readInt32(),
          }
          expect(p.readTaggedFields().size).toEqual(0)
          return partition
        }),
      }
      expect(d.readTaggedFields().size).toEqual(0)
      return topic
    })

    expect(topics).toEqual(args.topics)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('encodes byte-identically to the vector Apache own codec produced', async () => {
    const { buffer } = await RequestV4Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v4_request_java.json')))
  })
})
