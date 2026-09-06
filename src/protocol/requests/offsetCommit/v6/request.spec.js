const RequestV6Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > OffsetCommit > v6', () => {
  test('sends CommittedLeaderEpoch as -1', async () => {
    const { buffer } = await RequestV6Protocol({
      groupId: 'test-group',
      groupGenerationId: 1,
      memberId: 'member-1',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readString()).toEqual('test-group')
    expect(decoder.readInt32()).toEqual(1)
    expect(decoder.readString()).toEqual('member-1')

    const topics = decoder.readArray((d) => ({
      name: d.readString(),
      partitions: d.readArray((p) => ({
        partitionIndex: p.readInt32(),
        committedOffset: p.readInt64().toString(),
        committedLeaderEpoch: p.readInt32(),
        committedMetadata: p.readString(),
      })),
    }))

    expect(topics[0].partitions[0]).toEqual({
      partitionIndex: 0,
      committedOffset: '10',
      committedLeaderEpoch: -1,
      committedMetadata: null,
    })
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('does not send retentionTime (its range closed at v4)', async () => {
    const { buffer } = await RequestV6Protocol({
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      topics: [],
    }).encode()
    const decoder = new Decoder(buffer)
    decoder.readString()
    decoder.readInt32()
    decoder.readString()
    expect(decoder.readArray(() => {})).toEqual([])
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV6Protocol({
      groupId: 'test-group',
      groupGenerationId: 5,
      memberId: 'test-member',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v6_request_java.json')))
  })

  test('writes the leader epoch it is given, and -1 when given none', async () => {
    const withEpoch = await RequestV6Protocol({
      groupId: 'test-group',
      groupGenerationId: 1,
      memberId: 'test-member',
      topics: [
        { topic: 'test-topic', partitions: [{ partition: 0, offset: '10', leaderEpoch: 4 }] },
      ],
    }).encode()
    const withoutEpoch = await RequestV6Protocol({
      groupId: 'test-group',
      groupGenerationId: 1,
      memberId: 'test-member',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }).encode()

    // The field sits between CommittedOffset and CommittedMetadata; the two buffers differ only
    // in those four bytes.
    expect(withEpoch.buffer.length).toEqual(withoutEpoch.buffer.length)
    expect(withEpoch.buffer).not.toEqual(withoutEpoch.buffer)
    expect(withEpoch.buffer.includes(Buffer.from([0, 0, 0, 4]))).toBe(true)
    expect(withoutEpoch.buffer.includes(Buffer.from([0xff, 0xff, 0xff, 0xff]))).toBe(true)
  })
})
