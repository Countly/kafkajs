const RequestV7Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > OffsetCommit > v7', () => {
  test('sends the configured groupInstanceId', async () => {
    const { buffer } = await RequestV7Protocol({
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      groupInstanceId: 'instance-1',
      topics: [],
    }).encode()
    const decoder = new Decoder(buffer)
    decoder.readString()
    decoder.readInt32()
    decoder.readString()
    expect(decoder.readString()).toEqual('instance-1')
  })

  test('encodes null when static membership is not configured', async () => {
    const { buffer } = await RequestV7Protocol({
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      topics: [],
    }).encode()
    const decoder = new Decoder(buffer)
    decoder.readString()
    decoder.readInt32()
    decoder.readString()
    expect(decoder.readString()).toBeNull()
  })

  test('sends CommittedLeaderEpoch as -1', async () => {
    const { buffer } = await RequestV7Protocol({
      groupId: 'test-group',
      groupGenerationId: 1,
      memberId: 'member-1',
      groupInstanceId: 'instance-1',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }).encode()
    const decoder = new Decoder(buffer)

    decoder.readString()
    decoder.readInt32()
    decoder.readString()
    decoder.readString()

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

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV7Protocol({
      groupId: 'test-group',
      groupGenerationId: 5,
      memberId: 'test-member',
      groupInstanceId: 'test-instance',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v7_request_java.json')))
  })
})
