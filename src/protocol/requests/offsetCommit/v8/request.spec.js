const RequestV8Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > OffsetCommit > v8', () => {
  test('sends the configured groupInstanceId', async () => {
    const { buffer } = await RequestV8Protocol({
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      groupInstanceId: 'instance-1',
      topics: [],
    }).encode()
    const decoder = new Decoder(buffer)
    decoder.readUVarIntString()
    decoder.readInt32()
    decoder.readUVarIntString()
    expect(decoder.readUVarIntString()).toEqual('instance-1')
  })

  test('encodes null when static membership is not configured', async () => {
    const { buffer } = await RequestV8Protocol({
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      topics: [],
    }).encode()
    const decoder = new Decoder(buffer)
    decoder.readUVarIntString()
    decoder.readInt32()
    decoder.readUVarIntString()
    expect(decoder.readUVarIntString()).toBeNull()
  })

  test('sends CommittedLeaderEpoch as -1, with tag buffers throughout', async () => {
    const { buffer } = await RequestV8Protocol({
      groupId: 'test-group',
      groupGenerationId: 1,
      memberId: 'member-1',
      groupInstanceId: 'instance-1',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readUVarIntString()).toEqual('test-group')
    expect(decoder.readInt32()).toEqual(1)
    expect(decoder.readUVarIntString()).toEqual('member-1')
    expect(decoder.readUVarIntString()).toEqual('instance-1')

    const topics = decoder.readUVarIntArray((d) => {
      const topic = {
        name: d.readUVarIntString(),
        partitions: d.readUVarIntArray((p) => {
          const partition = {
            partitionIndex: p.readInt32(),
            committedOffset: p.readInt64().toString(),
            committedLeaderEpoch: p.readInt32(),
            committedMetadata: p.readUVarIntString(),
          }
          p.readTaggedFields()
          return partition
        }),
      }
      d.readTaggedFields()
      return topic
    })
    decoder.readTaggedFields()

    expect(topics[0].partitions[0]).toEqual({
      partitionIndex: 0,
      committedOffset: '10',
      committedLeaderEpoch: -1,
      committedMetadata: null,
    })
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
