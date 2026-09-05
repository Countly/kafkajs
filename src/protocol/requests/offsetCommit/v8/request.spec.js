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

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV8Protocol({
      groupId: 'test-group',
      groupGenerationId: 5,
      memberId: 'test-member',
      groupInstanceId: 'test-instance',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v8_request_java.json')))
  })

  test.each([
    ['4.3.1', '../fixtures/v8_request_kafka431.json'],
    ['4.1.2', '../fixtures/v8_request_kafka412.json'],
  ])('is the request a real Kafka %s cluster answered', (_version, fixture) => {
    const decoder = new Decoder(Buffer.from(require(fixture)))

    expect(decoder.readUVarIntString()).toEqual('kafkajs-capture-group')
    expect(decoder.readInt32()).toBeGreaterThanOrEqual(0) // generationId
    // The member id is assigned by the coordinator, so only its shape is knowable up front.
    expect(decoder.readUVarIntString()).toMatch(/^kafkajs-capture-/)
    expect(decoder.readUVarIntString()).toBeNull() // groupInstanceId

    const topics = decoder.readUVarIntArray((d) => {
      const topic = {
        name: d.readUVarIntString(),
        partitions: d.readUVarIntArray((p) => {
          const partition = {
            index: p.readInt32(),
            offset: p.readInt64().toString(),
            committedLeaderEpoch: p.readInt32(),
            metadata: p.readUVarIntString(),
          }
          p.readTaggedFields()
          return partition
        }),
      }
      d.readTaggedFields()
      return topic
    })

    expect(topics[0].name).toEqual('kafkajs-capture-topic')
    expect(topics[0].partitions[0].committedLeaderEpoch).toEqual(-1)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
