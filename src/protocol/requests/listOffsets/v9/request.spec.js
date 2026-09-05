const Decoder = require('../../../decoder')
const RequestV6Protocol = require('../v6/request')
const RequestV9Protocol = require('./request')

describe('Protocol > Requests > ListOffsets > v9', () => {
  test('request is byte-identical to v6, apart from the version header', async () => {
    const topics = [
      { topic: 'test-topic', partitions: [{ partition: 0, timestamp: -1, currentLeaderEpoch: 2 }] },
    ]

    const v6 = await RequestV6Protocol({ replicaId: -1, isolationLevel: 0, topics }).encode()
    const v9 = await RequestV9Protocol({ replicaId: -1, isolationLevel: 0, topics }).encode()

    expect(v9.buffer).toEqual(v6.buffer)
  })

  test('sets apiVersion to 9', () => {
    const { apiVersion } = RequestV9Protocol({ replicaId: -1, isolationLevel: 0, topics: [] })
    expect(apiVersion).toEqual(9)
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV9Protocol({
      replicaId: -1,
      isolationLevel: 0,
      topics: [
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, currentLeaderEpoch: 5, timestamp: -2 }],
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v9_request_java.json')))
  })

  test.each([
    ['4.3.1', '../fixtures/v9_request_kafka431.json'],
    ['4.1.2', '../fixtures/v9_request_kafka412.json'],
  ])('is the request a real Kafka %s cluster answered', (_version, fixture) => {
    const decoder = new Decoder(Buffer.from(require(fixture)))

    expect(decoder.readInt32()).toEqual(-1) // replicaId — this client is never a replica
    expect(decoder.readInt8()).toBeGreaterThanOrEqual(0) // isolationLevel

    const topics = decoder.readUVarIntArray((d) => {
      const topic = {
        name: d.readUVarIntString(),
        partitions: d.readUVarIntArray((p) => {
          const partition = {
            index: p.readInt32(),
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

    expect(topics[0].name).toEqual('kafkajs-capture-topic')
    expect(topics[0].partitions[0].timestamp).toEqual('-2') // EARLIEST
    expect(topics[0].partitions[0].currentLeaderEpoch).toBeGreaterThanOrEqual(0)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
