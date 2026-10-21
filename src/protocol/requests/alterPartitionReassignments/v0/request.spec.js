const RequestV0Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > AlterPartitionReassignments > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      topics: [
        {
          topic: 'test-topic-1',
          partitionAssignment: [
            { partition: 0, replicas: [0, 1] },
            { partition: 1, replicas: [1, 2] },
          ],
        },
        {
          topic: 'test-topic-2',
          partitionAssignment: [{ partition: 0, replicas: [0, 2] }],
        },
      ],
      timeout: 30000,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })

  test('encodes to match a captured Kafka 4.3.1 request', async () => {
    const { buffer } = await RequestV0Protocol({
      topics: [
        {
          topic: 'kafkajs-capture-topic',
          partitionAssignment: [{ partition: 0, replicas: [1, 2] }],
        },
      ],
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request_kafka431.json')))
  })

  test('reads the captured request as the fields capture.js sent: a timeout and one partition with two replicas', () => {
    const decoder = new Decoder(Buffer.from(require('../fixtures/v0_request_kafka431.json')))

    const timeout = decoder.readInt32()
    const topics = decoder.readUVarIntArray((topic) => {
      const read = {
        name: topic.readUVarIntString(),
        partitions: topic.readUVarIntArray((partition) => {
          const found = {
            index: partition.readInt32(),
            replicas: partition.readUVarIntArray((replica) => replica.readInt32()),
          }
          partition.readTaggedFields()
          return found
        }),
      }
      topic.readTaggedFields()
      return read
    })
    decoder.readTaggedFields()

    expect({ timeout, topics }).toEqual({
      timeout: 5000,
      topics: [{ name: 'kafkajs-capture-topic', partitions: [{ index: 0, replicas: [1, 2] }] }],
    })
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
