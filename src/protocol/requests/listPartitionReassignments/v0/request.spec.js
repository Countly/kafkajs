const RequestV0Protocol = require('./request')
const Decoder = require('../../../decoder')

/* TO-DO: Update the fixtures, data, and errors */

describe('Protocol > Requests > AlterPartitionReassignments > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      topics: [
        {
          topic: 'test-topic-1f131dd7f83b8d72a447-33298-d13ec602-1a34-41c8-b59e-0657aef3ad25',
          partitions: [0],
        },
      ],
      timeout: 5000,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })

  test('encodes to match a captured Kafka 4.3.1 request', async () => {
    const { buffer } = await RequestV0Protocol({}).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request_kafka431.json')))
  })

  test('reads the captured request as the fields capture.js sent: a timeout and every topic', () => {
    const decoder = new Decoder(Buffer.from(require('../fixtures/v0_request_kafka431.json')))

    const timeout = decoder.readInt32()
    const topics = decoder.readUVarIntArray(() => {
      throw new Error('the capture lists no topic')
    })
    decoder.readTaggedFields()

    expect({ timeout, topics }).toEqual({ timeout: 5000, topics: null })
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
