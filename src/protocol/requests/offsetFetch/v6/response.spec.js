const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const partition = ({
  partitionIndex = 0,
  offset = 10,
  committedLeaderEpoch = 3,
  metadata = null,
  errorCode = 0,
} = {}) =>
  new Encoder()
    .writeInt32(partitionIndex)
    .writeInt64(offset)
    .writeInt32(committedLeaderEpoch)
    .writeUVarIntString(metadata)
    .writeInt16(errorCode)
    .writeTaggedFields()

const topic = ({ name = 'test-topic', partitions = [partition()] } = {}) =>
  new Encoder().writeUVarIntString(name).writeUVarIntArray(partitions).writeTaggedFields()

const response = ({ throttleTime = 0, topics = [topic()], errorCode = 0 } = {}) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeUVarIntArray(topics)
    .writeInt16(errorCode)
    .writeTaggedFields()

describe('Protocol > Requests > OffsetFetch > v6', () => {
  test('response', async () => {
    const { buffer } = response()
    const data = await decode(buffer)
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 0, offset: '10', committedLeaderEpoch: 3, metadata: null, errorCode: 0 },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v6_response_java.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 0, offset: '10', committedLeaderEpoch: 3, metadata: null, errorCode: 0 },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
