const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const partition = () =>
  new Encoder()
    .writeInt32(0)
    .writeInt64(10)
    .writeInt32(3)
    .writeUVarIntString(null)
    .writeInt16(0)
    .writeTaggedFields()

const topic = () =>
  new Encoder()
    .writeUVarIntString('test-topic')
    .writeUVarIntArray([partition()])
    .writeTaggedFields()

const response = () =>
  new Encoder().writeInt32(0).writeUVarIntArray([topic()]).writeInt16(0).writeTaggedFields()

describe('Protocol > Requests > OffsetFetch > v7', () => {
  test('response is byte-identical to v6 (RequireStable is request-only)', async () => {
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
    const data = await decode(Buffer.from(require('../fixtures/v7_response_java.json')))

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
