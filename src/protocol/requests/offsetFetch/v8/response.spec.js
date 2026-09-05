const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

describe('Protocol > Requests > OffsetFetch > v8', () => {
  test('flattens the single group into the shape every earlier version returns', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response.json')))
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

  test('surfaces the group-level error code as the top-level errorCode', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response_group_error.json')))
    expect(data.errorCode).toEqual(16)
    await expect(parse(data)).rejects.toThrow()
  })

  test('a response with anything other than exactly one group is rejected', async () => {
    const noGroups = new Encoder().writeInt32(0).writeUVarIntArray([]).writeTaggedFields()
    await expect(decode(noGroups.buffer)).rejects.toThrow(/exactly one group/)

    const twoGroups = new Encoder()
      .writeInt32(0)
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('group-1')
          .writeUVarIntArray(null)
          .writeInt16(0)
          .writeTaggedFields(),
        new Encoder()
          .writeUVarIntString('group-2')
          .writeUVarIntArray(null)
          .writeInt16(0)
          .writeTaggedFields(),
      ])
      .writeTaggedFields()
    await expect(decode(twoGroups.buffer)).rejects.toThrow(/exactly one group/)
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response_java.json')))

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

  test.each([
    ['4.3.1', '../fixtures/v8_response_kafka431.json'],
    ['4.1.2', '../fixtures/v8_response_kafka412.json'],
  ])('decodes a real broker response from Kafka %s', async (_version, fixture) => {
    const data = await decode(Buffer.from(require(fixture)))

    expect(data.errorCode).toEqual(0)
    expect(data.responses[0].topic).toEqual('kafkajs-capture-topic')
    expect(data.responses[0].partitions.length).toBeGreaterThan(0)
    expect(data.responses[0].partitions[0].errorCode).toEqual(0)

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
