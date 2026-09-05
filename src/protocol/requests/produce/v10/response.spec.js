const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v10', () => {
  test('a response without the KIP-951 tags decodes exactly like v9', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v10_response.json')))
    expect(data.topics[0].partitions[0]).toEqual({
      partition: 0,
      errorCode: 0,
      baseOffset: '100',
      logAppendTime: '-1',
      logStartOffset: '0',
      recordErrors: [],
      errorMessage: null,
      currentLeader: null,
    })
    expect(data.nodeEndpoints).toEqual([])
    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('NOT_LEADER_FOR_PARTITION carries the current leader and the node endpoints on the thrown error', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v10_response_not_leader.json')))

    expect(data.topics[0].partitions[0].currentLeader).toEqual({ leaderId: 2, leaderEpoch: 7 })
    expect(data.nodeEndpoints).toEqual([{ nodeId: 2, host: '127.0.0.1', port: 9095, rack: null }])

    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'NOT_LEADER_FOR_PARTITION',
      code: 6,
      retriable: true,
      topic: 'test-topic',
      partition: 0,
      currentLeader: { leaderId: 2, leaderEpoch: 7 },
      nodeEndpoints: [{ nodeId: 2, host: '127.0.0.1', port: 9095, rack: null }],
    })
  })

  test('an unknown tag is skipped without desynchronising the decoder', async () => {
    // The decoder keeps unknown tags as raw bytes; the codec must read tag 0 by key, never by
    // position, so a future broker adding tag 1 cannot break this.
    const currentLeader = new Encoder().writeInt32(2).writeInt32(7)
    const partition = new Encoder()
      .writeInt32(0)
      .writeInt16(6)
      .writeInt64(100)
      .writeInt64(-1)
      .writeInt64(0)
      .writeUVarIntArray([])
      .writeUVarIntString(null)
      .writeTaggedFields([
        { tag: 0, value: currentLeader },
        { tag: 1, value: Buffer.from([0xab, 0xcd]) },
      ])
    const response = new Encoder()
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('test-topic')
          .writeUVarIntArray([partition])
          .writeTaggedFields(),
      ])
      .writeInt32(0)
      .writeTaggedFields()

    const data = await decode(response.buffer)
    expect(data.topics[0].partitions[0].currentLeader).toEqual({ leaderId: 2, leaderEpoch: 7 })
    expect(data.nodeEndpoints).toEqual([])
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v10_response_java.json')))

    expect(data.topics[0].partitions[0]).toEqual({
      partition: 0,
      errorCode: 0,
      baseOffset: '100',
      logAppendTime: '-1',
      logStartOffset: '0',
      recordErrors: [],
      errorMessage: null,
      currentLeader: { leaderId: 2, leaderEpoch: 7 },
    })
    expect(data.nodeEndpoints).toEqual([{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }])
  })
})
