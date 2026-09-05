const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v11', () => {
  test('decodes identically to v10 — v11 adds no response fields', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v11_response.json')))
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

  test('TRANSACTION_ABORTABLE surfaces as the non-retriable protocol error it already is', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v11_response.json')))
    data.topics[0].partitions[0].errorCode = 120
    await expect(parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      code: 120,
      retriable: false,
    })
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v11_response_java.json')))

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

  test.each([
    ['4.3.1', '../fixtures/v11_response_kafka431.json'],
    ['4.1.2', '../fixtures/v11_response_kafka412.json'],
  ])('decodes a real broker response from Kafka %s', async (_version, fixture) => {
    const data = await decode(Buffer.from(require(fixture)))

    expect(data.topics[0].topicName).toEqual('kafkajs-capture-topic')
    expect(data.topics[0].partitions[0].errorCode).toEqual(0)
    // A successful produce carries no KIP-951 leader hint.
    expect(data.topics[0].partitions[0].currentLeader).toBeNull()
    expect(data.nodeEndpoints).toEqual([])

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
