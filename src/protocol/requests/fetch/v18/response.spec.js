const { decode, parse } = require('./response')
const { decode: decodeV17, parse: parseV17 } = require('../v17/response')

describe('Protocol > Requests > Fetch > v18', () => {
  test('decodes identically to v17 — v18 adds no response fields', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v16_response_node_endpoints.json')))
    const dataV17 = await decodeV17(
      Buffer.from(require('../fixtures/v16_response_node_endpoints.json'))
    )

    expect(data).toEqual(dataV17)
    expect(parse).toBe(parseV17)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v18_response_java.json')))

    expect(data.responses[0].topicId).toEqual('01020304-0506-0708-090a-0b0c0d0e0f10')
    expect(data.responses[0].partitions[0].highWatermark).toEqual('100')
    expect(data.nodeEndpoints).toEqual([{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }])
  })

  test.each([
    ['4.3.1', '../fixtures/v18_response_kafka431.json'],
    ['4.1.2', '../fixtures/v18_response_kafka412.json'],
  ])('decodes a real broker response from Kafka %s', async (_version, fixture) => {
    const data = await decode(Buffer.from(require(fixture)))

    expect(data.errorCode).toEqual(0)
    expect(Array.isArray(data.responses)).toBe(true)
    expect(data.responses[0].topicId).toEqual('01020304-0506-0708-090a-0b0c0d0e0f10')
    expect(data.responses[0].partitions[0].errorCode).toEqual(0)
    // A fetch that hit no leadership error carries no KIP-951 hint.
    expect(data.nodeEndpoints).toEqual([])

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
