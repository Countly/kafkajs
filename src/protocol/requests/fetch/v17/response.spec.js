const { decode, parse } = require('./response')
const { decode: decodeV16, parse: parseV16 } = require('../v16/response')

describe('Protocol > Requests > Fetch > v17', () => {
  test('decodes identically to v16 — v17 adds no response fields', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v16_response_node_endpoints.json')))
    const dataV16 = await decodeV16(
      Buffer.from(require('../fixtures/v16_response_node_endpoints.json'))
    )

    expect(data).toEqual(dataV16)
    expect(parse).toBe(parseV16)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v17_response_java.json')))

    expect(data.responses[0].topicId).toEqual('01020304-0506-0708-090a-0b0c0d0e0f10')
    expect(data.responses[0].partitions[0].highWatermark).toEqual('100')
    expect(data.nodeEndpoints).toEqual([{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }])
  })
})
