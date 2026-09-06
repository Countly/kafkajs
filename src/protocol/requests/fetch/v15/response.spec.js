const { decode, parse } = require('./response')
const { decode: decodeV14, parse: parseV14 } = require('../v14/response')

describe('Protocol > Requests > Fetch > v15', () => {
  test('decodes identically to v14 — v15 adds no response fields', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response.json')))
    const dataV14 = await decodeV14(Buffer.from(require('../fixtures/v13_response.json')))

    expect(data).toEqual(dataV14)
    expect(parse).toBe(parseV14)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v15_response_java.json')))

    expect(data.responses[0].topicId).toEqual('01020304-0506-0708-090a-0b0c0d0e0f10')
    expect(data.responses[0].partitions[0].highWatermark).toEqual('100')
    expect(data.responses[0].partitions[0].lastStableOffset).toEqual('100')
    expect(data.responses[0].partitions[0].messages).toEqual([])
  })
})
