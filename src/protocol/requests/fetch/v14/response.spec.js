const { decode, parse } = require('./response')
const { decode: decodeV13, parse: parseV13 } = require('../v13/response')

describe('Protocol > Requests > Fetch > v14', () => {
  test('decodes identically to v13 — v14 adds only the OFFSET_MOVED_TO_TIERED_STORAGE error code, which is data', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response.json')))
    const dataV13 = await decodeV13(Buffer.from(require('../fixtures/v13_response.json')))

    expect(data).toEqual(dataV13)
    expect(parse).toBe(parseV13)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v14_response_java.json')))

    expect(data.responses[0].topicId).toEqual('01020304-0506-0708-090a-0b0c0d0e0f10')
    expect(data.responses[0].partitions[0].highWatermark).toEqual('100')
    expect(data.responses[0].partitions[0].lastStableOffset).toEqual('100')
    expect(data.responses[0].partitions[0].messages).toEqual([])
  })
})
