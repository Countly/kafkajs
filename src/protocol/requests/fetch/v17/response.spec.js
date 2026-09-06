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
})
