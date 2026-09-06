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
})
