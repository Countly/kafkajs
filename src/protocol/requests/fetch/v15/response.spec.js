const { decode, parse } = require('./response')
const { decode: decodeV14, parse: parseV14 } = require('../v14/response')

describe('Protocol > Requests > Fetch > v15', () => {
  test('decodes identically to v14 — v15 adds no response fields', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response.json')))
    const dataV14 = await decodeV14(Buffer.from(require('../fixtures/v13_response.json')))

    expect(data).toEqual(dataV14)
    expect(parse).toBe(parseV14)
  })
})
