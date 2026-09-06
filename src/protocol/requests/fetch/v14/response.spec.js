const { decode, parse } = require('./response')
const { decode: decodeV13, parse: parseV13 } = require('../v13/response')

describe('Protocol > Requests > Fetch > v14', () => {
  test('decodes identically to v13 — v14 adds only the OFFSET_MOVED_TO_TIERED_STORAGE error code, which is data', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response.json')))
    const dataV13 = await decodeV13(Buffer.from(require('../fixtures/v13_response.json')))

    expect(data).toEqual(dataV13)
    expect(parse).toBe(parseV13)
  })
})
