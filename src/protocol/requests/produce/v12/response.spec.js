const responseV12 = require('./response')
const responseV11 = require('../v11/response')

describe('Protocol > Requests > Produce > v12', () => {
  test('decodes v11 bytes identically and shares its parse', async () => {
    const raw = Buffer.from(require('../fixtures/v11_response.json'))

    expect(await responseV12.decode(raw)).toEqual(await responseV11.decode(raw))
    expect(responseV12.parse).toBe(responseV11.parse)
  })
})
