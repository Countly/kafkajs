const { decode, parse } = require('./response')
const Encoder = require('../../../encoder')
const golden = require('../fixtures/v1_response.json')

describe('Protocol > Requests > SASLHandshake > v1 response', () => {
  test('decodes the hand-computed golden', async () => {
    const decoded = await decode(Buffer.from(golden.hex, 'hex'))

    expect(decoded).toEqual(golden.fields)
    await expect(parse(decoded)).resolves.toBe(decoded)
  })

  test('parse throws the broker error for a mechanism it does not enable', async () => {
    const body = new Encoder().writeInt16(33).writeArray(['PLAIN'], 'string').buffer

    await expect(parse(await decode(body))).rejects.toMatchObject({
      type: 'UNSUPPORTED_SASL_MECHANISM',
      code: 33,
    })
  })
})
