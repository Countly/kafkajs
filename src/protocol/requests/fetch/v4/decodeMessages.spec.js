const Decoder = require('../../../decoder')
const Encoder = require('../../../encoder')
const decodeMessages = require('./decodeMessages')

describe('Protocol > Requests > Fetch > v4 > decodeMessages', () => {
  test('a compact null records block decodes as no messages', async () => {
    const decoder = new Decoder(new Encoder().writeUVarInt(0).buffer)
    await expect(decodeMessages(decoder, { compact: true })).resolves.toEqual({ messages: [] })
  })

  test('the compact size read is the only difference: the same batch decodes either way', async () => {
    const batch = Buffer.from(require('../fixtures/v11_response.json')) // any buffer works here
    const classic = new Decoder(new Encoder().writeInt32(batch.length).writeBuffer(batch).buffer)
    const compact = new Decoder(
      new Encoder().writeUVarInt(batch.length + 1).writeBuffer(batch).buffer
    )

    expect(classic.readInt32()).toEqual(batch.length)
    expect(compact.readUVarInt() - 1).toEqual(batch.length)
  })
})
