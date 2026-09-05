const RequestV7Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > OffsetFetch > v7', () => {
  test('sends RequireStable as false', async () => {
    const { buffer } = await RequestV7Protocol({ groupId: 'g', topics: [] }).encode()
    const decoder = new Decoder(buffer)

    decoder.readUVarIntString() // groupId
    decoder.readUVarIntArray(() => {}) // topics (null: empty list means "fetch all")
    expect(decoder.readBoolean()).toEqual(false)
    expect(decoder.readTaggedFields().size).toEqual(0)
  })
})
