const RequestV7Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > Metadata > v7', () => {
  test('request', async () => {
    const { buffer } = await RequestV7Protocol({ topics: ['test-topic'] }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readArray((d) => d.readString())).toEqual(['test-topic'])
    expect(decoder.readBoolean()).toBe(true)
  })

  test('an empty topic list asks for every topic (null array)', async () => {
    const { buffer } = await RequestV7Protocol({ topics: [] }).encode()
    expect(new Decoder(buffer).readInt32()).toEqual(-1)
  })
})
