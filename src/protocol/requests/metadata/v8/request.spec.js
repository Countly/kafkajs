const RequestV8Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > Metadata > v8', () => {
  test('request', async () => {
    const { buffer } = await RequestV8Protocol({ topics: ['test-topic'] }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readArray((d) => d.readString())).toEqual(['test-topic'])
    expect(decoder.readBoolean()).toBe(true)
  })

  test('an empty topic list asks for every topic (null array)', async () => {
    const { buffer } = await RequestV8Protocol({ topics: [] }).encode()
    expect(new Decoder(buffer).readInt32()).toEqual(-1)
  })

  test('sends both authorized-operations flags as false', async () => {
    const { buffer } = await RequestV8Protocol({ topics: ['test-topic'] }).encode()
    const decoder = new Decoder(buffer)

    decoder.readArray((d) => d.readString())
    expect(decoder.readBoolean()).toBe(true) // allowAutoTopicCreation
    expect(decoder.readBoolean()).toBe(false) // includeClusterAuthorizedOperations
    expect(decoder.readBoolean()).toBe(false) // includeTopicAuthorizedOperations
  })
})
