const RequestV9Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > Metadata > v9', () => {
  test('request', async () => {
    const { buffer } = await RequestV9Protocol({ topics: ['test-topic'] }).encode()
    const decoder = new Decoder(buffer)

    const topics = decoder.readUVarIntArray((d) => {
      const name = d.readUVarIntString()
      d.readTaggedFields()
      return name
    })

    expect(topics).toEqual(['test-topic'])
    expect(decoder.readBoolean()).toBe(true) // allowAutoTopicCreation
    expect(decoder.readBoolean()).toBe(false) // includeClusterAuthorizedOperations
    expect(decoder.readBoolean()).toBe(false) // includeTopicAuthorizedOperations
    expect(decoder.readTaggedFields().size).toEqual(0)
  })

  test('matches the vector produced by the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV9Protocol({ topics: ['test-topic'] }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v9_request_java.json')))
  })

  test('an empty topic list is the null compact array (every topic)', async () => {
    const { buffer } = await RequestV9Protocol({ topics: [] }).encode()
    expect(buffer[0]).toEqual(0)
  })

  test('still sends IncludeClusterAuthorizedOperations (removed after v10)', async () => {
    const { buffer } = await RequestV9Protocol({
      topics: ['t'],
      includeClusterAuthorizedOperations: true,
    }).encode()
    const decoder = new Decoder(buffer)
    decoder.readUVarIntArray((d) => {
      d.readUVarIntString()
      d.readTaggedFields()
    })
    decoder.readBoolean()
    expect(decoder.readBoolean()).toBe(true)
    expect(decoder.readBoolean()).toBe(false)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
