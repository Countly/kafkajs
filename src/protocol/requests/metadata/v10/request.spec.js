const RequestV10Protocol = require('./request')
const Decoder = require('../../../decoder')

const ZERO_UUID_BYTES = new Array(16).fill(0)

describe('Protocol > Requests > Metadata > v10', () => {
  test('sends the zero topic id alongside the name', async () => {
    const { buffer } = await RequestV10Protocol({ topics: ['test-topic'] }).encode()
    const decoder = new Decoder(buffer)

    const topics = decoder.readUVarIntArray((d) => {
      const topic = { topicId: d.readUuid(), name: d.readUVarIntString() }
      d.readTaggedFields()
      return topic
    })

    expect(topics).toEqual([{ topicId: null, name: 'test-topic' }])
    expect(decoder.readBoolean()).toBe(true) // allowAutoTopicCreation
    expect(decoder.readBoolean()).toBe(false) // includeClusterAuthorizedOperations
    expect(decoder.readBoolean()).toBe(false) // includeTopicAuthorizedOperations
    expect(decoder.readTaggedFields().size).toEqual(0)
  })

  test('the zero uuid is 16 zero bytes on the wire, not a null sentinel', async () => {
    const { buffer } = await RequestV10Protocol({ topics: ['test-topic'] }).encode()
    expect([...buffer.slice(1, 17)]).toEqual(ZERO_UUID_BYTES)
  })

  test('an empty topic list is the null compact array (every topic)', async () => {
    const { buffer } = await RequestV10Protocol({ topics: [] }).encode()
    expect(buffer[0]).toEqual(0)
  })

  test('still sends IncludeClusterAuthorizedOperations (removed after v10)', async () => {
    const { buffer } = await RequestV10Protocol({ topics: ['t'] }).encode()
    const decoder = new Decoder(buffer)
    decoder.readUVarIntArray((d) => {
      d.readUuid()
      d.readUVarIntString()
      d.readTaggedFields()
    })
    decoder.readBoolean()
    decoder.readBoolean() // includeClusterAuthorizedOperations
    expect(decoder.readBoolean()).toBe(false) // includeTopicAuthorizedOperations
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
