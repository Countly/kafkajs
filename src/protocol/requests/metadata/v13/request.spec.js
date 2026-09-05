const RequestV13Protocol = require('./request')
const Decoder = require('../../../decoder')

const ZERO_UUID_BYTES = new Array(16).fill(0)

describe('Protocol > Requests > Metadata > v13', () => {
  test('is byte-identical to v12 apart from the api version', async () => {
    const v13 = await RequestV13Protocol({ topics: ['test-topic'] }).encode()
    const v12 = await require('../v12/request')({ topics: ['test-topic'] }).encode()

    expect(v13.buffer).toEqual(v12.buffer)
    expect(RequestV13Protocol({ topics: [] }).apiVersion).toEqual(13)
  })

  test('sends the zero topic id alongside the name', async () => {
    const { buffer } = await RequestV13Protocol({ topics: ['test-topic'] }).encode()
    const decoder = new Decoder(buffer)
    const topics = decoder.readUVarIntArray((d) => {
      const topic = { topicId: d.readUuid(), name: d.readUVarIntString() }
      d.readTaggedFields()
      return topic
    })

    expect(topics).toEqual([{ topicId: null, name: 'test-topic' }])
    expect([...buffer.slice(1, 17)]).toEqual(ZERO_UUID_BYTES)
    expect(decoder.readBoolean()).toBe(true) // allowAutoTopicCreation
    expect(decoder.readBoolean()).toBe(false) // includeTopicAuthorizedOperations
    expect(decoder.readTaggedFields().size).toEqual(0)
  })

  test('an empty topic list is the null compact array (every topic)', async () => {
    const { buffer } = await RequestV13Protocol({ topics: [] }).encode()
    expect(buffer[0]).toEqual(0)
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV13Protocol({ topics: ['test-topic'] }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v13_request_java.json')))
  })
})
