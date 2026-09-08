const RequestV11Protocol = require('./request')
const Decoder = require('../../../decoder')

const ZERO_UUID_BYTES = new Array(16).fill(0)
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

describe('Protocol > Requests > Metadata > v11', () => {
  test('sends the zero topic id alongside the name', async () => {
    const { buffer } = await RequestV11Protocol({ topics: ['test-topic'] }).encode()
    const decoder = new Decoder(buffer)

    const topics = decoder.readUVarIntArray((d) => {
      const topic = { topicId: d.readUuid(), name: d.readUVarIntString() }
      d.readTaggedFields()
      return topic
    })

    expect(topics).toEqual([{ topicId: null, name: 'test-topic' }])
    expect(decoder.readBoolean()).toBe(true) // allowAutoTopicCreation
    expect(decoder.readBoolean()).toBe(false) // includeTopicAuthorizedOperations
    expect(decoder.readTaggedFields().size).toEqual(0)
  })

  test('the zero uuid is 16 zero bytes on the wire, not a null sentinel', async () => {
    const { buffer } = await RequestV11Protocol({ topics: ['test-topic'] }).encode()
    expect([...buffer.slice(1, 17)]).toEqual(ZERO_UUID_BYTES)
  })

  test('matches the vector produced by the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV11Protocol({ topics: ['test-topic'] }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v11_request_java.json')))
  })

  test('an empty topic list is the null compact array (every topic)', async () => {
    const { buffer } = await RequestV11Protocol({ topics: [] }).encode()
    expect(buffer[0]).toEqual(0)
  })

  test('does not send IncludeClusterAuthorizedOperations (removed after v10)', async () => {
    const { buffer } = await RequestV11Protocol({ topics: ['t'] }).encode()
    const decoder = new Decoder(buffer)
    decoder.readUVarIntArray((d) => {
      d.readUuid()
      d.readUVarIntString()
      d.readTaggedFields()
    })
    decoder.readBoolean()
    decoder.readBoolean()
    // only the body tag buffer is left
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('writes a topic id with a null name', async () => {
    const { buffer } = await RequestV11Protocol({
      topics: [{ topicId: TOPIC_ID }],
    }).encode()
    const decoder = new Decoder(buffer)
    const topics = decoder.readUVarIntArray((d) => {
      const topic = { topicId: d.readUuid(), name: d.readUVarIntString() }
      d.readTaggedFields()
      return topic
    })

    expect(topics).toEqual([{ topicId: TOPIC_ID, name: null }])
  })

  test('never asks a broker to auto-create a topic it identified only by id', async () => {
    // A topic requested by id carries no name, and a broker only auto-creates topics requested
    // BY NAME - so the flag cannot apply, and sending `true` would only be misleading.
    const { buffer } = await RequestV11Protocol({
      topics: [{ topicId: TOPIC_ID }],
      allowAutoTopicCreation: true,
    }).encode()
    const decoder = new Decoder(buffer)
    decoder.readUVarIntArray((d) => {
      d.readUuid()
      d.readUVarIntString()
      d.readTaggedFields()
    })

    expect(decoder.readBoolean()).toBe(false)
  })

  test('leaves the flag alone when any topic is named', async () => {
    // The rule is about a request that CANNOT create anything, not about mixed requests: one
    // named topic in the list is one topic the broker could auto-create.
    const { buffer } = await RequestV11Protocol({
      topics: ['test-topic', { topicId: TOPIC_ID }],
      allowAutoTopicCreation: true,
    }).encode()
    const decoder = new Decoder(buffer)
    const topics = decoder.readUVarIntArray((d) => {
      const topic = { topicId: d.readUuid(), name: d.readUVarIntString() }
      d.readTaggedFields()
      return topic
    })

    // A by-name topic writes the all-zero "no uuid" sentinel, which `readUuid` reads back as null.
    expect(topics).toEqual([
      { topicId: null, name: 'test-topic' },
      { topicId: TOPIC_ID, name: null },
    ])
    expect(decoder.readBoolean()).toBe(true)
  })
})
