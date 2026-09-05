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

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV7Protocol({
      groupId: 'test-group',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v7_request_java.json')))
  })
})
