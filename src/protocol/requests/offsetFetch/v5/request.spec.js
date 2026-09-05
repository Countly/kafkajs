const RequestV5Protocol = require('./request')

describe('Protocol > Requests > OffsetFetch > v5', () => {
  test('request', async () => {
    const groupId =
      'consumer-group-id-d1492d7a3c14a838a28f-20117-ae82781b-863d-4f23-9377-d165ca585f31'

    const topics = [
      {
        topic: 'test-topic-df48241c4bf2fca9d16b-20117-aff9b64c-69a2-4456-be7b-de5bcd78984e',
        partitions: [{ partition: 0 }],
      },
    ]

    const { buffer } = await RequestV5Protocol({ groupId, topics }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request.json')))
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV5Protocol({
      groupId: 'test-group',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v5_request_java.json')))
  })
})
