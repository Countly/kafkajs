const RequestV8Protocol = require('./request')
const RequestV7Protocol = require('../v7/request')

describe('Protocol > Requests > Produce > v8', () => {
  const topicData = [
    {
      topic: 'test-topic',
      partitions: [
        { partition: 0, messages: [{ key: 'k', value: 'v', timestamp: 1700000000000 }] },
      ],
    },
  ]

  test("is v7's request shape at apiVersion 8 - KIP-467 only changes the response", async () => {
    const v8 = RequestV8Protocol({ acks: -1, timeout: 30000, topicData })
    expect(v8.apiVersion).toEqual(8)

    const v8Encoded = await v8.encode()
    const v7Encoded = await RequestV7Protocol({ acks: -1, timeout: 30000, topicData }).encode()

    expect(v8Encoded.buffer).toEqual(v7Encoded.buffer)
  })

  test('expectResponse follows acks, as every earlier version does', () => {
    expect(RequestV8Protocol({ acks: 0, timeout: 1, topicData }).expectResponse()).toBe(false)
    expect(RequestV8Protocol({ acks: -1, timeout: 1, topicData }).expectResponse()).toBe(true)
  })
})
