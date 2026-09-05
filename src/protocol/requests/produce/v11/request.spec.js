const RequestV11Protocol = require('./request')

describe('Protocol > Requests > Produce > v11', () => {
  const topicData = [
    {
      topic: 'test-topic',
      partitions: [
        { partition: 0, messages: [{ key: 'k', value: 'v', timestamp: 1700000000000 }] },
      ],
    },
  ]

  test('is byte-identical to v10 — the request gains nothing until v13', async () => {
    const v11 = await RequestV11Protocol({ acks: -1, timeout: 30000, topicData }).encode()
    const v10 = await require('../v10/request')({ acks: -1, timeout: 30000, topicData }).encode()

    expect(v11.buffer).toEqual(v10.buffer)
  })

  test('expectResponse follows acks, as every earlier version does', () => {
    expect(RequestV11Protocol({ acks: 0, timeout: 1, topicData }).expectResponse()).toBe(false)
    expect(RequestV11Protocol({ acks: -1, timeout: 1, topicData }).expectResponse()).toBe(true)
  })
})
