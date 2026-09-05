const RequestV6Protocol = require('../v6/request')
const RequestV7Protocol = require('./request')

describe('Protocol > Requests > ListOffsets > v7', () => {
  test('request is byte-identical to v6, apart from the version header', async () => {
    const topics = [
      { topic: 'test-topic', partitions: [{ partition: 0, timestamp: -1, currentLeaderEpoch: 2 }] },
    ]

    const v6 = await RequestV6Protocol({ replicaId: -1, isolationLevel: 0, topics }).encode()
    const v7 = await RequestV7Protocol({ replicaId: -1, isolationLevel: 0, topics }).encode()

    expect(v7.buffer).toEqual(v6.buffer)
  })

  test('sets apiVersion to 7', () => {
    const { apiVersion } = RequestV7Protocol({ replicaId: -1, isolationLevel: 0, topics: [] })
    expect(apiVersion).toEqual(7)
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV7Protocol({
      replicaId: -1,
      isolationLevel: 0,
      topics: [
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, currentLeaderEpoch: 5, timestamp: -2 }],
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v7_request_java.json')))
  })
})
