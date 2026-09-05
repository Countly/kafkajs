const RequestV6Protocol = require('../v6/request')
const RequestV8Protocol = require('./request')

describe('Protocol > Requests > ListOffsets > v8', () => {
  test('request is byte-identical to v6, apart from the version header', async () => {
    const topics = [
      { topic: 'test-topic', partitions: [{ partition: 0, timestamp: -1, currentLeaderEpoch: 2 }] },
    ]

    const v6 = await RequestV6Protocol({ replicaId: -1, isolationLevel: 0, topics }).encode()
    const v8 = await RequestV8Protocol({ replicaId: -1, isolationLevel: 0, topics }).encode()

    expect(v8.buffer).toEqual(v6.buffer)
  })

  test('sets apiVersion to 8', () => {
    const { apiVersion } = RequestV8Protocol({ replicaId: -1, isolationLevel: 0, topics: [] })
    expect(apiVersion).toEqual(8)
  })
})
