const RequestV18Protocol = require('./request')
const RequestV17Protocol = require('../v17/request')

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const args = {
  replicaId: -1,
  maxWaitTime: 5000,
  minBytes: 1,
  maxBytes: 10485760,
  isolationLevel: 1,
  sessionId: 0,
  sessionEpoch: -1,
  forgottenTopics: [],
  rackId: '',
  topics: [
    { topic: 'test-topic', partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1024 }] },
  ],
  topicIds: new Map([['test-topic', TOPIC_ID]]),
}

describe('Protocol > Requests > Fetch > v18', () => {
  test('is byte-identical to v17 apart from the api version', async () => {
    const v18 = await RequestV18Protocol(args).encode()
    const v17 = await RequestV17Protocol(args).encode()

    expect(v18.buffer).toEqual(v17.buffer)
    expect(RequestV18Protocol(args).apiVersion).toEqual(18)
    expect(RequestV17Protocol(args).apiVersion).toEqual(17)
  })
})
