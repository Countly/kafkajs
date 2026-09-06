const RequestV16Protocol = require('./request')
const RequestV15Protocol = require('../v15/request')

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

describe('Protocol > Requests > Fetch > v16', () => {
  test('is byte-identical to v15 apart from the api version', async () => {
    const v16 = await RequestV16Protocol(args).encode()
    const v15 = await RequestV15Protocol(args).encode()

    expect(v16.buffer).toEqual(v15.buffer)
    expect(RequestV16Protocol(args).apiVersion).toEqual(16)
    expect(RequestV15Protocol(args).apiVersion).toEqual(15)
  })
})
