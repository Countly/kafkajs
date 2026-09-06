const RequestV18Protocol = require('./request')
const Decoder = require('../../../decoder')
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

// The values `GenerateVectors.java` encodes: a consumer's full fetch of one partition at offset 42.
const javaVectorArgs = {
  ...args,
  topics: [
    {
      topic: 'test-topic',
      partitions: [
        {
          partition: 0,
          currentLeaderEpoch: 5,
          fetchOffset: '42',
          logStartOffset: -1,
          maxBytes: 1048576,
        },
      ],
    },
  ],
}

describe('Protocol > Requests > Fetch > v18', () => {
  test('is byte-identical to v17 apart from the api version', async () => {
    const v18 = await RequestV18Protocol(args).encode()
    const v17 = await RequestV17Protocol(args).encode()

    expect(v18.buffer).toEqual(v17.buffer)
    expect(RequestV18Protocol(args).apiVersion).toEqual(18)
    expect(RequestV17Protocol(args).apiVersion).toEqual(17)
  })

  test('encodes byte-identically to the vector Apache own codec produced', async () => {
    const { buffer } = await RequestV18Protocol(javaVectorArgs).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v18_request_java.json')))
  })

  test.each([
    ['4.3.1', '../fixtures/v18_request_kafka431.json'],
    ['4.1.2', '../fixtures/v18_request_kafka412.json'],
  ])(
    'a real request this client sent to Kafka %s is a sessionless full fetch keyed by id',
    (_version, fixture) => {
      const decoder = new Decoder(Buffer.from(require(fixture)))

      // The body opens on MaxWaitMs: ReplicaId closed at v14 and the ReplicaState tag is never sent.
      expect(decoder.readInt32()).toEqual(5000)
      decoder.readInt32() // minBytes
      decoder.readInt32() // maxBytes
      decoder.readInt8() //  isolationLevel
      expect(decoder.readInt32()).toEqual(0) //  sessionId: every fetch is a full fetch
      expect(decoder.readInt32()).toEqual(-1) // sessionEpoch

      const topics = decoder.readUVarIntArray((t) => {
        const topic = {
          topicId: t.readUuid(),
          // The capture topic has two partitions and the leader epoch is whatever the run elected,
          // so only LastFetchedEpoch is read back by value.
          lastFetchedEpochs: t.readUVarIntArray((p) => {
            p.readInt32() // partition
            p.readInt32() // currentLeaderEpoch
            p.readInt64() // fetchOffset
            const lastFetchedEpoch = p.readInt32()
            p.readInt64() // logStartOffset
            p.readInt32() // partitionMaxBytes
            expect(p.readTaggedFields().size).toEqual(0)
            return lastFetchedEpoch
          }),
        }
        expect(t.readTaggedFields().size).toEqual(0)
        return topic
      })

      expect(topics).toHaveLength(1)
      expect(topics[0].topicId).toEqual(TOPIC_ID)
      expect(topics[0].lastFetchedEpochs).toEqual([-1])
      expect(decoder.readUVarIntArray(() => {})).toEqual([]) // forgottenTopicsData
      expect(decoder.readUVarIntString()).toEqual('') //        rackId
      expect(decoder.readTaggedFields().size).toEqual(0)
      expect(decoder.canReadBytes(1)).toBe(false)
    }
  )
})
