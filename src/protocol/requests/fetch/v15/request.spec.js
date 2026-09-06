const RequestV15Protocol = require('./request')
const RequestV14Protocol = require('../v14/request')
const Decoder = require('../../../decoder')

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

describe('Protocol > Requests > Fetch > v15', () => {
  test('writes no ReplicaId: the field closed at v14 and the replacement is replica-only', async () => {
    const v15 = await RequestV15Protocol(args).encode()
    const v14 = await RequestV14Protocol(args).encode()

    expect(v15.buffer.length).toEqual(v14.buffer.length - 4)
    expect(v15.buffer).toEqual(Buffer.from(require('../fixtures/v15_request.json')))
    expect(RequestV15Protocol(args).apiVersion).toEqual(15)

    // The request now opens on MaxWaitMs, not on ReplicaId.
    const decoder = new Decoder(v15.buffer)
    expect(decoder.readInt32()).toEqual(5000)
    expect(decoder.readInt32()).toEqual(1)
    expect(decoder.readInt32()).toEqual(10485760)
  })

  test('the ReplicaState tag is never written', async () => {
    const { buffer } = await RequestV15Protocol(args).encode()
    const decoder = new Decoder(buffer)

    // Walk to the body tag section and assert it is genuinely empty, rather than checking the last
    // byte - which the fixture byte-equality above already covers and which a trailing empty
    // struct would satisfy for the wrong reason.
    decoder.readInt32() // maxWaitMs
    decoder.readInt32() // minBytes
    decoder.readInt32() // maxBytes
    decoder.readInt8() //  isolationLevel
    decoder.readInt32() // sessionId
    decoder.readInt32() // sessionEpoch
    decoder.readUVarIntArray((d) => {
      d.readUuid()
      d.readUVarIntArray((p) => {
        p.readInt32()
        p.readInt32()
        p.readInt64()
        p.readInt32()
        p.readInt64()
        p.readInt32()
        p.readTaggedFields()
      })
      d.readTaggedFields()
    })
    decoder.readUVarIntArray(() => {}) // forgottenTopicsData
    decoder.readUVarIntString() //        rackId

    expect(decoder.readTaggedFields().size).toEqual(0) // no ReplicaState (tag 1), no ClusterId
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('encodes byte-identically to the vector Apache own codec produced', async () => {
    const { buffer } = await RequestV15Protocol(javaVectorArgs).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v15_request_java.json')))
  })
})
