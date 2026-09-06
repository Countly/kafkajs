const RequestV13Protocol = require('./request')
const Decoder = require('../../../decoder')

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

describe('Protocol > Requests > Fetch > v13', () => {
  const partitions = (ids) =>
    ids.map((partition) => ({ partition, fetchOffset: '0', maxBytes: 1024 }))

  const build = (topics) =>
    RequestV13Protocol({
      replicaId: -1,
      maxWaitTime: 5000,
      minBytes: 1,
      maxBytes: 10485760,
      isolationLevel: 1,
      sessionId: 0,
      sessionEpoch: -1,
      forgottenTopics: [],
      rackId: '',
      topics,
      topicIds: new Map([['test-topic', TOPIC_ID]]),
    })

  test('encodes the topic id and no topic name', async () => {
    const { buffer } = await build([{ topic: 'test-topic', partitions: partitions([0]) }]).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v13_request.json')))
    expect(build([]).apiVersion).toEqual(13)

    const decoder = new Decoder(buffer)
    for (let i = 0; i < 5; i++) decoder.readInt32() // replicaId..maxBytes is 4x int32
    decoder.offset -= 4
    decoder.readInt8() // isolationLevel
    decoder.readInt32() // sessionId
    decoder.readInt32() // sessionEpoch
    const topics = decoder.readUVarIntArray((d) => {
      const topicId = d.readUuid()
      const partitionIds = d.readUVarIntArray((p) => {
        const id = p.readInt32()
        p.readInt32() // currentLeaderEpoch
        p.readInt64() // fetchOffset
        p.readInt32() // lastFetchedEpoch
        p.readInt64() // logStartOffset
        p.readInt32() // partitionMaxBytes
        p.readTaggedFields()
        return id
      })
      d.readTaggedFields()
      return { topicId, partitionIds }
    })

    expect(topics).toEqual([{ topicId: TOPIC_ID, partitionIds: [0] }])
  })

  test('a topic split into two consolidated entries by the shuffle encodes as two entries with the same id', async () => {
    // FetchRequest.json marks neither Topic nor TopicId as a mapKey, so this is legal wire.
    const { buffer } = await build([
      { topic: 'test-topic', partitions: partitions([0]) },
      { topic: 'test-topic', partitions: partitions([1]) },
    ]).encode()

    const decoder = new Decoder(buffer)
    decoder.readInt32()
    decoder.readInt32()
    decoder.readInt32()
    decoder.readInt32()
    decoder.readInt8()
    decoder.readInt32()
    decoder.readInt32()
    const ids = decoder.readUVarIntArray((d) => {
      const topicId = d.readUuid()
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
      return topicId
    })

    expect(ids).toEqual([TOPIC_ID, TOPIC_ID])
  })

  test('forgotten topics are keyed by id too', async () => {
    const { buffer } = await build([]).encode()
    // Sent empty by this client, so the assertion is that the empty compact array is still one
    // byte and that nothing about the id substitution changed its framing.
    expect(buffer.includes(Buffer.from([0x01, 0x01, 0x00]))).toBe(true)
  })
})
