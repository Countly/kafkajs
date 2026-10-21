const RequestV13Protocol = require('./request')
const Decoder = require('../../../decoder')
const { Types } = require('../../../message/compression')
const { KafkaJSInvariantViolation } = require('../../../../errors')
const Long = require('../../../../utils/long')
const golden = require('../fixtures/v13_request_golden.json')

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

describe('Protocol > Requests > Produce > v13', () => {
  const build = (topicData) =>
    RequestV13Protocol({
      acks: -1,
      timeout: 30000,
      compression: Types.None,
      topicData,
      topicIds: new Map([['test-topic', TOPIC_ID]]),
    })

  test('encodes the topic id exactly once per topic, and no name', async () => {
    const { buffer } = await build([
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, firstSequence: 0, messages: [{ value: 'a', timestamp: 1700000000000 }] },
          { partition: 1, firstSequence: 0, messages: [{ value: 'b', timestamp: 1700000000000 }] },
        ],
      },
    ]).encode()

    const decoder = new Decoder(buffer)
    expect(decoder.readUVarIntString()).toBeNull() // transactionalId
    expect(decoder.readInt16()).toEqual(-1) // acks
    expect(decoder.readInt32()).toEqual(30000) // timeoutMs

    const topics = decoder.readUVarIntArray((d) => {
      const topicId = d.readUuid()
      const partitions = d.readUVarIntArray((p) => {
        const partition = p.readInt32()
        p.readUVarIntBytes()
        p.readTaggedFields()
        return partition
      })
      d.readTaggedFields()
      return { topicId, partitions }
    })

    // ProduceRequest.json marks both Name and TopicId as mapKey: exactly one entry per topic,
    // which is what the producer's grouping by topic already guarantees.
    expect(topics).toEqual([{ topicId: TOPIC_ID, partitions: [0, 1] }])
    expect(build([]).apiVersion).toEqual(13)
    expect(decoder.readTaggedFields().size).toEqual(0)
  })

  test('throws rather than encoding an all-zero uuid when a topic has no id', async () => {
    // Defensive only: `Broker#produce`'s v13 cap never reaches this codec without an id for every
    // topic, so this can only happen if a caller bypasses that cap.
    const { encode } = RequestV13Protocol({
      acks: -1,
      timeout: 30000,
      compression: Types.None,
      topicData: [
        {
          topic: 'unknown-topic',
          partitions: [{ partition: 0, firstSequence: 0, messages: [{ value: 'a' }] }],
        },
      ],
      topicIds: new Map(),
    })

    await expect(encode()).rejects.toThrow(KafkaJSInvariantViolation)
    await expect(encode()).rejects.toThrow(/unknown-topic/)
  })

  test('decodes the vector Apache own codec produced', async () => {
    // Decoded rather than byte-compared: the generator emits a null Records block, because a
    // record batch carries a wall-clock timestamp and a CRC over it no second encoder reproduces.
    const decoder = new Decoder(Buffer.from(require('../fixtures/v13_request_java.json')))

    expect(decoder.readUVarIntString()).toBeNull() // transactionalId
    expect(decoder.readInt16()).toEqual(-1) // acks
    expect(decoder.readInt32()).toEqual(30000) // timeoutMs

    const topics = decoder.readUVarIntArray((d) => {
      const topic = {
        topicId: d.readUuid(),
        partitions: d.readUVarIntArray((p) => {
          const partition = { index: p.readInt32(), records: p.readUVarIntBytes() }
          p.readTaggedFields()
          return partition
        }),
      }
      d.readTaggedFields()
      return topic
    })

    expect(topics).toEqual([{ topicId: TOPIC_ID, partitions: [{ index: 0, records: null }] }])
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})

describe('Protocol > Requests > Produce > v13 > a hand-computed request', () => {
  test('encodes the hand-computed golden byte for byte', async () => {
    const { buffer } = await require('./request')({
      acks: -1,
      timeout: 30000,
      producerId: Long.fromInt(1000),
      producerEpoch: 3,
      topicIds: new Map([['test-topic', '01020304-0506-0708-090a-0b0c0d0e0f10']]),
      topicData: [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              firstSequence: 7,
              messages: [{ key: 'k', value: 'v', timestamp: 1700000000000 }],
            },
          ],
        },
      ],
    }).encode()

    expect(buffer.toString('hex')).toEqual(golden.hex)
  })
})
