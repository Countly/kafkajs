const Decoder = require('../../../decoder')
const RequestV11Protocol = require('./request')
const Long = require('../../../../utils/long')
const golden = require('../fixtures/v11_request_golden.json')

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

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    // The generator emits a null Records field: a record batch carries a wall-clock timestamp and
    // a CRC over it, so no second encoder can reproduce one byte-for-byte.
    const decoder = new Decoder(Buffer.from(require('../fixtures/v11_request_java.json')))

    expect(decoder.readUVarIntString()).toBeNull() // transactionalId
    expect(decoder.readInt16()).toEqual(-1) // acks
    expect(decoder.readInt32()).toEqual(30000) // timeoutMs

    const topics = decoder.readUVarIntArray((d) => {
      const topic = {
        name: d.readUVarIntString(),
        partitions: d.readUVarIntArray((p) => {
          const partition = { index: p.readInt32(), records: p.readUVarIntBytes() }
          p.readTaggedFields()
          return partition
        }),
      }
      d.readTaggedFields()
      return topic
    })

    expect(topics).toEqual([{ name: 'test-topic', partitions: [{ index: 0, records: null }] }])
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})

describe('Protocol > Requests > Produce > v11 > a hand-computed request', () => {
  test('encodes the hand-computed golden byte for byte', async () => {
    const { buffer } = await require('./request')({
      acks: -1,
      timeout: 30000,
      producerId: Long.fromInt(1000),
      producerEpoch: 3,
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
