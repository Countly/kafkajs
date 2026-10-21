const RequestV12Protocol = require('./request')
const Decoder = require('../../../decoder')
const RequestV11Protocol = require('../v11/request')
const { Types } = require('../../../message/compression')
const Long = require('../../../../utils/long')
const golden = require('../fixtures/v12_request_golden.json')

// An explicit timestamp keeps the two record batches - and therefore their CRCs - reproducible;
// without it Date.now() makes a byte-equality comparison flaky.
const args = {
  acks: -1,
  timeout: 30000,
  compression: Types.None,
  topicData: [
    {
      topic: 'test-topic',
      partitions: [
        {
          partition: 0,
          firstSequence: 0,
          messages: [{ key: 'k', value: 'v', timestamp: 1700000000000 }],
        },
      ],
    },
  ],
}

describe('Protocol > Requests > Produce > v12', () => {
  test('is byte-identical to v11 apart from the api version', async () => {
    const v12 = await RequestV12Protocol(args).encode()
    const v11 = await RequestV11Protocol(args).encode()

    expect(v12.buffer).toEqual(v11.buffer)
    expect(RequestV12Protocol(args).apiVersion).toEqual(12)
    expect(RequestV11Protocol(args).apiVersion).toEqual(11)
  })

  test('decodes the vector Apache own codec produced', async () => {
    // Decoded rather than byte-compared: the generator emits a null Records block, because a
    // record batch carries a wall-clock timestamp and a CRC over it no second encoder reproduces.
    const decoder = new Decoder(Buffer.from(require('../fixtures/v12_request_java.json')))

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

describe('Protocol > Requests > Produce > v12 > a hand-computed request', () => {
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
