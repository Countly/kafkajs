const RequestV10Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > Produce > v10', () => {
  // A fixed timestamp keeps the record batch (and its CRC) deterministic across the two separate
  // `encode()` calls the "same as v9" test below makes; leaving it to `Date.now()` would make the
  // batch, and therefore the byte comparison, depend on which millisecond each call lands in.
  const topicData = [
    {
      topic: 'test-topic',
      partitions: [
        { partition: 0, messages: [{ key: 'k', value: 'v', timestamp: 1700000000000 }] },
      ],
    },
  ]

  test('encodes the request with compact strings, compact arrays and tag buffers', async () => {
    const { buffer } = await RequestV10Protocol({ acks: -1, timeout: 30000, topicData }).encode()
    const decoder = new Decoder(buffer)

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

    expect(topics).toHaveLength(1)
    expect(topics[0].name).toEqual('test-topic')
    expect(topics[0].partitions[0].index).toEqual(0)
    expect(topics[0].partitions[0].records.length).toBeGreaterThan(0)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('carries the same record batch as v9, unchanged by the response-only KIP-951 tags', async () => {
    const v10 = await RequestV10Protocol({ acks: -1, timeout: 30000, topicData }).encode()
    const v9 = await require('../v9/request')({ acks: -1, timeout: 30000, topicData }).encode()

    expect(v10.buffer).toEqual(v9.buffer)
  })

  test('expectResponse follows acks, as every earlier version does', () => {
    expect(RequestV10Protocol({ acks: 0, timeout: 1, topicData }).expectResponse()).toBe(false)
    expect(RequestV10Protocol({ acks: -1, timeout: 1, topicData }).expectResponse()).toBe(true)
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    // The generator emits a null Records field: a record batch carries a wall-clock timestamp and
    // a CRC over it, so no second encoder can reproduce one byte-for-byte.
    const decoder = new Decoder(Buffer.from(require('../fixtures/v10_request_java.json')))

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
