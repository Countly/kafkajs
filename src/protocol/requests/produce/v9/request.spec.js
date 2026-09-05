const RequestV9Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > Produce > v9', () => {
  // A fixed timestamp keeps the record batch (and its CRC) deterministic across the two separate
  // `encode()` calls the "same as v8" test below makes; leaving it to `Date.now()` would make the
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
    const { buffer } = await RequestV9Protocol({ acks: -1, timeout: 30000, topicData }).encode()
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

  test('carries the same record batch as v8, under compact framing', async () => {
    // The record batch itself is version-independent (built by the same buildRecordBatch helper);
    // only its framing changed, from an INT32-length BYTES field to COMPACT_NULLABLE_RECORDS. With
    // the fixed message timestamp above, batch construction is fully deterministic, so the two
    // batches must be byte-identical - not just "equal from some offset onward".
    const v9 = await RequestV9Protocol({ acks: -1, timeout: 30000, topicData }).encode()
    const v8 = await require('../v8/request')({ acks: -1, timeout: 30000, topicData }).encode()

    const v9Batch = (() => {
      const d = new Decoder(v9.buffer)
      d.readUVarIntString()
      d.readInt16()
      d.readInt32()
      let batch
      d.readUVarIntArray((t) => {
        t.readUVarIntString()
        t.readUVarIntArray((p) => {
          p.readInt32()
          batch = p.readUVarIntBytes()
          p.readTaggedFields()
        })
        t.readTaggedFields()
      })
      return batch
    })()

    const v8Batch = (() => {
      const d = new Decoder(v8.buffer)
      d.readString()
      d.readInt16()
      d.readInt32()
      let batch
      d.readArray((t) => {
        t.readString()
        t.readArray((p) => {
          p.readInt32()
          batch = p.readBytes()
        })
      })
      return batch
    })()

    expect(v9Batch).toEqual(v8Batch)
  })

  test('encodes the record batch straight from its own encoder, without an intermediate copy', async () => {
    // v9's COMPACT_NULLABLE_RECORDS framing must write the batch's own buffer view directly
    // (writeUVarInt(size + 1) + writeEncoder), not a Buffer.from(...) copy of it - the same
    // zero-extra-allocation shape the classic versions already use. writeUVarInt itself allocates
    // small scratch byte arrays via Buffer.from internally (for the varint's own bytes), so the
    // meaningful assertion is that Buffer.from is never handed a Buffer to copy.
    const spy = jest.spyOn(Buffer, 'from')
    await RequestV9Protocol({ acks: -1, timeout: 30000, topicData }).encode()
    const copiedABuffer = spy.mock.calls.some(([value]) => Buffer.isBuffer(value))
    spy.mockRestore()

    expect(copiedABuffer).toBe(false)
  })

  test('expectResponse follows acks, as every earlier version does', () => {
    expect(RequestV9Protocol({ acks: 0, timeout: 1, topicData }).expectResponse()).toBe(false)
    expect(RequestV9Protocol({ acks: -1, timeout: 1, topicData }).expectResponse()).toBe(true)
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    // The generator emits a null Records field: a record batch carries a wall-clock timestamp and
    // a CRC over it, so no second encoder can reproduce one byte-for-byte.
    const decoder = new Decoder(Buffer.from(require('../fixtures/v9_request_java.json')))

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
