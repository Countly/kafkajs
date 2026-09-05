const Decoder = require('../../../decoder')
const RequestV8Protocol = require('./request')
const RequestV7Protocol = require('../v7/request')

describe('Protocol > Requests > Produce > v8', () => {
  const topicData = [
    {
      topic: 'test-topic',
      partitions: [
        { partition: 0, messages: [{ key: 'k', value: 'v', timestamp: 1700000000000 }] },
      ],
    },
  ]

  test("is v7's request shape at apiVersion 8 - KIP-467 only changes the response", async () => {
    const v8 = RequestV8Protocol({ acks: -1, timeout: 30000, topicData })
    expect(v8.apiVersion).toEqual(8)

    const v8Encoded = await v8.encode()
    const v7Encoded = await RequestV7Protocol({ acks: -1, timeout: 30000, topicData }).encode()

    expect(v8Encoded.buffer).toEqual(v7Encoded.buffer)
  })

  test('expectResponse follows acks, as every earlier version does', () => {
    expect(RequestV8Protocol({ acks: 0, timeout: 1, topicData }).expectResponse()).toBe(false)
    expect(RequestV8Protocol({ acks: -1, timeout: 1, topicData }).expectResponse()).toBe(true)
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    // The generator emits a null Records field: a record batch carries a wall-clock timestamp and
    // a CRC over it, so no second encoder can reproduce one byte-for-byte.
    const decoder = new Decoder(Buffer.from(require('../fixtures/v8_request_java.json')))

    expect(decoder.readString()).toBeNull() // transactionalId
    expect(decoder.readInt16()).toEqual(-1) // acks
    expect(decoder.readInt32()).toEqual(30000) // timeoutMs
    expect(
      decoder.readArray((d) => ({
        name: d.readString(),
        partitions: d.readArray((p) => ({ index: p.readInt32(), records: p.readBytes() })),
      }))
    ).toEqual([{ name: 'test-topic', partitions: [{ index: 0, records: null }] }])
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
