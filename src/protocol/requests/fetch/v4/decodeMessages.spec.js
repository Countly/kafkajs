const Decoder = require('../../../decoder')
const Encoder = require('../../../encoder')
const decodeMessages = require('./decodeMessages')
const { batch, record } = require('../__tests__/fixtures/recordBatchBytes')

describe('Protocol > Requests > Fetch > v4 > decodeMessages', () => {
  test('a compact null records block decodes as no messages', async () => {
    const decoder = new Decoder(new Encoder().writeUVarInt(0).buffer)
    await expect(decodeMessages(decoder, { compact: true })).resolves.toEqual({ messages: [] })
  })

  test('the compact size read is the only difference: the same batch decodes either way', async () => {
    const batch = Buffer.from(require('../fixtures/v11_response.json')) // any buffer works here
    const classic = new Decoder(new Encoder().writeInt32(batch.length).writeBuffer(batch).buffer)
    const compact = new Decoder(
      new Encoder().writeUVarInt(batch.length + 1).writeBuffer(batch).buffer
    )

    expect(classic.readInt32()).toEqual(batch.length)
    expect(compact.readUVarInt() - 1).toEqual(batch.length)
  })

  describe('the end of the last complete batch', () => {
    /** The RECORDS field of a classic Fetch partition: an int32 length, then the batches. */
    const block = (...batches) => {
      const body = Buffer.concat(batches)
      const size = Buffer.alloc(4)
      size.writeInt32BE(body.length)
      return Buffer.concat([size, body])
    }
    const decode = (...batches) => decodeMessages(new Decoder(block(...batches)))
    const withRecords = (baseOffset, count, partitionLeaderEpoch = 0) =>
      batch({
        baseOffset,
        partitionLeaderEpoch,
        records: Array.from({ length: count }, (_, offsetDelta) => record({ offsetDelta })),
      })

    test('is reported after a CRC-valid empty batch followed by a partial batch', async () => {
      const empty = batch({ baseOffset: 0n, partitionLeaderEpoch: 3, lastOffsetDelta: 99 })
      const cut = withRecords(100n, 2).subarray(0, 30)

      await expect(decode(empty, cut)).resolves.toEqual({
        messages: [],
        nextBatchOffset: '100',
        nextBatchLeaderEpoch: 3,
      })
    })

    test('is that of the last of several complete batches', async () => {
      const decoded = await decode(withRecords(0n, 2, 1), withRecords(2n, 3, 2))

      expect(decoded.messages).toHaveLength(5)
      expect(decoded.nextBatchOffset).toBe('5')
      expect(decoded.nextBatchLeaderEpoch).toBe(2)
    })

    test('is the end of the last good batch before a corrupt one', async () => {
      const corrupt = batch({ baseOffset: 2n, crc: 0xdeadbeef, records: [record()] })
      const decoded = await decode(withRecords(0n, 2, 4), corrupt)

      expect(decoded.corruption).toMatchObject({ reason: 'checksum', offset: '2' })
      expect(decoded.messages).toHaveLength(2)
      expect(decoded.nextBatchOffset).toBe('2')
      expect(decoded.nextBatchLeaderEpoch).toBe(4)
    })

    test('is exact above 2^53', async () => {
      const decoded = await decode(
        batch({ baseOffset: 2n ** 53n + 1n, partitionLeaderEpoch: 1, lastOffsetDelta: 2 })
      )

      expect(decoded.nextBatchOffset).toBe('9007199254740996')
    })

    test('is reported up to the largest signed 64-bit offset', async () => {
      const decoded = await decode(batch({ baseOffset: 9223372036854775806n }))

      expect(decoded.nextBatchOffset).toBe('9223372036854775807')
    })

    test('is not reported when the next offset would pass the signed 64-bit range', async () => {
      const decoded = await decode(batch({ baseOffset: 9223372036854775807n }))

      expect(decoded).toEqual({ messages: [] })
    })

    test('is not reported for a negative last offset delta', async () => {
      const decoded = await decode(
        batch({ baseOffset: 5n, lastOffsetDelta: -1, records: [record()] })
      )

      expect(decoded.messages).toHaveLength(1)
      expect(decoded).not.toHaveProperty('nextBatchOffset')
      expect(decoded).not.toHaveProperty('nextBatchLeaderEpoch')
    })

    test('carries no leader epoch when the batch has none', async () => {
      const decoded = await decode(withRecords(5n, 1, -1))

      expect(decoded.nextBatchOffset).toBe('6')
      expect(decoded).not.toHaveProperty('nextBatchLeaderEpoch')
    })

    test('is not reported for a block with no batch', async () => {
      await expect(
        decodeMessages(new Decoder(new Encoder().writeInt32(0).buffer))
      ).resolves.toEqual({ messages: [] })
    })

    test('is not reported for a legacy message set', async () => {
      const { decode: decodeResponse } = require('./response')
      const data = await decodeResponse(
        Buffer.from(require('../fixtures/v4_response_010_format.json'))
      )
      const [partition] = data.responses[0].partitions

      expect(partition.messages.length).toBeGreaterThan(0)
      expect(partition).not.toHaveProperty('nextBatchOffset')
      expect(partition).not.toHaveProperty('nextBatchLeaderEpoch')
    })
  })
})
