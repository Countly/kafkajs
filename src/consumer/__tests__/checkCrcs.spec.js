const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')
const { requests } = require('../../protocol/requests')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')

/**
 * Builds a real record batch (via the production encoder, one record) and flips a byte inside
 * its CRC32C-covered region without touching the record content -- the same technique used by
 * `src/protocol/recordBatch/v0/crcValidation.spec.js`, kept here as a small local helper instead
 * of importing that spec file.
 * @returns {Promise<Buffer>} a record batch buffer with a corrupted CRC32C
 */
const buildCorruptBatch = async () => {
  const batch = Buffer.from(
    (
      await RecordBatch({
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
      })
    ).buffer
  )

  // FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4) + Magic(1) + CRC(4) + Attributes(2) +
  // LastOffsetDelta(4) + FirstTimestamp(8) + MaxTimestamp(8) + ProducerId(8): the next byte
  // (ProducerEpoch) is inside the covered region but is plain carried-through metadata.
  const PRODUCER_EPOCH_OFFSET = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8
  batch[PRODUCER_EPOCH_OFFSET] ^= 0x01
  return batch
}

/**
 * Builds a Fetch v11 response body carrying a single topic-partition whose record set is the
 * given batch buffer.
 * @param {Buffer} batch
 * @returns {Buffer}
 */
const buildFetchResponseBody = (batch) =>
  new Encoder()
    .writeInt32(0) // clientSideThrottleTime
    .writeInt16(0) // errorCode
    .writeInt32(0) // sessionId
    .writeArray(
      [
        new Encoder().writeString('test-topic').writeArray(
          [
            new Encoder()
              .writeInt32(0) // partition
              .writeInt16(0) // errorCode
              .writeInt64(10) // highWatermark
              .writeInt64(10) // lastStableOffset
              .writeInt64(0) // logStartOffset
              .writeArray([], 'object') // abortedTransactions
              .writeInt32(-1) // preferredReadReplica
              .writeBytes(batch), // record set
          ],
          'object'
        ),
      ],
      'object'
    ).buffer

describe('Consumer > checkCrcs (codec level)', () => {
  test('a truncated tail batch (maxBytes) is still swallowed, not reported as a mismatch', async () => {
    const validBatch = Buffer.from(
      (
        await RecordBatch({
          firstTimestamp: 1700000000000,
          maxTimestamp: 1700000000000,
          records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
        })
      ).buffer
    )
    // A second copy of the same batch, cut 5 bytes short, mimics the tail maxBytes routinely
    // leaves behind: decodeMessages must break out of its loop instead of throwing.
    const recordSet = Buffer.concat([validBatch, validBatch.slice(0, validBatch.length - 5)])
    const body = buildFetchResponseBody(recordSet)
    const { response } = requests.Fetch.protocol({ version: 11 })({
      replicaId: -1,
      maxWaitTime: 5000,
      minBytes: 1,
      maxBytes: 1048576,
      topics: [],
      checkCrcs: true,
    })

    const decoded = await response.decode(body)
    expect(decoded.responses[0].partitions[0].messages).toHaveLength(1)
  })

  test('rejects with a marked KafkaJSPartialMessageError when checkCrcs is on', async () => {
    const body = buildFetchResponseBody(await buildCorruptBatch())
    const { response } = requests.Fetch.protocol({ version: 11 })({
      replicaId: -1,
      maxWaitTime: 5000,
      minBytes: 1,
      maxBytes: 1048576,
      topics: [],
      checkCrcs: true,
    })

    await expect(response.decode(body)).rejects.toHaveProperty('crcMismatch', true)
  })

  test('resolves with the (garbage but parseable) records when checkCrcs is off', async () => {
    const body = buildFetchResponseBody(await buildCorruptBatch())
    const { response } = requests.Fetch.protocol({ version: 11 })({
      replicaId: -1,
      maxWaitTime: 5000,
      minBytes: 1,
      maxBytes: 1048576,
      topics: [],
      checkCrcs: false,
    })

    const decoded = await response.decode(body)
    expect(decoded.responses[0].partitions[0].messages).toHaveLength(1)
  })

  test('defaults to checkCrcs: true when the option is omitted', async () => {
    const body = buildFetchResponseBody(await buildCorruptBatch())
    const { response } = requests.Fetch.protocol({ version: 11 })({
      replicaId: -1,
      maxWaitTime: 5000,
      minBytes: 1,
      maxBytes: 1048576,
      topics: [],
    })

    await expect(response.decode(body)).rejects.toHaveProperty('crcMismatch', true)
  })

  test('a valid batch decodes cleanly regardless of checkCrcs', async () => {
    const batch = Buffer.from(
      (
        await RecordBatch({
          records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
        })
      ).buffer
    )
    const body = buildFetchResponseBody(batch)

    for (const checkCrcs of [true, false]) {
      const { response } = requests.Fetch.protocol({ version: 11 })({
        replicaId: -1,
        maxWaitTime: 5000,
        minBytes: 1,
        maxBytes: 1048576,
        topics: [],
        checkCrcs,
      })
      const decoded = await response.decode(new Decoder(body).buffer)
      expect(decoded.responses[0].partitions[0].messages).toHaveLength(1)
    }
  })
})
