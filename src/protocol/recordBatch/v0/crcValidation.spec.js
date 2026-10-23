const RecordBatchDecoder = require('./decoder')
const { RecordBatch } = require('./index')
const RecordEncoder = require('../record/v0')
const Decoder = require('../../decoder')
const { KafkaJSCorruptRecordError } = require('../../../errors')
const { Types: Compression } = require('../../message/compression')

// The committed fixture `src/protocol/recordBatch/fixtures/v0_recordbatch.json` carries a valid
// CRC32C over the covered region and is exactly `12 + length` bytes long -- but despite its name
// it is itself GZIP-compressed (attributes = 9: GZIP codec + LOG_APPEND_TIME). A second,
// freshly-built compressed batch below (via the production encoder) gives a control alongside the
// committed one. A ZSTD fixture that decoded with `attributes: 0` and that nothing referenced was
// removed; ZSTD batches are covered by `src/producer/compression.spec.js` and the live ZSTD case
// of `src/broker/__tests__/produce.spec.js`.
const committedFixture = () => Buffer.from(require('../fixtures/v0_recordbatch.json'))

// Fixed inputs (rather than the encoder's `Date.now()` defaults) so this batch, and the sign of
// its on-wire CRC32C, are reproducible from one run to the next.
const buildGzipBatch = async () =>
  Buffer.from(
    (
      await RecordBatch({
        compression: Compression.GZIP,
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key0'), value: Buffer.from('value0') })],
      })
    ).buffer
  )

const fixtures = {
  'committed fixture': committedFixture,
  'freshly-built gzip batch': buildGzipBatch,
}

// FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4) + Magic(1) + CRC(4) + Attributes(2) +
// LastOffsetDelta(4) + FirstTimestamp(8) + MaxTimestamp(8) + ProducerId(8): the byte right after
// this is the first byte of ProducerEpoch, which is inside the CRC32C-covered region but carried
// through as plain metadata -- flipping it changes the checksum without touching a compressed
// record payload, so it cannot also trip zlib's own gzip trailer check the way corrupting the
// batch's tail would for a compressed batch.
const PRODUCER_EPOCH_OFFSET = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8

/**
 * Flips a byte inside a record batch's CRC-covered region without touching record content.
 * @param {Buffer} batch
 * @returns {Buffer} a corrupted copy
 */
const corrupt = (batch) => {
  const copy = Buffer.from(batch)
  copy[PRODUCER_EPOCH_OFFSET] ^= 0x01
  return copy
}

// FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4) + Magic(1): the CRC int32 itself starts
// right after this, so flipping a byte here corrupts the on-wire value being compared against,
// rather than the computed side -- the other half of the comparison than `corrupt()` exercises.
const CRC_FIELD_OFFSET = 8 + 4 + 4 + 1

/**
 * Flips a byte inside the CRC field itself, leaving the covered region (and therefore the
 * computed checksum) untouched.
 * @param {Buffer} batch
 * @returns {Buffer} a copy whose on-wire CRC no longer matches the computed one
 */
const corruptCrcField = (batch) => {
  const copy = Buffer.from(batch)
  copy[CRC_FIELD_OFFSET] ^= 0x01
  return copy
}

test('the two valid batches cover both signs of the on-wire CRC32C', async () => {
  // `computed` is always unsigned (`crc32C` returns >= 0); `crc` is read as a signed int32. The
  // `| 0` in the decoder normalizes `computed` into the same signed domain before comparing, so
  // this is only a meaningful check if the wire values below actually straddle the int32 sign
  // boundary (2^31) -- confirming that here, not just letting it happen incidentally.
  const committed = await committedFixture()
  const builtGzip = await buildGzipBatch()
  const HEADER_TO_CRC = 8 + 4 + 4 + 1 // FirstOffset + Length + PartitionLeaderEpoch + Magic
  expect(committed.readInt32BE(HEADER_TO_CRC)).toBeLessThan(0)
  expect(builtGzip.readInt32BE(HEADER_TO_CRC)).toBeGreaterThanOrEqual(0)
})

describe.each(Object.entries(fixtures))(
  'Protocol > RecordBatch > CRC32C validation (%s)',
  (_name, validBatch) => {
    test('decodes a valid batch with checkCrcs on', async () => {
      const decoder = new Decoder(await validBatch())
      await expect(RecordBatchDecoder(decoder, { checkCrcs: true })).resolves.toBeTruthy()
    })

    test('throws a KafkaJSCorruptRecordError with reason checksum, and no next offset, on a corrupt batch', async () => {
      const decoder = new Decoder(corrupt(await validBatch()))
      const promise = RecordBatchDecoder(decoder, { checkCrcs: true })

      await expect(promise).rejects.toThrow(KafkaJSCorruptRecordError)
      await expect(promise).rejects.toMatchObject({
        reason: 'checksum',
        offset: expect.any(String),
      })
      await expect(promise).rejects.not.toHaveProperty('nextOffset')
    })

    test('throws the same when the CRC field itself is corrupted', async () => {
      const decoder = new Decoder(corruptCrcField(await validBatch()))
      const promise = RecordBatchDecoder(decoder, { checkCrcs: true })

      await expect(promise).rejects.toThrow(KafkaJSCorruptRecordError)
      await expect(promise).rejects.toMatchObject({ reason: 'checksum' })
    })

    test('does not throw when checkCrcs is off', async () => {
      const decoder = new Decoder(corrupt(await validBatch()))
      await expect(RecordBatchDecoder(decoder, { checkCrcs: false })).resolves.toBeTruthy()
    })

    test('defaults to checking', async () => {
      const decoder = new Decoder(corrupt(await validBatch()))
      await expect(RecordBatchDecoder(decoder)).rejects.toMatchObject({ reason: 'checksum' })
    })
  }
)
