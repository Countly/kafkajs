const Decoder = require('../../decoder')
const {
  KafkaJSCorruptRecordError,
  KafkaJSInvalidLongError,
  KafkaJSPartialMessageError,
} = require('../../../errors')
const {
  codecName,
  decompressBounded,
  lookupCodecByAttributes,
} = require('../../message/compression')
const { DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES } = require('../../../constants')
const RecordDecoder = require('../record/v0/decoder')
const TimestampTypes = require('../../timestampTypes')
const crc32C = require('../crc32C')

const TIMESTAMP_TYPE_FLAG_MASK = 0x8
const TRANSACTIONAL_FLAG_MASK = 0x10
const CONTROL_FLAG_MASK = 0x20

// Offset and length (`LOG_OVERHEAD`), then the partition leader epoch and the magic byte.
const LOG_OVERHEAD = 12
const HEADER_SIZE_UP_TO_MAGIC = 17
const LENGTH_OFFSET = 8
const MAGIC_OFFSET = 16
// The smallest length any magic can declare (`LegacyRecord.RECORD_OVERHEAD_V0`), and the bytes a
// v2 batch's length counts before its records: epoch, magic, CRC, attributes, last offset delta,
// two timestamps, producer id and epoch, base sequence and record count (`RECORD_BATCH_OVERHEAD`
// less `LOG_OVERHEAD`).
const MIN_LENGTH = 14
const V2_MIN_LENGTH = 49
// A record is at least its length, attributes, timestamp delta, offset delta, key length, value
// length and header count, one byte each.
const MIN_RECORD_BYTES = 7
// The bytes of `maxDecompressedBatchBytes` that pay for one record object: the heap a decoded
// record costs (208 to 321 bytes, measured) rounded up to a power of two.
const RECORD_CEILING_BYTES = 512

const CONTROL_KEY_BYTES = 4
const END_TRANSACTION_MARKER_TYPES = [0, 1]

/**
 * v0
 * RecordBatch =>
 *  FirstOffset => int64
 *  Length => int32
 *  PartitionLeaderEpoch => int32
 *  Magic => int8
 *  CRC => int32
 *  Attributes => int16
 *  LastOffsetDelta => int32
 *  FirstTimestamp => int64
 *  MaxTimestamp => int64
 *  ProducerId => int64
 *  ProducerEpoch => int16
 *  FirstSequence => int32
 *  Records => [Record]
 */

/**
 * @param {string} message
 * @param {'length'|'magic'|'count'|'control'|'checksum'} reason
 * @param {string} offset the first offset of the batch, a decimal string
 * @param {Error} [cause]
 * @param {{ limit?: number }} [extra]
 * @returns {KafkaJSCorruptRecordError}
 */
const corrupt = (message, reason, offset, cause, extra) =>
  new KafkaJSCorruptRecordError(message, { reason, offset, cause, ...extra })

/**
 * @param {string} firstOffset the batch's first offset, a decimal string
 * @param {number} lastOffsetDelta the batch header's last offset delta
 * @returns {string|undefined} the first offset after the batch, where `consumer.seek` goes to skip
 *   it; `undefined` for a negative delta, which no batch boundary can have
 */
const nextOffsetOf = (firstOffset, lastOffsetDelta) =>
  lastOffsetDelta < 0 ? undefined : (BigInt(firstOffset) + BigInt(lastOffsetDelta) + 1n).toString()

/**
 * Adds what only the batch decoder knows to a corrupt-record error raised below it: the batch's
 * first offset, the offset after it once the header has been accepted, and its codec.
 * @param {KafkaJSCorruptRecordError} error
 * @param {string} firstOffset
 * @param {number} lastOffsetDelta
 * @param {string} [codec] the name of the batch's codec, when it is compressed
 * @returns {KafkaJSCorruptRecordError} `error`
 */
const locate = (error, firstOffset, lastOffsetDelta, codec) => {
  const nextOffset = nextOffsetOf(firstOffset, lastOffsetDelta)
  error.offset = error.offset ?? firstOffset
  if (nextOffset !== undefined) error.nextOffset = error.nextOffset ?? nextOffset
  if (codec !== undefined) error.codec = error.codec ?? codec
  return error
}

/**
 * @param {Buffer} buffer
 * @param {number} offset where a batch starts, at least 8 bytes before the end of `buffer`
 * @returns {string} the batch's first offset, a decimal string
 */
const firstOffsetAt = (buffer, offset) => buffer.readBigInt64BE(offset).toString()

/**
 * Judges the batch at the decoder's position before any of it is decoded, with the rules of Java's
 * `ByteBufferLogInputStream.nextBatchSize` and `DefaultRecordBatch.ensureValid`: the length is
 * judged as soon as the length field is readable, the magic as soon as it is, a batch that is not
 * all here is a partial tail, and a magic 2 batch must at least hold its own header.
 * @param {Decoder} fetchDecoder
 * @throws {KafkaJSPartialMessageError} when fewer bytes remain than the batch declares: the tail of
 *         the block was cut where `maxBytes` ended
 * @throws {KafkaJSCorruptRecordError} for a length below the minimum or a magic outside 0..2, which
 *         `reason` names; both are judged before the CRC, so neither can be mistaken for a mismatch
 */
const assertEnvelope = ({ buffer, offset }) => {
  const remaining = buffer.length - offset

  if (remaining < LOG_OVERHEAD) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial record batch: ${remaining} byte(s) remain, a batch starts with ${LOG_OVERHEAD}`
    )
  }

  const length = buffer.readInt32BE(offset + LENGTH_OFFSET)

  if (length < MIN_LENGTH) {
    throw corrupt(
      `Record batch length ${length} is below the minimum ${MIN_LENGTH}`,
      'length',
      firstOffsetAt(buffer, offset)
    )
  }

  if (remaining < HEADER_SIZE_UP_TO_MAGIC) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial record batch: ${remaining} byte(s) remain, the magic byte is at ${HEADER_SIZE_UP_TO_MAGIC}`
    )
  }

  const magic = buffer.readInt8(offset + MAGIC_OFFSET)

  if (magic < 0 || magic > 2) {
    throw corrupt(
      `Record batch magic ${magic} is not 0, 1 or 2`,
      'magic',
      firstOffsetAt(buffer, offset)
    )
  }

  if (remaining < LOG_OVERHEAD + length) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial record batch: remainingBytes(${remaining - LOG_OVERHEAD}) < recordBatchLength(${length})`
    )
  }

  if (magic === 2 && length < V2_MIN_LENGTH) {
    throw corrupt(
      `Record batch length ${length} is below the ${V2_MIN_LENGTH} bytes of a magic 2 header`,
      'length',
      firstOffsetAt(buffer, offset)
    )
  }
}

/**
 * A control record's key holds a version and a type, int16 each, and a transaction marker's value
 * a version and the coordinator epoch. Java reads an unknown control type as `UNKNOWN`, not as an
 * error, and a key or value version above the highest it knows as the highest, so only a key
 * under 4 bytes, a negative version, or a marker value too short to hold its version is corrupt
 * (`ControlRecordType.parseTypeId`, `EndTransactionMarker.deserializeValue`).
 * @param {{ key: Buffer|null, value: Buffer|null }} record
 * @param {string} firstOffset the first offset of the batch
 * @throws {KafkaJSCorruptRecordError} with reason `control`
 */
const assertControlRecord = ({ key, value }, firstOffset) => {
  if (!key || key.length < CONTROL_KEY_BYTES) {
    throw corrupt(
      `Control record key has ${key ? key.length : 0} byte(s), at least ${CONTROL_KEY_BYTES} are needed`,
      'control',
      firstOffset
    )
  }

  if (key.readInt16BE(0) < 0) {
    throw corrupt(
      `Control record key version ${key.readInt16BE(0)} is negative`,
      'control',
      firstOffset
    )
  }

  if (
    END_TRANSACTION_MARKER_TYPES.includes(key.readInt16BE(2)) &&
    (!value || value.length < 2 || value.readInt16BE(0) < 0)
  ) {
    throw corrupt('Transaction marker value has no valid version', 'control', firstOffset)
  }
}

/**
 * Decodes a v0 record batch.
 * @param {Decoder} fetchDecoder positioned at the start of the record batch
 * @param {object} [options]
 * @param {boolean} [options.checkCrcs=true] verify the batch CRC32C (Java's `check.crcs`)
 * @param {number} [options.maxDecompressedBatchBytes] the most the batch may decompress to; it also
 *        sets the record ceiling, `max(1, floor(bytes / 512))` records
 * @returns {Promise<object>} the decoded record batch, including its records
 * @throws {KafkaJSPartialMessageError} on a truncated batch
 * @throws {KafkaJSCorruptRecordError} when the batch is all here and impossible or over a bound:
 *         its length or magic and its CRC (judged first, so a forged length cannot pass for a
 *         mismatch), its record count (judged after the CRC, so it also guards `checkCrcs: false`),
 *         a compressed body that is malformed or decompresses past the bound, a record that runs
 *         past the batch, or a malformed control record. `nextOffset` is set once the header and,
 *         with `checkCrcs`, the CRC have been accepted
 */
module.exports = async (
  fetchDecoder,
  { checkCrcs = true, maxDecompressedBatchBytes = DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES } = {}
) => {
  assertEnvelope(fetchDecoder)

  const firstOffset = fetchDecoder.readInt64().toString()
  const length = fetchDecoder.readInt32()
  const decoder = fetchDecoder.slice(length)
  fetchDecoder.forward(length)

  const partitionLeaderEpoch = decoder.readInt32()

  // The magic byte was read by the Fetch protocol to distinguish between
  // the record batch and the legacy message set. It's not used here but
  // it has to be read.
  const magicByte = decoder.readInt8()

  const crc = decoder.readInt32()

  if (checkCrcs) {
    // The CRC32C covers every byte after the CRC field itself, i.e. Attributes onwards.
    // @see https://kafka.apache.org/43/implementation/message-format
    const covered = decoder.buffer.slice(decoder.offset)
    const computed = crc32C(covered) | 0

    if (computed !== crc) {
      throw corrupt(
        `Record batch CRC32C mismatch: expected ${crc}, computed ${computed}`,
        'checksum',
        firstOffset
      )
    }
  }

  const attributes = decoder.readInt16()
  const lastOffsetDelta = decoder.readInt32()
  const firstTimestamp = decoder.readInt64().toString()
  const maxTimestamp = decoder.readInt64().toString()
  const producerId = decoder.readInt64().toString()
  const producerEpoch = decoder.readInt16()
  const firstSequence = decoder.readInt32()
  const count = decoder.readInt32()

  const inTransaction = (attributes & TRANSACTIONAL_FLAG_MASK) > 0
  const isControlBatch = (attributes & CONTROL_FLAG_MASK) > 0
  const timestampType =
    (attributes & TIMESTAMP_TYPE_FLAG_MASK) > 0
      ? TimestampTypes.LOG_APPEND_TIME
      : TimestampTypes.CREATE_TIME

  const codec = lookupCodecByAttributes(attributes)

  const recordContext = {
    firstOffset,
    firstTimestamp,
    partitionLeaderEpoch,
    inTransaction,
    isControlBatch,
    lastOffsetDelta,
    producerId,
    producerEpoch,
    firstSequence,
    maxTimestamp,
    timestampType,
  }

  // Computed once per batch and threaded to the record decoder as an argument, not as a field on
  // recordContext: that object is attached to every message as `batchContext`, and its shape is
  // part of the public message.
  const firstOffsetNumber = Number(firstOffset)

  try {
    const records = await decodeRecords(
      { codec, attributes, count, limit: maxDecompressedBatchBytes },
      decoder,
      { ...recordContext, magicByte },
      firstOffsetNumber
    )

    if (isControlBatch) {
      for (const controlRecord of records) {
        assertControlRecord(controlRecord, firstOffset)
      }
    }

    return {
      ...recordContext,
      records,
    }
  } catch (error) {
    // The batch is all here, so a read that runs out of bytes inside it is a length that lies, not
    // a tail that was cut.
    const failure =
      error instanceof KafkaJSPartialMessageError || error instanceof KafkaJSInvalidLongError
        ? corrupt(`Record batch body is malformed: ${error.message}`, 'length', firstOffset, error)
        : error

    throw failure instanceof KafkaJSCorruptRecordError
      ? locate(failure, firstOffset, lastOffsetDelta, codec ? codecName(attributes) : undefined)
      : failure
  }
}

/**
 * Decodes the records section of a batch, decompressing it first when the batch is compressed.
 * The count is checked before anything is allocated for it, twice: against the record ceiling the
 * byte bound implies, before the body is decompressed, and against the bytes the records have,
 * `MIN_RECORD_BYTES` each, once they are known. The body is decompressed within the byte bound.
 * @param {object} batch
 * @param {object|null} batch.codec the compression codec for this batch, or null when uncompressed
 * @param {number} batch.attributes the batch attributes
 * @param {number} batch.count the batch's record count
 * @param {number} batch.limit `maxDecompressedBatchBytes`
 * @param {Decoder} recordsDecoder positioned at the start of the records section
 * @param {object} recordContext the batch-level values shared by every record in the batch
 * @param {number} firstOffsetNumber `Number(firstOffset)`, computed once per batch
 * @returns {Promise<object[]>} the decoded records, in wire order
 * @throws {KafkaJSCorruptRecordError} with reason `count` for a negative count, one above the
 *         record ceiling or one the bytes cannot hold, and for a compressed body `decompressed-size`
 *         or `decompression`
 */
const decodeRecords = async (
  { codec, attributes, count, limit },
  recordsDecoder,
  recordContext,
  firstOffsetNumber
) => {
  if (count < 0) {
    throw corrupt(`Record count ${count} is negative`, 'count', recordContext.firstOffset)
  }

  if (count === 0) {
    return []
  }

  const recordLimit = Math.max(1, Math.floor(limit / RECORD_CEILING_BYTES))

  if (count > recordLimit) {
    throw corrupt(
      `Record count ${count} is above the ${recordLimit} records that maxDecompressedBatchBytes ${limit} allows`,
      'count',
      recordContext.firstOffset,
      undefined,
      { limit: recordLimit }
    )
  }

  const decoder = codec
    ? new Decoder(await decompressBounded(codec, attributes, recordsDecoder.readAll(), limit))
    : recordsDecoder
  const room = decoder.buffer.length - decoder.offset

  if (count > room / MIN_RECORD_BYTES) {
    throw corrupt(
      `Record count ${count} cannot fit in ${room} byte(s) at ${MIN_RECORD_BYTES} per record`,
      'count',
      recordContext.firstOffset
    )
  }

  const records = new Array(count)

  for (let i = 0; i < count; i++) {
    records[i] = decodeRecord(decoder, recordContext, firstOffsetNumber)
  }

  return records
}

/**
 * Decodes one length-prefixed record.
 * @param {Decoder} decoder positioned at the record's length varint
 * @param {object} recordContext the batch-level values shared by every record in the batch
 * @param {number} firstOffsetNumber `Number(firstOffset)`, computed once per batch
 * @returns {object} the decoded record
 * @throws {KafkaJSCorruptRecordError} with reason `length` for a record whose length is -1, the
 *         null of a varint length, which no record can have
 */
const decodeRecord = (decoder, recordContext, firstOffsetNumber) => {
  const recordBuffer = decoder.readVarIntBytes()

  if (recordBuffer === null) {
    throw corrupt('Record length is -1', 'length', recordContext.firstOffset)
  }

  return RecordDecoder(new Decoder(recordBuffer), recordContext, firstOffsetNumber)
}
