const Decoder = require('../../decoder')
const { KafkaJSPartialMessageError } = require('../../../errors')
const { lookupCodecByAttributes } = require('../../message/compression')
const RecordDecoder = require('../record/v0/decoder')
const TimestampTypes = require('../../timestampTypes')
const crc32C = require('../crc32C')

const TIMESTAMP_TYPE_FLAG_MASK = 0x8
const TRANSACTIONAL_FLAG_MASK = 0x10
const CONTROL_FLAG_MASK = 0x20

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
 * Decodes a v0 record batch.
 * @param {Decoder} fetchDecoder positioned at the start of the record batch
 * @param {object} [options]
 * @param {boolean} [options.checkCrcs=true] verify the batch CRC32C (Java's `check.crcs`);
 *        a mismatch throws {@link KafkaJSPartialMessageError} with `crcMismatch: true`, which
 *        `src/protocol/requests/fetch/v4/decodeMessages.js` re-throws instead of treating as a
 *        truncated tail
 * @returns {Promise<object>} the decoded record batch, including its records
 * @throws {KafkaJSPartialMessageError} on a truncated batch, or (when `checkCrcs`) a CRC32C mismatch
 */
module.exports = async (fetchDecoder, { checkCrcs = true } = {}) => {
  const firstOffset = fetchDecoder.readInt64().toString()
  const length = fetchDecoder.readInt32()
  const decoder = fetchDecoder.slice(length)
  fetchDecoder.forward(length)

  const remainingBytes = Buffer.byteLength(decoder.buffer)

  if (remainingBytes < length) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial record batch: remainingBytes(${remainingBytes}) < recordBatchLength(${length})`
    )
  }

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
      const error = new KafkaJSPartialMessageError(
        `Record batch CRC32C mismatch: expected ${crc}, computed ${computed}`
      )
      error.crcMismatch = true
      throw error
    }
  }

  const attributes = decoder.readInt16()
  const lastOffsetDelta = decoder.readInt32()
  const firstTimestamp = decoder.readInt64().toString()
  const maxTimestamp = decoder.readInt64().toString()
  const producerId = decoder.readInt64().toString()
  const producerEpoch = decoder.readInt16()
  const firstSequence = decoder.readInt32()

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

  const records = await decodeRecords(
    codec,
    decoder,
    { ...recordContext, magicByte },
    firstOffsetNumber
  )

  return {
    ...recordContext,
    records,
  }
}

/**
 * Decodes the records section of a batch, decompressing it first when the batch is compressed.
 * @param {object|null} codec the compression codec for this batch, or null when uncompressed
 * @param {Decoder} recordsDecoder positioned at the start of the records section
 * @param {object} recordContext the batch-level values shared by every record in the batch
 * @param {number} firstOffsetNumber `Number(firstOffset)`, computed once per batch
 * @returns {Promise<object[]>} the decoded records, in wire order
 */
const decodeRecords = async (codec, recordsDecoder, recordContext, firstOffsetNumber) => {
  if (!codec) {
    return recordsDecoder.readArray((decoder) =>
      decodeRecord(decoder, recordContext, firstOffsetNumber)
    )
  }

  const length = recordsDecoder.readInt32()

  if (length <= 0) {
    return []
  }

  const compressedRecordsBuffer = recordsDecoder.readAll()
  const decompressedRecordBuffer = await codec.decompress(compressedRecordsBuffer)
  const decompressedRecordDecoder = new Decoder(decompressedRecordBuffer)
  const records = new Array(length)

  for (let i = 0; i < length; i++) {
    records[i] = decodeRecord(decompressedRecordDecoder, recordContext, firstOffsetNumber)
  }

  return records
}

/**
 * Decodes one length-prefixed record.
 * @param {Decoder} decoder positioned at the record's length varint
 * @param {object} recordContext the batch-level values shared by every record in the batch
 * @param {number} firstOffsetNumber `Number(firstOffset)`, computed once per batch
 * @returns {object} the decoded record
 */
const decodeRecord = (decoder, recordContext, firstOffsetNumber) => {
  const recordBuffer = decoder.readVarIntBytes()
  return RecordDecoder(new Decoder(recordBuffer), recordContext, firstOffsetNumber)
}
