const Long = require('../../../utils/long')
const Encoder = require('../../encoder')
const crc32C = require('../crc32C')
const {
  Types: Compression,
  lookupCodec,
  COMPRESSION_CODEC_MASK,
} = require('../../message/compression')

const MAGIC_BYTE = 2
const TIMESTAMP_MASK = 0 // The fourth lowest bit, always set this bit to 0 (since 0.10.0)
const TRANSACTIONAL_MASK = 16 // The fifth lowest bit

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
 * Encodes one magic-2 record batch, forwarding its optional level to the selected codec.
 * @param {object} options batch fields
 * @param {number} [options.compression=Compression.None] codec id
 * @param {number} [options.compressionLevel] codec level; omission retains codec defaults
 * @param {Long|number|string} [options.firstOffset] first record offset, default 0
 * @param {number} [options.firstTimestamp] first record time, default current epoch milliseconds
 * @param {number} [options.maxTimestamp] largest record time, default current epoch milliseconds
 * @param {number} [options.partitionLeaderEpoch=0] partition leader epoch
 * @param {number} [options.lastOffsetDelta=0] last record's offset relative to the first
 * @param {boolean} [options.transactional=false] whether to set the transaction attribute
 * @param {Long|number|string} [options.producerId] producer id, default -1
 * @param {number} [options.producerEpoch=0] producer epoch
 * @param {number} [options.firstSequence=0] first record sequence
 * @param {Encoder[]} [options.records=[]] encoded records, kept in input order
 * @returns {Promise<Encoder>} batch bytes including the offset, length and CRC
 * @throws {Error} when encoding fails or the selected codec is unavailable or rejects
 */
const RecordBatch = async ({
  compression = Compression.None,
  compressionLevel,
  firstOffset = Long.fromInt(0),
  firstTimestamp = Date.now(),
  maxTimestamp = Date.now(),
  partitionLeaderEpoch = 0,
  lastOffsetDelta = 0,
  transactional = false,
  producerId = Long.fromValue(-1), // for idempotent messages
  producerEpoch = 0, // for idempotent messages
  firstSequence = 0, // for idempotent messages
  records = [],
}) => {
  const COMPRESSION_CODEC = compression & COMPRESSION_CODEC_MASK
  const IN_TRANSACTION = transactional ? TRANSACTIONAL_MASK : 0
  const attributes = COMPRESSION_CODEC | TIMESTAMP_MASK | IN_TRANSACTION

  const batchBody = new Encoder()
    .writeInt16(attributes)
    .writeInt32(lastOffsetDelta)
    .writeInt64(firstTimestamp)
    .writeInt64(maxTimestamp)
    .writeInt64(producerId)
    .writeInt16(producerEpoch)
    .writeInt32(firstSequence)

  if (compression === Compression.None) {
    if (records.every((v) => typeof v === typeof records[0])) {
      batchBody.writeArray(records, typeof records[0])
    } else {
      batchBody.writeArray(records)
    }
  } else {
    const compressedRecords = await compressRecords(compression, records, {
      level: compressionLevel,
    })
    batchBody.writeInt32(records.length).writeBuffer(compressedRecords)
  }

  // CRC32C validation is happening here:
  // https://github.com/apache/kafka/blob/0.11.0.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L148

  const batch = new Encoder()
    .writeInt32(partitionLeaderEpoch)
    .writeInt8(MAGIC_BYTE)
    .writeUInt32(crc32C(batchBody.buffer))
    .writeEncoder(batchBody)

  return new Encoder().writeInt64(firstOffset).writeBytes(batch.buffer)
}

/**
 * Compresses encoded records with the selected codec and optional level.
 * @param {number} compression codec id
 * @param {object[]} records encodable records
 * @param {{ level?: number }} options codec options
 * @returns {Promise<Buffer>} compressed record data
 * @throws {Error} for unavailable codecs or compression failures
 */
const compressRecords = async (compression, records, { level }) => {
  const codec = lookupCodec(compression)
  const recordsEncoder = new Encoder()

  recordsEncoder.writeEncoderArray(records)

  return codec.compress(recordsEncoder, { level })
}

module.exports = {
  RecordBatch,
  MAGIC_BYTE,
}
