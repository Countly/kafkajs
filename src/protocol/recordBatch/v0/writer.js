const Long = require('../../../utils/long')
const Encoder = require('../../encoder')
const crc32C = require('../crc32C')
const { KafkaJSNonRetriableError } = require('../../../errors')
const {
  Types: Compression,
  lookupCodec,
  COMPRESSION_CODEC_MASK,
} = require('../../message/compression')
const { uvarintSize, writeUVarint, bigUvarintSize, writeBigUVarint } = require('../varint')
const { probe, isGenuineBuffer, lengthOf, assertWritten, writeTimestamp } = require('../entries')
const { MAGIC_BYTE } = require('./index')

const TRANSACTIONAL_MASK = 16
/** `DefaultRecordBatch.RECORD_BATCH_OVERHEAD`: the bytes before the records. */
const BATCH_OVERHEAD = 61
/** Where the bytes the CRC covers begin: the attributes. */
const CRC_START = 21

/**
 * The records of a batch as the v2 format writes them, from owned entries. Each record is its
 * length, a zero attributes byte, its timestamp delta from the batch's first timestamp, its offset
 * delta and the entry's tail. Deltas are numbers while the batch's largest timestamp is a safe
 * integer, `bigint`s otherwise.
 */
class RecordsSection {
  /** @param {import('../entries').RecordEntries} entries */
  constructor(entries) {
    this.entries = entries
    this.exact = typeof entries.max === 'bigint'
    this.first = this.exact ? BigInt(entries.first) : entries.first
  }

  /**
   * @param {number} index
   * @returns {number|bigint} the zigzag of the record's timestamp delta, which is never negative
   */
  zigzagDelta(index) {
    return (this.entries.timestamp(index) - this.first) * (this.exact ? 2n : 2)
  }

  /**
   * @param {number} index
   * @returns {number} the bytes of the record after its length varint
   */
  bodyLength(index) {
    const delta = this.zigzagDelta(index)
    return (
      1 +
      (this.exact ? bigUvarintSize(delta) : uvarintSize(delta)) +
      uvarintSize(2 * index) +
      this.entries.tailLength(index)
    )
  }

  /** @returns {number} the bytes of every record */
  size() {
    let size = 0
    for (let index = 0; index < this.entries.count; index++) {
      const body = this.bodyLength(index)
      size += uvarintSize(2 * body) + body
    }
    return size
  }

  /**
   * Writes every record from `offset`.
   * @param {Buffer} target
   * @param {number} offset
   * @returns {number} the offset after the last record
   */
  write(target, offset) {
    for (let index = 0; index < this.entries.count; index++) {
      offset = writeUVarint(target, offset, 2 * this.bodyLength(index))
      target[offset++] = 0
      const delta = this.zigzagDelta(index)
      offset = this.exact
        ? writeBigUVarint(target, offset, delta)
        : writeUVarint(target, offset, delta)
      offset = writeUVarint(target, offset, 2 * index)
      offset = this.entries.copyTail(index, target, offset)
    }
    return offset
  }
}

/**
 * Hands the records to the registered codec: an `Encoder` over a fresh, unpooled records section
 * written in full.
 * @param {number} compression
 * @param {number} [level]
 * @param {RecordsSection} records
 * @returns {Promise<*>} what the codec answers, unread
 * @throws {TypeError} for a compression with no codec, as the record batch encoder did
 */
const compress = (compression, level, records) => {
  const codec = lookupCodec(compression)
  const section = Buffer.allocUnsafeSlow(records.size())
  assertWritten(section, records.write(section, 0))
  return codec.compress(Encoder.wrap(section), { level })
}

/**
 * Checks a codec's output with the typed-array intrinsics, in the step that copies it.
 * @param {*} output
 * @returns {number} its length
 * @throws {KafkaJSNonRetriableError} when it is not a Buffer, or its store is detached or shrunk
 */
const outputLength = (output) => {
  if (!isGenuineBuffer(output)) {
    throw new KafkaJSNonRetriableError('The compression codec did not return a Buffer')
  }
  probe(output, 'The compression codec returned a Buffer that cannot be read')
  return lengthOf(output)
}

/**
 * Writes one magic-2 record batch from owned entries into a buffer of exactly its size: sizes
 * first, one allocation, every field in place, the CRC32C computed over the buffer itself. Its
 * bytes are those the `RecordBatch` and `Record` encoders write for the same records. The first
 * offset and the partition leader epoch are 0; the first and max timestamps are the lowest and
 * highest of the records. A codec's output is measured with the typed-array intrinsics and copied
 * into the batch in the same synchronous step that follows the codec's answer: no own `length`,
 * `copy` or `buffer` of it runs, and nothing the codec does to it afterwards reaches the batch.
 * @param {object} options
 * @param {import('../entries').RecordEntries} options.entries
 * @param {number} [options.compression=Compression.None] codec id
 * @param {number} [options.compressionLevel] passed to the codec as `level`
 * @param {Long|number|string} [options.producerId=-1]
 * @param {number} [options.producerEpoch=0]
 * @param {number} [options.firstSequence=0]
 * @param {boolean} [options.transactional=false]
 * @returns {Promise<Buffer>} the batch, from its first offset to its last record
 * @throws {Error} what the codec throws, and a `TypeError` for a compression with no codec
 */
const writeRecordBatch = async ({
  entries,
  compression = Compression.None,
  compressionLevel,
  producerId = -1,
  producerEpoch = 0,
  firstSequence = 0,
  transactional = false,
}) => {
  const attributes =
    (compression & COMPRESSION_CODEC_MASK) | (transactional ? TRANSACTIONAL_MASK : 0)
  const id = Long.fromValue(producerId)
  const epoch = +producerEpoch
  const sequence = +firstSequence
  const records = new RecordsSection(entries)
  let batch
  if (compression === Compression.None) {
    batch = Buffer.allocUnsafeSlow(BATCH_OVERHEAD + records.size())
    assertWritten(batch, records.write(batch, BATCH_OVERHEAD))
  } else {
    const output = await compress(compression, compressionLevel, records)
    batch = Buffer.allocUnsafeSlow(BATCH_OVERHEAD + outputLength(output))
    batch.set(output, BATCH_OVERHEAD)
  }

  batch.fill(0, 0, 8)
  batch.writeInt32BE(batch.length - 12, 8)
  batch.writeInt32BE(0, 12)
  batch.writeInt8(MAGIC_BYTE, 16)
  batch.writeInt16BE(attributes, CRC_START)
  batch.writeInt32BE(entries.count - 1, 23)
  writeTimestamp(batch, 27, entries.first)
  writeTimestamp(batch, 35, entries.max)
  batch.writeInt32BE(id.getHighBits(), 43)
  batch.writeInt32BE(id.getLowBits(), 47)
  batch.writeInt16BE(epoch, 51)
  batch.writeInt32BE(sequence, 53)
  batch.writeInt32BE(entries.count, 57)
  batch.writeUInt32BE(crc32C(batch.subarray(CRC_START)), 17)
  return batch
}

module.exports = { writeRecordBatch, BATCH_OVERHEAD }
