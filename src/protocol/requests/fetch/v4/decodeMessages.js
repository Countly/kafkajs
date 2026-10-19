const Decoder = require('../../../decoder')
const MessageSetDecoder = require('../../../messageSet/decoder')
const RecordBatchDecoder = require('../../../recordBatch/v0/decoder')
const { KafkaJSCorruptRecordError } = require('../../../../errors')

// The magic byte is at the same offset for all current message formats, but the 4 bytes
// between the size and the magic is dependent on the version.
const MAGIC_OFFSET = 16

/**
 * @param {Buffer} buffer
 * @param {number} offset where a batch starts
 * @returns {boolean} whether the batch there is a legacy message set (magic 0 or 1): its magic
 *   byte is readable and says so
 */
const isLegacy = (buffer, offset) => {
  if (buffer.length - offset <= MAGIC_OFFSET) {
    return false
  }

  const magic = buffer.readInt8(offset + MAGIC_OFFSET)
  return magic === 0 || magic === 1
}

/**
 * Decodes the `RECORDS` portion of a Fetch response partition.
 *
 * A block that ends inside a batch, as one cut where `maxBytes` ended does, returns the records
 * before that batch. A complete batch that is impossible (see the record batch decoder) ends the
 * block as well, but is not swallowed: the records before it are returned with a `corruption`
 * marker naming it, so the valid partitions of the same response are not thrown away with it. A
 * batch of magic 0 or 1 after a magic 2 batch also ends the block: the next fetch starts at it
 * and takes the legacy path.
 * @param {import('../../../decoder')} decoder
 * @param {object} [options]
 * @param {boolean} [options.checkCrcs=true] forwarded to {@link RecordBatchDecoder}; see its JSDoc
 * @param {boolean} [options.compact=false] read the records block as COMPACT_RECORDS (a UVarInt of
 *        length + 1, 0 meaning null) rather than as an INT32 length. Fetch v12+ is flexible; every
 *        earlier version keeps the INT32. Only the size read changes — the record-batch loop, the
 *        CRC check and the partial-tail tolerance are shared.
 * @returns {Promise<{ messages: object[], corruption?: { reason: string, offset: string,
 *          error: KafkaJSCorruptRecordError } }>} the decoded records, and `corruption` naming the
 *          first corrupt complete batch when there was one; the property is absent otherwise, so a
 *          codec can spread the result into a partition without changing its shape
 * @throws {KafkaJSPartialMessageError} on a CRC32C mismatch (`crcMismatch: true`)
 */
const decodeMessages = async (decoder, options = {}) => {
  const messagesSize = options.compact ? decoder.readUVarInt() - 1 : decoder.readInt32()

  if (messagesSize <= 0 || !decoder.canReadBytes(messagesSize)) {
    return { messages: [] }
  }

  const messagesBuffer = decoder.readBytes(messagesSize)
  const messagesDecoder = new Decoder(messagesBuffer)

  if (isLegacy(messagesBuffer, 0)) {
    return { messages: await MessageSetDecoder(messagesDecoder, messagesSize) }
  }

  const messages = []

  while (messagesDecoder.canReadBytes(1) && !isLegacy(messagesBuffer, messagesDecoder.offset)) {
    try {
      const recordBatch = await RecordBatchDecoder(messagesDecoder, options)
      // push(...records) throws a RangeError above about 125 000 records
      for (const message of recordBatch.records) {
        messages.push(message)
      }
    } catch (e) {
      // The tail of the record batches can be an incomplete record because of how maxBytes
      // works, which is not an error. A CRC mismatch is.
      // See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
      if (e.name === 'KafkaJSPartialMessageError' && e.crcMismatch !== true) {
        break
      }

      if (e instanceof KafkaJSCorruptRecordError) {
        return { messages, corruption: { reason: e.reason, offset: e.offset, error: e } }
      }

      throw e
    }
  }

  return { messages }
}

module.exports = decodeMessages
