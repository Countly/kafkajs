const Decoder = require('../../../decoder')
const MessageSetDecoder = require('../../../messageSet/decoder')
const RecordBatchDecoder = require('../../../recordBatch/v0/decoder')
const { MAGIC_BYTE } = require('../../../recordBatch/v0')

// the magic offset is at the same offset for all current message formats, but the 4 bytes
// between the size and the magic is dependent on the version.
const MAGIC_OFFSET = 16
const RECORD_BATCH_OVERHEAD = 49

/**
 * Decodes the `RECORDS` portion of a Fetch response partition.
 * @param {import('../../../decoder')} decoder
 * @param {object} [options]
 * @param {boolean} [options.checkCrcs=true] forwarded to {@link RecordBatchDecoder}; see its JSDoc
 * @returns {Promise<object[]>} the decoded records
 * @throws {KafkaJSPartialMessageError} on a CRC32C mismatch (`crcMismatch: true`); a genuinely
 *         truncated tail batch (from `maxBytes`) is swallowed instead, as before
 */
const decodeMessages = async (decoder, options = {}) => {
  const messagesSize = decoder.readInt32()

  if (messagesSize <= 0 || !decoder.canReadBytes(messagesSize)) {
    return []
  }

  const messagesBuffer = decoder.readBytes(messagesSize)
  const messagesDecoder = new Decoder(messagesBuffer)
  const magicByte = messagesBuffer.slice(MAGIC_OFFSET).readInt8(0)

  if (magicByte === MAGIC_BYTE) {
    const records = []

    while (messagesDecoder.canReadBytes(RECORD_BATCH_OVERHEAD)) {
      try {
        const recordBatch = await RecordBatchDecoder(messagesDecoder, options)
        records.push(...recordBatch.records)
      } catch (e) {
        // The tail of the record batches can be an incomplete record because of how maxBytes
        // works, which is not an error. A CRC mismatch is.
        // See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
        if (e.name === 'KafkaJSPartialMessageError' && e.crcMismatch !== true) {
          break
        }

        throw e
      }
    }

    return records
  }

  return MessageSetDecoder(messagesDecoder, messagesSize)
}

module.exports = decodeMessages
