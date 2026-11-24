const Long = require('../../utils/long')
const Decoder = require('../decoder')
const MessageDecoder = require('../message/decoder')
const { decompressBounded, lookupCodecByAttributes } = require('../message/compression')
const { DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES } = require('../../constants')
const { RECORD_CEILING_BYTES } = require('../recordBatch/v0/decoder')
const { KafkaJSCorruptRecordError, KafkaJSPartialMessageError } = require('../../errors')

/**
 * Decodes a legacy message set in wire order, bounding each compressed message before decoding
 * its entries. A partial tail returns the preceding complete messages. On return, the primary
 * decoder advances past the set; a corrupt or oversized compressed stream rejects the response.
 *
 * MessageSet => [Offset MessageSize Message]
 *  Offset => int64
 *  MessageSize => int32
 *  Message => Bytes
 *
 * @param {Decoder} primaryDecoder positioned at the set, or its size prefix when size is omitted
 * @param {?number} [size=null] the set's byte length; omission reads its int32 size prefix
 * @param {object} [options]
 * @param {number} [options.maxDecompressedBatchBytes=1073741824] the output bound for each
 *   compressed message, forwarded as maxOutputLength to the codec; it also sets the record ceiling
 *   of each compressed message, `max(1, floor(bytes / 512))` inner messages
 * @returns {Promise<object[]>} complete messages before any partial tail, in wire order
 * @throws {KafkaJSCorruptRecordError} for a corrupt compressed stream or output above the bound,
 *   and with reason `count` for a compressed message holding more inner messages than the ceiling
 * @throws {KafkaJSNotImplemented} when the selected optional codec is unavailable
 */

module.exports = async (
  primaryDecoder,
  size = null,
  { maxDecompressedBatchBytes = DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES } = {}
) => {
  const messages = []
  const recordLimit = Math.max(1, Math.floor(maxDecompressedBatchBytes / RECORD_CEILING_BYTES))
  const messageSetSize = size || primaryDecoder.readInt32()
  const messageSetDecoder = primaryDecoder.slice(messageSetSize)

  while (messageSetDecoder.offset < messageSetSize) {
    try {
      const message = EntryDecoder(messageSetDecoder)
      const codec = lookupCodecByAttributes(message.attributes)

      if (codec) {
        const buffer = await decompressBounded(
          codec,
          message.attributes,
          message.value,
          maxDecompressedBatchBytes
        )
        // push(...entries) throws a RangeError above about 125 000 entries
        for (const entry of EntriesDecoder(new Decoder(buffer), message, recordLimit)) {
          messages.push(entry)
        }
      } else {
        messages.push(message)
      }
    } catch (e) {
      if (e.name === 'KafkaJSPartialMessageError') {
        // We tried to decode a partial message, it means that minBytes
        // is probably too low
        break
      }

      if (e.name === 'KafkaJSUnsupportedMagicByteInMessageSet') {
        // Received a MessageSet and a RecordBatch on the same response, the cluster is probably
        // upgrading the message format from 0.10 to 0.11. Stop processing this message set to
        // receive the full record batch on the next request
        break
      }

      throw e
    }
  }

  primaryDecoder.forward(messageSetSize)
  return messages
}

/**
 * Decodes the inner messages of one compressed legacy message and rebases magic 1 offsets on the
 * wrapper's offset.
 * @param {Decoder} decoder over the decompressed inner messages
 * @param {object} compressedMessage the wrapper
 * @param {number} recordLimit the most inner messages the wrapper may hold
 * @returns {object[]} the inner messages, in wire order
 * @throws {KafkaJSCorruptRecordError} with reason `count`, the wrapper's offset and `limit`, before
 *   the inner message that would pass `recordLimit` is decoded
 */
const EntriesDecoder = (decoder, compressedMessage, recordLimit) => {
  const messages = []

  while (decoder.offset < decoder.buffer.length) {
    if (messages.length === recordLimit) {
      throw new KafkaJSCorruptRecordError(
        `Compressed message holds more than the ${recordLimit} inner messages that maxDecompressedBatchBytes allows`,
        { reason: 'count', offset: compressedMessage.offset, limit: recordLimit }
      )
    }
    messages.push(EntryDecoder(decoder))
  }

  if (compressedMessage.magicByte > 0 && compressedMessage.offset >= 0) {
    const compressedOffset = Long.fromValue(compressedMessage.offset)
    const lastMessageOffset = Long.fromValue(messages[messages.length - 1].offset)
    const baseOffset = compressedOffset.subtract(lastMessageOffset)

    for (const message of messages) {
      message.offset = Long.fromValue(message.offset).add(baseOffset).toString()
    }
  }

  return messages
}

const EntryDecoder = (decoder) => {
  if (!decoder.canReadInt64()) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial message: There isn't enough bytes to read the offset`
    )
  }

  const offset = decoder.readInt64().toString()

  if (!decoder.canReadInt32()) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial message: There isn't enough bytes to read the message size`
    )
  }

  const size = decoder.readInt32()
  return MessageDecoder(offset, size, decoder)
}
