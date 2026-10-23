const Long = require('../../utils/long')
const Decoder = require('../decoder')
const MessageDecoder = require('../message/decoder')
const { decompressBounded, lookupCodecByAttributes } = require('../message/compression')
const { DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES } = require('../../constants')
const { KafkaJSPartialMessageError } = require('../../errors')

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
 *   compressed message, forwarded as maxOutputLength to the codec
 * @returns {Promise<object[]>} complete messages before any partial tail, in wire order
 * @throws {KafkaJSCorruptRecordError} for a corrupt compressed stream or output above the bound
 * @throws {KafkaJSNotImplemented} when the selected optional codec is unavailable
 */

module.exports = async (
  primaryDecoder,
  size = null,
  { maxDecompressedBatchBytes = DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES } = {}
) => {
  const messages = []
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
        for (const entry of EntriesDecoder(new Decoder(buffer), message)) {
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

const EntriesDecoder = (decoder, compressedMessage) => {
  const messages = []

  while (decoder.offset < decoder.buffer.length) {
    messages.push(EntryDecoder(decoder))
  }

  if (compressedMessage.magicByte > 0 && compressedMessage.offset >= 0) {
    const compressedOffset = Long.fromValue(compressedMessage.offset)
    const lastMessageOffset = Long.fromValue(messages[messages.length - 1].offset)
    const baseOffset = compressedOffset - lastMessageOffset

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
