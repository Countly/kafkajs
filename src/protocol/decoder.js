const {
  KafkaJSPartialMessageError,
  KafkaJSInvalidVarIntError,
  KafkaJSInvalidLongError,
} = require('../errors')
const Long = require('../utils/long')

const INT8_SIZE = 1
const INT16_SIZE = 2
const INT32_SIZE = 4
const INT64_SIZE = 8
const DOUBLE_SIZE = 8

const MOST_SIGNIFICANT_BIT = 0x80 // 128
const OTHER_BITS = 0x7f // 127

const UUID_SIZE = 16

/**
 * Shared result for the common empty tag section, so the per-partition tag buffers of a
 * flexible response cost no allocation. Its mutators throw so it can never be poisoned.
 * @type {Map<number, Buffer>}
 */
const EMPTY_TAGGED_FIELDS = Object.freeze(
  Object.assign(new Map(), {
    set: () => {
      throw new TypeError('The empty tagged-fields Map is shared and read-only')
    },
    delete: () => {
      throw new TypeError('The empty tagged-fields Map is shared and read-only')
    },
    clear: () => {
      throw new TypeError('The empty tagged-fields Map is shared and read-only')
    },
  })
)

module.exports = class Decoder {
  static int32Size() {
    return INT32_SIZE
  }

  static decodeZigZag(value) {
    return (value >>> 1) ^ -(value & 1)
  }

  static decodeZigZag64(longValue) {
    return longValue.shiftRightUnsigned(1).xor(longValue.and(Long.fromInt(1)).negate())
  }

  constructor(buffer) {
    this.buffer = buffer
    this.offset = 0
  }

  /**
   * A response frame can be truncated or corrupt. Reading past the end of the buffer crashes
   * with Node's ERR_BUFFER_OUT_OF_BOUNDS (upstream #1374) instead of a typed, catchable error.
   * @param {number} length bytes the caller is about to consume
   * @param {string} reader name of the calling reader, for the message
   */
  ensureReadable(length, reader) {
    if (length < 0 || !this.canReadBytes(length)) {
      throw new KafkaJSPartialMessageError(
        `Tried to read ${length} byte(s) with ${reader} at offset ${this.offset}, ` +
          `but only ${this.buffer.length - this.offset} byte(s) remain`
      )
    }
  }

  readInt8() {
    this.ensureReadable(INT8_SIZE, 'readInt8')
    const value = this.buffer.readInt8(this.offset)
    this.offset += INT8_SIZE
    return value
  }

  canReadInt16() {
    return this.canReadBytes(INT16_SIZE)
  }

  readInt16() {
    this.ensureReadable(INT16_SIZE, 'readInt16')
    const value = this.buffer.readInt16BE(this.offset)
    this.offset += INT16_SIZE
    return value
  }

  canReadInt32() {
    return this.canReadBytes(INT32_SIZE)
  }

  readInt32() {
    this.ensureReadable(INT32_SIZE, 'readInt32')
    const value = this.buffer.readInt32BE(this.offset)
    this.offset += INT32_SIZE
    return value
  }

  canReadInt64() {
    return this.canReadBytes(INT64_SIZE)
  }

  readInt64() {
    this.ensureReadable(INT64_SIZE, 'readInt64')
    const first = this.buffer[this.offset]
    const last = this.buffer[this.offset + 7]

    const low =
      (first << 24) + // Overflow
      this.buffer[this.offset + 1] * 2 ** 16 +
      this.buffer[this.offset + 2] * 2 ** 8 +
      this.buffer[this.offset + 3]
    const high =
      this.buffer[this.offset + 4] * 2 ** 24 +
      this.buffer[this.offset + 5] * 2 ** 16 +
      this.buffer[this.offset + 6] * 2 ** 8 +
      last
    this.offset += INT64_SIZE

    return (BigInt(low) << 32n) + BigInt(high)
  }

  readDouble() {
    this.ensureReadable(DOUBLE_SIZE, 'readDouble')
    const value = this.buffer.readDoubleBE(this.offset)
    this.offset += DOUBLE_SIZE
    return value
  }

  readString() {
    const byteLength = this.readInt16()

    if (byteLength === -1) {
      return null
    }

    this.ensureReadable(byteLength, 'readString')
    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength)
    const value = stringBuffer.toString('utf8')
    this.offset += byteLength
    return value
  }

  readVarIntString() {
    const byteLength = this.readVarInt()

    if (byteLength === -1) {
      return null
    }

    this.ensureReadable(byteLength, 'readVarIntString')
    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength)
    const value = stringBuffer.toString('utf8')
    this.offset += byteLength
    return value
  }

  readUVarIntString() {
    const byteLength = this.readUVarInt()

    if (byteLength === 0) {
      return null
    }

    this.ensureReadable(byteLength - 1, 'readUVarIntString')
    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength - 1)
    const value = stringBuffer.toString('utf8')

    this.offset += byteLength - 1
    return value
  }

  canReadBytes(length) {
    return this.buffer.length - this.offset >= length
  }

  readBytes(byteLength = this.readInt32()) {
    if (byteLength === -1) {
      return null
    }

    this.ensureReadable(byteLength, 'readBytes')
    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength)
    this.offset += byteLength
    return stringBuffer
  }

  readVarIntBytes() {
    const byteLength = this.readVarInt()

    if (byteLength === -1) {
      return null
    }

    this.ensureReadable(byteLength, 'readVarIntBytes')
    const stringBuffer = this.buffer.slice(this.offset, this.offset + byteLength)
    this.offset += byteLength
    return stringBuffer
  }

  /**
   * COMPACT_BYTES / COMPACT_NULLABLE_BYTES: an unsigned varint holding the payload length + 1,
   * then that many bytes. 0 is the null sentinel, 1 is empty.
   * @see https://kafka.apache.org/43/design/protocol
   * @returns {Buffer|null}
   */
  readUVarIntBytes() {
    const byteLength = this.readUVarInt()

    if (byteLength === 0) {
      return null
    }

    const payloadLength = byteLength - 1
    this.ensureReadable(payloadLength, 'readUVarIntBytes')
    const payload = this.buffer.slice(this.offset, this.offset + payloadLength)
    this.offset += payloadLength
    return payload
  }

  /**
   * A tagged field's size prefix is the raw serialized byte count, not the compact
   * "length + 1" used by COMPACT_BYTES, so tag sections cannot be read with
   * readUVarIntBytes().
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
   * @returns {Buffer} exactly the next N bytes, where N is the unsigned varint just read
   */
  readRawUVarIntBytes() {
    const byteLength = this.readUVarInt()
    this.ensureReadable(byteLength, 'readRawUVarIntBytes')
    const payload = this.buffer.slice(this.offset, this.offset + byteLength)
    this.offset += byteLength
    return payload
  }

  readBoolean() {
    return this.readInt8() === 1
  }

  /**
   * UUID: 16 raw big-endian bytes, rendered as the canonical lowercase 8-4-4-4-12 hex form.
   * The all-zero UUID is the "no UUID" sentinel and reads back as null.
   * @see https://kafka.apache.org/43/design/protocol
   * @returns {string|null}
   */
  readUuid() {
    this.ensureReadable(UUID_SIZE, 'readUuid')
    const hex = this.buffer.toString('hex', this.offset, this.offset + UUID_SIZE)
    this.offset += UUID_SIZE

    if (hex === '00000000000000000000000000000000') {
      return null
    }

    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(
      16,
      20
    )}-${hex.slice(20)}`
  }

  readAll() {
    const result = this.buffer.slice(this.offset)
    this.offset += Buffer.byteLength(this.buffer)
    return result
  }

  readArray(reader) {
    const length = this.readInt32()

    if (length === -1) {
      return []
    }

    this.ensureReadable(length, 'readArray')
    const array = new Array(length)
    for (let i = 0; i < length; i++) {
      array[i] = reader(this)
    }

    return array
  }

  readVarIntArray(reader) {
    const length = this.readVarInt()

    if (length === -1) {
      return []
    }

    this.ensureReadable(length, 'readVarIntArray')
    const array = new Array(length)
    for (let i = 0; i < length; i++) {
      array[i] = reader(this)
    }

    return array
  }

  /* According to the protocol type documentation: https://kafka.apache.org/protocol#protocol_types,
  a compact array with length zero is a null array. An array with length 1 is an empty array. */
  readUVarIntArray(reader) {
    const length = this.readUVarInt()

    if (length === 0) {
      return null
    }

    this.ensureReadable(length - 1, 'readUVarIntArray')
    const array = new Array(length - 1)
    for (let i = 0; i < length - 1; i++) {
      array[i] = reader(this)
    }

    return array
  }

  async readArrayAsync(reader) {
    const length = this.readInt32()

    if (length === -1) {
      return []
    }

    this.ensureReadable(length, 'readArrayAsync')
    const array = new Array(length)
    for (let i = 0; i < length; i++) {
      array[i] = await reader(this)
    }

    return array
  }

  readVarInt() {
    let currentByte
    let result = 0
    let i = 0

    do {
      this.ensureReadable(1, 'readVarInt')
      currentByte = this.buffer[this.offset++]
      result += (currentByte & OTHER_BITS) << i
      i += 7
    } while (currentByte >= MOST_SIGNIFICANT_BIT)

    return Decoder.decodeZigZag(result)
  }

  // By default JavaScript's numbers are of type float64, performing bitwise operations converts the numbers to a signed 32-bit integer
  // Unsigned Right Shift Operator >>> ensures the returned value is an unsigned 32-bit integer
  readUVarInt() {
    let currentByte
    let result = 0
    let i = 0
    this.ensureReadable(1, 'readUVarInt')
    while (((currentByte = this.buffer[this.offset++]) & MOST_SIGNIFICANT_BIT) !== 0) {
      result |= (currentByte & OTHER_BITS) << i
      i += 7
      if (i > 28) {
        throw new KafkaJSInvalidVarIntError('Invalid VarInt, must contain 5 bytes or less')
      }
      this.ensureReadable(1, 'readUVarInt')
    }
    result |= currentByte << i
    return result >>> 0
  }

  /**
   * TAG_BUFFER: every structure in a flexible version ends with one. The section is a count,
   * then that many `(tag, length, value)` triples in strictly ascending tag order; an empty
   * section is a single 0x00 byte. Values are returned raw — the codec that knows the tag
   * decodes them; unknown tags are kept as bytes so nothing is silently dropped.
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
   * The returned Map is **read-only**: an empty section returns a single shared instance, because
   * every struct of a flexible message ends with a tag buffer and almost all of them are empty —
   * a Metadata v12 response with 100 topics of 50 partitions would otherwise allocate ~5 100 Maps
   * per refresh.
   *
   * @returns {Map<number, Buffer>} raw tagged values, in the order they appeared
   * @throws {KafkaJSPartialMessageError} when the section is malformed or truncated
   */
  readTaggedFields() {
    const numberOfTaggedFields = this.readUVarInt()

    if (numberOfTaggedFields === 0) {
      return EMPTY_TAGGED_FIELDS
    }

    const taggedFields = new Map()
    let previousTag = -1

    for (let i = 0; i < numberOfTaggedFields; i++) {
      const tag = this.readUVarInt()

      if (tag <= previousTag) {
        throw new KafkaJSPartialMessageError(
          `Tagged fields must be serialized in ascending tag order without duplicates, ` +
            `read tag ${tag} after ${previousTag} at offset ${this.offset}`
        )
      }

      previousTag = tag
      taggedFields.set(tag, this.readRawUVarIntBytes())
    }

    return taggedFields
  }

  readVarLong() {
    let currentByte
    let result = Long.fromInt(0)
    let i = 0

    do {
      if (i > 63) {
        throw new KafkaJSInvalidLongError('Invalid Long, must contain 9 bytes or less')
      }
      this.ensureReadable(1, 'readVarLong')
      currentByte = this.buffer[this.offset++]
      result = result.add(Long.fromInt(currentByte & OTHER_BITS).shiftLeft(i))
      i += 7
    } while (currentByte >= MOST_SIGNIFICANT_BIT)

    return Decoder.decodeZigZag64(result)
  }

  slice(size) {
    return new Decoder(this.buffer.slice(this.offset, this.offset + size))
  }

  forward(size) {
    this.offset += size
  }
}
