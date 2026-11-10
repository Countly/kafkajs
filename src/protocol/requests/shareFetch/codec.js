const Encoder = require('../../encoder')
const Decoder = require('../../decoder')
const {
  KafkaJSNonRetriableError,
  KafkaJSPartialMessageError,
  KafkaJSInvalidVarIntError,
} = require('../../../errors')

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

/**
 * Encoder for the released share message bodies. Validation occurs at encode time so a
 * descriptor can be inspected before its required fields are supplied.
 */
class ShareEncoder extends Encoder {
  /** @param {string} value @returns {ShareEncoder} @throws {KafkaJSNonRetriableError} unless a canonical UUID string; a missing value is never written as the zero UUID */
  writeUuid(value) {
    if (typeof value !== 'string' || !UUID.test(value))
      throw new KafkaJSNonRetriableError('Expected canonical UUID string')
    return super.writeUuid(value)
  }

  /** @param {number} value @returns {ShareEncoder} @throws {KafkaJSNonRetriableError} outside int8 */
  writeInt8(value) {
    return this.writeInteger(value, 8)
  }

  /** @param {number} value @returns {ShareEncoder} @throws {KafkaJSNonRetriableError} outside int32 */
  writeInt32(value) {
    return this.writeInteger(value, 32)
  }

  /** @param {number} value @param {8|32} bits @returns {ShareEncoder} @throws {KafkaJSNonRetriableError} on invalid input */
  writeInteger(value, bits) {
    if (!Number.isInteger(value) || value < -(2 ** (bits - 1)) || value > 2 ** (bits - 1) - 1) {
      throw new KafkaJSNonRetriableError(`Invalid int${bits}: ${value}`)
    }
    return bits === 8 ? super.writeInt8(value) : super.writeInt32(value)
  }

  /** @param {string|bigint|number} value @returns {ShareEncoder} @throws {KafkaJSNonRetriableError} on negative, unsafe or overflowing offset */
  writeOffset(value) {
    if (
      (typeof value !== 'string' && typeof value !== 'bigint' && !Number.isSafeInteger(value)) ||
      !/^(0|[1-9][0-9]*)$/.test(String(value))
    ) {
      throw new KafkaJSNonRetriableError('Offset must be an exact nonnegative decimal integer')
    }
    const offset = BigInt(value)
    if (offset > 9223372036854775807n)
      throw new KafkaJSNonRetriableError('Offset exceeds signed int64')
    return super.writeInt64(offset)
  }

  /** @param {boolean} value @returns {ShareEncoder} @throws {KafkaJSNonRetriableError} on a non-boolean */
  writeBoolean(value) {
    if (typeof value !== 'boolean') throw new KafkaJSNonRetriableError('Expected boolean')
    return super.writeBoolean(value)
  }

  /** @param {string|null} value @param {boolean} [nullable=false] @returns {ShareEncoder} @throws {KafkaJSNonRetriableError} on invalid string */
  writeCompactString(value, nullable = false) {
    if (value === null && nullable) return super.writeUVarIntString(null)
    if (typeof value !== 'string') throw new KafkaJSNonRetriableError('Expected string')
    return super.writeUVarIntString(value)
  }

  /** @param {Array|null} values @param {function(*): Encoder} writer @param {boolean} [nullable=false] @returns {ShareEncoder} @throws {KafkaJSNonRetriableError} on invalid array */
  writeCompactArray(values, writer, nullable = false) {
    if (values === null && nullable) return super.writeUVarIntArray(null)
    if (!Array.isArray(values)) throw new KafkaJSNonRetriableError('Expected array')
    return super.writeUVarIntArray(values.map(writer))
  }
}

/** A bounded reader that refuses null required fields and unused trailing bytes. */
class ShareDecoder extends Decoder {
  /** @returns {number} @throws {KafkaJSInvalidVarIntError} on unsigned-int32 overflow */
  readUVarInt() {
    let result = 0
    for (let index = 0; ; index++) {
      this.ensureReadable(1, 'readUVarInt')
      const value = this.buffer[this.offset++]
      if (index === 4 && value > 15)
        throw new KafkaJSInvalidVarIntError('Unsigned VarInt exceeds int32')
      result += (value & 127) * 2 ** (index * 7)
      if (value < 128) return result
    }
  }

  /** @param {boolean} [nullable=false] @returns {string|null} @throws {KafkaJSPartialMessageError} on a null required string */
  readCompactString(nullable = false) {
    const value = super.readUVarIntString()
    if (value === null && !nullable) throw new KafkaJSPartialMessageError('Null required string')
    return value
  }

  /** @returns {number} @throws {KafkaJSPartialMessageError} on a null, over-limit or truncated required array */
  arrayLength() {
    const length = this.readUVarInt()
    if (length === 0) {
      throw new KafkaJSPartialMessageError('Null required array')
    }
    const count = this.assertCount(length - 1, 'compact array')
    this.ensureReadable(count, 'compact array')
    return count
  }

  /** @param {function(ShareDecoder): *} reader @returns {Array} Required compact array. */
  readCompactArray(reader) {
    const length = this.arrayLength()
    const result = []
    for (let index = 0; index < length; index++) result.push(reader(this))
    return result
  }

  /** @param {function(ShareDecoder): Promise<*>} reader @returns {Promise<Array>} Required compact array. */
  async readCompactArrayAsync(reader) {
    const length = this.arrayLength()
    const result = []
    for (let index = 0; index < length; index++) result.push(await reader(this))
    return result
  }

  /** @param {function(ShareDecoder): *} reader @returns {*|null} @throws {KafkaJSPartialMessageError} on an invalid presence marker */
  readNullableStruct(reader) {
    const marker = this.readInt8()
    if (marker === -1) return null
    if (marker !== 1) throw new KafkaJSPartialMessageError('Invalid nullable structure marker')
    return reader(this)
  }

  /** @param {boolean} [allowUnknown=false] @returns {string} @throws {KafkaJSPartialMessageError} outside the offset domain */
  readOffset(allowUnknown = false) {
    const value = this.readInt64()
    if (value < (allowUnknown ? -1n : 0n))
      throw new KafkaJSPartialMessageError('Invalid offset or lag')
    return value.toString()
  }

  /** @returns {void} @throws {KafkaJSPartialMessageError} when the body has trailing bytes */
  finish() {
    if (this.offset !== this.buffer.length)
      throw new KafkaJSPartialMessageError('Trailing share response bytes')
  }
}

module.exports = { ShareEncoder, ShareDecoder }
