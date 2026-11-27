/**
 * Unsigned LEB128 varints of the record format, written straight into a Buffer. The record
 * format's signed varints and varlongs are zigzag encoded: a non-negative `n` is written as the
 * unsigned varint of `2n`, and -1 (a null field) as the single byte 1.
 * @see https://kafka.apache.org/documentation/#recordbatch
 */

/**
 * @param {number} value an integer from 0 to 2^56 - 1 that a double holds exactly
 * @returns {number} its bytes as an unsigned varint, 1 to 8
 */
const uvarintSize = (value) =>
  value < 0x80
    ? 1
    : value < 0x4000
      ? 2
      : value < 0x200000
        ? 3
        : value < 0x10000000
          ? 4
          : value < 0x800000000
            ? 5
            : value < 0x40000000000
              ? 6
              : value < 0x2000000000000
                ? 7
                : 8

/**
 * Writes `value` as an unsigned varint.
 * @param {Buffer} buffer
 * @param {number} offset where the first byte goes
 * @param {number} value as for `uvarintSize`
 * @returns {number} the offset after the last byte
 */
const writeUVarint = (buffer, offset, value) => {
  while (value >= 0x80) {
    buffer[offset++] = (value % 0x80) | 0x80
    value = Math.floor(value / 0x80)
  }
  buffer[offset++] = value
  return offset
}

/**
 * @param {bigint} value from 0 to 2^64 - 1
 * @returns {number} its bytes as an unsigned varint, 1 to 10
 */
const bigUvarintSize = (value) => {
  let size = 1
  while (value >= 0x80n) {
    value >>= 7n
    size += 1
  }
  return size
}

/**
 * Writes `value` as an unsigned varint.
 * @param {Buffer} buffer
 * @param {number} offset where the first byte goes
 * @param {bigint} value from 0 to 2^64 - 1
 * @returns {number} the offset after the last byte
 */
const writeBigUVarint = (buffer, offset, value) => {
  while (value >= 0x80n) {
    buffer[offset++] = Number(value & 0x7fn) | 0x80
    value >>= 7n
  }
  buffer[offset++] = Number(value)
  return offset
}

module.exports = { uvarintSize, writeUVarint, bigUvarintSize, writeBigUVarint }
