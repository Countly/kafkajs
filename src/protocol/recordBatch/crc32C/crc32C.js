/**
 * CRC32C (Castagnoli) checksum, computed with the "slicing-by-8" technique: eight bytes are
 * folded per loop iteration against eight precomputed lookup tables, instead of one byte against
 * one table. Same polynomial and bit-reflection as the classic byte-at-a-time table implementation
 * this replaces (measured ~2.3x faster at the 64 KB batch size that dominates real fetch traffic;
 * see the CHANGELOG for the benchmark).
 * @see https://github.com/ashi009/node-fast-crc32c/blob/master/impls/js_crc32c.js (reference byte-at-a-time table)
 */

const REVERSED_POLYNOMIAL = 0x82f63b78

/**
 * Builds the eight 256-entry lookup tables slicing-by-8 folds against. Table 0 is the ordinary
 * reflected CRC32C table; tables 1-7 are table 0 applied repeatedly, one byte position further
 * down the four-byte word each time.
 * @returns {Uint32Array[]} eight tables of 256 entries each
 */
const buildTables = () => {
  const tables = Array.from({ length: 8 }, () => new Uint32Array(256))

  for (let byte = 0; byte < 256; byte++) {
    let crc = byte
    for (let bit = 0; bit < 8; bit++) {
      crc = crc & 1 ? (crc >>> 1) ^ REVERSED_POLYNOMIAL : crc >>> 1
    }
    tables[0][byte] = crc >>> 0
  }

  for (let byte = 0; byte < 256; byte++) {
    let crc = tables[0][byte]
    for (let table = 1; table < 8; table++) {
      crc = tables[0][crc & 0xff] ^ (crc >>> 8)
      tables[table][byte] = crc >>> 0
    }
  }

  return tables
}

const TABLES = buildTables()

/**
 * Computes the CRC32C (Castagnoli) checksum of a buffer.
 * @param {Buffer} buffer
 * @returns {number} unsigned 32-bit checksum
 */
const crc32C = (buffer) => {
  let crc = 0xffffffff
  const length = buffer.length
  const chunkedLength = length - (length % 8)
  let offset = 0

  for (; offset < chunkedLength; offset += 8) {
    crc =
      (crc ^
        (buffer[offset] |
          (buffer[offset + 1] << 8) |
          (buffer[offset + 2] << 16) |
          (buffer[offset + 3] << 24))) >>>
      0

    crc =
      (TABLES[7][crc & 0xff] ^
        TABLES[6][(crc >>> 8) & 0xff] ^
        TABLES[5][(crc >>> 16) & 0xff] ^
        TABLES[4][(crc >>> 24) & 0xff] ^
        TABLES[3][buffer[offset + 4]] ^
        TABLES[2][buffer[offset + 5]] ^
        TABLES[1][buffer[offset + 6]] ^
        TABLES[0][buffer[offset + 7]]) >>>
      0
  }

  for (; offset < length; offset++) {
    crc = (TABLES[0][(crc ^ buffer[offset]) & 0xff] ^ (crc >>> 8)) >>> 0
  }

  return (crc ^ 0xffffffff) >>> 0
}

module.exports = crc32C
