/**
 * CRC32C (Castagnoli) checksum, computed with the "slicing-by-8" technique: eight bytes are
 * folded per loop iteration against eight precomputed lookup tables, instead of one byte against
 * one table. Same polynomial and bit-reflection as the classic byte-at-a-time table implementation
 * this replaces (measured ~2.3x faster at the 64 KB batch size that dominates real fetch traffic;
 * see the CHANGELOG for the benchmark).
 * @see https://github.com/ashi009/node-fast-crc32c/blob/master/impls/js_crc32c.js (reference byte-at-a-time table)
 */

const os = require('os')
const { isSharedArrayBuffer } = require('util').types

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

// Eight local bindings: indexing an array of tables in the loop costs a second lookup per byte.
const [T0, T1, T2, T3, T4, T5, T6, T7] = buildTables()

/**
 * Folds `buffer[offset .. end)` into a running CRC, eight bytes per step, then byte by byte.
 * @param {number} crc running (pre-inverted) CRC
 * @param {Uint8Array} buffer
 * @param {number} offset first byte to fold
 * @param {number} end one past the last byte to fold
 * @returns {number} updated running CRC
 */
const foldBytes = (crc, buffer, offset, end) => {
  const chunkedEnd = end - ((end - offset) % 8)

  for (; offset < chunkedEnd; offset += 8) {
    crc =
      (crc ^
        (buffer[offset] |
          (buffer[offset + 1] << 8) |
          (buffer[offset + 2] << 16) |
          (buffer[offset + 3] << 24))) >>>
      0

    crc =
      (T7[crc & 0xff] ^
        T6[(crc >>> 8) & 0xff] ^
        T5[(crc >>> 16) & 0xff] ^
        T4[(crc >>> 24) & 0xff] ^
        T3[buffer[offset + 4]] ^
        T2[buffer[offset + 5]] ^
        T1[buffer[offset + 6]] ^
        T0[buffer[offset + 7]]) >>>
      0
  }

  for (; offset < end; offset++) {
    crc = (T0[(crc ^ buffer[offset]) & 0xff] ^ (crc >>> 8)) >>> 0
  }

  return crc
}

/**
 * Folds little-endian 32-bit words into a running CRC, two words (eight bytes) per step. The same
 * table lookups as {@link foldBytes}, with each byte taken from a word instead of read on its own.
 * @param {number} crc running (pre-inverted) CRC
 * @param {Uint32Array} words an even number of words
 * @returns {number} updated running CRC
 */
const foldWords = (crc, words) => {
  for (let i = 0; i < words.length; i += 2) {
    const low = crc ^ words[i]
    const high = words[i + 1]
    crc =
      T7[low & 0xff] ^
      T6[(low >>> 8) & 0xff] ^
      T5[(low >>> 16) & 0xff] ^
      T4[low >>> 24] ^
      T3[high & 0xff] ^
      T2[(high >>> 8) & 0xff] ^
      T1[(high >>> 16) & 0xff] ^
      T0[high >>> 24]
  }

  return crc >>> 0
}

// Below this length the word view costs more than it saves (measured in a local probe).
const MIN_WORD_LENGTH = 64

/**
 * Builds a CRC32C function. On a little-endian machine the bulk of a buffer is read as aligned
 * 32-bit words through one `Uint32Array` view of its own bytes: a few head bytes up to the first
 * 4-byte boundary, then whole eight-byte steps, then the tail, each by bytes. Buffers shorter than
 * {@link MIN_WORD_LENGTH}, views over shared memory and big-endian machines read every byte on its own. Both give the same
 * checksum for every input.
 * @param {boolean} littleEndian whether this machine stores words little-endian
 * @returns {(buffer: Uint8Array) => number} the checksum function
 */
const createCrc32C = (littleEndian) => (buffer) => {
  const length = buffer.length

  if (!littleEndian || length < MIN_WORD_LENGTH) {
    return (foldBytes(0xffffffff, buffer, 0, length) ^ 0xffffffff) >>> 0
  }

  // Shared memory can change under the read, and a word load would see a write differently.
  if (isSharedArrayBuffer(buffer.buffer)) {
    return (foldBytes(0xffffffff, buffer, 0, length) ^ 0xffffffff) >>> 0
  }

  const head = (4 - (buffer.byteOffset & 3)) & 3
  const wordCount = 2 * Math.floor((length - head) / 8)
  const tail = head + wordCount * 4

  let crc = foldBytes(0xffffffff, buffer, 0, head)
  crc = foldWords(crc, new Uint32Array(buffer.buffer, buffer.byteOffset + head, wordCount))
  crc = foldBytes(crc, buffer, tail, length)

  return (crc ^ 0xffffffff) >>> 0
}

/**
 * Computes the CRC32C (Castagnoli) checksum of a buffer.
 * @type {(buffer: Uint8Array) => number} returns the unsigned 32-bit checksum
 */
const crc32C = createCrc32C(os.endianness() === 'LE')

module.exports = { crc32C, createCrc32C }
