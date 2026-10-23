/**
 * Hand-built LZ4 frames for the codec specs. A frame is written field by field from the LZ4
 * frame and block format documents, so a spec states the bytes it feeds a decoder instead of
 * taking them from the decoder under test.
 * @see https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
 * @see https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
 */

const PRIME1 = 2654435761
const PRIME2 = 2246822519
const PRIME3 = 3266489917
const PRIME4 = 668265263
const PRIME5 = 374761393

const MAGIC = 0x184d2204
const BLOCK_MAXIMUM = { 4: 65536, 5: 262144, 6: 1048576, 7: 4194304 }

const rotl = (value, bits) => ((value << bits) | (value >>> (32 - bits))) >>> 0

/**
 * xxHash32 with seed 0, the checksum LZ4 frames use for the header, blocks and content.
 * @param {Buffer} input
 * @returns {number} the unsigned 32-bit hash
 */
const xxh32 = (input) => {
  let at = 0
  let hash
  if (input.length >= 16) {
    const lanes = [(PRIME1 + PRIME2) >>> 0, PRIME2, 0, (0 - PRIME1) >>> 0]
    for (; at + 16 <= input.length; at += 16) {
      for (let lane = 0; lane < 4; lane++) {
        const word = input.readUInt32LE(at + lane * 4)
        lanes[lane] =
          Math.imul(rotl((lanes[lane] + Math.imul(word, PRIME2)) >>> 0, 13), PRIME1) >>> 0
      }
    }
    hash = (rotl(lanes[0], 1) + rotl(lanes[1], 7) + rotl(lanes[2], 12) + rotl(lanes[3], 18)) >>> 0
  } else {
    hash = PRIME5
  }
  hash = (hash + input.length) >>> 0
  for (; at + 4 <= input.length; at += 4) {
    hash = (hash + Math.imul(input.readUInt32LE(at), PRIME3)) >>> 0
    hash = Math.imul(rotl(hash, 17), PRIME4) >>> 0
  }
  for (; at < input.length; at++) {
    hash = (hash + Math.imul(input[at], PRIME5)) >>> 0
    hash = Math.imul(rotl(hash, 11), PRIME1) >>> 0
  }
  hash = Math.imul(hash ^ (hash >>> 15), PRIME2) >>> 0
  hash = Math.imul(hash ^ (hash >>> 13), PRIME3) >>> 0
  return (hash ^ (hash >>> 16)) >>> 0
}

/**
 * A length of 15 or more continues in bytes of 255, ended by a byte below 255.
 * @param {number} length the part of the length above 15
 * @returns {Buffer}
 */
const extension = (length) => {
  const full = Math.floor(length / 255)
  return Buffer.concat([Buffer.alloc(full, 255), Buffer.from([length % 255])])
}

/**
 * One sequence of a block: a token, the literal length extension, the literals, and unless it is
 * the last one the match offset and match length extension.
 * @param {object} fields
 * @param {Buffer} [fields.literals]
 * @param {number} [fields.offset] the match offset; omitted for the last, literals-only sequence
 * @param {number} [fields.match] the match length, at least 4
 * @returns {Buffer}
 */
const sequence = ({ literals = Buffer.alloc(0), offset, match }) => {
  const literalNibble = Math.min(literals.length, 15)
  const matchNibble = offset === undefined ? 0 : Math.min(match - 4, 15)
  const parts = [Buffer.from([(literalNibble << 4) | matchNibble])]
  if (literals.length >= 15) parts.push(extension(literals.length - 15))
  parts.push(literals)
  if (offset !== undefined) {
    parts.push(Buffer.from([offset & 255, offset >>> 8]))
    if (match - 4 >= 15) parts.push(extension(match - 4 - 15))
  }
  return Buffer.concat(parts)
}

/**
 * A compressed block that decodes to `length` copies of `byte`: one literal, one overlapping match
 * of offset 1, then the five literals every block ends with.
 * @param {number} byte
 * @param {number} length at least 10
 * @returns {Buffer} the block's data
 */
const runBlock = (byte, length) =>
  Buffer.concat([
    sequence({ literals: Buffer.from([byte]), offset: 1, match: length - 6 }),
    sequence({ literals: Buffer.alloc(5, byte) }),
  ])

/**
 * @param {Buffer} data
 * @param {boolean} stored whether the block is stored uncompressed
 * @returns {{ data: Buffer, stored: boolean }}
 */
const block = (data, stored = false) => ({ data, stored })

/**
 * A frame.
 * @param {object} [fields]
 * @param {boolean} [fields.independent=true] block independence flag
 * @param {4|5|6|7} [fields.maxSizeCode=4] block maximum size code: 64 KiB, 256 KiB, 1 MiB, 4 MiB
 * @param {bigint|number} [fields.contentSize] written to the header when given
 * @param {boolean} [fields.blockChecksums=false] append xxh32 of each block's data
 * @param {Buffer} [fields.contentChecksumOf] write the content checksum of these decoded bytes
 * @param {Array<{ data: Buffer, stored: boolean }>} [fields.blocks]
 * @param {boolean} [fields.endMark=true]
 * @param {Buffer} [fields.trailer] bytes after the frame
 * @param {number} [fields.flg] replaces the computed FLG byte
 * @param {number} [fields.bd] replaces the computed BD byte
 * @returns {Buffer}
 */
const frame = ({
  independent = true,
  maxSizeCode = 4,
  contentSize,
  blockChecksums = false,
  contentChecksumOf,
  blocks = [],
  endMark = true,
  trailer = Buffer.alloc(0),
  flg,
  bd,
} = {}) => {
  const flags =
    flg ??
    (1 << 6) |
      (independent ? 1 << 5 : 0) |
      (blockChecksums ? 1 << 4 : 0) |
      (contentSize !== undefined ? 1 << 3 : 0) |
      (contentChecksumOf ? 1 << 2 : 0)
  const claim = Buffer.alloc(contentSize === undefined ? 0 : 8)
  if (contentSize !== undefined) claim.writeBigUInt64LE(BigInt(contentSize))
  const header = Buffer.concat([Buffer.from([flags, bd ?? maxSizeCode << 4]), claim])
  const parts = [
    Buffer.from([0x04, 0x22, 0x4d, 0x18]),
    header,
    Buffer.from([(xxh32(header) >>> 8) & 255]),
  ]
  for (const { data, stored } of blocks) {
    const word = Buffer.alloc(4)
    word.writeUInt32LE((data.length | (stored ? 0x80000000 : 0)) >>> 0)
    parts.push(word, data)
    if (blockChecksums) {
      const checksum = Buffer.alloc(4)
      checksum.writeUInt32LE(xxh32(data))
      parts.push(checksum)
    }
  }
  if (endMark) parts.push(Buffer.alloc(4))
  if (endMark && contentChecksumOf) {
    const checksum = Buffer.alloc(4)
    checksum.writeUInt32LE(xxh32(contentChecksumOf))
    parts.push(checksum)
  }
  parts.push(trailer)
  return Buffer.concat(parts)
}

module.exports = { MAGIC, BLOCK_MAXIMUM, xxh32, extension, sequence, runBlock, block, frame }
