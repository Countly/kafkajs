/**
 * @module protocol/message/compression/lz4
 * @description Kafka's LZ4 frame format (KIP-57). Node has no built-in LZ4, so this codec is
 * backed by the optional native dependency `lz4-napi`, which runs on the libuv threadpool and
 * does not block the event loop. Install it to enable LZ4: `npm install lz4-napi`.
 */
const { KafkaJSNotImplemented } = require('../../../errors')

let lz4 = null
try {
  lz4 = require('lz4-napi')
} catch (e) {
  lz4 = null
}

const NOT_SUPPORTED_MESSAGE =
  'LZ4 compression requires the optional dependency "lz4-napi". Install it with `npm install lz4-napi`, ' +
  'or register your own codec with CompressionCodecs[CompressionTypes.LZ4].'

const unavailable = () => {
  throw new KafkaJSNotImplemented(NOT_SUPPORTED_MESSAGE)
}

const FRAME_MAGIC = 0x184d2204
const BLOCK_MAXIMUMS = { 4: 65536, 5: 262144, 6: 1048576, 7: 4194304 }
const LARGEST_BLOCK = BLOCK_MAXIMUMS[7]
const SLICE = 262144

/** A frame the binding would refuse, or an output the bound refuses; `reason` is the typed one. */
class Lz4FrameError extends Error {
  /**
   * @param {string} message
   * @param {'decompression'|'decompressed-size'} [reason]
   */
  constructor(message, reason = 'decompression') {
    super(message)
    this.name = 'Lz4FrameError'
    this.reason = reason
  }
}

/**
 * Reads the descriptor of the first frame, the only one the binding decodes.
 * @param {Buffer} input
 * @returns {?{ independent: boolean, blockChecksum: boolean, claim: ?bigint, max: number, first: number }}
 *   the frame's block mode, block maximum, content size claim (never a size) and where its first
 *   block starts, or `null` when the input ends inside the descriptor, which the binding answers
 * @throws {Lz4FrameError} for a magic, version, reserved bit, dictionary or block maximum the
 *   binding refuses
 */
const readHeader = (input) => {
  if (input.length < 6) return null
  if (input.readUInt32LE(0) !== FRAME_MAGIC) throw new Lz4FrameError('Not the first LZ4 frame')
  const flags = input[4]
  const descriptor = input[5]
  const max = BLOCK_MAXIMUMS[(descriptor >> 4) & 7]
  if (flags >> 6 !== 1 || flags & 3 || descriptor & 0x8f || !max) {
    throw new Lz4FrameError('Unsupported LZ4 frame descriptor')
  }
  const sized = (flags & 8) !== 0
  const first = 6 + (sized ? 8 : 0) + 1
  if (input.length < first) return null
  return {
    independent: (flags & 32) !== 0,
    blockChecksum: (flags & 16) !== 0,
    claim: sized ? input.readBigUInt64LE(6) : null,
    max,
    first,
  }
}

/**
 * Walks the sequences of one compressed block and adds up what they decode to, yielding to the
 * event loop every `SLICE` bytes. The last sequence holds literals only.
 * @param {Buffer} input
 * @param {number} start first byte of the block
 * @param {number} end exclusive end of the block
 * @param {{ work: number }} slice bytes walked since the last yield
 * @returns {Generator<void, number>} the block's decoded length
 * @throws {Lz4FrameError} for a length, literal run or offset field that leaves the block, or a
 *   length extension above the largest block
 */
function* decodedLength(input, start, end, slice) {
  let at = start
  let total = 0
  /**
   * Reads the extension of a length nibble of 15. No length above the largest block is valid, for
   * literals or for a match, so a longer run is refused where it is read: a scan stays within
   * about 16 KiB, however long the run in the input.
   * @returns {number} the length
   */
  const extended = () => {
    let length = 15
    let next = 255
    while (next === 255) {
      if (at >= end) throw new Lz4FrameError('LZ4 block ends inside a length')
      next = input[at++]
      length += next
      if (length > LARGEST_BLOCK) throw new Lz4FrameError('LZ4 length is above the largest block')
    }
    return length
  }
  while (at < end) {
    const token = input[at++]
    const literals = token >> 4 === 15 ? extended() : token >> 4
    if (at + literals > end) throw new Lz4FrameError('LZ4 literals run past the block')
    at += literals
    total += literals
    if (at === end) break
    if (at + 2 > end || (input[at] === 0 && input[at + 1] === 0)) {
      throw new Lz4FrameError('LZ4 block has no valid match offset')
    }
    at += 2
    total += 4 + ((token & 15) === 15 ? extended() : token & 15)
    slice.work += at - start
    start = at
    if (slice.work >= SLICE) {
      slice.work = 0
      yield
    }
  }
  return total
}

/**
 * Adds up what the first frame decodes to: stored blocks by their size and compressed blocks by
 * their exact decoded length (`exact`) or, for the fast bound of an independent frame, by the most
 * 255 to 1 expansion allows within the frame's block maximum.
 * @param {Buffer} input
 * @param {object} header from `readHeader`
 * @param {boolean} exact whether to walk the tokens of compressed blocks
 * @returns {Generator<void, number>} the total
 * @throws {Lz4FrameError} for a block above the frame's maximum, a block that runs past the input
 *   or a decoded block above the maximum
 */
function* measure(input, header, exact) {
  const slice = { work: 0 }
  let at = header.first
  let total = 0
  while (at + 4 <= input.length) {
    const word = input.readUInt32LE(at)
    const size = word & 0x7fffffff
    at += 4
    if (word === 0) break
    if (size > header.max)
      throw new Lz4FrameError(`LZ4 block of ${size} bytes is above its maximum`)
    const end = at + size
    if (end + (header.blockChecksum ? 4 : 0) > input.length) {
      throw new Lz4FrameError('LZ4 block runs past the input')
    }
    if (word >= 0x80000000) {
      total += size
    } else if (exact) {
      const decoded = yield* decodedLength(input, at, end, slice)
      if (decoded > header.max) throw new Lz4FrameError('LZ4 block decodes above its maximum')
      total += decoded
    } else {
      total += Math.min(header.max, size * 255)
    }
    at = end + (header.blockChecksum ? 4 : 0)
    slice.work += size + 4
    if (slice.work >= SLICE) {
      slice.work = 0
      yield
    }
  }
  return total
}

/**
 * Runs a generator that yields at slice boundaries, taking one event-loop turn at each.
 * @template T
 * @param {Generator<void, T>} steps
 * @returns {Promise<T>} the generator's result
 */
const drive = async (steps) => {
  let step = steps.next()
  while (!step.done) {
    await new Promise((resolve) => setImmediate(resolve))
    step = steps.next()
  }
  return step.value
}

/**
 * Refuses a frame that would decode to more than `limit` bytes before the binding sees it: the
 * binding allocates the whole output and takes no limit. The first frame is read the way the
 * binding reads it (later frames and trailing bytes are ignored). An independent frame passes on
 * the fast bound when that fits the limit; every other frame, and an independent one that fast
 * bound refuses, gets the exact length from the tokens. A content size in the header is a claim,
 * never a size.
 * @param {Buffer} input
 * @param {number} limit
 * @returns {Promise<void>}
 * @throws {Lz4FrameError} `decompressed-size` when the exact length passes `limit` and the claim,
 *   if there is one, agrees with it; `decompression` for anything the binding would refuse
 */
const preflight = async (input, limit) => {
  const header = readHeader(input)
  if (header === null) return
  if (header.independent && (await drive(measure(input, header, false))) <= limit) return
  const total = await drive(measure(input, header, true))
  if (header.claim !== null && header.claim !== BigInt(total)) {
    throw new Lz4FrameError(
      `LZ4 content size ${header.claim} disagrees with ${total} decoded bytes`
    )
  }
  if (total > limit) {
    throw new Lz4FrameError(`LZ4 frame decodes to ${total} bytes`, 'decompressed-size')
  }
}

module.exports = {
  /** @type {boolean} Whether `lz4-napi` loaded successfully; `index.js` uses this to decide
   *  between returning this codec and throwing `KafkaJSNotImplemented`. */
  isSupported: lz4 != null,

  /** The message thrown when `isSupported` is false; exported so `index.js` and this module's
   *  own guard never drift apart. */
  NOT_SUPPORTED_MESSAGE,

  /**
   * @param {{ buffer: Buffer }} encoder
   * @returns {Promise<Buffer>}
   * @throws {KafkaJSNotImplemented} when `lz4-napi` is not installed
   */
  async compress(encoder) {
    if (!lz4) unavailable()
    return await lz4.compressFrame(encoder.buffer)
  },

  /**
   * @param {Buffer} buffer
   * @param {{ maxOutputLength?: number }} [options] the most bytes to produce; without it the
   *   frame is not checked first
   * @returns {Promise<Buffer>}
   * @throws {KafkaJSNotImplemented} when `lz4-napi` is not installed
   * @throws {Lz4FrameError} when the frame is refused before the binding is called
   * @throws {Error} the binding's own failure for a frame it refuses
   */
  async decompress(buffer, { maxOutputLength } = {}) {
    if (!lz4) unavailable()
    if (maxOutputLength !== undefined) await preflight(buffer, maxOutputLength)
    return await lz4.decompressFrame(buffer)
  },
}
