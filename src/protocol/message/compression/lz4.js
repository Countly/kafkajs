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
 * Accounts for input skipped without reading it. The walkers take the turns it returns, so a
 * skip costs arithmetic and no generator.
 * @param {{ work: number }} slice input bytes consumed since the last turn
 * @param {number} bytes input bytes to skip
 * @returns {number} the turns owed, one per slice boundary the skip crosses
 */
const skip = (slice, bytes) => {
  const used = slice.work + bytes
  if (used <= SLICE) {
    slice.work = used
    return 0
  }
  slice.work = ((used - 1) % SLICE) + 1
  return Math.floor((used - 1) / SLICE)
}

/**
 * Walks one compressed block, including length extensions and final literals, within the shared
 * input slice budget. Every byte is read inline, behind a check that takes a turn at the slice
 * boundary. The last sequence holds literals only.
 * @param {Buffer} input
 * @param {number} start first byte of the block
 * @param {number} end exclusive end of the block
 * @param {{ work: number }} slice input bytes consumed since the last turn
 * @returns {Generator<void, number>} the block's decoded length
 * @throws {Lz4FrameError} for a malformed sequence or a length above the largest block
 */
function* decodedLength(input, start, end, slice) {
  let at = start
  let total = 0
  while (at < end) {
    if (slice.work === SLICE) {
      slice.work = 0
      yield
    }
    const token = input[at++]
    slice.work++
    let literals = token >> 4
    if (literals === 15) {
      let next
      do {
        if (at >= end) throw new Lz4FrameError('LZ4 block ends inside a length')
        if (slice.work === SLICE) {
          slice.work = 0
          yield
        }
        next = input[at++]
        slice.work++
        literals += next
        if (literals > LARGEST_BLOCK)
          throw new Lz4FrameError('LZ4 length is above the largest block')
      } while (next === 255)
    }
    if (at + literals > end) throw new Lz4FrameError('LZ4 literals run past the block')
    at += literals
    total += literals
    for (let turns = skip(slice, literals); turns > 0; turns--) yield
    if (at === end) break
    if (at + 2 > end) throw new Lz4FrameError('LZ4 block has no valid match offset')
    if (slice.work > SLICE - 2) {
      slice.work = 0
      yield
    }
    const low = input[at++]
    const high = input[at++]
    slice.work += 2
    if (low === 0 && high === 0) throw new Lz4FrameError('LZ4 block has no valid match offset')
    let match = token & 15
    if (match === 15) {
      let next
      do {
        if (at >= end) throw new Lz4FrameError('LZ4 block ends inside a length')
        if (slice.work === SLICE) {
          slice.work = 0
          yield
        }
        next = input[at++]
        slice.work++
        match += next
        if (match > LARGEST_BLOCK) throw new Lz4FrameError('LZ4 length is above the largest block')
      } while (next === 255)
    }
    total += 4 + match
  }
  return total
}

/**
 * Measures the first frame, retaining one input budget across blocks and both measuring passes.
 * Stored blocks use their size; compressed blocks use their exact length or the independent
 * frame's fast expansion bound.
 * @param {Buffer} input
 * @param {object} header the validated frame descriptor
 * @param {boolean} exact whether to walk compressed sequences
 * @param {{ work: number }} slice input bytes consumed since the last turn
 * @returns {Generator<void, number>} the total decoded length or its fast upper bound
 * @throws {Lz4FrameError} for a truncated or oversized block
 */
function* measure(input, header, exact, slice) {
  let at = header.first
  let total = 0
  while (at + 4 <= input.length) {
    if (slice.work > SLICE - 4) {
      slice.work = 0
      yield
    }
    const word = input.readUInt32LE(at)
    slice.work += 4
    const size = word & 0x7fffffff
    at += 4
    if (word === 0) break
    if (size > header.max)
      throw new Lz4FrameError(`LZ4 block of ${size} bytes is above its maximum`)
    let unread = header.blockChecksum ? 4 : 0
    const end = at + size
    if (end + unread > input.length) {
      throw new Lz4FrameError('LZ4 block runs past the input')
    }
    if (word >= 0x80000000) {
      total += size
      unread += size
    } else if (exact) {
      const decoded = yield* decodedLength(input, at, end, slice)
      if (decoded > header.max) throw new Lz4FrameError('LZ4 block decodes above its maximum')
      total += decoded
    } else {
      total += Math.min(header.max, size * 255)
      unread += size
    }
    for (let turns = skip(slice, unread); turns > 0; turns--) yield
    at = end + (header.blockChecksum ? 4 : 0)
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
  const slice = { work: header.first }
  if (header.independent && (await drive(measure(input, header, false, slice))) <= limit) return
  const total = await drive(measure(input, header, true, slice))
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
