const { constants } = require('buffer')
const HEADER = Buffer.from('82534e41505059000000000100000001', 'hex')
const BLOCK = 32768
const SLICE = 262144
const SLICE_OPS = 4096

/** A malformed stream or an output size refused before allocation. */
class SnappyDecodeError extends Error {
  /**
   * @param {string} message the invalid field or size
   * @param {string} [reason] decompression or decompressed-size
   */
  constructor(message, reason = 'decompression') {
    super(message)
    this.name = 'SnappyDecodeError'
    this.reason = reason
  }
}

/**
 * Reads the uint32 decoded size without allocating from it.
 * @param {Buffer} input raw block
 * @param {number} start first byte
 * @param {number} end exclusive block boundary
 * @returns {{ size: number, next: number }} length and first token position
 * @throws {SnappyDecodeError} on a truncated or overflowing varint
 */
const lengthOf = (input, start, end) => {
  let size = 0
  for (let i = 0; i < 5 && start < end; i += 1) {
    const byte = input[start++]
    if (i === 4 && byte > 15) break
    size += (byte & 127) * 2 ** (7 * i)
    if (byte < 128) return { size, next: start }
  }
  throw new SnappyDecodeError('Invalid decoded length')
}

/**
 * Yields one turn after a completed bounded slice.
 * @returns {Promise<void>}
 */
const yieldTurn = () => new Promise((resolve) => setImmediate(resolve))

/**
 * Work done since the last real event-loop turn. A turn is due once input bytes, output bytes or
 * block-plus-token operations reach their ceiling, so neither large copies nor many tiny blocks
 * and tokens can hold the loop.
 */
class Budget {
  /** Starts empty: input bytes read, output bytes written, and blocks plus tokens done. */
  constructor() {
    this.input = 0
    this.output = 0
    this.ops = 0
  }

  /** @returns {boolean} whether a real turn is due */
  get spent() {
    return this.input >= SLICE || this.output >= SLICE || this.ops >= SLICE_OPS
  }

  /** Starts a new slice after a real turn. */
  reset() {
    this.input = this.output = this.ops = 0
  }
}

/**
 * Runs resumable work, taking one real event-loop turn at each yielded slice boundary and then
 * starting a fresh budget. Only a boundary awaits, so tiny blocks and tokens cost no promise each.
 * @template T
 * @param {Generator<void, T>} slices work that yields once its budget is spent
 * @param {Budget} budget the budget the work spends
 * @returns {Promise<T>} the generator's return value
 */
const drive = async (slices, budget) => {
  let step = slices.next()
  while (!step.done) {
    await yieldTurn()
    budget.reset()
    step = slices.next()
  }
  return step.value
}

/**
 * Builds the refusal for a declared output over the caller's ceiling.
 * @returns {SnappyDecodeError} the decompressed-size error
 */
const tooLarge = () => new SnappyDecodeError('Snappy output exceeds limit', 'decompressed-size')

/**
 * Sums the declared output of every block before anything is allocated, refusing the stream as
 * soon as the sum passes the limit. No per-block state is kept; each block is one operation, so
 * empty blocks also reach a turn.
 * @param {Buffer} input raw Snappy or xerial stream
 * @param {boolean} framed whether the stream carries xerial framing
 * @param {number} limit output byte ceiling
 * @param {Budget} budget work since the last turn
 * @returns {Generator<void, number>} yields at turn boundaries and returns the total size
 * @throws {SnappyDecodeError} for malformed framing or a size over the limit
 */
function* declaredSize(input, framed, limit, budget) {
  if (!framed) {
    const { size } = lengthOf(input, 0, input.length)
    if (size > limit) throw tooLarge()
    return size
  }
  let total = 0
  for (let at = 16; at < input.length;) {
    if (at + 4 > input.length) throw new SnappyDecodeError('Truncated xerial block length')
    const length = input.readUInt32BE(at)
    const end = at + 4 + length
    if (length === 0 || end > input.length)
      throw new SnappyDecodeError('Invalid xerial block length')
    total += lengthOf(input, at + 4, end).size
    if (total > limit) throw tooLarge()
    budget.input += length + 4
    budget.ops += 1
    at = end
    if (budget.spent) yield
  }
  return total
}

/**
 * Decodes every raw block into its exact reserved output interval. A copy cannot refer to a
 * prior xerial block. Long literals are copied in slices and a copy token produces at most 64
 * bytes. Blocks and tokens are counted, so tiny ones also reach a turn.
 * @param {Buffer} input raw Snappy or xerial stream, already size-checked
 * @param {boolean} framed whether the stream carries xerial framing
 * @param {Buffer} output exact final output
 * @param {Budget} budget work since the last turn
 * @returns {Generator<void, void>} yields at turn boundaries
 * @throws {SnappyDecodeError} on an invalid token, block boundary or decoded length
 */
function* decodeBlocks(input, framed, output, budget) {
  let base = 0
  for (let at = framed ? 16 : 0; at < input.length;) {
    const end = framed ? at + 4 + input.readUInt32BE(at) : input.length
    const { size, next } = lengthOf(input, framed ? at + 4 : 0, end)
    budget.input += next - at
    budget.ops += 1
    if (budget.spent) yield
    let start = next
    let written = 0
    while (start < end) {
      const tokenStart = start
      const tag = input[start++]
      const kind = tag & 3
      let count
      budget.ops += 1
      if (kind === 0) {
        count = (tag >>> 2) + 1
        if (count > 60) {
          const bytes = count - 60
          if (start + bytes > end) throw new SnappyDecodeError('Truncated literal length')
          count = 1
          for (let i = 0; i < bytes; i += 1) count += input[start++] * 2 ** (8 * i)
        }
        if (start + count > end || written + count > size) {
          throw new SnappyDecodeError('Literal exceeds block')
        }
        budget.input += start - tokenStart
        while (count > 0) {
          if (budget.spent) yield
          const take = Math.min(count, SLICE - Math.max(budget.input, budget.output))
          input.copy(output, base + written, start, start + take)
          start += take
          written += take
          count -= take
          budget.input += take
          budget.output += take
        }
      } else {
        const bytes = kind === 1 ? 1 : kind === 2 ? 2 : 4
        if (start + bytes > end) throw new SnappyDecodeError('Truncated copy offset')
        count = kind === 1 ? 4 + ((tag >>> 2) & 7) : 1 + (tag >>> 2)
        let offset = kind === 1 ? (tag & 224) * 8 : 0
        for (let i = 0; i < bytes; i += 1) offset += input[start++] * 2 ** (8 * i)
        if (offset === 0 || offset > written || written + count > size) {
          throw new SnappyDecodeError('Copy exceeds block')
        }
        const destination = base + written
        if (offset === 1) output.fill(output[destination - 1], destination, destination + count)
        else if (offset >= count)
          output.copy(output, destination, destination - offset, destination - offset + count)
        else
          for (let i = 0; i < count; i += 1)
            output[destination + i] = output[destination + i - offset]
        written += count
        budget.input += start - tokenStart
        budget.output += count
      }
      if (budget.spent) yield
    }
    if (written !== size) throw new SnappyDecodeError('Decoded length mismatch')
    base += size
    at = end
  }
}

/**
 * Bounds this encoder, including the 16-byte xerial header. A match of m >= 4 emits
 * 3 * ceil(m / 64) bytes, saving at least one byte. Each non-final literal run is followed by a
 * match: charge its first header byte to that match saving. Its extra header bytes are zero
 * through 60 literal bytes, one through 256 and two thereafter, at most literalBytes / 32.
 * The final literal reserves one more byte. A block adds at most three raw-length bytes and
 * four framing bytes, hence <= n + 8 + floor(n / 32). Reserve 12 per block and sum; sums of
 * floors cannot exceed floor(total / 32). This proves these emission rules, independently of
 * MaxCompressedLength. Arithmetic division preserves the formula above the signed-int32 range.
 * @param {number} n input byte length
 * @returns {number} preallocation ceiling
 * @throws {RangeError} for an invalid or unallocatable size
 */
const encodedBound = (n) => {
  if (!Number.isSafeInteger(n) || n < 0) throw new RangeError('Invalid Snappy input length')
  const bound = 16 + n + 12 * Math.ceil(n / BLOCK) + Math.floor(n / 32)
  if (bound > constants.MAX_LENGTH) throw new RangeError('Snappy output exceeds Buffer limit')
  return bound
}

/**
 * Encodes one block with greedy four-byte hash matches and two-byte copy offsets.
 * @param {Buffer} input source block
 * @param {Buffer} output preallocated result
 * @param {number} at output cursor
 * @returns {number} end cursor
 */
const encodeBlock = (input, output, at) => {
  let n = input.length
  do {
    output[at++] = (n & 127) | (n >= 128 ? 128 : 0)
    n >>>= 7
  } while (n)
  const table = new Int32Array(16384).fill(-1)
  let literal = 0
  let cursor = 0
  /**
   * Emits the pending literal run, whose largest header is three bytes in a 32 KiB block.
   * @param {number} end exclusive literal end
   * @returns {void}
   */
  const emitLiteral = (end) => {
    const count = end - literal
    if (!count) return
    if (count <= 60) output[at++] = (count - 1) * 4
    else if (count <= 256) {
      output[at++] = 240
      output[at++] = count - 1
    } else {
      output[at++] = 244
      output.writeUInt16LE(count - 1, at)
      at += 2
    }
    input.copy(output, at, literal, end)
    at += count
  }
  while (cursor + 4 <= input.length) {
    const word = input.readUInt32LE(cursor)
    const hash = Math.imul(word, 0x1e35a7bd) >>> 18
    const previous = table[hash]
    table[hash] = cursor
    if (previous < 0 || input.readUInt32LE(previous) !== word) {
      cursor += 1
      continue
    }
    emitLiteral(cursor)
    let count = 4
    while (cursor + count < input.length && input[previous + count] === input[cursor + count])
      count += 1
    const offset = cursor - previous
    cursor += count
    literal = cursor
    while (count > 0) {
      const take = Math.min(count, 64)
      output[at++] = ((take - 1) << 2) | 2
      output.writeUInt16LE(offset, at)
      at += 2
      count -= take
    }
  }
  emitLiteral(input.length)
  return at
}

module.exports = {
  encodedBound,
  /**
   * Writes independent 32 KiB raw blocks inside xerial framing. Snappy has no level.
   * @param {{ buffer: Buffer }} encoder source bytes
   * @returns {Promise<Buffer>} xerial stream
   * @throws {RangeError} for an unallocatable output
   */
  async compress(encoder) {
    const input = encoder.buffer
    const output = Buffer.allocUnsafe(encodedBound(input.length))
    HEADER.copy(output)
    let at = HEADER.length
    let work = 0
    for (let start = 0; start < input.length; start += BLOCK) {
      const block = input.subarray(start, start + BLOCK)
      const end = encodeBlock(block, output, at + 4)
      output.writeUInt32BE(end - at - 4, at)
      work += Math.max(block.length, end - at)
      at = end
      if (work >= SLICE) {
        await yieldTurn()
        work = 0
      }
    }
    return output.subarray(0, at)
  },
  /**
   * Checks the entire xerial declared-length sum before allocating output. The pre-scan retains
   * no per-block objects. Each raw block is validated within its own boundaries. Both passes
   * give the event loop a turn whenever their byte or block-and-token budget is spent.
   * @param {Buffer} input raw Snappy or xerial stream
   * @param {{ maxOutputLength?: number }} [options] output byte ceiling
   * @returns {Promise<Buffer>} exactly the declared output
   * @throws {SnappyDecodeError} for malformed bytes or a refused size
   */
  async decompress(input, { maxOutputLength = 2147483647 } = {}) {
    const framed = input.length >= 16 && input.subarray(0, 8).equals(HEADER.subarray(0, 8))
    const limit = Math.min(maxOutputLength, constants.MAX_LENGTH)
    if (!Number.isSafeInteger(limit) || limit < 0)
      throw new SnappyDecodeError('Invalid output limit')
    if (framed && (input.readUInt32BE(8) < 1 || input.readUInt32BE(12) > 1)) {
      throw new SnappyDecodeError('Unsupported xerial version')
    }
    const budget = new Budget()
    const total = await drive(declaredSize(input, framed, limit, budget), budget)
    const output = Buffer.allocUnsafe(total)
    await drive(decodeBlocks(input, framed, output, budget), budget)
    return output
  },
}
