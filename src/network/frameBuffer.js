const { KafkaJSInvalidFrameError } = require('../errors')

/**
 * The first allocation of the buffer a frame spanning several chunks is assembled in, in bytes.
 */
const INITIAL_CAPACITY = 64 * 1024

const LENGTH_PREFIX_SIZE = 4

/** The four size bytes of an HTTP response (`HTTP`) read as a signed int32. */
const HTTP_AS_SIZE = 0x48545450

/**
 * @param {Buffer} prefix the four size bytes of a frame
 * @returns {string} what the bytes look like when they are not a size, or an empty string
 */
const looksLike = (prefix) => {
  if (prefix.readInt32BE(0) === HTTP_AS_SIZE) {
    return ' The four size bytes read "HTTP": is the broker port correct?'
  }

  if (prefix[0] === 21 && prefix[1] === 3) {
    return ' The four size bytes look like a TLS alert: is this a plaintext connection to a TLS endpoint?'
  }

  return ''
}

/**
 * Splits a byte stream into Kafka response frames and validates each one before it waits for or
 * allocates its body. One per connection; `reset()` on every connect and every close.
 *
 * A frame is a 4-byte signed length, then that many bytes whose first four are the correlation id
 * of the request it answers. A frame that arrives whole inside one chunk is delivered as a
 * `subarray` of it, with no copy. A frame that spans chunks is assembled in one buffer that starts
 * at `min(size, 64 KiB)` (or the first chunk, when larger) and doubles up to `size`, so its
 * capacity never exceeds `size` or `max(64 KiB, 2 × bytes received)`, and the bytes copied in total
 * stay below three times `size`. A peer that announces 100 MiB and sends one byte holds 64 KiB.
 */
class FrameBuffer {
  /**
   * @param {object} options
   * @param {number} options.maxBodySize the largest body accepted, the 4-byte length prefix
   *   excluded; at least 4
   * @param {(body: Buffer) => void} options.onFrame called once per complete frame, in order, with
   *   the body, whose first four bytes are the correlation id. The body belongs to the callee.
   * @param {(correlationId: number) => number} [options.minBodyOf] the smallest body the request
   *   the correlation id names can have: 4, or 5 when its response header is flexible. Answers 4
   *   for an unknown id, whose frame is delivered all the same.
   * @param {string} [options.broker] `host:port`, named in the errors
   */
  constructor({ maxBodySize, onFrame, minBodyOf = () => 4, broker }) {
    this.maxBodySize = maxBodySize
    this.onFrame = onFrame
    this.minBodyOf = minBodyOf
    this.broker = broker
    this.prefix = Buffer.allocUnsafe(LENGTH_PREFIX_SIZE)
    this.copied = 0
    this.reset()
  }

  /**
   * Drops every buffered byte and the count of bytes copied.
   * @returns {void}
   */
  reset() {
    this.prefixLength = 0
    this.size = -1
    this.body = null
    this.received = 0
    this.headerChecked = false
    this.copied = 0
  }

  /**
   * What the buffer holds and has moved, for diagnostics: the bytes of the frame in progress, the
   * capacity of the buffer it is assembled in, and the bytes copied since the last reset.
   * @returns {{ received: number, capacity: number, copied: number }}
   */
  get usage() {
    return {
      received: this.received,
      capacity: this.body === null ? 0 : this.body.length,
      copied: this.copied,
    }
  }

  /**
   * Delivers through `onFrame`, in order, every complete valid frame this chunk finishes, and
   * stops at the first invalid one.
   * @param {Buffer} chunk
   * @returns {void}
   * @throws {KafkaJSInvalidFrameError} at the first invalid frame, after the valid frames before it
   *   in the chunk were delivered; the buffer is cleared first, and nothing after the bad frame is
   *   delivered
   * @throws {Error} whatever `onFrame` throws, after the buffer is cleared
   */
  push(chunk) {
    let offset = 0
    while (offset < chunk.length) {
      if (this.size === -1) {
        offset = this.takePrefix(chunk, offset)
      } else if (this.received === 0 && chunk.length - offset >= this.size) {
        offset = this.deliverWhole(chunk, offset)
      } else {
        offset = this.accumulate(chunk, offset)
      }
    }
  }

  /**
   * @private
   * @param {Buffer} chunk
   * @param {number} offset
   * @returns {number} the offset after the prefix bytes taken
   * @throws {KafkaJSInvalidFrameError} when the size is negative, below a correlation id or above
   *   the limit
   */
  takePrefix(chunk, offset) {
    const taken = Math.min(LENGTH_PREFIX_SIZE - this.prefixLength, chunk.length - offset)
    chunk.copy(this.prefix, this.prefixLength, offset, offset + taken)
    this.prefixLength += taken

    if (this.prefixLength === LENGTH_PREFIX_SIZE) {
      this.beginFrame(this.prefix.readInt32BE(0))
    }

    return offset + taken
  }

  /**
   * @private
   * @param {number} size the size the prefix declared
   * @throws {KafkaJSInvalidFrameError}
   */
  beginFrame(size) {
    if (size < 0) {
      this.refuse(`A response frame declared a negative size (${size}).`, size, 'negative')
    }

    if (size < 4) {
      this.refuse(
        `A response frame declared ${size} bytes, too few for a correlation id.`,
        size,
        'too-short'
      )
    }

    if (size > this.maxBodySize) {
      this.refuse(
        `A response frame declared ${size} bytes, above maxResponseSize (${this.maxBodySize}).${looksLike(this.prefix)}`,
        size,
        'too-large'
      )
    }

    this.size = size
    this.received = 0
    this.prefixLength = 0
    this.headerChecked = false
  }

  /**
   * Delivers a frame that sits whole in the chunk, as a view of it.
   * @private
   * @param {Buffer} chunk
   * @param {number} offset where the body starts
   * @returns {number} the offset after the frame
   */
  deliverWhole(chunk, offset) {
    const body = chunk.subarray(offset, offset + this.size)
    this.checkHeader(body.readInt32BE(0))
    this.endFrame()
    this.deliver(body)

    return offset + body.length
  }

  /**
   * Copies what the chunk holds of the frame in progress into its buffer, and delivers the frame
   * when that completes it.
   * @private
   * @param {Buffer} chunk
   * @param {number} offset
   * @returns {number} the offset after the bytes taken
   */
  accumulate(chunk, offset) {
    const taken = Math.min(this.size - this.received, chunk.length - offset)
    this.grow(this.received + taken, taken)
    chunk.copy(this.body, this.received, offset, offset + taken)
    this.received += taken
    this.copied += taken

    if (!this.headerChecked && this.received >= 4) {
      this.checkHeader(this.body.readInt32BE(0))
    }

    if (this.received === this.size) {
      const body = this.body
      this.endFrame()
      this.deliver(body)
    }

    return offset + taken
  }

  /**
   * Makes the buffer hold `needed` bytes: allocated on first use, and replaced by one twice as
   * large (up to `size`) when it is too small.
   * @private
   * @param {number} needed
   * @param {number} first the bytes of the chunk, which size the first allocation when large
   */
  grow(needed, first) {
    if (this.body === null) {
      this.body = Buffer.allocUnsafe(Math.min(this.size, Math.max(INITIAL_CAPACITY, first)))
    } else if (needed > this.body.length) {
      const larger = Buffer.allocUnsafe(Math.min(this.size, Math.max(this.body.length * 2, needed)))
      this.body.copy(larger, 0, 0, this.received)
      this.copied += this.received
      this.body = larger
    }
  }

  /**
   * @private
   * @param {number} correlationId read from the body
   * @throws {KafkaJSInvalidFrameError} when the body is smaller than its request's header needs
   */
  checkHeader(correlationId) {
    this.headerChecked = true
    const minimum = this.minBodyOf(correlationId)
    if (this.size < minimum) {
      this.refuse(
        `A response frame declared ${this.size} bytes, too few for the response header of correlation id ${correlationId} (${minimum}).`,
        this.size,
        'too-short'
      )
    }
  }

  /**
   * Ends the frame in progress: forgets its size, body and prefix bytes, and keeps the cumulative
   * copy counter, which only `reset()` clears.
   * @private
   * @returns {void}
   */
  endFrame() {
    this.size = -1
    this.body = null
    this.received = 0
    this.prefixLength = 0
    this.headerChecked = false
  }

  /**
   * @private
   * @param {Buffer} body
   * @throws whatever `onFrame` throws, after the buffer is cleared
   */
  deliver(body) {
    try {
      this.onFrame(body)
    } catch (error) {
      this.reset()
      throw error
    }
  }

  /**
   * @private
   * @param {string} message
   * @param {number} size
   * @param {'negative'|'too-short'|'too-large'} reason
   * @throws {KafkaJSInvalidFrameError} always, after the buffer is cleared
   */
  refuse(message, size, reason) {
    this.reset()
    throw new KafkaJSInvalidFrameError(message, { broker: this.broker, size, reason })
  }
}

module.exports = FrameBuffer
