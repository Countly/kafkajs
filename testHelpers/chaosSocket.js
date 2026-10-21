const { EventEmitter } = require('events')
const { mulberry32 } = require('./schedule')

/** The size prefix of every Kafka frame: a signed 32-bit length of the bytes that follow it. */
const SIZE_BYTES = 4
/** A request frame's leading bytes up to its correlation id: size, api key, api version, id. */
const REQUEST_HEADER_BYTES = 12
/** A response frame's leading bytes up to its correlation id: size, then the id. */
const RESPONSE_HEADER_BYTES = 8

/**
 * Joins neighbouring pieces that are adjacent views of one buffer, so a frame that arrived in one
 * chunk and passes leaves in one piece.
 * @param {Buffer[]} pieces
 * @returns {Buffer[]}
 */
const coalesce = (pieces) =>
  pieces.reduce((joined, piece) => {
    const last = joined[joined.length - 1]
    if (
      last &&
      last.buffer === piece.buffer &&
      last.byteOffset + last.length === piece.byteOffset
    ) {
      joined[joined.length - 1] = Buffer.from(
        last.buffer,
        last.byteOffset,
        last.length + piece.length
      )
    } else if (piece.length) {
      joined.push(piece)
    }
    return joined
  }, [])

/**
 * Follows the size prefix of a stream of Kafka frames across any chunk boundaries and decides once
 * per frame whether its bytes pass. The decision is taken when the frame's first `headerBytes`
 * bytes are known, or at the frame's end when the frame is shorter; until then those bytes are
 * held. Bytes that pass keep their order and content. A size below zero cannot be followed, so the
 * rest of the stream passes untouched.
 */
class FrameFilter {
  /**
   * @param {object} options
   * @param {number} options.headerBytes how many leading bytes of a frame `decide` needs
   * @param {(header: Buffer) => boolean} options.decide true to drop the frame
   */
  constructor({ headerBytes, decide }) {
    this.headerBytes = headerBytes
    this.decide = decide
    this.header = Buffer.alloc(0)
    this.remaining = 0
    this.dropping = false
    this.untracked = false
  }

  /**
   * @param {Buffer} chunk bytes as the stream delivered them
   * @returns {Buffer[]} the bytes that pass, in order
   */
  push(chunk) {
    if (this.untracked) return [chunk]
    const passed = []
    let offset = 0
    while (offset < chunk.length) {
      if (this.remaining > 0) {
        const length = Math.min(this.remaining, chunk.length - offset)
        if (!this.dropping) passed.push(chunk.subarray(offset, offset + length))
        this.remaining -= length
        offset += length
        continue
      }

      const take = chunk.subarray(offset, offset + this.wanted(chunk, offset))
      this.header = this.header.length ? Buffer.concat([this.header, take]) : take
      offset += take.length
      if (this.header.length < SIZE_BYTES) continue
      if (this.header.readInt32BE(0) < 0) {
        this.untracked = true
        passed.push(this.header, chunk.subarray(offset))
        return coalesce(passed)
      }
      if (this.header.length < this.decisionBytes()) continue

      this.dropping = this.decide(this.header)
      if (!this.dropping) passed.push(this.header)
      this.remaining = SIZE_BYTES + this.header.readInt32BE(0) - this.header.length
      this.header = Buffer.alloc(0)
    }
    return coalesce(passed)
  }

  /**
   * How many bytes from `offset` to add to the held header: up to the decision point, read in one
   * piece when the size prefix is already in the chunk.
   * @param {Buffer} chunk
   * @param {number} offset
   * @returns {number}
   */
  wanted(chunk, offset) {
    if (this.header.length >= SIZE_BYTES) return this.decisionBytes() - this.header.length
    if (this.header.length === 0 && chunk.length - offset >= SIZE_BYTES) {
      const size = chunk.readInt32BE(offset)
      return size < 0 ? SIZE_BYTES : Math.min(this.headerBytes, SIZE_BYTES + size)
    }
    return SIZE_BYTES - this.header.length
  }

  /** @returns {number} the bytes of the current frame to hold before deciding */
  decisionBytes() {
    return Math.min(this.headerBytes, SIZE_BYTES + this.header.readInt32BE(0))
  }
}

/**
 * Delivers items in the order they were pushed, each no earlier than its own delay after its push
 * and never before the items ahead of it. One timer at most; `clear` drops what is still queued.
 */
class DelayLine {
  /** @param {(item: *) => void} deliver */
  constructor(deliver) {
    this.deliver = deliver
    this.queue = []
    this.timer = null
  }

  /**
   * @param {*} item
   * @param {number} delayMs
   * @returns {void}
   */
  push(item, delayMs) {
    if (delayMs <= 0 && this.queue.length === 0) {
      this.deliver(item)
      return
    }
    this.queue.push({ item, due: Date.now() + delayMs })
    if (!this.timer) this.arm()
  }

  /** @returns {void} */
  arm() {
    this.timer = setTimeout(
      () => {
        this.timer = null
        while (this.queue.length && this.queue[0].due <= Date.now()) {
          this.deliver(this.queue.shift().item)
        }
        if (this.queue.length) this.arm()
      },
      Math.max(0, this.queue[0].due - Date.now())
    )
  }

  /** @returns {void} */
  clear() {
    clearTimeout(this.timer)
    this.timer = null
    this.queue = []
  }
}

/**
 * A socket that sits between the product and a real one and injects faults at the application
 * level: what the product writes and what it reads can be delayed, swallowed, cut or dropped frame
 * by frame, and the connection can be killed. It has the surface `src/network/socket.js` and
 * `Connection` use (`on`, `write`, `end`, `destroy`, `destroyed`, `removeAllListeners`, `unref`).
 *
 * It is not a proxy and proves nothing about the kernel: no RST, FIN, keepalive or real network
 * partition is produced, only the client's reaction to bytes that do or do not arrive.
 */
class ChaosSocket extends EventEmitter {
  /**
   * @param {import('net').Socket} inner the real socket
   * @param {{ host: string, port: number }} endpoint what the product dialled
   * @param {() => number} random a seeded generator in [0, 1)
   */
  constructor(inner, { host, port }, random) {
    super()
    this.inner = inner
    this.endpoint = { host, port }
    this.random = random
    this.blackholed = false
    this.closed = false
    this.readDelay = 0
    this.writeDelay = 0
    this.truncateAt = null
    this.killTimer = null
    this.requestRule = null
    this.responseRule = null
    this.pending = new Map()
    this.requests = new FrameFilter({
      headerBytes: REQUEST_HEADER_BYTES,
      decide: (header) => this.dropRequest(header),
    })
    this.responses = new FrameFilter({
      headerBytes: RESPONSE_HEADER_BYTES,
      decide: (header) => this.dropResponse(header),
    })
    this.writes = new DelayLine((chunk) => this.inner.write(chunk))
    this.reads = new DelayLine((item) => (item === null ? this.close() : this.emit('data', item)))

    inner.on('data', (chunk) => this.receive(chunk))
    for (const event of ['connect', 'end', 'error', 'close', 'timeout']) {
      inner.on(event, (...args) => this.emit(event, ...args))
    }
  }

  /** @returns {boolean} whether the real socket is destroyed */
  get destroyed() {
    return this.inner.destroyed
  }

  /**
   * Writes what the armed modes let through: a dropped request frame and everything written while
   * blackholed never reach the real socket.
   * @param {Buffer|string} data
   * @param {BufferEncoding} [encoding]
   * @returns {boolean} true, as an unbounded buffer would
   */
  write(data, encoding) {
    if (this.blackholed) return true
    for (const chunk of this.requests.push(Buffer.from(data, encoding))) {
      this.writes.push(chunk, this.writeDelay)
    }
    return true
  }

  /** @param {...*} args forwarded to the real socket @returns {this} */
  end(...args) {
    this.inner.end(...args)
    return this
  }

  /**
   * Destroys the real socket and drops every queued delivery and timer.
   * @param {Error} [error]
   * @returns {this}
   */
  destroy(error) {
    this.stop()
    this.inner.destroy(error)
    return this
  }

  /** @returns {this} */
  unref() {
    this.inner.unref()
    return this
  }

  /** @param {...*} args forwarded to the real socket @returns {this} */
  setKeepAlive(...args) {
    this.inner.setKeepAlive(...args)
    return this
  }

  /**
   * Destroys the real socket with an `ECONNRESET` error once its lifetime ends.
   * @param {number|[number, number]} lifetime ms, or a range a seeded draw picks from
   * @returns {void}
   */
  killAfter(lifetime) {
    const [min, max] = Array.isArray(lifetime) ? lifetime : [lifetime, lifetime]
    clearTimeout(this.killTimer)
    this.killTimer = setTimeout(
      () => this.destroy(Object.assign(new Error('Connection killed'), { code: 'ECONNRESET' })),
      min + Math.floor(this.random() * (max - min + 1))
    )
  }

  /**
   * Swallows both directions from now on, including deliveries still queued, without closing.
   * @returns {void}
   */
  blackhole() {
    this.blackholed = true
    this.writes.clear()
    this.reads.clear()
  }

  /**
   * Delays each later chunk, per direction, keeping the order.
   * @param {{ read?: number, write?: number }} delays ms
   * @returns {void}
   */
  delay({ read = 0, write = 0 }) {
    this.readDelay = read
    this.writeDelay = write
  }

  /**
   * Drops the next `count` complete response frames, or only those answering a request of
   * `apiKey` written on this socket; the bytes around them arrive intact.
   * @param {number} count
   * @param {{ apiKey?: number }} [select]
   * @returns {void}
   */
  dropResponses(count, { apiKey } = {}) {
    this.responseRule = { count, apiKey }
  }

  /**
   * Drops the next `count` request frames of `apiKey`, read from the written request header, so
   * those requests never leave the process; every other frame is written intact.
   * @param {{ apiKey: number, count?: number }} rule
   * @returns {void}
   */
  dropRequests({ apiKey, count = 1 }) {
    this.requestRule = { count, apiKey }
  }

  /**
   * Delivers only the next `bytes` bytes the product would read, then closes as a peer would.
   * @param {number} bytes
   * @returns {void}
   */
  truncateAndClose(bytes) {
    this.truncateAt = bytes
  }

  /**
   * @param {Buffer} chunk what the real socket delivered
   * @returns {void}
   */
  receive(chunk) {
    if (this.blackholed || this.closed) return
    for (let data of this.responses.push(chunk)) {
      if (this.truncateAt !== null) {
        data = data.subarray(0, this.truncateAt)
        this.truncateAt -= data.length
      }
      if (data.length) this.reads.push(data, this.readDelay)
      if (this.truncateAt === 0) {
        this.closed = true
        this.reads.push(null, this.readDelay)
        return
      }
    }
  }

  /**
   * @param {Buffer} header a request frame's first bytes
   * @returns {boolean} whether to drop it
   */
  dropRequest(header) {
    if (header.length < REQUEST_HEADER_BYTES) return false
    const apiKey = header.readInt16BE(4)
    this.pending.set(header.readInt32BE(8), apiKey)
    const rule = this.requestRule
    if (!rule || rule.count === 0 || rule.apiKey !== apiKey) return false
    rule.count -= 1
    return true
  }

  /**
   * @param {Buffer} header a response frame's first bytes
   * @returns {boolean} whether to drop it
   */
  dropResponse(header) {
    const correlationId = header.length >= RESPONSE_HEADER_BYTES ? header.readInt32BE(4) : null
    const apiKey = this.pending.get(correlationId)
    this.pending.delete(correlationId)
    const rule = this.responseRule
    if (!rule || rule.count === 0) return false
    if (rule.apiKey !== undefined && rule.apiKey !== apiKey) return false
    rule.count -= 1
    return true
  }

  /** The end of a truncation: the peer's `end`, then the real socket is destroyed. @returns {void} */
  close() {
    this.emit('end')
    this.destroy()
  }

  /** @returns {void} */
  stop() {
    clearTimeout(this.killTimer)
    this.killTimer = null
    this.writes.clear()
    this.reads.clear()
  }
}

/**
 * Wraps a `socketFactory` so every socket it creates is a `ChaosSocket` the test can arm.
 * @param {Function} inner the factory to wrap, e.g. `require('src/network/socketFactory')()`
 * @param {object} [options] applied to every socket the factory creates
 * @param {number|[number, number]} [options.killAfter] each socket's lifetime, see `killAfter`
 * @param {number} [options.seed=1] the seed of the lifetime draws, so a run can be repeated
 * @returns {Function & { sockets: ChaosSocket[], arm: Function }} the factory; `sockets` lists
 *   every socket it created, oldest first, and `arm` arms an endpoint's sockets
 */
const createChaosSocketFactory = (inner, { killAfter, seed = 1 } = {}) => {
  const random = mulberry32(seed)
  const armings = new Set()
  const matches = ({ endpoint }, { host, port }) =>
    (host === undefined || endpoint.host === host) && (port === undefined || endpoint.port === port)
  const factory = (options) => {
    const socket = new ChaosSocket(inner(options), options, random)
    if (killAfter !== undefined) socket.killAfter(killAfter)
    for (const { select, apply } of armings) if (matches(socket, select)) apply(socket)
    factory.sockets.push(socket)
    return socket
  }
  factory.sockets = []
  /**
   * Applies `apply` to every open socket to the selected endpoint and to each one created later,
   * so a mode outlives the reconnect a fault causes.
   * @param {{ host?: string, port?: number }} select
   * @param {(socket: ChaosSocket) => void} apply
   * @returns {() => void} stops applying it to sockets created later
   */
  factory.arm = (select, apply) => {
    const arming = { select, apply }
    factory.sockets.filter((socket) => !socket.destroyed && matches(socket, select)).forEach(apply)
    armings.add(arming)
    return () => {
      armings.delete(arming)
    }
  }
  return factory
}

module.exports = { createChaosSocketFactory, ChaosSocket, FrameFilter }
