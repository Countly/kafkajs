const { EventEmitter } = require('events')

/** Splits a byte stream into whole Kafka frames by their size prefix. */
class FrameSplitter {
  constructor() {
    this.pending = Buffer.alloc(0)
  }

  /**
   * @param {Buffer} chunk bytes as they arrived
   * @returns {Buffer[]} every frame completed by the chunk, size prefix included
   * @throws {Error} on a negative size prefix
   */
  push(chunk) {
    this.pending = this.pending.length ? Buffer.concat([this.pending, chunk]) : chunk
    const frames = []
    while (this.pending.length >= 4) {
      const size = this.pending.readInt32BE(0)
      if (size < 0) throw new Error(`HoldingSocket: a frame announced size ${size}`)
      if (this.pending.length < 4 + size) break
      frames.push(Buffer.from(this.pending.subarray(0, 4 + size)))
      this.pending = this.pending.subarray(4 + size)
    }
    return frames
  }
}

/** One request frame kept from the broker until the test forwards it. */
class RequestHold {
  /** @param {{ apiKey: number, match?: (frame: Buffer) => boolean }} select */
  constructor(select) {
    this.select = select
    this.socket = null
    this.frame = null
    this.forwarded = null
    this.arrival = new Promise((resolve) => {
      this.announce = resolve
    })
  }

  /** @returns {Promise<RequestHold>} this hold, once its frame was written and kept */
  arrived() {
    return this.arrival
  }

  /**
   * Writes the kept frame on the broker side, whatever the client side's state.
   * @returns {Promise<Buffer>} the broker's response frame to it, never delivered to the client
   * @throws {Error} before the frame arrived, or when it was already forwarded
   */
  forward() {
    if (!this.socket || this.forwarded) throw new Error('RequestHold: nothing to forward')
    this.forwarded = this.socket.forward(this.frame)
    return this.forwarded
  }
}

/** One response frame kept from the client. */
class ResponseHold {
  /** @param {{ apiKey: number }} select which request's response to keep */
  constructor(select) {
    this.select = select
    this.arrival = new Promise((resolve) => {
      this.announce = resolve
    })
  }

  /** @returns {Promise<Buffer>} the kept response frame, once the broker sent it */
  arrived() {
    return this.arrival
  }
}

/**
 * A socket between the client and the broker-side socket (`upstream`) that can keep one request
 * frame from the broker, report the connection closed to the client while the broker side stays
 * open, write the kept frame on the broker side later and report the broker's response to it. It
 * can also keep one response from the client. It has the surface `src/network/socket.js` and
 * `Connection` use. Its broker-side socket is `upstream`, never `inner`, so an observer that
 * unwraps `inner` sees this socket.
 */
class HoldingSocket extends EventEmitter {
  /**
   * @param {object} upstream the broker-side socket
   * @param {{ host: string, port: number }} endpoint what the client dialled
   * @param {{ requests: RequestHold[], responses: ResponseHold[] }} rules the factory's holds
   */
  constructor(upstream, { host, port }, rules) {
    super()
    this.upstream = upstream
    this.endpoint = { host, port }
    this.rules = rules
    this.clientClosed = false
    this.keeping = 0
    this.requests = new FrameSplitter()
    this.responses = new FrameSplitter()
    /** @type {Map<number, number>} api key of each written request by correlation id */
    this.written = new Map()
    /** @type {Map<number, (frame: Buffer) => void>} forwarded frames awaiting a response */
    this.awaiting = new Map()

    upstream.on('data', (chunk) => this.receive(chunk))
    for (const event of ['connect', 'end', 'error', 'close', 'timeout']) {
      upstream.on(event, (...args) => {
        if (!this.clientClosed) this.emit(event, ...args)
      })
    }
  }

  /** @returns {boolean} whether the client side is closed */
  get destroyed() {
    return this.clientClosed || this.upstream.destroyed
  }

  /**
   * Passes each request frame to the broker side, except one a hold picks.
   * @param {Buffer|string} data
   * @param {BufferEncoding} [encoding]
   * @returns {boolean} true
   */
  write(data, encoding) {
    if (this.clientClosed) return true
    for (const frame of this.requests.push(Buffer.from(data, encoding))) {
      const apiKey = frame.readInt16BE(4)
      this.written.set(frame.readInt32BE(8), apiKey)
      const hold = take(
        this.rules.requests,
        ({ select }) => select.apiKey === apiKey && (!select.match || select.match(frame))
      )
      if (!hold) {
        this.upstream.write(frame)
        continue
      }
      this.keeping += 1
      hold.socket = this
      hold.frame = frame
      hold.announce(hold)
    }
    return true
  }

  /**
   * Reports the connection closed to the client as a peer close (`end`, then `close`), keeping
   * the broker side open.
   * @returns {void}
   */
  closeClientSide() {
    if (this.clientClosed) return
    this.clientClosed = true
    queueMicrotask(() => {
      this.emit('end')
      this.emit('close', false)
    })
  }

  /**
   * The client's close. The broker side closes with it unless a kept frame still needs it.
   * @param {Error} [error]
   * @returns {this}
   */
  destroy(error) {
    if (this.clientClosed) return this
    this.clientClosed = true
    if (this.keeping === 0) this.upstream.destroy()
    queueMicrotask(() => {
      if (error) this.emit('error', error)
      this.emit('close', Boolean(error))
    })
    return this
  }

  /** @returns {this} */
  end() {
    if (this.keeping === 0) this.upstream.end()
    return this
  }

  /** @returns {this} */
  unref() {
    this.upstream.unref?.()
    return this
  }

  /** @returns {void} closes the broker side */
  closeUpstream() {
    this.upstream.destroy()
  }

  /**
   * @internal
   * @param {Buffer} frame a kept request frame
   * @returns {Promise<Buffer>} the broker's response frame to it
   */
  forward(frame) {
    const response = new Promise((resolve) => this.awaiting.set(frame.readInt32BE(8), resolve))
    this.upstream.write(frame)
    return response
  }

  /**
   * @private
   * @param {Buffer} chunk bytes from the broker side
   * @returns {void}
   */
  receive(chunk) {
    for (const frame of this.responses.push(chunk)) {
      const correlationId = frame.readInt32BE(4)
      const apiKey = this.written.get(correlationId)
      this.written.delete(correlationId)
      const reply = this.awaiting.get(correlationId)
      if (reply) {
        this.awaiting.delete(correlationId)
        this.keeping -= 1
        reply(frame)
        continue
      }
      const hold = take(this.rules.responses, ({ select }) => select.apiKey === apiKey)
      if (hold) hold.announce(frame)
      else if (!this.clientClosed) this.emit('data', frame)
    }
  }
}

/**
 * Removes and returns the first item that `picks`.
 * @template T
 * @param {T[]} items
 * @param {(item: T) => boolean} picks
 * @returns {T|undefined}
 */
const take = (items, picks) => {
  const index = items.findIndex(picks)
  return index === -1 ? undefined : items.splice(index, 1)[0]
}

/**
 * Wraps a socket factory so every socket it builds is a `HoldingSocket`. Place it below any
 * observer (an observer wraps this factory), so the observer sees what the client wrote and read.
 * @param {Function} inner the broker-side socket factory
 * @returns {Function & { sockets: HoldingSocket[], holdRequest: Function,
 *   withholdResponse: Function, closeUpstreams: Function }} the factory: `holdRequest({ apiKey,
 *   match? })` keeps the next matching request frame of any socket, `withholdResponse({ apiKey })`
 *   keeps the next response to a request of that key from the client, `closeUpstreams()` closes
 *   every broker-side socket
 */
const createHoldingSocketFactory = (inner) => {
  const rules = { requests: [], responses: [] }
  const factory = (options) => {
    const socket = new HoldingSocket(inner(options), options, rules)
    factory.sockets.push(socket)
    return socket
  }
  factory.sockets = []
  /**
   * @param {{ apiKey: number, match?: (frame: Buffer) => boolean }} select
   * @returns {RequestHold}
   */
  factory.holdRequest = (select) => {
    const hold = new RequestHold(select)
    rules.requests.push(hold)
    return hold
  }
  /**
   * @param {{ apiKey: number }} select
   * @returns {ResponseHold}
   */
  factory.withholdResponse = (select) => {
    const hold = new ResponseHold(select)
    rules.responses.push(hold)
    return hold
  }
  /** @returns {void} */
  factory.closeUpstreams = () => {
    for (const socket of factory.sockets) socket.closeUpstream()
  }
  return factory
}

module.exports = { createHoldingSocketFactory, HoldingSocket }
