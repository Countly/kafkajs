const Broker = require('../../../broker')
const Encoder = require('../../../protocol/encoder')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { requests } = require('../../../protocol/requests')
const ConnectionPool = require('../../../network/connectionPool')
const defaultSocketFactory = require('../../../network/socketFactory')
const crc32C = require('../../../protocol/recordBatch/crc32C')
const { createLogger, LEVELS } = require('../../../loggers')
const LoggerConsole = require('../../../loggers/console')
const { createChaosSocketFactory } = require('../../../../testHelpers/chaosSocket')

/** Where a record batch keeps its magic byte, relative to the batch's first byte. */
const MAGIC_AT = 16
/** The bytes before a v2 batch's records. */
const BATCH_HEADER_BYTES = 61
/** The smallest `batchLength` of a v2 batch: everything after the length field, no records. */
const MIN_BATCH_LENGTH = 49
/** The compression codec of a batch, by the low three bits of its attributes. */
const CODEC_NAMES = Object.freeze(['none', 'gzip', 'snappy', 'lz4', 'zstd'])

/**
 * One request frame the client wrote.
 * @typedef {object} Frame
 * @property {string} endpoint `host:port` the socket was opened to
 * @property {number} connection the order the socket was created in, which tells a reconnect apart
 * @property {number} apiKey
 * @property {number} apiVersion
 * @property {number} correlationId
 * @property {number} bytes the whole frame, length prefix included
 * @property {number} at `performance.now()` when it was written
 * @property {Buffer} [buffer] the frame itself, kept only for the api keys a log retains
 */

/**
 * Records every request frame the sockets of a client write, passing each through untouched: the
 * connection writes one frame per `write`, so this is a pass-through observer of the wire and not
 * a second opinion of what the client believes it sent.
 */
class FrameLog {
  /**
   * @param {object} [options]
   * @param {number[]} [options.retain] api keys whose frame bytes are kept as `Frame#buffer`
   */
  constructor({ retain = [] } = {}) {
    /** @type {Frame[]} */
    this.frames = []
    this.connections = 0
    this.retain = new Set(retain)
  }

  /**
   * @param {Function} inner the socket factory to wrap, e.g. a chaos factory
   * @returns {Function} a socket factory whose sockets report every written frame to this log
   */
  observe(inner) {
    return (options) => {
      const socket = inner(options)
      const write = socket.write.bind(socket)
      const endpoint = `${options.host}:${options.port}`
      const connection = (this.connections += 1)
      socket.write = (data, ...rest) => {
        this.record(endpoint, Buffer.isBuffer(data) ? data : Buffer.from(data, rest[0]), connection)
        return write(data, ...rest)
      }
      return socket
    }
  }

  /**
   * @param {string} endpoint
   * @param {Buffer} data one written frame
   * @param {number} [connection=0] the socket that wrote it
   * @returns {void}
   */
  record(endpoint, data, connection = 0) {
    const known = data.length >= 12
    const frame = {
      endpoint,
      connection,
      apiKey: known ? data.readInt16BE(4) : -1,
      apiVersion: known ? data.readInt16BE(6) : -1,
      correlationId: known ? data.readInt32BE(8) : -1,
      bytes: data.length,
      at: performance.now(),
    }
    if (this.retain.has(frame.apiKey)) frame.buffer = data
    this.frames.push(frame)
  }

  /**
   * @returns {number} a position to hand to `since`
   */
  mark() {
    return this.frames.length
  }

  /**
   * @param {number} apiKey
   * @param {number} [from=0] a position `mark` returned
   * @returns {Frame[]} the frames of that api written since, oldest first
   */
  since(apiKey, from = 0) {
    return this.frames.slice(from).filter((frame) => frame.apiKey === apiKey)
  }

  /**
   * @param {number} [limit=300] how many of the latest frames
   * @returns {Array<Omit<Frame, 'buffer'>>} the frames' facts, for a scenario's evidence
   */
  trace(limit = 300) {
    return this.frames.slice(-limit).map(({ buffer, ...facts }) => facts)
  }

  /**
   * Drops the retained frame bytes, keeping the frame facts.
   * @returns {void}
   */
  release() {
    for (const frame of this.frames) delete frame.buffer
  }
}

/**
 * Suppresses one answer of one api on the connections to one broker, once for the whole run: the
 * request still reaches the broker, so the broker applies it, and the client never reads the
 * answer. Every armed socket shares one rule, consumed at the socket's own response decision, so
 * the first matching answer on any of them is the only one dropped, however many connections are
 * open. Sockets opened after that are not armed, so a reconnect does not drop a second answer.
 */
class ResponseDrop {
  /**
   * @param {{ arm: Function }} chaos the chaos socket factory the client dials through
   * @param {object} target
   * @param {number} target.port the broker's port
   * @param {number} target.apiKey the api whose next answer is dropped
   */
  constructor(chaos, { port, apiKey }) {
    this.chaos = chaos
    this.port = port
    this.apiKey = apiKey
    /** The one rule every armed socket holds; its count is the drops still available. */
    this.rule = { count: 1, apiKey }
    /** @type {Map<object, Function|null>} each armed socket, with its own `dropResponse` if it had one */
    this.armed = new Map()
    /** @type {object|null} the socket whose answer was dropped */
    this.consumer = null
    /** How many answers were dropped so far. */
    this.dropped = 0
    this.disarm = null
  }

  /**
   * Arms the open sockets to the broker and every socket opened to it until the drop happened.
   * @returns {this}
   */
  arm() {
    this.disarm = this.chaos.arm({}, (socket) => {
      if (Number(socket.endpoint.port) === this.port && this.consumer === null) this.attach(socket)
    })
    return this
  }

  /**
   * Gives a socket the shared rule and watches its response decisions, calling the socket's own
   * decision with its own `this` so the api filter and its bookkeeping stay as they are.
   * @param {object} socket a chaos socket
   * @returns {void}
   */
  attach(socket) {
    const own = Object.hasOwn(socket, 'dropResponse') ? socket.dropResponse : null
    const decide = socket.dropResponse
    socket.responseRule = this.rule
    socket.dropResponse = (header) => {
      const dropped = decide.call(socket, header)
      if (dropped) {
        this.dropped += 1
        this.consumer ??= socket
      }
      return dropped
    }
    this.armed.set(socket, own)
  }

  /**
   * Disarms the drop: sockets opened later are not armed, and the rule an armed socket still holds
   * unused is spent, so none of them can drop an answer.
   * @returns {void}
   */
  stop() {
    this.disarm?.()
    this.disarm = null
    this.rule.count = 0
    for (const socket of this.armed.keys()) {
      if (socket.responseRule === this.rule) socket.responseRule = null
    }
  }

  /**
   * Stops, then resets the connection whose answer was dropped with an `ECONNRESET` error, as a
   * lost connection surfaces: the client listens for `error` and `end`, not `close`, so it fails
   * what is in flight on that connection, retires what the request held and reconnects on its
   * next send. Every socket it armed gets its own decision back, whatever happened.
   * @returns {void}
   */
  release() {
    this.stop()
    try {
      this.consumer?.destroy(
        Object.assign(new Error('Connection reset after a dropped answer'), { code: 'ECONNRESET' })
      )
    } finally {
      for (const [socket, own] of this.armed) {
        if (own === null) delete socket.dropResponse
        else socket.dropResponse = own
      }
      this.armed.clear()
    }
  }
}

/**
 * A record batch found in a stream of bytes.
 * @typedef {object} ScannedBatch
 * @property {Buffer} bytes the whole batch, as the wire carried it
 * @property {Buffer} body the records section: compressed when a codec is set
 * @property {string} baseOffset decimal
 * @property {number} attributes
 * @property {number} codec the low three bits of `attributes`
 * @property {string} codecName one of `CODEC_NAMES`
 * @property {boolean} logAppendTime whether the batch carries LogAppendTime
 * @property {boolean} transactional
 * @property {boolean} control
 * @property {number} lastOffsetDelta
 * @property {string} firstTimestamp decimal
 * @property {string} producerId decimal
 * @property {number} producerEpoch
 * @property {number} baseSequence
 * @property {number} count the batch's record count
 */

/**
 * Finds the magic 2 record batches in bytes read off the wire, whatever surrounds them: a Produce
 * request, a Fetch response, or a concatenation of either. A candidate counts only when its length
 * fits and its CRC32C matches, so a byte pattern inside a record's text or a compressed body is
 * never taken for a batch.
 * @param {Buffer} buffer
 * @returns {ScannedBatch[]} in the order found
 */
const scanBatches = (buffer) => {
  const found = []
  for (let at = 0; at + BATCH_HEADER_BYTES <= buffer.length; at += 1) {
    if (buffer[at + MAGIC_AT] !== 2) continue
    const length = buffer.readInt32BE(at + 8)
    const end = at + 12 + length
    if (length < MIN_BATCH_LENGTH || end > buffer.length) continue
    if (crc32C(buffer.subarray(at + 21, end)) !== buffer.readUInt32BE(at + 17)) continue
    const attributes = buffer.readInt16BE(at + 21)
    found.push({
      bytes: buffer.subarray(at, end),
      body: buffer.subarray(at + BATCH_HEADER_BYTES, end),
      baseOffset: buffer.readBigInt64BE(at).toString(),
      attributes,
      codec: attributes & 7,
      codecName: CODEC_NAMES[attributes & 7],
      logAppendTime: (attributes & 8) !== 0,
      transactional: (attributes & 16) !== 0,
      control: (attributes & 32) !== 0,
      lastOffsetDelta: buffer.readInt32BE(at + 23),
      firstTimestamp: buffer.readBigInt64BE(at + 27).toString(),
      producerId: buffer.readBigInt64BE(at + 43).toString(),
      producerEpoch: buffer.readInt16BE(at + 51),
      baseSequence: buffer.readInt32BE(at + 53),
      count: buffer.readInt32BE(at + 57),
    })
    at = end - 1
  }
  return found
}

/**
 * Collects the bytes the connections of one reader received, so the record batches the broker stored
 * can be read as the broker framed them: the decoded records of `readLog` carry no compression
 * attributes. `open` is the `readLog` option that connects its brokers; `socketFactory` is for a
 * client under test.
 */
class WireTap {
  /** Starts with no socket; each socket made by `socketFactory` or `open` is recorded from then on. */
  constructor() {
    /** @type {Map<object, Buffer[]>} the chunks each socket received */
    this.sockets = new Map()
    const inner = defaultSocketFactory()
    /** The socket factory whose sockets report what they receive, for a client under test. */
    this.socketFactory = (options) => {
      const socket = inner(options)
      const chunks = []
      this.sockets.set(socket, chunks)
      socket.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
      return socket
    }
    this.open = this.open.bind(this)
  }

  /**
   * @param {{ host: string, port: number }} address
   * @returns {Promise<Broker>} connected, without client, retry or metadata cache
   */
  async open({ host, port }) {
    const logger = createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole })
    const broker = new Broker({
      connectionPool: new ConnectionPool({
        host,
        port,
        logger,
        socketFactory: this.socketFactory,
        connectionTimeout: 3000,
        requestTimeout: 30000,
      }),
      logger,
    })
    await broker.connect()
    return broker
  }

  /**
   * @returns {ScannedBatch[]} every distinct batch any of the sockets received, by first offset
   */
  batches() {
    const distinct = new Map()
    for (const chunks of this.sockets.values()) {
      for (const batch of scanBatches(Buffer.concat(chunks))) {
        distinct.set(`${batch.baseOffset}:${batch.bytes.readUInt32BE(17)}`, batch)
      }
    }
    return [...distinct.values()].sort((a, b) =>
      Number(BigInt(a.baseOffset) - BigInt(b.baseOffset))
    )
  }
}

/**
 * The Produce request of one raw record batch, which no client API builds: the batch's bytes go
 * into the request as they are. Hand it to `ConnectionPool#send` with a connection of the pool.
 * @param {Broker} broker a connected broker, which picks the Produce version (7 at most)
 * @param {Buffer} batch one complete record batch
 * @param {string} topic
 * @param {number} partition
 * @returns {{ request: object, response: object }} acks -1, no transactional id
 */
const rawProduceRequest = (broker, batch, topic, partition) => {
  const protocolRequest = broker.lookupRequestWithMax(
    apiKeys.Produce,
    requests.Produce,
    7
  )({ acks: -1, timeout: 30000, topicData: [] })
  protocolRequest.request.encode = async () =>
    new Encoder()
      .writeString(null)
      .writeInt16(-1)
      .writeInt32(30000)
      .writeArray([
        new Encoder()
          .writeString(topic)
          .writeArray([
            new Encoder().writeInt32(partition).writeInt32(batch.length).writeBuffer(batch),
          ]),
      ])
  return protocolRequest
}

/**
 * What the client's `REQUEST` events say of the answers it read, for a scenario's evidence.
 * @param {Array<object>} requests the event payloads
 * @param {number} [limit=300] how many of the latest answers
 * @returns {Array<object>} api, correlation id, broker, size and timing of each answer
 */
const repliesOf = (requests, limit = 300) =>
  requests
    .slice(-limit)
    .map(({ apiName, apiKey, correlationId, broker, size, sentAt, duration }) => ({
      apiName,
      apiKey,
      correlationId,
      broker,
      size,
      sentAt,
      duration,
    }))

/**
 * The sockets of one live client: a chaos factory the scenario arms, behind a frame log.
 * @param {object} [options] see `FrameLog`
 * @returns {{ chaos: Function, frames: FrameLog, socketFactory: Function }} hand `socketFactory`
 *   to the `Kafka` options
 */
const createLiveSockets = (options) => {
  const chaos = createChaosSocketFactory(defaultSocketFactory())
  const frames = new FrameLog(options)
  return { chaos, frames, socketFactory: frames.observe(chaos) }
}

module.exports = {
  CODEC_NAMES,
  FrameLog,
  ResponseDrop,
  WireTap,
  createLiveSockets,
  rawProduceRequest,
  repliesOf,
  scanBatches,
}
