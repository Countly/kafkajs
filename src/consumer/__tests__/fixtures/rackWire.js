const Decoder = require('../../../protocol/decoder')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { requests } = require('../../../protocol/requests')
const { responseHeaderVersion, RESPONSE_HEADER_V1 } = require('../../../protocol/headers')
const defaultSocketFactory = require('../../../network/socketFactory')
const { readRequestHeader } = require('testHelpers')
const { decodeFetchRequest } = require('./fetchHarness')

/** The api keys whose answer frames a transcript keeps. */
const KEPT_ANSWERS = new Set([apiKeys.Fetch, apiKeys.SyncGroup])

/** The bytes of a v3 assignment before its first topic name: version int16, topic count int32. */
const ASSIGNMENT_PREFIX_BYTES = 6

/**
 * @typedef {object} Exchange
 * @property {number} connection the order the socket was created in
 * @property {string} endpoint `host:port` the socket was opened to
 * @property {number} apiKey
 * @property {number} apiVersion
 * @property {number} correlationId
 * @property {number} at `performance.now()` when the request frame was written
 * @property {object} [fetch] the decoded Fetch request: `topics`, `rackId` and the rest
 * @property {Error} [fetchError] why a Fetch request could not be decoded, so a reader fails loudly
 * @property {{ at: number, frame: Buffer }} [answer] the answer frame, kept for Fetch and SyncGroup
 */

/**
 * Rebinds the topic of an independent Java assignment vector to a live topic name.
 * @param {string} hex a v3 assignment for exactly one topic, as the vector prints it
 * @param {string} topic the live topic name
 * @returns {string} the same assignment bytes, hex, naming `topic`
 */
const bindTopic = (hex, topic) => {
  const head = ASSIGNMENT_PREFIX_BYTES * 2
  const nameBytes = 2 + Buffer.from(hex.slice(head), 'hex').readInt16BE(0)
  const name = Buffer.alloc(2 + Buffer.byteLength(topic))
  name.writeInt16BE(Buffer.byteLength(topic))
  name.write(topic, 2)
  return hex.slice(0, head) + name.toString('hex') + hex.slice(head + nameBytes * 2)
}

/**
 * Records the request frames a client writes and the answers it reads, per socket and correlation
 * id, passing every byte through untouched. It is a wire observer, not a second opinion of what the
 * client believes it sent: a Fetch's node is the socket's endpoint, and the preferred replica is
 * read from the answer's own bytes.
 */
class WireTranscript {
  /**
   * @param {Function} [inner] the socket factory to wrap
   */
  constructor(inner = defaultSocketFactory()) {
    this.connections = 0
    /** @type {Exchange[]} oldest first */
    this.exchanges = []
    /** @type {Map<string, Exchange>} by `connection:correlationId` */
    this.open = new Map()
    /** @type {Map<string, number>} node id by `host:port`, set once the cluster is known */
    this.nodes = new Map()
    /** The socket factory to hand to the `Kafka` options. */
    this.socketFactory = (options) => this.observe(inner(options), options)
  }

  /**
   * @param {object} socket a socket of the client
   * @param {{ host: string, port: number }} address
   * @returns {object} the same socket, now reporting what it writes and reads
   */
  observe(socket, { host, port }) {
    const connection = (this.connections += 1)
    const endpoint = `${host}:${port}`
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => {
      this.write(connection, endpoint, Buffer.isBuffer(data) ? data : Buffer.from(data, rest[0]))
      return write(data, ...rest)
    }
    let pending = Buffer.alloc(0)
    socket.on('data', (chunk) => {
      pending = Buffer.concat([pending, chunk])
      while (pending.length >= 4 && pending.length >= 4 + pending.readInt32BE(0)) {
        const size = 4 + pending.readInt32BE(0)
        this.read(connection, Buffer.from(pending.subarray(0, size)))
        pending = pending.subarray(size)
      }
    })
    return socket
  }

  /**
   * @param {number} connection
   * @param {string} endpoint
   * @param {Buffer} frame one written request frame
   * @returns {void}
   */
  write(connection, endpoint, frame) {
    const { apiKey, apiVersion, correlationId } = readRequestHeader(frame)
    const exchange = {
      connection,
      endpoint,
      apiKey,
      apiVersion,
      correlationId,
      at: performance.now(),
    }
    if (apiKey === apiKeys.Fetch) {
      try {
        exchange.fetch = decodeFetchRequest([frame])
      } catch (error) {
        exchange.fetchError = error
      }
    }
    this.exchanges.push(exchange)
    this.open.set(`${connection}:${correlationId}`, exchange)
  }

  /**
   * @param {number} connection
   * @param {Buffer} frame one complete answer frame, size prefix included
   * @returns {void}
   */
  read(connection, frame) {
    const key = `${connection}:${frame.readInt32BE(4)}`
    const exchange = this.open.get(key)
    if (!exchange) return
    this.open.delete(key)
    if (KEPT_ANSWERS.has(exchange.apiKey)) exchange.answer = { at: performance.now(), frame }
  }

  /**
   * @param {Exchange} exchange
   * @returns {number|undefined} the node id of the broker the exchange went to
   */
  nodeOf({ endpoint }) {
    return this.nodes.get(endpoint)
  }

  /**
   * @param {number} apiKey
   * @param {number} [connection] one socket; every socket when absent
   * @returns {Exchange|undefined} the last answered exchange of that api
   */
  lastAnswered(apiKey, connection) {
    return this.exchanges
      .filter(
        (x) =>
          x.apiKey === apiKey &&
          x.answer &&
          (connection === undefined || x.connection === connection)
      )
      .at(-1)
  }

  /**
   * Decodes every kept Fetch answer and lists the partitions it gave a preferred read replica.
   * @returns {Promise<Array<{ exchange: Exchange, node: number|undefined, topic: string|null,
   *   topicId: string|null, partition: number, preferredReadReplica: number, at: number }>>}
   *   oldest first; the answer's own bytes decide, never the client's routing state
   */
  async fetchHints() {
    const hints = []
    for (const exchange of this.exchanges) {
      if (exchange.apiKey !== apiKeys.Fetch || !exchange.answer) continue
      const { response } = requests.Fetch.protocol({ version: exchange.apiVersion })({
        maxWaitTime: 0,
        minBytes: 0,
        maxBytes: 0,
        topics: [],
        topicIds: new Map(),
        rackId: '',
      })
      const { frame } = exchange.answer
      const header = new Decoder(frame)
      header.readInt32()
      header.readInt32()
      if (responseHeaderVersion(apiKeys.Fetch, exchange.apiVersion) === RESPONSE_HEADER_V1) {
        header.readTaggedFields()
      }
      const decoded = await response.decode(frame.subarray(header.offset))
      for (const { topic = null, topicId = null, partitions } of decoded.responses) {
        for (const { partition, preferredReadReplica } of partitions) {
          if (preferredReadReplica < 0) continue
          hints.push({
            exchange,
            node: this.nodeOf(exchange),
            topic,
            topicId,
            partition,
            preferredReadReplica,
            at: exchange.answer.at,
          })
        }
      }
    }
    return hints
  }

  /**
   * @param {{ at: number, partition: number, topic: string|null, topicId: string|null }} hint
   * @returns {Exchange|undefined} the first Fetch written after the hint's answer arrived that
   *   asks for the hinted partition of the hinted topic
   * @throws {Error} the decode failure of a Fetch request written after the hint
   */
  nextFetch({ at, partition, topic, topicId }) {
    return this.exchanges.find((x) => {
      if (x.apiKey !== apiKeys.Fetch || x.at <= at) return false
      if (x.fetchError) throw x.fetchError
      return x.fetch.topics.some(
        (t) =>
          (topicId ? t.topicId === topicId : t.topic === topic) &&
          t.partitions.some((p) => p.partition === partition)
      )
    })
  }
}

module.exports = { WireTranscript, bindTopic }
