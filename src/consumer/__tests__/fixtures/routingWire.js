const apiKeys = require('../../../protocol/requests/apiKeys')
const Decoder = require('../../../protocol/decoder')
const { requests } = require('../../../protocol/requests')
const { responseHeaderVersion, RESPONSE_HEADER_V1 } = require('../../../protocol/headers')
const { WireTranscript } = require('./rackWire')

const FETCH = apiKeys.Fetch
const METADATA = apiKeys.Metadata

/** The most Fetch rows one trace keeps; later ones are counted, never stored. */
const MAX_TRACE_ROWS = 300

/**
 * Records every frame a client writes and answers it reads, as `WireTranscript` does, and also the
 * Metadata answers and the time each socket closed, so a case can place the moment an old leader
 * was lost and the moment a new one became known.
 */
class RoutingWire extends WireTranscript {
  /**
   * Starts with no closed socket.
   * @param {Function} [inner] socket factory; defaults to the client socket factory
   */
  constructor(inner) {
    super(inner)
    /** @type {Array<{ connection: number, endpoint: string, at: number }>} when each socket closed */
    this.closes = []
  }

  /**
   * @param {object} socket a socket of the client
   * @param {{ host: string, port: number }} address
   * @returns {object} the socket, also noting when it closes
   */
  observe(socket, address) {
    const observed = super.observe(socket, address)
    const connection = this.connections
    const endpoint = `${address.host}:${address.port}`
    observed.once('close', () => this.closes.push({ connection, endpoint, at: performance.now() }))
    return observed
  }

  /**
   * @param {number} connection
   * @param {Buffer} frame one complete answer frame
   * @returns {void}
   */
  read(connection, frame) {
    const exchange = this.open.get(`${connection}:${frame.readInt32BE(4)}`)
    super.read(connection, frame)
    if (exchange?.apiKey === METADATA && !exchange.answer) {
      exchange.answer = { at: performance.now(), frame }
    }
  }

  /**
   * Decodes the answers of one API, once each.
   * @param {number} apiKey `Fetch` or `Metadata`
   * @returns {Promise<Array<{ exchange: object, at: number, decoded: object }>>} in request order
   */
  async answers(apiKey) {
    const kept = this.exchanges.filter((x) => x.apiKey === apiKey && x.answer)
    return Promise.all(
      kept.map(async (exchange) => {
        exchange.decoded ??= this.decode(exchange)
        return { exchange, at: exchange.answer.at, decoded: await exchange.decoded }
      })
    )
  }

  /**
   * Summarizes the Fetches the client wrote in a window as compact rows, a diagnostic that reads
   * the transcript and changes nothing in it. A Fetch whose request or answer cannot be decoded
   * still has its row, with no partitions named.
   * @param {object} options
   * @param {number} options.from `performance.now()` of the first write wanted
   * @param {number} options.to `performance.now()` of the last write wanted
   * @param {Array<{ label: string, topic: string, topicId?: string }>} options.targets the topics
   *   whose partition 0 each row says it asked for and what its answer said
   * @returns {Promise<{ rows: Array<{ node: number|string, at: number, answeredAt: number|null,
   *   asked: string[], errors: Record<string, number> }>, dropped: number }>} oldest first, at most
   *   `MAX_TRACE_ROWS`; `errors` maps a label to the error code its partition 0 came back with
   */
  async fetchTrace({ from, to, targets }) {
    const inWindow = this.exchanges.filter((x) => x.apiKey === FETCH && x.at >= from && x.at <= to)
    const rows = []
    for (const exchange of inWindow.slice(0, MAX_TRACE_ROWS)) {
      const answered = await this.decodedAnswer(exchange)
      const asked = []
      const errors = {}
      for (const { label, topic, topicId } of targets) {
        const asks = exchange.fetch?.topics.some(
          (t) =>
            (topicId ? t.topicId === topicId : t.topic === topic) &&
            t.partitions.some((p) => p.partition === 0)
        )
        if (asks) asked.push(label)
        for (const entry of answered?.responses ?? []) {
          if (!(topicId ? entry.topicId === topicId : entry.topicName === topic)) continue
          const partition = entry.partitions.find((p) => p.partition === 0)
          if (partition) errors[label] = partition.errorCode
        }
      }
      rows.push({
        node: this.nodeOf(exchange) ?? exchange.endpoint,
        at: exchange.at,
        answeredAt: exchange.answer?.at ?? null,
        asked,
        errors,
      })
    }
    return { rows, dropped: inWindow.length - rows.length }
  }

  /**
   * @param {object} exchange a Fetch exchange
   * @returns {Promise<object|null>} its decoded answer, null when it has none or cannot be read
   */
  async decodedAnswer(exchange) {
    if (!exchange.answer) return null
    try {
      exchange.decoded ??= this.decode(exchange)
      return await exchange.decoded
    } catch {
      return null
    }
  }

  /**
   * @param {object} exchange an exchange whose answer was kept
   * @returns {Promise<object>} the answer's decoded body
   */
  async decode(exchange) {
    const { response } = requests[exchange.apiKey === FETCH ? 'Fetch' : 'Metadata'].protocol({
      version: exchange.apiVersion,
    })({
      maxWaitTime: 0,
      minBytes: 0,
      maxBytes: 0,
      topics: [],
      topicIds: new Map(),
      rackId: '',
      allowAutoTopicCreation: false,
    })
    const { frame } = exchange.answer
    const header = new Decoder(frame)
    header.readInt32()
    header.readInt32()
    if (responseHeaderVersion(exchange.apiKey, exchange.apiVersion) === RESPONSE_HEADER_V1) {
      header.readTaggedFields()
    }
    return response.decode(frame.subarray(header.offset))
  }
}

module.exports = { RoutingWire, MAX_TRACE_ROWS }
