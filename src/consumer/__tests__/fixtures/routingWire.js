const apiKeys = require('../../../protocol/requests/apiKeys')
const Decoder = require('../../../protocol/decoder')
const { requests } = require('../../../protocol/requests')
const { responseHeaderVersion, RESPONSE_HEADER_V1 } = require('../../../protocol/headers')
const { WireTranscript } = require('./rackWire')

const FETCH = apiKeys.Fetch
const METADATA = apiKeys.Metadata

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
   * @param {number} at `performance.now()`
   * @param {boolean} [inclusive] whether an answer at `at` counts
   * @returns {Promise<Set<number>|null>} the broker ids of the last Metadata answer before `at`,
   *   the client's own view of the cluster; null when it had none
   */
  async nodeSetAt(at, inclusive = false) {
    const answers = (await this.answers(METADATA)).filter((a) =>
      inclusive ? a.at <= at : a.at < at
    )
    const last = answers.sort((a, b) => a.at - b.at).at(-1)
    return last ? new Set(last.decoded.brokers.map(({ nodeId }) => nodeId)) : null
  }

  /**
   * Measures one shared drain for a recreation episode, across every destination. The first
   * Fetch written at or after the route belongs to the recreated set; only earlier requests
   * contribute responses or, if unanswered, socket closes. Recreated Fetches never contribute.
   * @param {number} routeAt time the changed node set became known
   * @returns {number|null} last pre-recreation completion before the global restart, or null
   */
  drainEnd(routeAt) {
    const fetches = this.exchanges.filter((x) => x.apiKey === FETCH)
    const first = fetches.filter((x) => x.at >= routeAt).sort((a, b) => a.at - b.at)[0]
    if (!first) return null
    const ended = fetches
      .filter((x) => x.at < first.at)
      .map(
        (x) =>
          x.answer?.at ??
          this.closes.find(({ connection, at }) => connection === x.connection && at >= x.at)?.at
      )
      .filter((at) => at !== undefined && at <= first.at)
    return ended.length ? Math.max(...ended) : null
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

module.exports = { RoutingWire }
