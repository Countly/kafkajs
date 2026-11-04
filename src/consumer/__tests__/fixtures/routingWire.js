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
