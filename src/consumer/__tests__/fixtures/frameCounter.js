const apiKeys = require('../../../protocol/requests/apiKeys')
const { FrameFilter } = require('../../../../testHelpers/chaosSocket')

/** @param {number} apiKey @returns {string} the name the protocol gives the key */
const apiName = (apiKey) => {
  const name = Object.keys(apiKeys).find((candidate) => apiKeys[candidate] === apiKey)
  return name === 'GroupCoordinator' ? 'FindCoordinator' : name
}

/**
 * The request frames one client wrote, counted by API. Every consumer of a case has its own
 * counter, so the traffic of the admin, the producer and a grouped peer never reaches it.
 */
class FrameCounter {
  constructor() {
    /** @type {Map<string, number>} */
    this.counts = new Map()
    /** @type {Array<{ api: string, version: number }>} */
    this.frames = []
    /** @type {Array<{ api: string, socket: object }>} the sockets that wrote a request, newest last */
    this.writers = []
  }

  /**
   * @param {string} api
   * @returns {number} how many frames of the API this client wrote
   */
  count(api) {
    return this.counts.get(api) ?? 0
  }

  /**
   * @param {string[]} apis
   * @returns {Record<string, number>} the counts of the named APIs
   */
  of(apis) {
    return Object.fromEntries(apis.map((api) => [api, this.count(api)]))
  }

  /**
   * Destroys the newest connection that wrote a Fetch, as a lost connection does.
   * @returns {void}
   */
  killFetchConnection() {
    const writer = [...this.writers].reverse().find(({ api }) => api === 'Fetch')
    writer.socket.destroy(new Error('connection lost'))
  }

  /**
   * @param {Function} inner the socket factory the client would have used
   * @returns {Function} a socket factory that counts every request frame written through it
   */
  wrap(inner) {
    return (options) => {
      const socket = inner(options)
      const write = socket.write.bind(socket)
      const filter = new FrameFilter({
        headerBytes: 12,
        decide: (header) => {
          const api = apiName(header.readInt16BE(4))
          this.counts.set(api, this.count(api) + 1)
          this.frames.push({ api, version: header.readInt16BE(6) })
          this.writers.push({ api, socket })
          return false
        },
      })
      socket.write = (data, encoding) => {
        filter.push(Buffer.from(data, encoding))
        return write(data, encoding)
      }
      return socket
    }
  }
}

module.exports = { FrameCounter, apiName }
