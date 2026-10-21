const Broker = require('../../../broker')
const ConnectionPool = require('../../connectionPool')
const { FrameFilter } = require('../../../../testHelpers/chaosSocket')
const { connectionOpts, newLogger } = require('testHelpers')

/** How many Metadata calls the overload makes at once. */
const CALLS = 50
/** The `maxInFlightRequests` the overloaded connection is built with. */
const LIMIT = 5

/**
 * Wraps a socket factory so that each socket counts the requests written on it and not yet
 * answered, following the frames' size prefixes across any chunking; the highest count any socket
 * reached is kept.
 * @param {Function} inner the socket factory to wrap
 * @returns {Function & { counts: { written: number, answered: number, max: number } }}
 */
const countInFlight = (inner) => {
  const counts = { written: 0, answered: 0, max: 0 }
  const factory = (options) => {
    const socket = inner(options)
    let inFlight = 0
    const requests = new FrameFilter({
      headerBytes: 4,
      decide: () => {
        counts.written++
        counts.max = Math.max(counts.max, ++inFlight)
        return false
      },
    })
    const responses = new FrameFilter({
      headerBytes: 4,
      decide: () => {
        counts.answered++
        inFlight--
        return false
      },
    })
    const write = socket.write
    socket.write = (data, ...rest) => {
      requests.push(Buffer.from(data))
      return write.call(socket, data, ...rest)
    }
    socket.on('data', (chunk) => responses.push(chunk))
    return socket
  }
  factory.counts = counts
  return factory
}

/**
 * The overload: `CALLS` Metadata calls at once on one broker whose connection allows `LIMIT` in
 * flight, counted at the socket.
 * @param {{ host: string, port: number, socketFactory: Function, topics: string[] }} target
 * @returns {Promise<{ resolved: number, maxInFlight: number, written: number }>} how many calls
 *   resolved with brokers, the most requests one socket had unanswered, and the requests written
 */
const overload = async ({ host, port, socketFactory, topics }) => {
  const counting = countInFlight(socketFactory)
  const broker = new Broker({
    connectionPool: new ConnectionPool(
      connectionOpts({ host, port, socketFactory: counting, maxInFlightRequests: LIMIT })
    ),
    logger: newLogger(),
  })

  try {
    await broker.connect()
    const results = await Promise.all(Array.from({ length: CALLS }, () => broker.metadata(topics)))
    return {
      resolved: results.filter(({ brokers }) => brokers.length > 0).length,
      maxInFlight: counting.counts.max,
      written: counting.counts.written,
    }
  } finally {
    await broker.disconnect()
  }
}

module.exports = { CALLS, LIMIT, countInFlight, overload }
