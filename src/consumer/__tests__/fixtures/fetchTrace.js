const Broker = require('../../../broker')

/**
 * Records every partition a client asks one topic for in a Fetch, in the order the requests are
 * sent, without altering the request or its answer. A broker keeps its client id on its
 * connection pool, not on itself.
 * @param {object} options
 * @param {string} options.topic the only topic that is recorded
 * @param {Array<{clientId: string, partition: number, ordinal: number}>} options.sink receives one
 *   entry per partition per Fetch
 * @param {() => number} options.tick the next ordinal of the caller's monotonic clock
 * @returns {import('jest-mock').SpyInstance} the spy; `mockRestore()` removes it
 */
const traceFetches = ({ topic, sink, tick }) => {
  const original = Broker.prototype.fetch
  return jest.spyOn(Broker.prototype, 'fetch').mockImplementation(function (request, ...rest) {
    for (const entry of request.topics ?? []) {
      if (entry.topic !== topic) continue
      for (const { partition } of entry.partitions) {
        sink.push({ clientId: this.connectionPool.clientId, partition, ordinal: tick() })
      }
    }
    return original.call(this, request, ...rest)
  })
}

module.exports = { traceFetches }
