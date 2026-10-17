const Encoder = require('../../src/protocol/encoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const {
  fetchResponse,
  decodeFetchRequest,
} = require('../../src/consumer/__tests__/fixtures/fetchHarness')
const { fetchV18: expected } = require('./expected')

/**
 * Fetch v18 (flexible, topics by id), the existing builder and reader of the fetch harness
 * reused as they are. The builder writes the one topic and topic id the harness is built around
 * (`test-topic`); a partition may carry `records` (a record batch, `true` for the harness's own,
 * or `false` for none), `preferredReadReplica`, `abortedTransactions` and `currentLeader`.
 *
 * Evidence: Java's own bytes (`fixtures/v18_*_java.json`, bytes only); the harness builder given
 * the fields of `./expected` reproduces the response vector, and its reader returns the request
 * vector's fields, which are the generator's constants.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/FetchResponse.json
 */
module.exports = {
  api: 'Fetch',
  apiKey: apiKeys.Fetch,
  version: 18,

  /**
   * @param {object} [fields] over the vector's: `partitions` (see above), `nodeEndpoints` (`{
   *   nodeId, host, port, rack }`) and `topLevel`, the top-level error code
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const { partitions, nodeEndpoints, topLevel } = { ...expected.response, ...fields }

    return fetchResponse(partitions, { apiVersion: 18, nodeEndpoints, topLevel })
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {object} what the harness's `decodeFetchRequest` returns: `replicaId` (null from v15),
   *   `maxWaitMs`, `minBytes`, `maxBytes`, `isolationLevel`, `sessionId`, `sessionEpoch`, `topics`
   *   and `rackId`
   * @throws {Error} when the body is cut short or a byte follows its tag buffer, which the harness
   *   cannot tell on its own: the body's last byte is the tag buffer it reads, so without it the
   *   request must no longer read
   */
  readRequest(body) {
    const header = new Encoder()
      .writeInt32(0)
      .writeInt16(apiKeys.Fetch)
      .writeInt16(18)
      .writeInt32(0)
      .writeString('')
      .writeTaggedFields().buffer
    const read = (bytes) => decodeFetchRequest([Buffer.concat([header, bytes])])
    const request = read(body)

    try {
      read(body.subarray(0, -1))
    } catch {
      return request
    }

    throw new Error('Fetch v18 request has a byte after its tag buffer')
  },
}
