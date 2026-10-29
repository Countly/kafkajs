const Encoder = require('../../src/protocol/encoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const {
  fetchResponse,
  decodeFetchRequest,
} = require('../../src/consumer/__tests__/fixtures/fetchHarness')
const { fetchV12: expected } = require('./expected')

/**
 * Fetch v12 (flexible, topics by name): the version a consumer falls back to when a topic of the
 * request has no known id. The fetch harness's builder and reader are reused as they are; the
 * builder writes the harness's one topic (`test-topic`) by name, and a partition may carry
 * `records` (a record batch, `true` for the harness's own, or `false` for none),
 * `preferredReadReplica`, `abortedTransactions` and `currentLeader`. A v12 body has no node
 * endpoints.
 *
 * Evidence: Java's own bytes (`fixtures/v12_*_java.json`, bytes only); the harness builder given
 * the fields of `./expected` reproduces the response vector, and its reader returns the request
 * vector's fields, which are the generator's constants.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/FetchRequest.json
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/FetchResponse.json
 */
module.exports = {
  api: 'Fetch',
  apiKey: apiKeys.Fetch,
  version: 12,

  /**
   * @param {object} [fields] over the vector's: `partitions` (see above) and `topLevel`, the
   *   top-level error code
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const { partitions, topLevel } = { ...expected.response, ...fields }

    return fetchResponse(partitions, { apiVersion: 12, topLevel })
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {object} what the harness's `decodeFetchRequest` returns: `replicaId`, `maxWaitMs`,
   *   `minBytes`, `maxBytes`, `isolationLevel`, `sessionId`, `sessionEpoch`, `topics` (by name)
   *   and `rackId`
   * @throws {Error} when the body is cut short, malformed or has bytes after its tag buffer
   */
  readRequest(body) {
    const header = new Encoder()
      .writeInt32(0)
      .writeInt16(apiKeys.Fetch)
      .writeInt16(12)
      .writeInt32(0)
      .writeString('')
      .writeTaggedFields().buffer
    return decodeFetchRequest([Buffer.concat([header, body])])
  },
}
