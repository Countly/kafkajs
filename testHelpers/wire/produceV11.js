const apiKeys = require('../../src/protocol/requests/apiKeys')
const { createProduceLayout } = require('./produceLayout')
const { produceV11: expected } = require('./expected')

const layout = createProduceLayout(11)

/**
 * Produce v11 (flexible, request header v2), topics keyed by name. The request carries the transactional
 * id (null unless the producer is transactional), the acks and timeout, and per partition its
 * `Records`: one or more record batches whose headers hold the producer id, epoch and sequence.
 * The response answers per partition and carries the KIP-951 tagged fields.
 *
 * Evidence: both messages are Java's own bytes (`fixtures/v11_*_java.json`, bytes only), and
 * `./expected` holds the fields of their generator. Java's request has a null `Records` and a null
 * transactional id, so the batch header and a populated transactional id are checked against the
 * hand-computed goldens of `./expected` (`recordBatchHeaderV2`, `produceRequestTransactionalV11`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ProduceResponse.json
 */
module.exports = {
  api: 'Produce',
  apiKey: apiKeys.Produce,
  version: 11,

  /**
   * @param {object} [fields] over the vector's: `responses` (`{ topic, partitions }`, a partition being `{ partition,
   *   errorCode, baseOffset, logAppendTime, logStartOffset, leader }`), `throttleTime` and
   *   `nodeEndpoints`
   * @returns {Buffer} the response body
   */
  buildResponse: (fields = {}) => layout.buildResponse({ ...expected.response, ...fields }),

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ transactionalId: string|null, acks: number, timeoutMs: number, topics: Array<{
   *   topic: string, partitions: Array<{ partition: number, batches: object[]|null }> }> }}
   * @throws {Error} when the body is cut short, a byte follows its tag buffer, or its Records are
   *   not whole batches
   */
  readRequest: layout.readRequest,
}
