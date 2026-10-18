const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/addPartitionsToTxn/fixtures/java/addPartitionsToTxnResponse-v3-ok.json')

const encodePartitionError = ({ partition, errorCode }) =>
  new Encoder().writeInt32(partition).writeInt16(errorCode).writeTaggedFields()

const encodeTopicErrors = ({ topic, partitionErrors }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitionErrors.map(encodePartitionError))
    .writeTaggedFields()

/**
 * AddPartitionsToTxn v3 (flexible), the last version a client sends: from v4 the broker batches
 * several transactions in one request. The request names the transactional id, producer id and
 * epoch and the topics and partitions to add; the response answers per partition.
 *
 * Evidence: Java's structured vectors `addPartitionsToTxnRequest-v3-basic.json` and
 * `addPartitionsToTxnResponse-v3-{ok,error}.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/AddPartitionsToTxnResponse.json
 */
module.exports = {
  api: 'AddPartitionsToTxn',
  apiKey: apiKeys.AddPartitionsToTxn,
  version: 3,

  /**
   * @param {object} [fields] over the vector's: `throttleTime` and `errors` (`{ topic,
   *   partitionErrors: [{ partition, errorCode }] }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeUVarIntArray(response.errors.map(encodeTopicErrors))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ transactionalId: string, producerId: string, producerEpoch: number, topics:
   *   Array<{ topic: string, partitions: number[] }> }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      transactionalId: decoder.readUVarIntString(),
      producerId: decoder.readInt64().toString(),
      producerEpoch: decoder.readInt16(),
      topics: decoder.readUVarIntArray((topic) => {
        const entry = {
          topic: topic.readUVarIntString(),
          partitions: topic.readUVarIntArray((partition) => partition.readInt32()),
        }
        topic.readTaggedFields()
        return entry
      }),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('AddPartitionsToTxn v3 request has a byte after its tag buffer')
    }

    return request
  },
}
