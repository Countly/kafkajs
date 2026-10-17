const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { findCoordinatorV6: expected } = require('./expected')

const encodeCoordinator = ({ key, nodeId, host, port, errorCode, errorMessage }) =>
  new Encoder()
    .writeUVarIntString(key)
    .writeInt32(nodeId)
    .writeUVarIntString(host)
    .writeInt32(port)
    .writeInt16(errorCode)
    .writeUVarIntString(errorMessage)
    .writeTaggedFields()

/**
 * FindCoordinator v6 (flexible, batched keys). The request names a coordinator key type (0 =
 * group, 1 = transaction) and the keys; the response answers each key with a node.
 *
 * Evidence: both messages are Java's own bytes (`fixtures/v6_*_java.json`, bytes only), and
 * `./expected` holds the fields of their generator.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/FindCoordinatorResponse.json
 */
module.exports = {
  api: 'FindCoordinator',
  apiKey: apiKeys.GroupCoordinator,
  version: 6,

  /**
   * @param {object} [fields] over the vector's: `throttleTime` and `coordinators` (`{ key,
   *   nodeId, host, port, errorCode, errorMessage }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...expected.response, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeUVarIntArray(response.coordinators.map(encodeCoordinator))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ keyType: number, coordinatorKeys: string[] }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const keyType = decoder.readInt8()
    const coordinatorKeys = decoder.readUVarIntArray((d) => d.readUVarIntString())
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('FindCoordinator v6 request has a byte after its tag buffer')
    }

    return { keyType, coordinatorKeys }
  },
}
