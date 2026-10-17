const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/heartbeat/fixtures/java/heartbeatResponse-v4-ok.json')

/**
 * Heartbeat v4 (flexible). The request names the group, generation and member; the response is a
 * throttle time and an error code (27 is REBALANCE_IN_PROGRESS).
 *
 * Evidence: Java's structured vectors `heartbeatRequest-v4-basic.json` and
 * `heartbeatResponse-v4-ok.json` (`hex` and `fields`); `heartbeatResponse-v4-error.json` is the
 * same message with error 27.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/HeartbeatResponse.json
 */
module.exports = {
  api: 'Heartbeat',
  apiKey: apiKeys.Heartbeat,
  version: 4,

  /**
   * @param {object} [fields] over the vector's: `throttleTime` and `errorCode`
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ groupId: string, groupGenerationId: number, memberId: string, groupInstanceId:
   *   string|null }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      groupId: decoder.readUVarIntString(),
      groupGenerationId: decoder.readInt32(),
      memberId: decoder.readUVarIntString(),
      groupInstanceId: decoder.readUVarIntString(),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('Heartbeat v4 request has a byte after its tag buffer')
    }

    return request
  },
}
