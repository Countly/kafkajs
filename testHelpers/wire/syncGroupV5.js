const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/syncGroup/fixtures/java/syncGroupResponse-v5-ok.json')

const asBuffer = (bytes) => (Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes, 'hex'))

/**
 * SyncGroup v5 (flexible). The leader's request carries every member's assignment; the response
 * hands each member its own. Bytes are hex strings, as in the vector.
 *
 * Evidence: Java's structured vectors `syncGroupRequest-v5-assignment.json` and
 * `syncGroupResponse-v5-ok.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/SyncGroupResponse.json
 */
module.exports = {
  api: 'SyncGroup',
  apiKey: apiKeys.SyncGroup,
  version: 5,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `errorCode`, `protocolType`,
   *   `protocolName` and `memberAssignment`
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeUVarIntString(response.protocolType)
      .writeUVarIntString(response.protocolName)
      .writeUVarIntBytes(asBuffer(response.memberAssignment))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ groupId: string, generationId: number, memberId: string, groupInstanceId:
   *   string|null, protocolType: string|null, protocolName: string|null, groupAssignment:
   *   Array<{ memberId: string, memberAssignment: string }> }} `memberAssignment` is hex
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      groupId: decoder.readUVarIntString(),
      generationId: decoder.readInt32(),
      memberId: decoder.readUVarIntString(),
      groupInstanceId: decoder.readUVarIntString(),
      protocolType: decoder.readUVarIntString(),
      protocolName: decoder.readUVarIntString(),
      groupAssignment: decoder.readUVarIntArray((d) => {
        const assignment = {
          memberId: d.readUVarIntString(),
          memberAssignment: d.readUVarIntBytes().toString('hex'),
        }
        d.readTaggedFields()
        return assignment
      }),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('SyncGroup v5 request has a byte after its tag buffer')
    }

    return request
  },
}
