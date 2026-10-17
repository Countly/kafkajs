const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/leaveGroup/fixtures/java/leaveGroupResponse-v5-ok.json')

const encodeMember = ({ memberId, groupInstanceId, errorCode }) =>
  new Encoder()
    .writeUVarIntString(memberId)
    .writeUVarIntString(groupInstanceId)
    .writeInt16(errorCode)
    .writeTaggedFields()

/**
 * LeaveGroup v5 (flexible, batched members). The request lists the members that leave, each with
 * an optional reason; the response answers each one with an error code.
 *
 * Evidence: Java's structured vectors `leaveGroupRequest-v5-oneMember.json` and
 * `leaveGroupResponse-v5-ok.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/LeaveGroupResponse.json
 */
module.exports = {
  api: 'LeaveGroup',
  apiKey: apiKeys.LeaveGroup,
  version: 5,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `errorCode` and `members`
   *   (`{ memberId, groupInstanceId, errorCode }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeUVarIntArray(response.members.map(encodeMember))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ groupId: string, members: Array<{ memberId: string, groupInstanceId: string|null,
   *   reason: string|null }> }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      groupId: decoder.readUVarIntString(),
      members: decoder.readUVarIntArray((d) => {
        const member = {
          memberId: d.readUVarIntString(),
          groupInstanceId: d.readUVarIntString(),
          reason: d.readUVarIntString(),
        }
        d.readTaggedFields()
        return member
      }),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('LeaveGroup v5 request has a byte after its tag buffer')
    }

    return request
  },
}
