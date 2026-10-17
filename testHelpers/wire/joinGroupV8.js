const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/joinGroup/fixtures/java/joinGroupResponse-v8-ok.json')

const asBuffer = (bytes) => (Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes, 'hex'))

const encodeMember = ({ memberId, groupInstanceId, memberMetadata }) =>
  new Encoder()
    .writeUVarIntString(memberId)
    .writeUVarIntString(groupInstanceId)
    .writeUVarIntBytes(asBuffer(memberMetadata))
    .writeTaggedFields()

/**
 * JoinGroup v8 (flexible). The request offers the member's protocols; the response names the
 * generation, the chosen protocol, the leader and, to the leader, every member's metadata. Bytes
 * are hex strings, as in the vector.
 *
 * Evidence: Java's structured vectors `joinGroupRequest-v8-basic.json` and
 * `joinGroupResponse-v8-ok.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/JoinGroupResponse.json
 */
module.exports = {
  api: 'JoinGroup',
  apiKey: apiKeys.JoinGroup,
  version: 8,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `errorCode`, `generationId`,
   *   `protocolType`, `groupProtocol`, `leaderId`, `memberId` and `members` (`{ memberId,
   *   groupInstanceId, memberMetadata }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeInt32(response.generationId)
      .writeUVarIntString(response.protocolType)
      .writeUVarIntString(response.groupProtocol)
      .writeUVarIntString(response.leaderId)
      .writeUVarIntString(response.memberId)
      .writeUVarIntArray(response.members.map(encodeMember))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ groupId: string, sessionTimeout: number, rebalanceTimeout: number, memberId:
   *   string, groupInstanceId: string|null, protocolType: string, groupProtocols: Array<{ name:
   *   string, metadata: string }>, reason: string|null }} `metadata` is hex
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      groupId: decoder.readUVarIntString(),
      sessionTimeout: decoder.readInt32(),
      rebalanceTimeout: decoder.readInt32(),
      memberId: decoder.readUVarIntString(),
      groupInstanceId: decoder.readUVarIntString(),
      protocolType: decoder.readUVarIntString(),
      groupProtocols: decoder.readUVarIntArray((d) => {
        const protocol = {
          name: d.readUVarIntString(),
          metadata: d.readUVarIntBytes().toString('hex'),
        }
        d.readTaggedFields()
        return protocol
      }),
      reason: decoder.readUVarIntString(),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('JoinGroup v8 request has a byte after its tag buffer')
    }

    return request
  },
}
