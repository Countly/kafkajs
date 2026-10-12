const Encoder = require('../../../encoder')
const { SyncGroup: apiKey } = require('../../apiKeys')

/**
 * SyncGroup Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * GroupId => COMPACT_STRING
 * GenerationId => INT32
 * MemberId => COMPACT_STRING
 * GroupInstanceId => COMPACT_NULLABLE_STRING
 * Assignments => COMPACT_ARRAY
 *   MemberId => COMPACT_STRING
 *   Assignment => COMPACT_BYTES
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v3's fields in compact framing.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/SyncGroupRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {number} options.generationId
 * @param {string} options.memberId
 * @param {string|null} [options.groupInstanceId=null] static membership id (KIP-345)
 * @param {{memberId: string, memberAssignment: Buffer}[]} options.groupAssignment empty for a follower
 * @returns {object} the protocol request
 */
module.exports = ({
  groupId,
  generationId,
  memberId,
  groupInstanceId = null,
  groupAssignment,
}) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'SyncGroup',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(groupId)
      .writeInt32(generationId)
      .writeUVarIntString(memberId)
      .writeUVarIntString(groupInstanceId)
      .writeUVarIntArray(groupAssignment.map(encodeAssignment))
      .writeTaggedFields(),
})

const encodeAssignment = ({ memberId, memberAssignment }) =>
  new Encoder().writeUVarIntString(memberId).writeUVarIntBytes(memberAssignment).writeTaggedFields()

module.exports.schema = {
  api: 'SyncGroup',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'GenerationId', type: 'int32', versions: '0+' },
    { name: 'MemberId', type: 'string', versions: '0+' },
    { name: 'GroupInstanceId', type: 'string', versions: '3+', nullable: true },
    { name: 'Assignments', type: '[]SyncGroupRequestAssignment', versions: '0+' },
    { name: 'Assignments.MemberId', type: 'string', versions: '0+' },
    { name: 'Assignments.Assignment', type: 'bytes', versions: '0+' },
  ],
}
