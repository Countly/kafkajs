const Encoder = require('../../../encoder')
const { DeleteGroups: apiKey } = require('../../apiKeys')

/**
 * DeleteGroups Request (Version: 2) — flexible (KIP-482), request header v2
 *
 * GroupsNames => COMPACT_ARRAY of COMPACT_STRING (0+)
 * TAG_BUFFER
 *
 * The v1 field in compact framing.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteGroupsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {string[]} groupIds the ids of the groups to delete
 * @returns {object} the protocol request
 */
module.exports = (groupIds) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'DeleteGroups',
  encode: async () => {
    return new Encoder().writeUVarIntArray(groupIds.map(encodeGroup)).writeTaggedFields()
  },
})

const encodeGroup = (group) => new Encoder().writeUVarIntString(group)

module.exports.schema = {
  api: 'DeleteGroups',
  version: 2,
  direction: 'request',
  fields: [{ name: 'GroupsNames', type: '[]string', versions: '0+' }],
}
