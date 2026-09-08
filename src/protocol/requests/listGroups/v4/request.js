const Encoder = require('../../../encoder')
const { ListGroups: apiKey } = require('../../apiKeys')

const encodeFilter = (value) => new Encoder().writeUVarIntString(value)

/**
 * ListGroups Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * StatesFilter => COMPACT_ARRAY of COMPACT_STRING (4+)
 * TAG_BUFFER
 *
 * The filter carries DISPLAY names (`"Stable"`), because Kafka's `GroupState` enum serialises its
 * `name` field. It is not nullable: an empty filter is the empty compact array, which the broker
 * reads as "no filter". A type filter cannot be expressed at this version at all - the broker
 * method is where that becomes an error or a documented drop.
 * @see clients/src/main/resources/common/message/ListGroupsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} [options]
 * @param {string[]} [options.states=[]] wire display names
 */
module.exports = ({ states = [] } = {}) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'ListGroups',
  encode: async () => new Encoder().writeUVarIntArray(states.map(encodeFilter)).writeTaggedFields(),
})

module.exports.schema = {
  api: 'ListGroups',
  version: 4,
  direction: 'request',
  fields: [{ name: 'StatesFilter', type: '[]string', versions: '4+' }],
}
