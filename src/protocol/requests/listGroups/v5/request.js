const Encoder = require('../../../encoder')
const { ListGroups: apiKey } = require('../../apiKeys')

const encodeFilter = (value) => new Encoder().writeUVarIntString(value)

/**
 * ListGroups Request (Version: 5) — flexible (KIP-482), request header v2
 *
 * StatesFilter => COMPACT_ARRAY of COMPACT_STRING (4+)
 * TypesFilter => COMPACT_ARRAY of COMPACT_STRING (5+)
 * TAG_BUFFER
 *
 * Both filters carry DISPLAY names (`"Stable"`, `"Consumer"`), because Kafka's `GroupState` and
 * `GroupType` enums serialise their `name` field. Neither is nullable: an empty filter is the
 * empty compact array, which the broker reads as "no filter".
 * @see clients/src/main/resources/common/message/ListGroupsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} [options]
 * @param {string[]} [options.states=[]] wire display names
 * @param {string[]} [options.types=[]] wire display names
 */
module.exports = ({ states = [], types = [] } = {}) => ({
  apiKey,
  apiVersion: 5,
  apiName: 'ListGroups',
  encode: async () =>
    new Encoder()
      .writeUVarIntArray(states.map(encodeFilter))
      .writeUVarIntArray(types.map(encodeFilter))
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'ListGroups',
  version: 5,
  direction: 'request',
  fields: [
    { name: 'StatesFilter', type: '[]string', versions: '4+' },
    { name: 'TypesFilter', type: '[]string', versions: '5+' },
  ],
}
