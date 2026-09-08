const Encoder = require('../../../encoder')
const { ListGroups: apiKey } = require('../../apiKeys')

/**
 * ListGroups Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * TAG_BUFFER
 *
 * v3 is the first flexible version of an API whose request has never carried a field, so the
 * whole body is one empty tag buffer. The two filter options are accepted and ignored, because
 * the version map hands every version the same options object.
 * @see clients/src/main/resources/common/message/ListGroupsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} [options] accepted and unused at this version
 */
module.exports = () => ({
  apiKey,
  apiVersion: 3,
  apiName: 'ListGroups',
  encode: async () => new Encoder().writeTaggedFields(),
})

module.exports.schema = {
  api: 'ListGroups',
  version: 3,
  direction: 'request',
  fields: [],
}
