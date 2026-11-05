const requestV2 = require('../v2/request')

/**
 * DeleteGroups Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * GroupsNames => COMPACT_ARRAY of COMPACT_STRING (0+)
 * TAG_BUFFER
 *
 * The body is v2's: version 3 only adds the per-group ErrorMessage to the response.
 * @see clients/src/main/resources/common/message/DeleteGroupsRequest.json (Apache Kafka 4.4.0)
 *
 * @param {string[]} groupIds the ids of the groups to delete
 * @returns {object} the protocol request
 */
module.exports = (groupIds) => ({ ...requestV2(groupIds), apiVersion: 3 })

module.exports.schema = { ...requestV2.schema, version: 3 }
