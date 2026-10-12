const requestV6 = require('../v6/request')

/**
 * JoinGroup Request (Version: 7) — flexible (KIP-482), request header v2
 *
 * Byte-identical to v6: the version only adds `ProtocolType` and a nullable `ProtocolName` to the
 * response.
 * @see clients/src/main/resources/common/message/JoinGroupRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options as v6
 * @returns {object} the protocol request
 */
module.exports = (options) => Object.assign(requestV6(options), { apiVersion: 7 })

module.exports.schema = { ...requestV6.schema, version: 7 }
