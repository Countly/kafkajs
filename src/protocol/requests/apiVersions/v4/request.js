const requestV3 = require('../v3/request')

/**
 * ApiVersionsRequest (Version: 4) — wire-identical to v3.
 *
 * It exists only so the broker may report `SupportedFeatures.MinVersion = 0`, which v0–v3
 * responses omit (KAFKA-17011).
 * @see clients/src/main/resources/common/message/ApiVersionsRequest.json
 *
 * @param {object} [options]
 * @param {string} [options.clientSoftwareName]
 * @param {string} [options.clientSoftwareVersion]
 */
module.exports = (options) => ({ ...requestV3(options), apiVersion: 4 })

module.exports.schema = { ...requestV3.schema, version: 4, fields: [...requestV3.schema.fields] }
