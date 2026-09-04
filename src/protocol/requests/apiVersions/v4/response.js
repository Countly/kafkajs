const responseV3 = require('../v3/response')

/**
 * ApiVersionsResponse (Version: 4) — wire-identical to v3.
 * @see clients/src/main/resources/common/message/ApiVersionsResponse.json
 */
module.exports = {
  decode: responseV3.decode,
  parse: responseV3.parse,
}

module.exports.schema = { ...responseV3.schema, version: 4, fields: [...responseV3.schema.fields] }
