const responseV3 = require('../v3/response')

/**
 * ApiVersionsResponse (Version: 5) — wire-identical to v3 and v4, including the feature tags and
 * the v0-shaped body of an UNSUPPORTED_VERSION answer.
 *
 * It exists because v5 is the version whose answer may be REBOOTSTRAP_REQUIRED (129): a broker
 * that disagrees with the cluster id or node id the request named answers with that code and an
 * empty api list, which `parse` raises as a typed error. The response header stays v0.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1242%3A+Rebootstrap+on+cluster+or+node+mismatch
 * @see clients/src/main/resources/common/message/ApiVersionsResponse.json (Apache Kafka 4.4.0)
 */
module.exports = {
  decode: responseV3.decode,
  parse: responseV3.parse,
}

module.exports.schema = { ...responseV3.schema, version: 5, fields: [...responseV3.schema.fields] }
