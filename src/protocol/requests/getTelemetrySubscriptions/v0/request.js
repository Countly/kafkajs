const Encoder = require('../../../encoder')
const { GetTelemetrySubscriptions: apiKey } = require('../../apiKeys')

/**
 * Encodes a flexible GetTelemetrySubscriptions v0 body with one UUID and terminal tags.
 * A null UUID encodes the all-zero first-request value; identity policy belongs to the caller.
 * @param {object} [options] request fields
 * @param {string|Buffer|null} [options.clientInstanceId=null] canonical UUID or sixteen bytes
 * @returns {object} request metadata and asynchronous encoder; no retries or shared state
 * @throws {Error} from encode if a supplied UUID is invalid
 */
module.exports = ({ clientInstanceId = null } = {}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'GetTelemetrySubscriptions',
  /** @returns {Promise<Encoder>} the body, without a request header */
  encode: async () => new Encoder().writeUuid(clientInstanceId).writeTaggedFields(),
})
module.exports.schema = {
  api: 'GetTelemetrySubscriptions',
  version: 0,
  direction: 'request',
  fields: [{ name: 'ClientInstanceId', type: 'uuid', versions: '0+' }],
}
