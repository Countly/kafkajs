const request = require('./v0/request')
const response = require('./v0/response')

/**
 * Builds the version-zero telemetry request and response definitions without encoding yet.
 * @param {object} [options] protocol fields; required byte values are checked at encode time
 * @returns {{request: object, response: object}} the independent request and response codecs
 */
const build = (options = {}) => ({ request: request(options), response })

module.exports = {
  versions: ['0'],
  /**
   * Selects the sole implemented telemetry codec; negotiation owns unsupported versions.
   * @param {{version: number}} selection the negotiated version
   * @returns {Function|undefined} the v0 builder, or undefined for another version
   */
  protocol: ({ version }) => (Number(version) === 0 ? build : undefined),
}
