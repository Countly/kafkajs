/**
 * Selects the one released UnregisterController codec after version negotiation.
 * @param {{version: number}} options Negotiated version.
 * @returns {((options?: object) => {request: object, response: object})|undefined} Request/response
 *   builder, or undefined for a version this client does not implement.
 */
const protocol = ({ version }) => {
  const versions = {
    0: (options) => ({
      request: require('./v0/request')(options),
      response: require('./v0/response'),
    }),
  }
  return versions[version]
}

module.exports = { versions: ['0'], protocol }
