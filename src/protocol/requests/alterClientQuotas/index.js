/**
 * Selects the negotiated AlterClientQuotas codec without silently dropping options.
 * @param {object} options negotiated version
 * @returns {Function} wire-field builder for a request/response codec pair
 * @throws {Error} when the selected version is not implemented
 */
const protocol =
  ({ version }) =>
  (fields) => ({
    request: require(`./v${version}/request`)(fields),
    response: require(`./v${version}/response`),
  })
module.exports = { versions: [0, 1], protocol }
