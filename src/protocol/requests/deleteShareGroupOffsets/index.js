/**
 * Select the sole released wire codec factory after version negotiation.
 * @param {{version: number}} options Negotiated version, not the broker's full source range.
 * @returns {((options?: object) => object)|undefined} Request/response builder, or undefined for another version.
 */
const protocol = ({ version }) => {
  const versions = {
    0: (options = {}) => ({
      request: require('./v0/request')(options),
      response: require('./v0/response'),
    }),
  }
  return versions[version]
}
module.exports = { versions: ['0'], protocol }
