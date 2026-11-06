/**
 * Select the sole released wire codec factory after version negotiation.
 * @param {{version: number}} options Negotiated version, not the broker's full source range.
 * @returns {((options?: object) => object)|undefined} Request/response builder, or undefined for another version.
 */
const protocol = ({ version }) => {
  const versions = {
    2: (options = {}) => ({
      request: require('./v2/request')(options),
      response: require('./v2/response'),
    }),
  }
  return versions[version]
}
module.exports = { versions: ['2'], protocol }
