const versions = {
  0: () => ({ request: require('./v0/request'), response: require('./v0/response') }),
  1: () => ({ request: require('./v1/request'), response: require('./v1/response') }),
  2: () => ({ request: require('./v2/request'), response: require('./v2/response') }),
}
/** Selects one implemented body pair. @param {object} options version @returns {Function} request/response builder @throws {Error} invalid version */
module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => {
    const { request, response } = versions[version]()
    return (options) => ({ request: request(options), response })
  },
}
