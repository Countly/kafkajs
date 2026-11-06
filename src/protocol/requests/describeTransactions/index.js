const versions = {
  0: (options) => ({
    request: require('./v0/request')(options),
    response: require('./v0/response'),
  }),
}

module.exports = {
  versions: Object.keys(versions),
  /** Selects the builder. @param {object} options version @returns {Function} builder @throws {Error} unsupported internal version. */
  protocol: ({ version }) => versions[version],
}
