const versions = {
  1: (options) => ({
    request: require('./v1/request')(options),
    response: require('./v1/response'),
  }),
  2: (options) => ({
    request: require('./v2/request')(options),
    response: require('./v2/response'),
  }),
}
module.exports = { versions: Object.keys(versions), protocol: ({ version }) => versions[version] }
