const versions = {
  1: (options) => ({
    request: require('./v1/request')(options),
    response: require('./v1/response'),
  }),
  2: (options) => ({
    request: require('./v2/request')(options),
    response: require('./v2/response'),
  }),
  3: (options) => ({
    request: require('./v3/request')(options),
    response: require('./v3/response'),
  }),
  4: (options) => ({
    request: require('./v4/request')(options),
    response: require('./v4/response'),
  }),
  5: (options) => ({
    request: require('./v5/request')(options),
    response: require('./v5/response'),
  }),
}
module.exports = { versions: Object.keys(versions), protocol: ({ version }) => versions[version] }
