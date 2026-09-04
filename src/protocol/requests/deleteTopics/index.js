const versions = {
  1: ({ topics, timeout }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ topics, timeout }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
