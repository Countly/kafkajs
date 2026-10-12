const versions = {
  1: ({ creations }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ creations }), response }
  },
  2: ({ creations }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ creations }), response }
  },
  3: ({ creations }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ creations }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
