const versions = {
  1: ({ creations }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ creations }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
