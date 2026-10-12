const versions = {
  1: ({ filters }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ filters }), response }
  },
  2: ({ filters }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ filters }), response }
  },
  3: ({ filters }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ filters }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
