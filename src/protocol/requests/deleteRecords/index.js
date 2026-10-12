const versions = {
  0: ({ topics, timeout }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ topics, timeout }), response: response({ topics }) }
  },
  1: ({ topics, timeout }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ topics, timeout }), response: response({ topics }) }
  },
  2: ({ topics, timeout }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ topics, timeout }), response: response({ topics }) }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
