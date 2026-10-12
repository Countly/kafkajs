const versions = {
  1: ({ topics, timeout }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ topics, timeout }), response }
  },
  2: ({ topics, timeout }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ topics, timeout }), response }
  },
  3: ({ topics, timeout }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ topics, timeout }), response }
  },
  4: ({ topics, timeout }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ topics, timeout }), response }
  },
  5: ({ topics, timeout }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ topics, timeout }), response }
  },
  6: ({ topics, timeout }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return { request: request({ topics, timeout }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
