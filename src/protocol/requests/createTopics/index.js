const versions = {
  2: ({ topics, validateOnly, timeout }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ topics, validateOnly, timeout }), response }
  },
  3: ({ topics, validateOnly, timeout }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ topics, validateOnly, timeout }), response }
  },
  4: ({ topics, validateOnly, timeout }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ topics, validateOnly, timeout }), response }
  },
  5: ({ topics, validateOnly, timeout }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ topics, validateOnly, timeout }), response }
  },
  6: ({ topics, validateOnly, timeout }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return { request: request({ topics, validateOnly, timeout }), response }
  },
  7: ({ topics, validateOnly, timeout }) => {
    const request = require('./v7/request')
    const response = require('./v7/response')
    return { request: request({ topics, validateOnly, timeout }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
