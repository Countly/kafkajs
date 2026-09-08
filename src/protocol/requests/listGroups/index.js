const versions = {
  0: () => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request(), response }
  },
  1: () => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request(), response }
  },
  2: () => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request(), response }
  },
  3: () => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request(), response }
  },
  4: ({ states } = {}) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ states }), response }
  },
  5: ({ states, types } = {}) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ states, types }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
