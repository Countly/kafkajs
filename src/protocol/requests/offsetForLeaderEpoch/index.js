const versions = {
  3: ({ topics }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ topics }), response }
  },
  4: ({ topics }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ topics }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
