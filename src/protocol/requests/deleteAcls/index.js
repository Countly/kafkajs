const versions = {
  1: ({ filters }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ filters }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
