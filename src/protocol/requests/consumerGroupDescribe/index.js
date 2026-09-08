const versions = {
  0: ({ groupIds, includeAuthorizedOperations }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ groupIds, includeAuthorizedOperations }), response }
  },
  1: ({ groupIds, includeAuthorizedOperations }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ groupIds, includeAuthorizedOperations }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
