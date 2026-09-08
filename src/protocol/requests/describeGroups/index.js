const versions = {
  0: ({ groupIds }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ groupIds }), response }
  },
  1: ({ groupIds }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ groupIds }), response }
  },
  2: ({ groupIds }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ groupIds }), response }
  },
  3: ({ groupIds, includeAuthorizedOperations } = {}) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ groupIds, includeAuthorizedOperations }), response }
  },
  4: ({ groupIds, includeAuthorizedOperations } = {}) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ groupIds, includeAuthorizedOperations }), response }
  },
  5: ({ groupIds, includeAuthorizedOperations } = {}) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ groupIds, includeAuthorizedOperations }), response }
  },
  6: ({ groupIds, includeAuthorizedOperations } = {}) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return { request: request({ groupIds, includeAuthorizedOperations }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
