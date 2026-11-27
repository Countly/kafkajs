const COORDINATOR_TYPES = require('../../coordinatorTypes')

/**
 * From v4 a broker echoes the requested key on every answer, so the decoder checks it. v0 to v3
 * carry no key.
 * @param {{ decode: Function, parse: Function }} response a v4 or later codec
 * @param {string} key the key the request asks for
 * @returns {{ decode: (rawData: Buffer) => Promise<object>, parse: Function }}
 */
const keyed = ({ decode, parse }, key) => ({ decode: (rawData) => decode(rawData, { key }), parse })

const versions = {
  0: ({ groupId }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ groupId }), response }
  },
  1: ({ groupId, coordinatorType = COORDINATOR_TYPES.GROUP }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ coordinatorKey: groupId, coordinatorType }), response }
  },
  2: ({ groupId, coordinatorType = COORDINATOR_TYPES.GROUP }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ coordinatorKey: groupId, coordinatorType }), response }
  },
  3: ({ groupId, coordinatorType = COORDINATOR_TYPES.GROUP }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ coordinatorKey: groupId, coordinatorType }), response }
  },
  4: ({ groupId, coordinatorType = COORDINATOR_TYPES.GROUP }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return {
      request: request({ coordinatorKey: groupId, coordinatorType }),
      response: keyed(response, groupId),
    }
  },
  5: ({ groupId, coordinatorType = COORDINATOR_TYPES.GROUP }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return {
      request: request({ coordinatorKey: groupId, coordinatorType }),
      response: keyed(response, groupId),
    }
  },
  6: ({ groupId, coordinatorType = COORDINATOR_TYPES.GROUP }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return {
      request: request({ coordinatorKey: groupId, coordinatorType }),
      response: keyed(response, groupId),
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
