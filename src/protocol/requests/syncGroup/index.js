const NETWORK_DELAY = 5000

/**
 * The SYNC_GROUP request blocks on the coordinator until the group leader delivers the
 * assignment, which can take up to rebalanceTimeout. Using the client-wide requestTimeout
 * (30s by default) kills slow rebalances client-side and starts a non-converging rejoin loop.
 * Mirrors the JoinGroup rule in ../joinGroup/index.js, including its overflow guard:
 * sessionTimeout can be configured as Number.MAX_SAFE_INTEGER.
 */
const requestTimeout = ({ rebalanceTimeout, sessionTimeout }) => {
  const timeout = rebalanceTimeout || sessionTimeout
  if (timeout == null) {
    return undefined
  }
  return Number.isSafeInteger(timeout + NETWORK_DELAY) ? timeout + NETWORK_DELAY : timeout
}

const versions = {
  0: ({ groupId, generationId, memberId, groupAssignment, sessionTimeout, rebalanceTimeout }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return {
      request: request({ groupId, generationId, memberId, groupAssignment }),
      response,
      requestTimeout: requestTimeout({ rebalanceTimeout, sessionTimeout }),
    }
  },
  1: ({ groupId, generationId, memberId, groupAssignment, sessionTimeout, rebalanceTimeout }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return {
      request: request({ groupId, generationId, memberId, groupAssignment }),
      response,
      requestTimeout: requestTimeout({ rebalanceTimeout, sessionTimeout }),
    }
  },
  2: ({ groupId, generationId, memberId, groupAssignment, sessionTimeout, rebalanceTimeout }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return {
      request: request({ groupId, generationId, memberId, groupAssignment }),
      response,
      requestTimeout: requestTimeout({ rebalanceTimeout, sessionTimeout }),
    }
  },
  3: ({
    groupId,
    generationId,
    memberId,
    groupInstanceId,
    groupAssignment,
    sessionTimeout,
    rebalanceTimeout,
  }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return {
      request: request({ groupId, generationId, memberId, groupInstanceId, groupAssignment }),
      response,
      requestTimeout: requestTimeout({ rebalanceTimeout, sessionTimeout }),
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
