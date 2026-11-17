const NETWORK_DELAY = 5000

/**
 * The SYNC_GROUP request blocks on the coordinator until the group leader delivers the
 * assignment, bounded by the group's rebalance timeout, so its per-request allowance is the
 * rebalance timeout plus the network delay, as JoinGroup's is. The rebalance round deadline that
 * started at round entry can end the operation sooner. A peer with a larger rebalance timeout can
 * hold the group longer than this member's allowance.
 * `||`, not `??`: a configured rebalanceTimeout of 0 falls back to sessionTimeout.
 * @param {{ rebalanceTimeout?: number, sessionTimeout?: number }} options the wire values
 * @returns {number|undefined} the allowance in ms, the raw timeout when adding the delay would
 *   leave the safe-integer range, or undefined when neither timeout is given (client default)
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
  4: (options) => ({
    request: require('./v4/request')(options),
    response: require('./v4/response'),
    requestTimeout: requestTimeout(options),
  }),
  5: (options) => ({
    request: require('./v5/request')(options),
    response: require('./v5/response'),
    requestTimeout: requestTimeout(options),
  }),
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
