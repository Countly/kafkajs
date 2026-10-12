const NETWORK_DELAY = 5000

/**
 * The SYNC_GROUP request blocks on the coordinator until the group leader delivers the
 * assignment. The client-wide requestTimeout (30s by default) kills slow rebalances client-side
 * and starts a non-converging rejoin loop, so the timeout is derived from the rebalance timeout
 * as JoinGroup's is - but CAPPED, because the rebalance timeout is now the 5-minute
 * max.poll.interval.ms while Java sends SyncGroup with the plain request.timeout.ms.
 * `||`, not `??`: a configured rebalanceTimeout of 0 falls back to sessionTimeout, unchanged.
 * The cap subsumes the previous overflow guard - min(timeout, 60000) + 5000 cannot leave the
 * safe-integer range however large sessionTimeout is configured - so that guard is removed
 * rather than left as unreachable code.
 */
const MAX_TIMEOUT = 60000

const requestTimeout = ({ rebalanceTimeout, sessionTimeout }) => {
  const timeout = rebalanceTimeout || sessionTimeout
  if (timeout == null) {
    return undefined
  }
  return Math.min(timeout, MAX_TIMEOUT) + NETWORK_DELAY
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
