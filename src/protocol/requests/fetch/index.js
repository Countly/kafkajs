const ISOLATION_LEVEL = require('../../isolationLevel')

// For normal consumers, use -1
const REPLICA_ID = -1
const NETWORK_DELAY = 100

/**
 * The FETCH request can block up to maxWaitTime, which can be bigger than the configured
 * request timeout. It's safer to always use the maxWaitTime
 **/
const requestTimeout = (timeout) =>
  Number.isSafeInteger(timeout + NETWORK_DELAY) ? timeout + NETWORK_DELAY : timeout

/**
 * The Fetch response modules are stateless singletons, so the per-request decode options are
 * bound here rather than threaded through `Connection#send`.
 * @param {{ decode: Function, parse: Function }} response
 * @param {{ checkCrcs: boolean, maxDecompressedBatchBytes?: number }} options an omitted
 *   `maxDecompressedBatchBytes` is the default bound
 * @returns {{ decode: Function, parse: Function }}
 */
const withOptions = (response, options) => ({
  decode: (rawData) => response.decode(rawData, options),
  parse: response.parse,
})

const versions = {
  4: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return {
      request: request({ replicaId, isolationLevel, maxWaitTime, minBytes, maxBytes, topics }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  5: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return {
      request: request({ replicaId, isolationLevel, maxWaitTime, minBytes, maxBytes, topics }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  6: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return {
      request: request({ replicaId, isolationLevel, maxWaitTime, minBytes, maxBytes, topics }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  7: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v7/request')
    const response = require('./v7/response')
    return {
      request: request({
        replicaId,
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  8: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v8/request')
    const response = require('./v8/response')
    return {
      request: request({
        replicaId,
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  9: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v9/request')
    const response = require('./v9/response')
    return {
      request: request({
        replicaId,
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  10: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v10/request')
    const response = require('./v10/response')
    return {
      request: request({
        replicaId,
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  11: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    rackId,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v11/request')
    const response = require('./v11/response')
    return {
      request: request({
        replicaId,
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
        rackId,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  12: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    rackId,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v12/request')
    const response = require('./v12/response')
    return {
      request: request({
        replicaId,
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
        rackId,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  13: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    topicIds,
    rackId,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v13/request')
    const response = require('./v13/response')
    return {
      request: request({
        replicaId,
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
        topicIds,
        rackId,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  14: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    topicIds,
    rackId,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v14/request')
    const response = require('./v14/response')
    return {
      request: request({
        replicaId,
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
        topicIds,
        rackId,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  // v15 replaces the top-level ReplicaId with the replica-only ReplicaState tag (KIP-903): the
  // request builder below has no such field, so replicaId is kept in the destructured parameter
  // list (with its usual default) but deliberately not forwarded.
  15: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    topicIds,
    rackId,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v15/request')
    const response = require('./v15/response')
    return {
      request: request({
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
        topicIds,
        rackId,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  16: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    topicIds,
    rackId,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v16/request')
    const response = require('./v16/response')
    return {
      request: request({
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
        topicIds,
        rackId,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  17: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    topicIds,
    rackId,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v17/request')
    const response = require('./v17/response')
    return {
      request: request({
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
        topicIds,
        rackId,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
  18: ({
    replicaId = REPLICA_ID,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    sessionId = 0,
    sessionEpoch = -1,
    forgottenTopics = [],
    maxWaitTime,
    minBytes,
    maxBytes,
    topics,
    topicIds,
    rackId,
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) => {
    const request = require('./v18/request')
    const response = require('./v18/response')
    return {
      request: request({
        isolationLevel,
        sessionId,
        sessionEpoch,
        forgottenTopics,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics,
        topicIds,
        rackId,
      }),
      response: withOptions(response, { checkCrcs, maxDecompressedBatchBytes }),
      requestTimeout: requestTimeout(maxWaitTime),
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
