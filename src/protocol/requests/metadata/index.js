const { failure, createErrorFromCode } = require('../../error')
const { checkPartitions: checkPartitionsV0 } = require('./v0/response')

const LEADER_NOT_AVAILABLE = 5

const versions = {
  0: ({ topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ topics }), response }
  },
  1: ({ topics }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ topics }), response }
  },
  2: ({ topics }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ topics }), response }
  },
  3: ({ topics }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ topics }), response }
  },
  4: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  5: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  6: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  7: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v7/request')
    const response = require('./v7/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  8: ({
    topics,
    allowAutoTopicCreation,
    includeTopicAuthorizedOperations,
    includeClusterAuthorizedOperations,
  }) => {
    const request = require('./v8/request')
    const response = require('./v8/response')
    return {
      request: request({
        topics,
        allowAutoTopicCreation,
        includeTopicAuthorizedOperations,
        includeClusterAuthorizedOperations,
      }),
      response,
    }
  },
  9: ({
    topics,
    allowAutoTopicCreation,
    includeTopicAuthorizedOperations,
    includeClusterAuthorizedOperations,
  }) => {
    const request = require('./v9/request')
    const response = require('./v9/response')
    return {
      request: request({
        topics,
        allowAutoTopicCreation,
        includeTopicAuthorizedOperations,
        includeClusterAuthorizedOperations,
      }),
      response,
    }
  },
  10: ({
    topics,
    allowAutoTopicCreation,
    includeTopicAuthorizedOperations,
    includeClusterAuthorizedOperations,
  }) => {
    const request = require('./v10/request')
    const response = require('./v10/response')
    return {
      request: request({
        topics,
        allowAutoTopicCreation,
        includeTopicAuthorizedOperations,
        includeClusterAuthorizedOperations,
      }),
      response,
    }
  },
  11: ({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }) => {
    const request = require('./v11/request')
    const response = require('./v11/response')
    return {
      request: request({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }),
      response,
    }
  },
  12: ({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }) => {
    const request = require('./v12/request')
    const response = require('./v12/response')
    return {
      request: request({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }),
      response,
    }
  },
  13: ({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }) => {
    const request = require('./v13/request')
    const response = require('./v13/response')
    return {
      request: request({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }),
      response,
    }
  },
}

/**
 * Parses the answer to a request for every topic. Only the top-level error code, which v13 added
 * (`REBOOTSTRAP_REQUIRED`, say), throws; a topic or partition error code stays in the data, so
 * one leaderless partition anywhere in the cluster does not fail the whole answer. The Java
 * `Admin#listTopics` reads such an answer the same way.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java
 * @param {object} data the decoded response
 * @returns {Promise<object>} data, unchanged
 * @throws {import('../../../errors').KafkaJSProtocolError} on a top-level error code
 */
const parseAllTopics = async (data) => {
  if (data.errorCode != null && failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

/**
 * Wraps a version's own parse so that a partition without a leader stays in the data. Every other
 * topic or partition error still throws, as the wrapped parse decides.
 * @param {(data: object) => Promise<object>|object} parse the version's own parse
 * @returns {(data: object) => Promise<object>} a parse that resolves with `data`, unchanged
 */
const allowingLeaderless = (parse) => async (data) => {
  await parse({
    ...data,
    topicMetadata: data.topicMetadata.map((topic) => ({
      ...topic,
      partitionMetadata: topic.partitionMetadata.filter(
        ({ partitionErrorCode }) => partitionErrorCode !== LEADER_NOT_AVAILABLE
      ),
    })),
  })
  return data
}

/**
 * Wraps a version's parse so that only a top-level error code throws: every topic and partition
 * error code stays in the data. The caller judges each topic's own error first and then calls
 * `checkPartitions` on the topics it keeps, so the strict precedence holds for it too: a
 * top-level error, then a topic error, then a partition error.
 * @param {(data: object) => Promise<object>|object} parse the version's own parse
 * @returns {(data: object) => Promise<object>} a parse that resolves with `data`, unchanged
 */
const retainingTopicErrors = (parse) => async (data) => {
  await parse({ ...data, topicMetadata: [] })
  return data
}

/**
 * The partition step of the strict parse, for topics a caller kept after judging their own errors.
 * @param {object[]} topicMetadata decoded topics answered without a topic error
 * @param {{ allowLeaderless?: boolean }} [options] lets a partition without a leader through
 * @returns {void}
 * @throws {import('../../../errors').KafkaJSProtocolError} the first other partition error code
 */
const checkPartitions = (topicMetadata, { allowLeaderless = false } = {}) =>
  checkPartitionsV0(
    allowLeaderless
      ? topicMetadata.map((topic) => ({
          ...topic,
          partitionMetadata: topic.partitionMetadata.filter(
            ({ partitionErrorCode }) => partitionErrorCode !== LEADER_NOT_AVAILABLE
          ),
        }))
      : topicMetadata
  )

/**
 * Builds one request at `version`. A request for every topic, an empty `topics` list written as
 * the null array, is parsed by `parseAllTopics`; a request that names topics keeps its version's
 * own parse, which throws the first topic, then partition, error code, unless `allowLeaderless`
 * lets a partition without a leader through and `retainTopicErrors` leaves topic and partition
 * errors to the caller (see `checkPartitions`).
 * @param {number|string} version
 * @param {{ topics?: Array<string|{ topicId: string }>, allowAutoTopicCreation?: boolean, lenient?: boolean, allowLeaderless?: boolean, retainTopicErrors?: boolean }} options
 *   `topics` is absent only when a request is built for its api key and version alone
 * @returns {{ request: object, response: { decode: Function, parse: Function } }}
 */
const build = (version, options) => {
  const built = versions[version](options)
  if (options.lenient || options.topics?.length === 0) {
    return { ...built, response: { ...built.response, parse: parseAllTopics } }
  }
  let { parse } = built.response
  if (options.allowLeaderless) parse = allowingLeaderless(parse)
  if (options.retainTopicErrors) parse = retainingTopicErrors(parse)
  return parse === built.response.parse
    ? built
    : { ...built, response: { ...built.response, parse } }
}

module.exports = {
  versions: Object.keys(versions),
  protocol:
    ({ version }) =>
    (options) =>
      build(version, options),
  checkPartitions,
}
