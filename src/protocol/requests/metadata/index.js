const { failure, createErrorFromCode } = require('../../error')

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
  8: ({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }) => {
    const request = require('./v8/request')
    const response = require('./v8/response')
    return {
      request: request({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }),
      response,
    }
  },
  9: ({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }) => {
    const request = require('./v9/request')
    const response = require('./v9/response')
    return {
      request: request({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }),
      response,
    }
  },
  10: ({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }) => {
    const request = require('./v10/request')
    const response = require('./v10/response')
    return {
      request: request({ topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }),
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
 * Builds one request at `version`. A request for every topic, an empty `topics` list written as
 * the null array, is parsed by `parseAllTopics`; a request that names topics keeps its version's
 * own parse, which throws the first topic, then partition, error code, unless `allowLeaderless`
 * lets a partition without a leader through.
 * @param {number|string} version
 * @param {{ topics?: Array<string|{ topicId: string }>, allowAutoTopicCreation?: boolean, lenient?: boolean, allowLeaderless?: boolean }} options
 *   `topics` is absent only when a request is built for its api key and version alone
 * @returns {{ request: object, response: { decode: Function, parse: Function } }}
 */
const build = (version, options) => {
  const built = versions[version](options)
  if (options.lenient || options.topics?.length === 0) {
    return { ...built, response: { ...built.response, parse: parseAllTopics } }
  }
  return options.allowLeaderless
    ? { ...built, response: { ...built.response, parse: allowingLeaderless(built.response.parse) } }
    : built
}

module.exports = {
  versions: Object.keys(versions),
  protocol:
    ({ version }) =>
    (options) =>
      build(version, options),
}
