const versions = {
  1: ({ topics, timeout }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ topics, timeout }), response }
  },
  2: ({ topics, timeout }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ topics, timeout }), response }
  },
  3: ({ topics, timeout }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ topics, timeout }), response }
  },
  4: ({ topics, timeout }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ topics, timeout }), response }
  },
  5: ({ topics, timeout }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ topics, timeout }), response }
  },
  6: ({ topics, topicIds, timeout }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    if (topicIds === undefined) return { request: request({ topics, timeout }), response }
    const { KafkaJSInvariantViolation } = require('../../../errors')
    const expected = new Set(topicIds)
    return {
      request: request({ topics, topicIds, timeout }),
      response: {
        ...response,
        /** Validates the complete attempted ID set before the legacy parser filters failures.
         * @param {object} data all decoded success and failure rows
         * @returns {Promise<object>} legacy parsed response
         * @throws {KafkaJSInvariantViolation} missing, foreign or duplicate result identity
         */
        parse: async (data) => {
          const seen = new Set()
          for (const row of data.topicErrors) {
            if (!expected.has(row.topicId) || seen.has(row.topicId))
              throw new KafkaJSInvariantViolation('Invalid deletion response identity')
            seen.add(row.topicId)
          }
          if (seen.size !== expected.size)
            throw new KafkaJSInvariantViolation('Incomplete deletion response identities')
          return response.parse(data)
        },
      },
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
