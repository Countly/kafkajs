const { namesById } = require('../topicIds')

const versions = {
  1: ({ groupId, topics }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ groupId, topics }), response }
  },
  2: ({ groupId, topics }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ groupId, topics }), response }
  },
  3: ({ groupId, topics }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ groupId, topics }), response }
  },
  4: ({ groupId, topics }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ groupId, topics }), response }
  },
  5: ({ groupId, topics }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ groupId, topics }), response }
  },
  6: ({ groupId, topics }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return { request: request({ groupId, topics }), response }
  },
  7: ({ groupId, topics, requireStable }) => {
    const request = require('./v7/request')
    const response = require('./v7/response')
    return { request: request({ groupId, topics, requireStable }), response }
  },
  8: ({ groupId, topics, groups, requireStable }) => {
    const request = require('./v8/request')
    const response = require('./v8/response')
    return {
      request: request({ groupId, topics, groups, requireStable }),
      response:
        groups === undefined
          ? response
          : {
              decode: (raw) => response.decode(raw, { groups }),
              parse: async (data) => data,
            },
    }
  },
  9: ({ groupId, topics, groups, memberId, memberEpoch, requireStable }) => {
    const request = require('./v9/request')
    const response = require('./v9/response')
    return {
      request: request({ groupId, topics, groups, memberId, memberEpoch, requireStable }),
      response:
        groups === undefined
          ? response
          : {
              decode: (raw) => response.decode(raw, { groups }),
              parse: async (data) => data,
            },
    }
  },
  // Keyed by topic id: the request's own name-to-id map is bound onto the response codec, so the
  // decoded answer still names its topics.
  10: ({ groupId, topics, topicIds, memberId, memberEpoch, requireStable }) => {
    const request = require('./v10/request')
    const response = require('./v10/response')
    return {
      request: request({ groupId, topics, topicIds, memberId, memberEpoch, requireStable }),
      response: {
        decode: (rawData) => response.decode(rawData, { topicNames: namesById(topicIds) }),
        parse: response.parse,
      },
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
