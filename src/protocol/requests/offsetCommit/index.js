const { namesById } = require('../topicIds')

// This value signals to the broker that its default configuration should be used.
const RETENTION_TIME = -1

const versions = {
  2: ({ groupId, groupGenerationId, memberId, retentionTime = RETENTION_TIME, topics }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        retentionTime,
        topics,
      }),
      response,
    }
  },
  3: ({ groupId, groupGenerationId, memberId, retentionTime = RETENTION_TIME, topics }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        retentionTime,
        topics,
      }),
      response,
    }
  },
  4: ({ groupId, groupGenerationId, memberId, retentionTime = RETENTION_TIME, topics }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        retentionTime,
        topics,
      }),
      response,
    }
  },
  5: ({ groupId, groupGenerationId, memberId, topics }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        topics,
      }),
      response,
    }
  },
  6: ({ groupId, groupGenerationId, memberId, topics }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        topics,
      }),
      response,
    }
  },
  7: ({ groupId, groupGenerationId, memberId, groupInstanceId, topics }) => {
    const request = require('./v7/request')
    const response = require('./v7/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        groupInstanceId,
        topics,
      }),
      response,
    }
  },
  8: ({ groupId, groupGenerationId, memberId, groupInstanceId, topics }) => {
    const request = require('./v8/request')
    const response = require('./v8/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        groupInstanceId,
        topics,
      }),
      response,
    }
  },
  // `groupGenerationId` carries the member epoch under the KIP-848 consumer protocol; the bytes
  // are v8's.
  9: ({ groupId, groupGenerationId, memberId, groupInstanceId, topics }) => {
    const request = require('./v9/request')
    const response = require('./v9/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        groupInstanceId,
        topics,
      }),
      response,
    }
  },
  // Keyed by topic id: the request's own name-to-id map is bound onto the response codec, so the
  // decoded answer still names its topics.
  10: ({ groupId, groupGenerationId, memberId, groupInstanceId, topics, topicIds }) => {
    const request = require('./v10/request')
    const response = require('./v10/response')
    return {
      request: request({
        groupId,
        groupGenerationId,
        memberId,
        groupInstanceId,
        topics,
        topicIds,
      }),
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
