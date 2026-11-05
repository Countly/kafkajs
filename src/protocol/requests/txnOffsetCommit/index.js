const { namesById } = require('../topicIds')

/**
 * Version map for TxnOffsetCommit. v0-v2 builders take no group metadata (`generationId`,
 * `memberId`, `groupInstanceId`) and drop it if given, where the Java builder throws
 * UnsupportedVersionException below v3. No Kafka 4.0+ broker can make this client pick such a
 * version, since every one offers v3 and above.
 *
 * v6 keys topics by id (KIP-1319): its builder also takes `topicIds`, the request's own
 * name-to-id map, and binds the response to the topics that request named - captured when the
 * request is built, so a metadata change while it is in flight cannot rename an answer - and
 * refuses an answer that does not correlate with them exactly.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/requests/TxnOffsetCommitRequest.java
 */
const versions = {
  0: ({ transactionalId, groupId, producerId, producerEpoch, topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return {
      request: request({ transactionalId, groupId, producerId, producerEpoch, topics }),
      response,
    }
  },
  1: ({ transactionalId, groupId, producerId, producerEpoch, topics }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return {
      request: request({ transactionalId, groupId, producerId, producerEpoch, topics }),
      response,
    }
  },
  2: ({ transactionalId, groupId, producerId, producerEpoch, topics }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return {
      request: request({ transactionalId, groupId, producerId, producerEpoch, topics }),
      response,
    }
  },
  3: ({
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    generationId,
    memberId,
    groupInstanceId,
    topics,
  }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return {
      request: request({
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        generationId,
        memberId,
        groupInstanceId,
        topics,
      }),
      response,
    }
  },
  4: ({
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    generationId,
    memberId,
    groupInstanceId,
    topics,
  }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return {
      request: request({
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        generationId,
        memberId,
        groupInstanceId,
        topics,
      }),
      response,
    }
  },
  5: ({
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    generationId,
    memberId,
    groupInstanceId,
    topics,
  }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return {
      request: request({
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        generationId,
        memberId,
        groupInstanceId,
        topics,
      }),
      response,
    }
  },
  6: ({
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    generationId,
    memberId,
    groupInstanceId,
    topics,
    topicIds,
  }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    const topicNames = namesById(topicIds)
    return {
      request: request({
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        generationId,
        memberId,
        groupInstanceId,
        topics,
        topicIds,
      }),
      response: {
        decode: (rawData) => response.decode(rawData, { topicNames }),
        parse: async (data) => {
          response.validate(data, topics)
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
