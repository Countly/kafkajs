/**
 * Version map for TxnOffsetCommit. v0-v2 builders take no group metadata (`generationId`,
 * `memberId`, `groupInstanceId`) and drop it if given, where the Java builder throws
 * UnsupportedVersionException below v3. No Kafka 4.0+ broker can make this client pick such a
 * version, since every one offers v3 and above.
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
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
