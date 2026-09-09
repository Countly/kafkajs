const versions = {
  0: ({
    groupId,
    memberId,
    memberEpoch,
    instanceId,
    rackId,
    rebalanceTimeoutMs,
    subscribedTopicNames,
    serverAssignor,
    topicPartitions,
  } = {}) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return {
      request: request({
        groupId,
        memberId,
        memberEpoch,
        instanceId,
        rackId,
        rebalanceTimeoutMs,
        subscribedTopicNames,
        serverAssignor,
        topicPartitions,
      }),
      response,
    }
  },
  1: ({
    groupId,
    memberId,
    memberEpoch,
    instanceId,
    rackId,
    rebalanceTimeoutMs,
    subscribedTopicNames,
    subscribedTopicRegex,
    serverAssignor,
    topicPartitions,
  } = {}) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return {
      request: request({
        groupId,
        memberId,
        memberEpoch,
        instanceId,
        rackId,
        rebalanceTimeoutMs,
        subscribedTopicNames,
        subscribedTopicRegex,
        serverAssignor,
        topicPartitions,
      }),
      response,
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
