const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * MetadataResponse (Version: 8)
 *
 * Adds Topics.TopicAuthorizedOperations (8+, default -2147483648) after Partitions, and
 * ClusterAuthorizedOperations (8-10, default -2147483648) after Topics. The fork never requests
 * either bitfield (both request flags are sent `false`), so brokers return the default; nothing
 * in this client routes on these values yet.
 * @see clients/src/main/resources/common/message/MetadataResponse.json
 */

const decodeBroker = (decoder) => ({
  nodeId: decoder.readInt32(),
  host: decoder.readString(),
  port: decoder.readInt32(),
  rack: decoder.readString(),
})

const decodePartition = (decoder) => ({
  partitionErrorCode: decoder.readInt16(),
  partitionId: decoder.readInt32(),
  leader: decoder.readInt32(),
  leaderEpoch: decoder.readInt32(),
  replicas: decoder.readArray((d) => d.readInt32()),
  isr: decoder.readArray((d) => d.readInt32()),
  offlineReplicas: decoder.readArray((d) => d.readInt32()),
})

const decodeTopic = (decoder) => ({
  topicErrorCode: decoder.readInt16(),
  topic: decoder.readString(),
  isInternal: decoder.readBoolean(),
  partitionMetadata: decoder.readArray(decodePartition),
  topicAuthorizedOperations: decoder.readInt32(),
})

/**
 * Decodes a MetadataResponse v8 payload.
 * @param {Buffer} rawData
 * @returns {Promise<object>} decoded response, keyed the same as v7 plus
 *   `topicAuthorizedOperations` on each topic and `clusterAuthorizedOperations` at the top level
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    brokers: decoder.readArray(decodeBroker),
    clusterId: decoder.readString(),
    controllerId: decoder.readInt32(),
    topicMetadata: decoder.readArray(decodeTopic),
    clusterAuthorizedOperations: decoder.readInt32(),
  }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Metadata',
  version: 8,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '3+' },
    { name: 'Brokers', type: '[]MetadataResponseBroker', versions: '0+' },
    { name: 'Brokers.NodeId', type: 'int32', versions: '0+' },
    { name: 'Brokers.Host', type: 'string', versions: '0+' },
    { name: 'Brokers.Port', type: 'int32', versions: '0+' },
    { name: 'Brokers.Rack', type: 'string', versions: '1+', nullable: true },
    { name: 'ClusterId', type: 'string', versions: '2+', nullable: true },
    { name: 'ControllerId', type: 'int32', versions: '1+' },
    { name: 'Topics', type: '[]MetadataResponseTopic', versions: '0+' },
    { name: 'Topics.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.IsInternal', type: 'bool', versions: '1+' },
    { name: 'Topics.Partitions', type: '[]MetadataResponsePartition', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.LeaderId', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.LeaderEpoch', type: 'int32', versions: '7+' },
    { name: 'Topics.Partitions.ReplicaNodes', type: '[]int32', versions: '0+' },
    { name: 'Topics.Partitions.IsrNodes', type: '[]int32', versions: '0+' },
    { name: 'Topics.Partitions.OfflineReplicas', type: '[]int32', versions: '5+' },
    { name: 'Topics.TopicAuthorizedOperations', type: 'int32', versions: '8+' },
    { name: 'ClusterAuthorizedOperations', type: 'int32', versions: '8-10' },
  ],
}
