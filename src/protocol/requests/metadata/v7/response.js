const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * MetadataResponse (Version: 7)
 *
 * Adds Topics.Partitions.LeaderEpoch (7+, default -1) after LeaderId. From v6 on, brokers send
 * the response before throttling (KIP-219), which is why throttleTime is surfaced as
 * clientSideThrottleTime.
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
})

/**
 * Decodes a MetadataResponse v7 payload.
 * @param {Buffer} rawData
 * @returns {Promise<object>} decoded response, keyed the same as v0-v6 plus `leaderEpoch` on
 *   each partition
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
  }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Metadata',
  version: 7,
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
  ],
}
