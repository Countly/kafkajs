const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * MetadataResponse (Version: 12) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (3+)
 * Brokers => COMPACT_ARRAY (0+)
 *   NodeId => INT32, Host => COMPACT_STRING, Port => INT32, Rack => COMPACT_NULLABLE_STRING (1+)
 *   TAG_BUFFER
 * ClusterId => COMPACT_NULLABLE_STRING (2+)
 * ControllerId => INT32 (1+)
 * Topics => COMPACT_ARRAY (0+)
 *   ErrorCode => INT16, Name => COMPACT_STRING (nullable from 12), TopicId => UUID (10+),
 *   IsInternal => BOOLEAN (1+)
 *   Partitions => COMPACT_ARRAY (0+)
 *     ErrorCode, PartitionIndex, LeaderId, LeaderEpoch (7+), ReplicaNodes, IsrNodes,
 *     OfflineReplicas (5+)
 *     TAG_BUFFER
 *   TopicAuthorizedOperations => INT32 (8+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * TopicId follows Name here, but precedes it in the request struct. A null Name (decoded as
 * `topic: null`) arrives when the broker reports a topic-level error for a topic the client asked
 * for by id; `parse` throws on that error code before any caller reads the name.
 * @see clients/src/main/resources/common/message/MetadataResponse.json
 */

const decodeInt32 = (decoder) => decoder.readInt32()

const decodeBroker = (decoder) => {
  const broker = {
    nodeId: decoder.readInt32(),
    host: decoder.readUVarIntString(),
    port: decoder.readInt32(),
    rack: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return broker
}

const decodePartition = (decoder) => {
  const partition = {
    partitionErrorCode: decoder.readInt16(),
    partitionId: decoder.readInt32(),
    leader: decoder.readInt32(),
    leaderEpoch: decoder.readInt32(),
    replicas: decoder.readUVarIntArray(decodeInt32) || [],
    isr: decoder.readUVarIntArray(decodeInt32) || [],
    offlineReplicas: decoder.readUVarIntArray(decodeInt32) || [],
  }

  decoder.readTaggedFields()
  return partition
}

const decodeTopic = (decoder) => {
  const topic = {
    topicErrorCode: decoder.readInt16(),
    topic: decoder.readUVarIntString(),
    topicId: decoder.readUuid(),
    isInternal: decoder.readBoolean(),
    partitionMetadata: decoder.readUVarIntArray(decodePartition) || [],
    topicAuthorizedOperations: decoder.readInt32(),
  }

  decoder.readTaggedFields()
  return topic
}

/**
 * Decodes a MetadataResponse v12 payload.
 * @param {Buffer} rawData
 * @returns {Promise<object>} decoded response, keyed the same as v11
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const brokers = decoder.readUVarIntArray(decodeBroker) || []
  const clusterId = decoder.readUVarIntString()
  const controllerId = decoder.readInt32()
  const topicMetadata = decoder.readUVarIntArray(decodeTopic) || []

  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    brokers,
    clusterId,
    controllerId,
    topicMetadata,
  }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Metadata',
  version: 12,
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
    { name: 'Topics.Name', type: 'string', versions: '0+', nullable: true },
    { name: 'Topics.TopicId', type: 'uuid', versions: '10+' },
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
  ],
}
