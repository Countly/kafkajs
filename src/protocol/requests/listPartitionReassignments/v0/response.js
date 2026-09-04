const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * ListPartitionReassignments Response (Version: 0) => throttle_time_ms error_code error_message [topics] TAG_BUFFER
 *  throttle_time_ms => INT32
 *  error_code => INT16
 *  error_message => COMPACT_NULLABLE_STRING
 *  topics => name [partitions] TAG_BUFFER
 *    name => COMPACT_STRING
 *    partitions => partition_index [replicas] [adding_replicas] [removing_replicas] TAG_BUFFER
 *      partition_index => INT32
 *       replicas => INT32
 *       adding_replicas => INT32
 *       removing_replicas => INT32
 */

const decodeReplicas = (decoder) => {
  return decoder.readInt32()
}

const decodePartitions = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
    replicas: decoder.readUVarIntArray(decodeReplicas),
    addingReplicas: decoder.readUVarIntArray(decodeReplicas),
    removingReplicas: decoder.readUVarIntArray(decodeReplicas),
  }

  // Read tagged fields
  decoder.readTaggedFields()
  return partition
}

const decodeTopics = (decoder) => {
  const topic = {
    name: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartitions),
  }

  // Read tagged fields
  decoder.readTaggedFields()
  return topic
}
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)

  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  // Read error message
  decoder.readUVarIntString()
  return {
    throttleTime,
    errorCode,
    topics: decoder.readUVarIntArray(decodeTopics),
  }
}

const parse = async (data) => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ListPartitionReassignments',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Topics', type: '[]OngoingTopicReassignment', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]OngoingPartitionReassignment', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.Replicas', type: '[]int32', versions: '0+' },
    { name: 'Topics.Partitions.AddingReplicas', type: '[]int32', versions: '0+' },
    { name: 'Topics.Partitions.RemovingReplicas', type: '[]int32', versions: '0+' },
  ],
}
