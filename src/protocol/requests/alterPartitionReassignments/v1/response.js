const { KafkaJSInvariantViolation } = require('../../../../errors')
const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * AlterPartitionReassignments Response (Version: 1) => throttle_time_ms allow_replication_factor_change error_code error_message [responses] TAG_BUFFER
 * throttle_time_ms => INT32
 * allow_replication_factor_change => BOOLEAN (v1+)
 * error_code => INT16
 * error_message => COMPACT_NULLABLE_STRING
 * responses => name [partitions] TAG_BUFFER
 *  name => COMPACT_STRING
 *  partitions => partition_index error_code error_message TAG_BUFFER
 *    partition_index => INT32
 *    error_code => INT16
 *    error_message => COMPACT_NULLABLE_STRING
 */

const decodeResponses = (decoder) => {
  const response = {
    topic: decoder.readUVarIntString(),
    partitions: Decoder.requireArray(
      decoder.readUVarIntArray(decodePartitions),
      'AlterPartitionReassignmentsResponse.Responses.Partitions'
    ),
  }

  decoder.readTaggedFields()
  return response
}

const decodePartitions = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
  }
  decoder.readUVarIntString()
  decoder.readTaggedFields()
  return partition
}

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const allowReplicationFactorChange = decoder.readBoolean()
  const errorCode = decoder.readInt16()
  decoder.readUVarIntString()
  const responses = Decoder.requireArray(
    decoder.readUVarIntArray(decodeResponses),
    'AlterPartitionReassignmentsResponse.Responses'
  )
  decoder.readTaggedFields()
  if (decoder.canReadBytes(1))
    throw new KafkaJSInvariantViolation('AlterPartitionReassignments response has trailing bytes')
  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    allowReplicationFactorChange,
    errorCode,
    responses,
  }
}

module.exports = { decode, parse }

module.exports.schema = {
  api: 'AlterPartitionReassignments',
  version: 1,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'AllowReplicationFactorChange', type: 'bool', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Responses', type: '[]ReassignableTopicResponse', versions: '0+' },
    { name: 'Responses.Name', type: 'string', versions: '0+' },
    { name: 'Responses.Partitions', type: '[]ReassignablePartitionResponse', versions: '0+' },
    { name: 'Responses.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Responses.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.Partitions.ErrorMessage', type: 'string', versions: '0+', nullable: true },
  ],
}
