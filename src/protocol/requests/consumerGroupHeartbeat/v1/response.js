const { decode, parse } = require('../v0/response')

/**
 * ConsumerGroupHeartbeatResponse (Version: 1) — flexible (KIP-482), response header v1
 *
 * v1's body is byte-identical to v0's: only the *request* gains a field at v1, and the version
 * travels in the header, so this re-exports v0's reader rather than duplicating it.
 * @see clients/src/main/resources/common/message/ConsumerGroupHeartbeatResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ConsumerGroupHeartbeat',
  version: 1,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'MemberId', type: 'string', versions: '0+', nullable: true },
    { name: 'MemberEpoch', type: 'int32', versions: '0+' },
    { name: 'HeartbeatIntervalMs', type: 'int32', versions: '0+' },
    { name: 'Assignment', type: 'Assignment', versions: '0+', nullable: true },
    { name: 'Assignment.TopicPartitions', type: '[]TopicPartitions', versions: '0+' },
    { name: 'Assignment.TopicPartitions.TopicId', type: 'uuid', versions: '0+' },
    { name: 'Assignment.TopicPartitions.Partitions', type: '[]int32', versions: '0+' },
  ],
}
