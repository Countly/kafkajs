const Encoder = require('../../../encoder')
const { CreatePartitions: apiKey } = require('../../apiKeys')

/**
 * CreatePartitions Request (Version: 2) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   Count => INT32 (0+)
 *   Assignments => COMPACT_NULLABLE_ARRAY (0+)
 *     BrokerIds => COMPACT_ARRAY of INT32 (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TimeoutMs => INT32 (0+)
 * ValidateOnly => BOOLEAN (0+)
 * TAG_BUFFER
 *
 * The v1 fields in compact framing; v3 sends the same bytes. An absent, null or empty
 * `assignments` is written as the compact null (0x00), which asks the controller to choose the
 * replicas: the 4.3 controller reads any non-null list as an explicit assignment and rejects it
 * when its size is not the number of partitions added, so the empty list (0x01) is never written.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/CreatePartitionsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ topic: string, count: number, assignments?: number[][]|null }[]} options.topicPartitions
 * @param {boolean} [options.validateOnly=false]
 * @param {number} [options.timeout=5000]
 * @returns {object} the protocol request
 */
module.exports = ({ topicPartitions, validateOnly = false, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'CreatePartitions',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(topicPartitions.map(encodeTopicPartitions))
      .writeInt32(timeout)
      .writeBoolean(validateOnly)
      .writeTaggedFields()
  },
})

const encodeTopicPartitions = ({ topic, count, assignments }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeInt32(count)
    .writeUVarIntArray(
      assignments && assignments.length > 0 ? assignments.map(encodeAssignment) : null
    )
    .writeTaggedFields()
}

const encodeAssignment = (brokerIds) => {
  return new Encoder().writeUVarIntArray(brokerIds, 'int32').writeTaggedFields()
}

module.exports.schema = {
  api: 'CreatePartitions',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]CreatePartitionsTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Count', type: 'int32', versions: '0+' },
    {
      name: 'Topics.Assignments',
      type: '[]CreatePartitionsAssignment',
      versions: '0+',
      nullable: true,
    },
    { name: 'Topics.Assignments.BrokerIds', type: '[]int32', versions: '0+' },
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
    { name: 'ValidateOnly', type: 'bool', versions: '0+' },
  ],
}
