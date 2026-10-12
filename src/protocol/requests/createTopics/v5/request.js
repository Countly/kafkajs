const Encoder = require('../../../encoder')
const { CreateTopics: apiKey } = require('../../apiKeys')

/**
 * CreateTopics Request (Version: 5) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   NumPartitions => INT32 (0+)
 *   ReplicationFactor => INT16 (0+)
 *   Assignments => COMPACT_ARRAY (0+)
 *     PartitionIndex => INT32 (0+)
 *     BrokerIds => COMPACT_ARRAY of INT32 (0+)
 *     TAG_BUFFER
 *   Configs => COMPACT_ARRAY (0+)
 *     Name => COMPACT_STRING (0+)
 *     Value => COMPACT_NULLABLE_STRING (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * timeoutMs => INT32 (0+)
 * validateOnly => BOOLEAN (1+)
 * TAG_BUFFER
 *
 * The v4 fields in compact framing; v6 and v7 send the same bytes. A topic that names neither
 * partitions nor replication factor sends -1 for both and no assignments, the broker's defaults.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/CreateTopicsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ topic: string, numPartitions?: number, replicationFactor?: number,
 *   replicaAssignment?: { partition: number, replicas: number[] }[],
 *   configEntries?: { name: string, value: string|null }[] }[]} options.topics
 * @param {boolean} [options.validateOnly=false]
 * @param {number} [options.timeout=5000]
 * @returns {object} the protocol request
 */
module.exports = ({ topics, validateOnly = false, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 5,
  apiName: 'CreateTopics',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(topics.map(encodeTopics))
      .writeInt32(timeout)
      .writeBoolean(validateOnly)
      .writeTaggedFields()
  },
})

const encodeTopics = ({
  topic,
  numPartitions = -1,
  replicationFactor = -1,
  replicaAssignment = [],
  configEntries = [],
}) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeInt32(numPartitions)
    .writeInt16(replicationFactor)
    .writeUVarIntArray(replicaAssignment.map(encodeReplicaAssignment))
    .writeUVarIntArray(configEntries.map(encodeConfigEntries))
    .writeTaggedFields()
}

const encodeReplicaAssignment = ({ partition, replicas }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeUVarIntArray(replicas, 'int32')
    .writeTaggedFields()
}

const encodeConfigEntries = ({ name, value }) => {
  return new Encoder().writeUVarIntString(name).writeUVarIntString(value).writeTaggedFields()
}

module.exports.schema = {
  api: 'CreateTopics',
  version: 5,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]CreatableTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.NumPartitions', type: 'int32', versions: '0+' },
    { name: 'Topics.ReplicationFactor', type: 'int16', versions: '0+' },
    { name: 'Topics.Assignments', type: '[]CreatableReplicaAssignment', versions: '0+' },
    { name: 'Topics.Assignments.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Assignments.BrokerIds', type: '[]int32', versions: '0+' },
    { name: 'Topics.Configs', type: '[]CreatableTopicConfig', versions: '0+' },
    { name: 'Topics.Configs.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Configs.Value', type: 'string', versions: '0+', nullable: true },
    { name: 'timeoutMs', type: 'int32', versions: '0+' },
    { name: 'validateOnly', type: 'bool', versions: '1+' },
  ],
}
