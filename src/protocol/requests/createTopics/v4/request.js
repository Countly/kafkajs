const requestV3 = require('../v3/request')

/**
 * CreateTopics Request (Version: 4) => [topics] timeout_ms validate_only
 *   topics => name num_partitions replication_factor [assignments] [configs]
 *     name => STRING
 *     num_partitions => INT32
 *     replication_factor => INT16
 *     assignments => partition_index [broker_ids]
 *       partition_index => INT32
 *       broker_ids => INT32
 *     configs => name value
 *       name => STRING
 *       value => NULLABLE_STRING
 *   timeout_ms => INT32
 *   validate_only => BOOLEAN
 *
 * The v3 body. Version 4 lets a topic send -1 partitions and -1 replication factor with no
 * assignments to take the broker's defaults, which the encoder already does for a topic that
 * names neither.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-464%3A+Defaults+for+AdminClient%23createTopic
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
module.exports = ({ topics, validateOnly, timeout }) =>
  Object.assign(requestV3({ topics, validateOnly, timeout }), { apiVersion: 4 })

module.exports.schema = {
  api: 'CreateTopics',
  version: 4,
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
