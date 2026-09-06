const Encoder = require('../../../encoder')
const { Fetch: apiKey } = require('../../apiKeys')
const ISOLATION_LEVEL = require('../../../isolationLevel')

/**
 * The epoch of the last record this client fetched for the partition. This client always sends the
 * field's own default: the Java consumer sends `Optional.empty()` here, and a leader that cannot
 * resolve a supplied epoch answers `OFFSET_OUT_OF_RANGE`, which would reset a position that is in
 * fact perfectly valid whenever the leader's epoch cache is behind or empty.
 * @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
 */
const LAST_FETCHED_EPOCH = -1

/**
 * FetchRequest (Version: 12) — flexible (KIP-482), request header v2
 *
 * ReplicaId => INT32
 * MaxWaitMs => INT32
 * MinBytes => INT32
 * MaxBytes => INT32
 * IsolationLevel => INT8
 * SessionId => INT32
 * SessionEpoch => INT32
 * Topics => COMPACT_ARRAY
 *   Topic => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY
 *     Partition => INT32
 *     CurrentLeaderEpoch => INT32
 *     FetchOffset => INT64
 *     LastFetchedEpoch => INT32
 *     LogStartOffset => INT64
 *     PartitionMaxBytes => INT32
 *     TAG_BUFFER
 *   TAG_BUFFER
 * ForgottenTopicsData => COMPACT_ARRAY of Topic(COMPACT_STRING) Partitions(COMPACT_ARRAY of INT32)
 * RackId => COMPACT_STRING
 * TAG_BUFFER
 *   tag 0 ClusterId => COMPACT_NULLABLE_STRING (not sent)
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
 */
module.exports = ({
  replicaId,
  maxWaitTime,
  minBytes,
  maxBytes,
  topics,
  rackId = '',
  isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
  sessionId = 0,
  sessionEpoch = -1,
  forgottenTopics = [],
}) => ({
  apiKey,
  apiVersion: 12,
  apiName: 'Fetch',
  encode: async () =>
    new Encoder()
      .writeInt32(replicaId)
      .writeInt32(maxWaitTime)
      .writeInt32(minBytes)
      .writeInt32(maxBytes)
      .writeInt8(isolationLevel)
      .writeInt32(sessionId)
      .writeInt32(sessionEpoch)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeUVarIntArray(forgottenTopics.map(encodeForgottenTopics))
      .writeUVarIntString(rackId)
      .writeTaggedFields(),
})

const encodeForgottenTopics = ({ topic, partitions }) =>
  new Encoder().writeUVarIntString(topic).writeUVarIntArray(partitions, 'int32').writeTaggedFields()

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

const encodePartition = ({
  partition,
  currentLeaderEpoch = -1,
  fetchOffset,
  logStartOffset = -1,
  maxBytes,
}) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt32(currentLeaderEpoch)
    .writeInt64(fetchOffset)
    .writeInt32(LAST_FETCHED_EPOCH)
    .writeInt64(logStartOffset)
    .writeInt32(maxBytes)
    .writeTaggedFields()

// Exported so v13's identical encoder can reuse it rather than redeclare the same constant.
module.exports.LAST_FETCHED_EPOCH = LAST_FETCHED_EPOCH

module.exports.schema = {
  api: 'Fetch',
  version: 12,
  direction: 'request',
  fields: [
    { name: 'ClusterId', type: 'string', versions: '12+', nullable: true, tag: 0 },
    { name: 'ReplicaId', type: 'int32', versions: '0-14' },
    { name: 'MaxWaitMs', type: 'int32', versions: '0+' },
    { name: 'MinBytes', type: 'int32', versions: '0+' },
    { name: 'MaxBytes', type: 'int32', versions: '3+' },
    { name: 'IsolationLevel', type: 'int8', versions: '4+' },
    { name: 'SessionId', type: 'int32', versions: '7+' },
    { name: 'SessionEpoch', type: 'int32', versions: '7+' },
    { name: 'Topics', type: '[]FetchTopic', versions: '0+' },
    { name: 'Topics.Topic', type: 'string', versions: '0-12' },
    { name: 'Topics.Partitions', type: '[]FetchPartition', versions: '0+' },
    { name: 'Topics.Partitions.Partition', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CurrentLeaderEpoch', type: 'int32', versions: '9+' },
    { name: 'Topics.Partitions.FetchOffset', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.LastFetchedEpoch', type: 'int32', versions: '12+' },
    { name: 'Topics.Partitions.LogStartOffset', type: 'int64', versions: '5+' },
    { name: 'Topics.Partitions.PartitionMaxBytes', type: 'int32', versions: '0+' },
    { name: 'ForgottenTopicsData', type: '[]ForgottenTopic', versions: '7+' },
    { name: 'ForgottenTopicsData.Topic', type: 'string', versions: '7-12' },
    { name: 'ForgottenTopicsData.Partitions', type: '[]int32', versions: '7+' },
    { name: 'RackId', type: 'string', versions: '11+' },
  ],
}
