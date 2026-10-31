const Encoder = require('../../../encoder')
const { Fetch: apiKey } = require('../../apiKeys')
const ISOLATION_LEVEL = require('../../../isolationLevel')
const { LAST_FETCHED_EPOCH } = require('../v12/request')
const { requireTopicId } = require('../v13/request')

/**
 * FetchRequest (Version: 15) — flexible, KIP-903
 *
 * Drops the top-level `ReplicaId` field, whose version range closes at v14 (`"0-14"` in
 * `FetchRequest.json`), and replaces it with `ReplicaState` (tag 1): a replica-only tagged field
 * carrying `ReplicaId`/`ReplicaEpoch` that a consumer — never a follower — does not send. The
 * body therefore opens on `MaxWaitMs` instead of `ReplicaId`, four bytes shorter than v14's.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-903%3A+Replicas+with+stale+broker+epoch+should+not+be+allowed+to+join+the+ISR
 * @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
 *
 * @param {Map<string, string>} topicIds topic name -> topic id, as in v13/v14
 */
module.exports = ({
  maxWaitTime,
  minBytes,
  maxBytes,
  topics,
  topicIds,
  rackId = '',
  isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
  sessionId = 0,
  sessionEpoch = -1,
  forgottenTopics = [],
}) => ({
  apiKey,
  apiVersion: 15,
  apiName: 'Fetch',
  encode: async () =>
    new Encoder()
      .writeInt32(maxWaitTime)
      .writeInt32(minBytes)
      .writeInt32(maxBytes)
      .writeInt8(isolationLevel)
      .writeInt32(sessionId)
      .writeInt32(sessionEpoch)
      .writeUVarIntArray(topics.map((topic) => encodeTopic(topic, topicIds)))
      .writeUVarIntArray(forgottenTopics.map((topic) => encodeForgottenTopics(topic, topicIds)))
      .writeUVarIntString(rackId)
      .writeTaggedFields(),
})

/**
 * Encodes a removal using the session's recorded ID, even after cache retirement.
 * @param {object} entry topic, optional recorded topicId and partition numbers
 * @param {Map<string,string>} topicIds current request mapping for legacy callers
 * @returns {Encoder} forgotten topic fields
 * @throws {KafkaJSInvariantViolation} no recorded or current ID
 */
const encodeForgottenTopics = ({ topic, topicId, partitions }, topicIds) =>
  new Encoder()
    .writeUuid(topicId ?? requireTopicId(topicIds, topic))
    .writeUVarIntArray(partitions, 'int32')
    .writeTaggedFields()

const encodeTopic = ({ topic, partitions }, topicIds) =>
  new Encoder()
    .writeUuid(requireTopicId(topicIds, topic))
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

module.exports.schema = {
  api: 'Fetch',
  version: 15,
  direction: 'request',
  fields: [
    { name: 'ClusterId', type: 'string', versions: '12+', nullable: true, tag: 0 },
    { name: 'ReplicaState', type: 'ReplicaState', versions: '15+', tag: 1 },
    { name: 'ReplicaState.ReplicaId', type: 'int32', versions: '15+' },
    { name: 'ReplicaState.ReplicaEpoch', type: 'int64', versions: '15+' },
    { name: 'MaxWaitMs', type: 'int32', versions: '0+' },
    { name: 'MinBytes', type: 'int32', versions: '0+' },
    { name: 'MaxBytes', type: 'int32', versions: '3+' },
    { name: 'IsolationLevel', type: 'int8', versions: '4+' },
    { name: 'SessionId', type: 'int32', versions: '7+' },
    { name: 'SessionEpoch', type: 'int32', versions: '7+' },
    { name: 'Topics', type: '[]FetchTopic', versions: '0+' },
    { name: 'Topics.TopicId', type: 'uuid', versions: '13+' },
    { name: 'Topics.Partitions', type: '[]FetchPartition', versions: '0+' },
    { name: 'Topics.Partitions.Partition', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CurrentLeaderEpoch', type: 'int32', versions: '9+' },
    { name: 'Topics.Partitions.FetchOffset', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.LastFetchedEpoch', type: 'int32', versions: '12+' },
    { name: 'Topics.Partitions.LogStartOffset', type: 'int64', versions: '5+' },
    { name: 'Topics.Partitions.PartitionMaxBytes', type: 'int32', versions: '0+' },
    { name: 'ForgottenTopicsData', type: '[]ForgottenTopic', versions: '7+' },
    { name: 'ForgottenTopicsData.TopicId', type: 'uuid', versions: '13+' },
    { name: 'ForgottenTopicsData.Partitions', type: '[]int32', versions: '7+' },
    { name: 'RackId', type: 'string', versions: '11+' },
  ],
}
