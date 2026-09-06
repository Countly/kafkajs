const Encoder = require('../../../encoder')
const { Fetch: apiKey } = require('../../apiKeys')
const ISOLATION_LEVEL = require('../../../isolationLevel')
const { LAST_FETCHED_EPOCH } = require('../v12/request')
const { KafkaJSInvariantViolation } = require('../../../../errors')

/**
 * FetchRequest (Version: 13) — flexible (KIP-482), topic ids (KIP-516)
 *
 * Identical to v12 except that `Topics.Topic` and `ForgottenTopicsData.Topic` are replaced by
 * `TopicId` (uuid). Neither `Topic` nor `TopicId` is a `mapKey` in `FetchRequest.json`, so the
 * same topic id may legally appear as several entries.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
 *
 * @param {Map<string, string>} topicIds topic name -> topic id, built by the caller before the
 *   KIP-74 shuffle: the shuffle rebuilds consolidated `{ topic, partitions }` runs and drops any
 *   per-topic field, so ids travel beside `topics` rather than on them. One topic may appear as
 *   several runs, which is legal here — neither `Topic` nor `TopicId` is a `mapKey` in
 *   `FetchRequest.json`, unlike `ProduceRequest.json`.
 */
module.exports = ({
  replicaId,
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
  apiVersion: 13,
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
      .writeUVarIntArray(topics.map((topic) => encodeTopic(topic, topicIds)))
      .writeUVarIntArray(forgottenTopics.map((topic) => encodeForgottenTopics(topic, topicIds)))
      .writeUVarIntString(rackId)
      .writeTaggedFields(),
})

/**
 * @param {Map<string, string>} topicIds
 * @param {string} topic
 * @returns {string} the topic's id
 * @throws {import('../../../../errors').KafkaJSInvariantViolation} when `topic` has no id -
 *   callers (`Broker#fetch`'s v13 cap) never reach this codec without one for every topic, so this
 *   can only mean a caller bypassed that cap; an all-zero uuid would otherwise go out on the wire
 *   silently instead.
 */
const requireTopicId = (topicIds, topic) => {
  const topicId = topicIds.get(topic)

  if (topicId === undefined) {
    throw new KafkaJSInvariantViolation(
      `Fetch v13 request is missing a topic id for topic "${topic}"`
    )
  }

  return topicId
}

const encodeForgottenTopics = ({ topic, partitions }, topicIds) =>
  new Encoder()
    .writeUuid(requireTopicId(topicIds, topic))
    .writeUVarIntArray(partitions)
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
  version: 13,
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
