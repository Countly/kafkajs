const requestV4 = require('../v4/request')

/**
 * OffsetFetch Request (Version: 5) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 *
 * v5 adds no request fields — `CommittedLeaderEpoch` is a response-only addition, decoded per
 * partition alongside the committed offset.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetFetchRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {{topic: string, partitions: {partition: number}[]}[]} options.topics
 */
module.exports = ({ groupId, topics }) =>
  Object.assign(requestV4({ groupId, topics }), { apiVersion: 5 })

module.exports.schema = {
  api: 'OffsetFetch',
  version: 5,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0-7' },
    { name: 'Topics', type: '[]OffsetFetchRequestTopic', versions: '0-7', nullable: true },
    { name: 'Topics.Name', type: 'string', versions: '0-7' },
    { name: 'Topics.PartitionIndexes', type: '[]int32', versions: '0-7' },
  ],
}
