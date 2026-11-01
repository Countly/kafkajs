const Encoder = require('../../../encoder')
const { OffsetFetch: apiKey } = require('../../apiKeys')

/**
 * OffsetFetch Request (Version: 3) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 */

/** Builds a nullable all-topic request; existing empty-array and explicit bytes are retained.
 * @param {object} options request selection
 * @param {string} options.groupId group identity
 * @param {object[]|null} options.topics null requests all topics
 * @returns {object} v3 protocol request, also used by v4/v5 without retry work
 */
module.exports = ({ groupId, topics }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'OffsetFetch',
  encode: async () => {
    return new Encoder().writeString(groupId).writeNullableArray(topics?.map(encodeTopic) ?? [])
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))
}

const encodePartition = ({ partition }) => {
  return new Encoder().writeInt32(partition)
}
