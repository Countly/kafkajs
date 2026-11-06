const Encoder = require('../../../encoder')
const { OffsetDelete: apiKey } = require('../../apiKeys')
/** Builds an OffsetDelete v0 body. Classic headers are request v1 and response v0.
 * @param {object} options groupId and named topic/partition cells
 * @returns {object} immutable protocol request description
 */
module.exports = ({ groupId, topics }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'OffsetDelete',
  encode: async () =>
    new Encoder()
      .writeString(groupId)
      .writeArray(
        topics.map(({ topic, partitions }) =>
          new Encoder()
            .writeString(topic)
            .writeArray(partitions.map(({ partition }) => new Encoder().writeInt32(partition)))
        )
      ),
})
module.exports.schema = {
  api: 'OffsetDelete',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'Topics', type: '[]OffsetDeleteRequestTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]OffsetDeleteRequestPartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
  ],
}
