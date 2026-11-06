const Encoder = require('../../../encoder')
const { ElectLeaders: apiKey } = require('../../apiKeys')
/** Builds a versioned body; capability validation is owned by Broker. @param {object} options wire fields @returns {object} protocol request @throws {Error} invalid encoding */
module.exports = ({ electionType = 0, topicPartitions = [], timeoutMs = 60000 } = {}) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'ElectLeaders',
  encode: async () =>
    new Encoder()
      .writeInt8(electionType)
      .writeUVarIntArray(
        topicPartitions === null
          ? null
          : topicPartitions.map((item) =>
              new Encoder()
                .writeUVarIntString(item.topic)
                .writeUVarIntArray(
                  item.partitions === null
                    ? null
                    : item.partitions.map((item) => new Encoder().writeInt32(item))
                )
                .writeTaggedFields()
            )
      )
      .writeInt32(timeoutMs)
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'ElectLeaders',
  version: 2,
  direction: 'request',
  fields: [
    {
      name: 'ElectionType',
      type: 'int8',
      versions: '1+',
    },
    {
      name: 'TopicPartitions',
      type: '[]TopicPartitions',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'TopicPartitions.Topic',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'TopicPartitions.Partitions',
      type: '[]int32',
      versions: '0+',
    },
    {
      name: 'TimeoutMs',
      type: 'int32',
      versions: '0+',
    },
  ],
}
