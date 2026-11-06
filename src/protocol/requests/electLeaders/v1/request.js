const Encoder = require('../../../encoder')
const { ElectLeaders: apiKey } = require('../../apiKeys')
/** Builds a versioned body; capability validation is owned by Broker. @param {object} options wire fields @returns {object} protocol request @throws {Error} invalid encoding */
module.exports = ({ electionType = 0, topicPartitions = [], timeoutMs = 60000 } = {}) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'ElectLeaders',
  encode: async () =>
    new Encoder()
      .writeInt8(electionType)
      .writeEncoder(
        topicPartitions === null
          ? new Encoder().writeInt32(-1)
          : new Encoder().writeArray(
              topicPartitions.map((item) =>
                new Encoder()
                  .writeString(item.topic)
                  .writeArray(
                    item.partitions === null
                      ? null
                      : item.partitions.map((item) => new Encoder().writeInt32(item))
                  )
              )
            )
      )
      .writeInt32(timeoutMs),
})

module.exports.schema = {
  api: 'ElectLeaders',
  version: 1,
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
