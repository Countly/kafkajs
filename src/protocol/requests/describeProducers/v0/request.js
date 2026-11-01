const Encoder = require('../../../encoder')
const { DescribeProducers: apiKey } = require('../../apiKeys')

/** Encodes DescribeProducers v0 with compact collections and struct tags.
 * @param {object} options validated fields; decimal strings preserve int64 values
 * @returns {object} request with header v2 and lazy body encoder
 * @throws {Error} invalid internal fields during encoding
 */
module.exports = (options = {}) => {
  options = { ...{}, ...options }
  return {
    apiKey,
    apiVersion: 0,
    apiName: 'DescribeProducers',
    encode: async () =>
      new Encoder()
        .writeUVarIntArray(
          options.topics.map((cell0) =>
            new Encoder()
              .writeUVarIntString(cell0.topic)
              .writeUVarIntArray(cell0.partitions.map((value) => new Encoder().writeInt32(value)))
              .writeTaggedFields()
          )
        )
        .writeTaggedFields(),
  }
}

module.exports.schema = {
  api: 'DescribeProducers',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]TopicRequest', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.PartitionIndexes', type: '[]int32', versions: '0+' },
  ],
}
