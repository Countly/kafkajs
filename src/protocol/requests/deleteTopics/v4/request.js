const Encoder = require('../../../encoder')
const { DeleteTopics: apiKey } = require('../../apiKeys')

/**
 * DeleteTopics Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * TopicNames => COMPACT_ARRAY of COMPACT_STRING (0-5)
 * TimeoutMs => INT32 (0+)
 * TAG_BUFFER
 *
 * The v3 fields in compact framing; v5 sends the same bytes.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteTopicsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string[]} options.topics the names of the topics to delete
 * @param {number} [options.timeout=5000]
 * @returns {object} the protocol request
 */
module.exports = ({ topics, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'DeleteTopics',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(topics.map(encodeName))
      .writeInt32(timeout)
      .writeTaggedFields()
  },
})

const encodeName = (name) => new Encoder().writeUVarIntString(name)

module.exports.schema = {
  api: 'DeleteTopics',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'TopicNames', type: '[]string', versions: '0-5' },
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
  ],
}
