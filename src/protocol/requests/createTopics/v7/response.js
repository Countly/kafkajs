const { buildDecode, parse } = require('../v5/response')

/**
 * CreateTopics Response (Version: 7) — flexible (KIP-482), response header v1
 *
 * The v6 body with `TopicId` (UUID) between `Name` and `ErrorCode` of every topic. A topic that
 * was not created carries the zero uuid, which decodes as `topicId: null`.
 * @see clients/src/main/resources/common/message/CreateTopicsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(7), parse }

module.exports.schema = {
  api: 'CreateTopics',
  version: 7,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '2+' },
    { name: 'Topics', type: '[]CreatableTopicResult', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.TopicId', type: 'uuid', versions: '7+' },
    { name: 'Topics.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.ErrorMessage', type: 'string', versions: '1+', nullable: true },
    { name: 'Topics.TopicConfigErrorCode', type: 'int16', versions: '5+', tag: 0 },
    { name: 'Topics.NumPartitions', type: 'int32', versions: '5+' },
    { name: 'Topics.ReplicationFactor', type: 'int16', versions: '5+' },
    { name: 'Topics.Configs', type: '[]CreatableTopicConfigs', versions: '5+', nullable: true },
    { name: 'Topics.Configs.Name', type: 'string', versions: '5+' },
    { name: 'Topics.Configs.Value', type: 'string', versions: '5+', nullable: true },
    { name: 'Topics.Configs.ReadOnly', type: 'bool', versions: '5+' },
    { name: 'Topics.Configs.ConfigSource', type: 'int8', versions: '5+' },
    { name: 'Topics.Configs.IsSensitive', type: 'bool', versions: '5+' },
  ],
}
