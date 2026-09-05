const requestV6 = require('../v6/request')

/**
 * MetadataRequest (Version: 7) — identical to v6 on the wire; v7 only adds LeaderEpoch to the
 * response.
 * @see clients/src/main/resources/common/message/MetadataRequest.json
 *
 * @param {object} options
 * @param {string[]} options.topics
 * @param {boolean} [options.allowAutoTopicCreation=true]
 */
module.exports = ({ topics, allowAutoTopicCreation = true }) =>
  Object.assign(requestV6({ topics, allowAutoTopicCreation }), { apiVersion: 7 })

module.exports.schema = {
  api: 'Metadata',
  version: 7,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]MetadataRequestTopic', versions: '0+', nullable: true },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'AllowAutoTopicCreation', type: 'bool', versions: '4+' },
  ],
}
