const { KafkaJSInvariantViolation } = require('../../errors')

/** The id a broker uses for "no topic". It is never a valid id to send. */
const NO_TOPIC_ID = '00000000-0000-0000-0000-000000000000'

/**
 * The id of each named topic, in the order given. A request keyed by topic id must never carry a
 * missing or all-zero id: the broker would answer UNKNOWN_TOPIC_ID, so the builder refuses instead.
 * @param {string} api the API name, for the message
 * @param {string[]} names
 * @param {Map<string, string>} [topicIds] topic name to topic id
 * @returns {string[]} one id per name
 * @throws {KafkaJSInvariantViolation} naming the first topic without a usable id
 */
const requireTopicIds = (api, names, topicIds) =>
  names.map((name) => {
    const topicId = topicIds == null ? undefined : topicIds.get(name)

    if (topicId == null || topicId === NO_TOPIC_ID) {
      throw new KafkaJSInvariantViolation(
        `${api} v10 request is missing a topic id for topic "${name}"`
      )
    }

    return topicId
  })

/**
 * The inverse of a request's own name-to-id map. A response codec bound to it names the topics it
 * decodes without reading the metadata cache, which a recreated topic would have renamed.
 * @param {Map<string, string>|null} [topicIds] topic name to topic id; none names nothing
 * @returns {Map<string, string>} topic id to topic name
 */
const namesById = (topicIds) =>
  new Map(Array.from(topicIds ?? [], ([name, topicId]) => [topicId, name]))

/**
 * The schema of a v10 offsets codec: the previous version's fields with each `Name` replaced by a
 * `TopicId`. `protocol:check` compares the result with the 4.3.1 definition field by field.
 * @param {{ fields: object[] }} previous the v9 schema of the same codec
 * @returns {object} the v10 schema
 */
const topicIdSchema = (previous) => ({
  ...previous,
  version: 10,
  fields: previous.fields.map((field) =>
    field.name.endsWith('.Name')
      ? { name: field.name.replace(/Name$/, 'TopicId'), type: 'uuid', versions: '10+' }
      : field
  ),
})

module.exports = { NO_TOPIC_ID, requireTopicIds, namesById, topicIdSchema }
