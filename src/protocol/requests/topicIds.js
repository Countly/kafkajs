const { KafkaJSInvariantViolation } = require('../../errors')

/** The id a broker uses for "no topic". It is never a valid id to send. */
const NO_TOPIC_ID = '00000000-0000-0000-0000-000000000000'

/**
 * Whether an id may be sent: present and not the all-zero id that stands for "no topic".
 * @param {string|null|undefined} topicId
 * @returns {boolean}
 */
const isUsableTopicId = (topicId) => topicId != null && topicId !== NO_TOPIC_ID

/**
 * The id of every named topic, or nothing when any one lacks a usable id. All or nothing, because
 * a request keyed by id cannot name one topic by id and another by name.
 * @param {string[]} names the topics a request names
 * @param {(name: string) => string|null|undefined} idOf where an id is read from
 * @returns {Map<string, string>|undefined} topic name to topic id, undefined for no names or when
 *   any topic has no usable id
 */
const usableTopicIds = (names, idOf) => {
  if (names.length === 0) {
    return undefined
  }

  const topicIds = new Map()
  for (const name of names) {
    const topicId = idOf(name)

    if (!isUsableTopicId(topicId)) {
      return undefined
    }

    topicIds.set(name, topicId)
  }

  return topicIds
}

/**
 * The id of each named topic, in the order given. A request keyed by topic id must never carry a
 * missing or all-zero id: the broker would answer UNKNOWN_TOPIC_ID, so the builder refuses instead.
 * @param {string} api the API name, for the message
 * @param {string[]} names
 * @param {Map<string, string>} [topicIds] topic name to topic id
 * @param {number} [version=10] the first version keyed by id, for the message
 * @returns {string[]} one id per name
 * @throws {KafkaJSInvariantViolation} naming the first topic without a usable id
 */
const requireTopicIds = (api, names, topicIds, version = 10) =>
  names.map((name) => {
    const topicId = topicIds == null ? undefined : topicIds.get(name)

    if (!isUsableTopicId(topicId)) {
      throw new KafkaJSInvariantViolation(
        `${api} v${version} request is missing a topic id for topic "${name}"`
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

module.exports = { NO_TOPIC_ID, usableTopicIds, requireTopicIds, namesById, topicIdSchema }
