const { KafkaJSInvariantViolation } = require('../../../errors')
/** Checks a response's complete identity set without assuming response order.
 * @param {object[]} actual response rows
 * @param {object[]|null} expected requested rows, null means all
 * @param {string} field identity property
 * @param {string} label diagnostic context
 * @returns {void}
 * @throws {KafkaJSInvariantViolation} duplicate, foreign or missing identity
 */
const exact = (actual, expected, field, label) => {
  if (!Array.isArray(actual)) throw new KafkaJSInvariantViolation(`Missing ${label}`)
  const wanted = expected == null ? null : new Set(expected.map((row) => row[field]))
  const seen = new Set()
  for (const row of actual) {
    const key = row?.[field]
    const valid =
      field === 'partition'
        ? Number.isInteger(key) && key >= 0 && key <= 2147483647
        : typeof key === 'string' && key.length > 0 && !key.includes('\0')
    if (!valid) throw new KafkaJSInvariantViolation(`Invalid ${label} identity`)
    if (seen.has(key) || (wanted && !wanted.has(key)))
      throw new KafkaJSInvariantViolation(`Invalid ${label} identity ${key}`)
    seen.add(key)
  }
  if (wanted && seen.size !== wanted.size)
    throw new KafkaJSInvariantViolation(`Missing ${label} identity`)
}
/** Validates topic/partition correlation, including all-topic responses' uniqueness.
 * @param {object} group decoded group with errorCode and topics
 * @param {object[]|null} topics requested topic/partition cells, null means all
 * @returns {void}
 * @throws {KafkaJSInvariantViolation} an answer cannot be correlated exactly
 */
const validateTopics = (group, topics) => {
  if (group.errorCode !== 0) return
  exact(group.topics, topics, 'topic', 'OffsetFetch topic')
  const requestedTopics = topics == null ? null : new Map(topics.map((row) => [row.topic, row]))
  for (const actual of group.topics) {
    const requested = requestedTopics?.get(actual.topic)
    exact(actual.partitions, requested?.partitions ?? null, 'partition', 'OffsetFetch partition')
  }
}
module.exports = { exact, validateTopics }
