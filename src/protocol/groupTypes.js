const { KafkaJSNonRetriableError } = require('../errors')

/**
 * Group types and states, keyed by their lower-case API form and valued by the DISPLAY NAME the
 * wire carries. Kafka's `GroupType`/`GroupState` enums serialise their `name` field, so the
 * filters a client sends and the values a broker returns are `"Consumer"`, `"Stable"` and so on -
 * not enum constants.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/GroupType.java
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/GroupState.java
 * @type {{ [lowerCase: string]: string }}
 */
const GROUP_TYPES = {
  unknown: 'Unknown',
  consumer: 'Consumer',
  classic: 'Classic',
  share: 'Share',
  streams: 'Streams',
}

/** @type {{ [lowerCase: string]: string }} */
const GROUP_STATES = {
  unknown: 'Unknown',
  preparingrebalance: 'PreparingRebalance',
  completingrebalance: 'CompletingRebalance',
  stable: 'Stable',
  dead: 'Dead',
  empty: 'Empty',
  assigning: 'Assigning',
  reconciling: 'Reconciling',
  notready: 'NotReady',
}

/**
 * @param {{ [lowerCase: string]: string }} table `GROUP_TYPES` or `GROUP_STATES`
 * @param {string} value the caller's value, in any casing
 * @param {string} label what to call it in the error message
 * @returns {string} the wire display name
 * @throws {KafkaJSNonRetriableError} when the value is not one Kafka defines - a filter the
 *   broker cannot understand would silently widen the answer instead of narrowing it
 */
const toDisplayName = (table, value, label) => {
  const displayName = table[String(value).toLowerCase()]

  if (displayName === undefined) {
    throw new KafkaJSNonRetriableError(
      `Invalid ${label} "${value}"; accepted values are ${Object.keys(table).sort().join(', ')}`
    )
  }

  return displayName
}

module.exports = { GROUP_TYPES, GROUP_STATES, toDisplayName }
