const { KafkaJSNonRetriableError } = require('../errors')

/** The highest partition id Kafka can name: partitions are int32 on the wire. */
const MAX_PARTITION = 2 ** 31 - 1

/**
 * @param {unknown} value
 * @returns {string} the value as text, or its type when it cannot be converted
 */
const printable = (value) => {
  try {
    return String(value)
  } catch (_) {
    return typeof value
  }
}

/**
 * Checks that a value is a Kafka partition id: an integer from 0 to 2^31 - 1. A numeric string
 * such as `'0'`, `NaN`, a fraction or a negative number is not.
 * @param {unknown} partition
 * @returns {void}
 * @throws {KafkaJSNonRetriableError} when it is not a partition id
 */
const assertPartitionId = (partition) => {
  if (!Number.isInteger(partition) || partition < 0 || partition > MAX_PARTITION) {
    throw new KafkaJSNonRetriableError(
      `Invalid partition, expected a number received ${printable(partition)}`
    )
  }
}

module.exports = { assertPartitionId }
