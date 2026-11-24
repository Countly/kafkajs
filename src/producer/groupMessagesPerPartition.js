const { KafkaJSInvalidConfigurationError } = require('../errors')

/** The largest partition id a Kafka INT32 can carry. */
const MAX_PARTITION = 2147483647

/**
 * Whether a partitioner's result names a partition id: an integer from 0 to 2147483647, as a
 * number or as its canonical decimal string (`'12'`, not `'012'`).
 * @param {unknown} partition
 * @returns {boolean}
 */
const valid = (partition) =>
  typeof partition === 'number'
    ? Number.isInteger(partition) && partition >= 0 && partition <= MAX_PARTITION
    : typeof partition === 'string' &&
      /^(0|[1-9][0-9]{0,9})$/.test(partition) &&
      Number(partition) <= MAX_PARTITION

/**
 * Shows a rejected partition without coercing it: a Symbol or an object whose `toString` throws
 * is named by its type only.
 * @param {unknown} partition
 * @returns {string}
 */
const shown = (partition) =>
  typeof partition === 'number'
    ? String(partition)
    : typeof partition === 'string'
      ? JSON.stringify(partition)
      : typeof partition

/**
 * Groups a topic's records by the partition the partitioner picks for each. The partition an
 * in-range integer names may be above the known partition count: the send path waits for
 * metadata for it.
 * @param {object} params
 * @param {string} params.topic
 * @param {{ partitionId: number, leader: number }[]} params.partitionMetadata
 * @param {object[]} params.messages
 * @param {Function} params.partitioner
 * @returns {Object<string, object[]>} the records per partition id, in order; `{}` without metadata
 * @throws {KafkaJSInvalidConfigurationError} when the partitioner returns anything but a partition
 *   id, before any record of the call is used
 */
module.exports = ({ topic, partitionMetadata, messages, partitioner }) => {
  if (partitionMetadata.length === 0) {
    return {}
  }

  const grouping = {}
  for (const message of messages) {
    const partition = partitioner({ topic, partitionMetadata, message })
    if (!valid(partition)) {
      throw new KafkaJSInvalidConfigurationError(
        `Invalid partition ${shown(partition)} for topic "${topic}"`
      )
    }
    if (grouping[partition] === undefined) {
      grouping[partition] = []
    }
    grouping[partition].push(message)
  }

  return grouping
}
