const { KafkaJSNonRetriableError } = require('../../errors')

/**
 * Whether a stored committed offset is one no reset policy can resolve. A broker stores whatever
 * offset a client committed. `-1` (latest) and `-2` (earliest) are what `admin.resetOffsets`
 * stores on purpose, and the consumer reads them as "no position, apply the reset policy". Any
 * other negative offset would stay unresolved and be read again every round.
 * @param {string|null|undefined} offset a decimal int64 string from an OffsetFetch answer
 * @returns {boolean}
 */
const isUnusableStoredOffset = (offset) =>
  typeof offset === 'string' && offset[0] === '-' && offset !== '-1' && offset !== '-2'

/**
 * @param {string} groupId the group whose offsets were read
 * @param {string} topic
 * @param {number} partition
 * @param {string} offset the unusable stored offset
 * @returns {KafkaJSNonRetriableError} naming the cell and the admin calls that change it
 */
const unusableStoredOffsetError = (groupId, topic, partition, offset) =>
  new KafkaJSNonRetriableError(
    `Group "${groupId}" stores offset ${offset} for partition ${partition} of topic ${topic}, which cannot be used; set it with admin.setOffsets or admin.resetOffsets`
  )

/**
 * Refuses an OffsetFetch answer in which any code-0 row stores an unusable offset
 * (`isUnusableStoredOffset`).
 * @param {string} groupId the group whose offsets were read
 * @param {{topic: string, partitions: {partition: number, offset: string|null, errorCode: number}[]}[]} responses
 *   the topic entries of an OffsetFetch answer
 * @returns {void}
 * @throws {KafkaJSNonRetriableError} naming the first such partition
 */
const refuseUnusableStoredOffsets = (groupId, responses) => {
  for (const { topic, partitions } of responses) {
    for (const { partition, offset, errorCode } of partitions) {
      if (errorCode === 0 && isUnusableStoredOffset(offset)) {
        throw unusableStoredOffsetError(groupId, topic, partition, offset)
      }
    }
  }
}

module.exports = { isUnusableStoredOffset, unusableStoredOffsetError, refuseUnusableStoredOffsets }
