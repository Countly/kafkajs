const { KafkaJSNonRetriableError } = require('../../errors')

/**
 * Refuses a stored committed offset that no reset policy can resolve. A broker stores whatever
 * offset a client committed. `-1` (latest) and `-2` (earliest) are what `admin.resetOffsets`
 * stores on purpose, and the consumer reads them as "no position, apply the reset policy". Any
 * other negative offset would stay unresolved and be read again every round.
 * @param {string} groupId the group whose offsets were read
 * @param {{topic: string, partitions: {partition: number, offset: string|null, errorCode: number}[]}[]} responses
 *   the topic entries of an OffsetFetch answer
 * @returns {void}
 * @throws {KafkaJSNonRetriableError} naming the first partition whose code-0 row stores an offset
 *   below `-2`, and the admin calls that change it
 */
const refuseUnusableStoredOffsets = (groupId, responses) => {
  for (const { topic, partitions } of responses) {
    for (const { partition, offset, errorCode } of partitions) {
      if (
        errorCode === 0 &&
        typeof offset === 'string' &&
        offset[0] === '-' &&
        offset !== '-1' &&
        offset !== '-2'
      ) {
        throw new KafkaJSNonRetriableError(
          `Group "${groupId}" stores offset ${offset} for partition ${partition} of topic ${topic}, which cannot be used; set it with admin.setOffsets or admin.resetOffsets`
        )
      }
    }
  }
}

module.exports = { refuseUnusableStoredOffsets }
