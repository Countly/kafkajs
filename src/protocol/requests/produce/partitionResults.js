const { failure, createErrorFromCode } = require('../../error')

/**
 * One partition's outcome in a Produce response, as `Broker#produce({ perPartitionResults: true })`
 * resolves it. The int64 fields are the decimal strings the codec decodes them to.
 * @typedef {object} PartitionResult
 * @property {string|null} topicName null only for a topic id the request did not ask for (v13)
 * @property {number} partition
 * @property {number} errorCode 0 on success
 * @property {string} baseOffset
 * @property {string} logAppendTime
 * @property {string} logStartOffset '-1' below v5, which does not carry it (the field's default)
 * @property {import('../../../errors').KafkaJSProtocolError|null} error the partition's error,
 *   enriched exactly as a thrown produce error is; null on success
 */

/**
 * The error a Produce response reports for one failing partition, carrying what the response says
 * about it: the partition's address; from v8 the broker's message and the batch index of every
 * record it rejected (KIP-467); from v10 the leader hint on a leadership error (KIP-951).
 *
 * `recordErrors[].batchIndex` indexes the records as grouped for that partition, not the caller's
 * flat message list; the producer resolves each index to its record. `currentLeader` and
 * `nodeEndpoints` are internal: they let the producer route its next attempt to the new leader
 * without waiting for a metadata refresh, and are not part of the error's documented surface.
 * @param {{ nodeEndpoints?: object[] }} data the decoded response
 * @param {string|null} topicName
 * @param {object} partition a decoded partition response whose error code is a failure
 * @returns {import('../../../errors').KafkaJSProtocolError}
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-467%3A+Augment+ProduceResponse+error+messaging+for+specific+culprit+records
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
 */
const partitionError = (data, topicName, partition) => {
  const error = createErrorFromCode(partition.errorCode)
  error.topic = topicName
  error.partition = partition.partition

  if (partition.errorMessage !== undefined) {
    error.errorMessage = partition.errorMessage
  }

  if (partition.recordErrors !== undefined) {
    error.recordErrors = partition.recordErrors.map(({ batchIndex, batchIndexErrorMessage }) => ({
      batchIndex,
      errorMessage: batchIndexErrorMessage,
    }))
  }

  if (partition.currentLeader) {
    error.currentLeader = partition.currentLeader
    error.nodeEndpoints = data.nodeEndpoints
  }

  return error
}

/**
 * The throwing form every Produce version from v8 parses with: a response reports at most one
 * failure to the caller - the first failing partition across every topic, the rule every version
 * of this codec has used - enriched as `partitionError` describes.
 * @param {object} data decoded response
 * @returns {Promise<object>} data, when every partition succeeded
 * @throws {import('../../../errors').KafkaJSProtocolError}
 */
const parse = async (data) => {
  for (const { topicName, partitions } of data.topics) {
    for (const partition of partitions) {
      if (failure(partition.errorCode)) {
        throw partitionError(data, topicName, partition)
      }
    }
  }

  return data
}

/**
 * Every partition's outcome in a decoded Produce response of any version, in response order, with
 * no throw: what a caller that settles each partition on its own needs, including the
 * `logStartOffset` the idempotent producer reads an UNKNOWN_PRODUCER_ID by.
 * @param {{ topics: { topicName: string|null, partitions: object[] }[] }} data decoded response
 * @returns {PartitionResult[]}
 */
const partitionResults = (data) =>
  data.topics.flatMap(({ topicName, partitions }) =>
    partitions.map((partition) => ({
      topicName,
      partition: partition.partition,
      errorCode: partition.errorCode,
      baseOffset: partition.baseOffset,
      logAppendTime: partition.logAppendTime,
      logStartOffset: partition.logStartOffset === undefined ? '-1' : partition.logStartOffset,
      error: failure(partition.errorCode) ? partitionError(data, topicName, partition) : null,
    }))
  )

module.exports = { parse, partitionResults }
