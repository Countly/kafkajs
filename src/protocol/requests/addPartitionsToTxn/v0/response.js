const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')
const { KafkaJSTransactionAbortableError } = require('../../../../errors')

/**
 * AddPartitionsToTxn Response (Version: 0) => throttle_time_ms [errors]
 *   throttle_time_ms => INT32
 *   errors => topic [partition_errors]
 *     topic => STRING
 *     partition_errors => partition error_code
 *       partition => INT32
 *       error_code => INT16
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errors = await decoder.readArrayAsync(decodeError)

  return {
    throttleTime,
    errors,
  }
}

const decodeError = async (decoder) => ({
  topic: decoder.readString(),
  partitionErrors: await decoder.readArrayAsync(decodePartitionError),
})

const decodePartitionError = (decoder) => ({
  partition: decoder.readInt32(),
  errorCode: decoder.readInt16(),
})

const OPERATION_NOT_ATTEMPTED = 55

/**
 * @param {import('../../../../errors').KafkaJSProtocolError} error
 * @returns {number} 0 for a non-retriable error other than OPERATION_NOT_ATTEMPTED, 1 for a
 *   retriable one, 2 for OPERATION_NOT_ATTEMPTED
 */
const rankOf = (error) =>
  error.code === OPERATION_NOT_ATTEMPTED ? 2 : error.retriable === true ? 1 : 0

/**
 * Throws when any partition failed, picking one failure whatever the row order: a non-retriable
 * error other than OPERATION_NOT_ATTEMPTED (55) first, then a retriable one, the lowest code first
 * within each. A broker fills the partitions it did not try with 55, so 55 alone means nothing was
 * attempted and the transaction can only be aborted.
 * @param {object} data the decoded answer
 * @returns {Promise<object>} `data`, when no partition failed
 * @throws {import('../../../../errors').KafkaJSProtocolError} the chosen partition's error
 * @throws {KafkaJSTransactionAbortableError} when every failure is 55, with the 55 error as `cause`
 */
const parse = async (data) => {
  const failures = data.errors
    .flatMap(({ partitionErrors }) => partitionErrors)
    .filter(({ errorCode }) => failure(errorCode))
    .map(({ errorCode }) => createErrorFromCode(errorCode))
  if (failures.length === 0) {
    return data
  }

  const [chosen] = failures.sort((a, b) => rankOf(a) - rankOf(b) || a.code - b.code)
  if (chosen.code === OPERATION_NOT_ATTEMPTED) {
    throw new KafkaJSTransactionAbortableError(
      'No partition of the AddPartitionsToTxn request was attempted',
      { cause: chosen }
    )
  }
  throw chosen
}

module.exports = {
  decode,
  parse,
}
