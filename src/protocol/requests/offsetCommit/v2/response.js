const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * OffsetCommit Response (Version: 2) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 */

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  return {
    responses: decoder.readArray(decodeResponses),
  }
}

const decodeResponses = (decoder) => ({
  topic: decoder.readString(),
  partitions: decoder.readArray(decodePartitions),
})

const decodePartitions = (decoder) => ({
  partition: decoder.readInt32(),
  errorCode: decoder.readInt16(),
})

/**
 * Throws the first failed partition's error code as a protocol error that names the partition,
 * as a Produce failure does (`topic`, `partition`); a response without a failure is returned.
 * Every OffsetCommit version parses here.
 * @param {{ responses: Array<{ topic: string|null, partitions: Array<{ partition: number,
 *   errorCode: number }> }> }} data the decoded response
 * @returns {Promise<object>} the response
 * @throws {import('../../../../errors').KafkaJSProtocolError} carrying `topic` and `partition`
 */
const parse = async (data) => {
  for (const { topic, partitions } of data.responses) {
    const failed = partitions.find((partition) => failure(partition.errorCode))
    if (failed) {
      const error = createErrorFromCode(failed.errorCode)
      error.topic = topic
      error.partition = failed.partition
      throw error
    }
  }

  return data
}

module.exports = {
  decode,
  parse,
}
