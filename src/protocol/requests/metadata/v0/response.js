const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * Metadata Response (Version: 0) => [brokers] [topic_metadata]
 *   brokers => node_id host port
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 *   topic_metadata => topic_error_code topic [partition_metadata]
 *     topic_error_code => INT16
 *     topic => STRING
 *     partition_metadata => partition_error_code partition_id leader [replicas] [isr]
 *       partition_error_code => INT16
 *       partition_id => INT32
 *       leader => INT32
 *       replicas => INT32
 *       isr => INT32
 */

const broker = (decoder) => ({
  nodeId: decoder.readInt32(),
  host: decoder.readString(),
  port: decoder.readInt32(),
})

const topicMetadata = (decoder) => ({
  topicErrorCode: decoder.readInt16(),
  topic: decoder.readString(),
  partitionMetadata: decoder.readArray(partitionMetadata),
})

const partitionMetadata = (decoder) => ({
  partitionErrorCode: decoder.readInt16(),
  partitionId: decoder.readInt32(),
  // leader: The node id for the kafka broker currently acting as leader
  // for this partition
  leader: decoder.readInt32(),
  replicas: decoder.readArray((d) => d.readInt32()),
  isr: decoder.readArray((d) => d.readInt32()),
})

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  return {
    brokers: decoder.readArray(broker),
    topicMetadata: decoder.readArray(topicMetadata),
  }
}

/**
 * Throws the first partition error code of the given topics. A partition without a code, which no
 * decoded answer has, counts as healthy.
 * @param {object[]} topicMetadata decoded topics
 * @returns {void}
 * @throws {import('../../../../errors').KafkaJSProtocolError} on a partition error code
 */
const checkPartitions = (topicMetadata) => {
  const errors = topicMetadata.flatMap((topic) => {
    return topic.partitionMetadata.filter(
      ({ partitionErrorCode }) => partitionErrorCode != null && failure(partitionErrorCode)
    )
  })

  if (errors.length > 0) {
    const { partitionErrorCode } = errors[0]
    throw createErrorFromCode(partitionErrorCode)
  }
}

/**
 * Throws the first topic error code, and only then the first partition error code.
 * @param {object} data decoded response
 * @returns {Promise<object>} data, unchanged
 * @throws {import('../../../../errors').KafkaJSProtocolError} on a topic or partition error code
 */
const parse = async (data) => {
  const topicsWithErrors = data.topicMetadata.filter((topic) => failure(topic.topicErrorCode))
  if (topicsWithErrors.length > 0) {
    const { topicErrorCode } = topicsWithErrors[0]
    throw createErrorFromCode(topicErrorCode)
  }

  checkPartitions(data.topicMetadata)
  return data
}

module.exports = {
  decode,
  parse,
  checkPartitions,
}
