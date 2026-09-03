const Decoder = require('../../../decoder')
const { KafkaJSOffsetOutOfRange } = require('../../../../errors')
const { failure, createErrorFromCode, errorCodes } = require('../../../error')
const decodeMessages = require('./decodeMessages')

/**
 * Fetch Response (Version: 4) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition_header record_set
 *       partition_header => partition error_code high_watermark last_stable_offset [aborted_transactions]
 *         partition => INT32
 *         error_code => INT16
 *         high_watermark => INT64
 *         last_stable_offset => INT64
 *         aborted_transactions => producer_id first_offset
 *           producer_id => INT64
 *           first_offset => INT64
 *       record_set => RECORDS
 */

const decodeAbortedTransactions = (decoder) => ({
  producerId: decoder.readInt64().toString(),
  firstOffset: decoder.readInt64().toString(),
})

const decodePartition = async (decoder) => ({
  partition: decoder.readInt32(),
  errorCode: decoder.readInt16(),
  highWatermark: decoder.readInt64().toString(),
  lastStableOffset: decoder.readInt64().toString(),
  abortedTransactions: decoder.readArray(decodeAbortedTransactions),
  messages: await decodeMessages(decoder),
})

const decodeResponse = async (decoder) => ({
  topicName: decoder.readString(),
  partitions: await decoder.readArrayAsync(decodePartition),
})

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const responses = await decoder.readArrayAsync(decodeResponse)

  return {
    throttleTime,
    responses,
  }
}

const { code: OFFSET_OUT_OF_RANGE_ERROR_CODE } = errorCodes.find(
  (e) => e.type === 'OFFSET_OUT_OF_RANGE'
)

/**
 * Shared by every Fetch response version. Moved here from v0/response.js when the
 * pre-4.0 codecs were deleted (Kafka 4.0 broker minVersion for Fetch is 4, KIP-896).
 */
const parse = async (data) => {
  const errors = data.responses.flatMap(({ topicName, partitions }) => {
    return partitions
      .filter((partition) => failure(partition.errorCode))
      .map((partition) => Object.assign({}, partition, { topic: topicName }))
  })

  if (errors.length > 0) {
    const { errorCode, topic, partition } = errors[0]
    if (errorCode === OFFSET_OUT_OF_RANGE_ERROR_CODE) {
      throw new KafkaJSOffsetOutOfRange(createErrorFromCode(errorCode), { topic, partition })
    }

    throw createErrorFromCode(errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
