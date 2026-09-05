const Decoder = require('../../../decoder')
const { parse: parseV4 } = require('../v4/response')
const decodeMessages = require('../v4/decodeMessages')

/**
 * Fetch Response (Version: 7) => throttle_time_ms error_code session_id [responses]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   session_id => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition_header record_set
 *       partition_header => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions]
 *         partition => INT32
 *         error_code => INT16
 *         high_watermark => INT64
 *         last_stable_offset => INT64
 *         log_start_offset => INT64
 *         aborted_transactions => producer_id first_offset
 *           producer_id => INT64
 *           first_offset => INT64
 *       record_set => RECORDS
 */

const decodeAbortedTransactions = (decoder) => ({
  producerId: decoder.readInt64().toString(),
  firstOffset: decoder.readInt64().toString(),
})

/**
 * Builds the reader chain for one value of `checkCrcs`. Called twice at module load so a fetch
 * response allocates no closures.
 * @param {{ checkCrcs: boolean }} options
 * @returns {(rawData: Buffer) => Promise<object>}
 */
const buildDecode = (options) => {
  const decodePartition = async (decoder) => ({
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    highWatermark: decoder.readInt64().toString(),
    lastStableOffset: decoder.readInt64().toString(),
    lastStartOffset: decoder.readInt64().toString(),
    abortedTransactions: decoder.readArray(decodeAbortedTransactions),
    messages: await decodeMessages(decoder, options),
  })

  const decodeResponse = async (decoder) => ({
    topicName: decoder.readString(),
    partitions: await decoder.readArrayAsync(decodePartition),
  })

  return async (rawData) => {
    const decoder = new Decoder(rawData)
    const throttleTime = decoder.readInt32()
    const errorCode = decoder.readInt16()
    const sessionId = decoder.readInt32()
    const responses = await decoder.readArrayAsync(decodeResponse)

    return { throttleTime, errorCode, sessionId, responses }
  }
}

const decoders = {
  true: buildDecode({ checkCrcs: true }),
  false: buildDecode({ checkCrcs: false }),
}

/**
 * @param {Buffer} rawData
 * @param {{ checkCrcs?: boolean }} [options]
 * @returns {Promise<object>}
 */
const decode = (rawData, options = {}) => decoders[options.checkCrcs !== false](rawData)

module.exports = {
  decode,
  parse: parseV4,
}
