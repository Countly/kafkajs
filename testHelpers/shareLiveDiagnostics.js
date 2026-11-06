const { pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
const { describeError } = require('../src/producer/__tests__/fixtures/liveProbe')
const shareFetch = require('./wire/shareFetchV2')
const shareAcknowledge = require('./wire/shareAcknowledgeV2')

/**
 * Keeps public acknowledgement outcomes, including the broker code nested in a failure's cause.
 * @param {object[]} results Public ACKNOWLEDGE payloads.
 * @returns {object[]} Compact ranges and their actual outcomes.
 */
const acknowledgementResults = (results) =>
  results.map(({ topic, partition, firstOffset, lastOffset, type, error }) => ({
    topic,
    partition,
    firstOffset,
    lastOffset,
    type,
    error: describeError(error),
  }))

/**
 * Reads acknowledgement-bearing requests already captured by a spec's socket tap.
 * @param {object[]} frames Captured frames with Kafka request headers.
 * @returns {object[]} Captured session epochs, destinations and acknowledgement batches.
 */
const acknowledgementRequests = (frames) =>
  frames.flatMap(({ apiKey, apiVersion, correlationId, label, socket, bytes, headerBytes }) => {
    if (apiKey !== 78 && apiKey !== 79) return []
    if (apiVersion !== 2) return [{ apiKey, apiVersion, error: 'Unsupported diagnostic reader' }]
    try {
      const reader = apiKey === 78 ? shareFetch : shareAcknowledge
      const request = reader.readRequest(bytes.subarray(headerBytes))
      const topics = request.topics
        .map(({ topicId, partitions }) => ({
          topicId,
          partitions: partitions.filter(
            ({ acknowledgementBatches }) => acknowledgementBatches.length
          ),
        }))
        .filter(({ partitions }) => partitions.length)
      if (!topics.length) return []
      const { groupId, memberId, shareSessionEpoch, isRenewAck } = request
      return [
        {
          apiKey,
          apiVersion,
          correlationId,
          label,
          socket,
          groupId,
          memberId,
          shareSessionEpoch,
          isRenewAck,
          topics,
        },
      ]
    } catch (error) {
      return [{ apiKey, apiVersion, correlationId, label, socket, error: describeError(error) }]
    }
  })

/**
 * Records the last observation before cleanup, retaining the original predicate and poll budget.
 * @param {AbortSignal} signal Case scope.
 * @param {number} ms Whole poll budget.
 * @param {Function} check Original poll predicate.
 * @param {object} options Poll options plus snapshot and record callbacks.
 * @param {Function} options.snapshot Last completed reads, without more I/O.
 * @param {Function} options.record Writes one evidence row.
 * @returns {Promise<void>} Once the original predicate holds.
 * @throws {Error} Original failure with the last observation appended to its message.
 */
const pollSharePosition = async (signal, ms, check, { snapshot, record, ...options }) => {
  try {
    await pollUntil(signal, ms, check, options)
  } catch (error) {
    const last = snapshot()
    record({ status: 'failed', last, error: describeError(error) })
    throw new Error(`${error.message}; last share observation: ${JSON.stringify(last)}`, {
      cause: error,
    })
  }
  record({ status: 'settled', last: snapshot() })
}

module.exports = { acknowledgementResults, acknowledgementRequests, pollSharePosition }
