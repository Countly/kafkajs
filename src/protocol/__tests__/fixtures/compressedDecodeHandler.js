const { performance } = require('perf_hooks')
const { KafkaJSError } = require('../../../errors')
const response = require('../../requests/fetch/v12/response')

/**
 * Decodes one Fetch response body through the real codecs and describes the outcome, as plain
 * data a worker thread can post back. The body's first partition is the one described.
 * @param {object} fuzzCase
 * @param {Uint8Array} fuzzCase.buffer a Fetch v12 response body
 * @param {{ checkCrcs?: boolean, maxDecompressedBatchBytes?: number }} [fuzzCase.options]
 * @returns {Promise<{ outcome: string, violation: string|null, milliseconds: number,
 *   messages: number, error: ?{ reason: string, codec?: string, limit?: number,
 *   nextOffset?: string, offset?: string } }>} `outcome` is `decoded`, `corrupt:<reason>` for a
 *   partition carrying a corruption marker, or `thrown:<class>`; `violation` says what a decoder
 *   may never do: throw an error that is not a KafkaJS one, such as the `RangeError` and
 *   `TypeError` of an untyped failure
 */
module.exports = async ({ buffer, options }) => {
  const started = performance.now()
  let decoded
  let thrown = null

  try {
    decoded = await response.decode(Buffer.from(buffer), options)
  } catch (error) {
    thrown = error
  }

  const milliseconds = performance.now() - started
  const partition = decoded && decoded.responses[0] && decoded.responses[0].partitions[0]
  const corruption = partition && partition.corruption

  const outcome = thrown
    ? `thrown:${thrown.constructor.name}`
    : corruption
      ? `corrupt:${corruption.reason}`
      : 'decoded'
  const { reason, codec, limit, nextOffset, offset } = corruption ? corruption.error : {}

  return {
    outcome,
    violation:
      thrown && !(thrown instanceof KafkaJSError) ? `${thrown.name}: ${thrown.message}` : null,
    milliseconds,
    messages: partition ? partition.messages.length : 0,
    error: corruption ? { reason, codec, limit, nextOffset, offset } : null,
  }
}
