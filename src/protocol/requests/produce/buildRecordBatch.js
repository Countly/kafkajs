const Record = require('../../recordBatch/record/v0')
const { RecordBatch } = require('../../recordBatch/v0')

/**
 * Builds the record batch for one partition's messages. Identical across every Produce request
 * version — only the outer framing differs (classic INT32-length `BYTES` vs flexible
 * `COMPACT_NULLABLE_RECORDS`), which each version's own `partitionsEncoder` applies to the
 * `Encoder` this returns.
 *
 * @param {object} options
 * @param {import('../../message/compression').CompressionTypes} options.compression
 * @param {object[]} options.messages
 * @param {string} [options.transactionalId]
 * @param {number} [options.firstSequence]
 * @param {import('../../../utils/long')} options.producerId
 * @param {number} options.producerEpoch
 * @returns {Promise<import('../../encoder')>} the encoded record batch; callers read
 *   `.size()`/`.buffer` to frame it for their own request version
 */
module.exports = async ({
  compression,
  messages,
  transactionalId,
  firstSequence,
  producerId,
  producerEpoch,
}) => {
  const dateNow = Date.now()
  const messageTimestamps = messages
    .map((m) => m.timestamp)
    .filter((timestamp) => timestamp != null)
    .sort()

  const timestamps = messageTimestamps.length === 0 ? [dateNow] : messageTimestamps
  const firstTimestamp = timestamps[0]
  const maxTimestamp = timestamps[timestamps.length - 1]

  const records = messages.map((message, i) =>
    Record({
      ...message,
      offsetDelta: i,
      timestampDelta: (message.timestamp || dateNow) - firstTimestamp,
    })
  )

  return RecordBatch({
    compression,
    records,
    firstTimestamp,
    maxTimestamp,
    producerId,
    producerEpoch,
    firstSequence,
    transactional: !!transactionalId,
    lastOffsetDelta: records.length - 1,
  })
}
