const Record = require('../../recordBatch/record/v0')
const { RecordBatch } = require('../../recordBatch/v0')

/**
 * Builds the record batch for one partition's messages. Identical across every Produce request
 * version — only the outer framing differs (classic INT32-length `BYTES` vs flexible
 * `COMPACT_NULLABLE_RECORDS`), which each version's own `partitionsEncoder` applies to the
 * `Encoder` this returns.
 *
 * Each record's timestamp is turned into a number once, from whichever form it was given in
 * (number, numeric string, `Date`, `BigInt` or `Long`). A falsy timestamp, such as an absent one or
 * `0`, is the send time, taken once for the batch, as in 2.2.4. The batch header's first and max
 * timestamps are the lowest and highest of those numbers and every record's delta is taken from
 * the first, so the header and the records always agree.
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
  const timestamps = Float64Array.from(messages, ({ timestamp }) =>
    timestamp ? Number(timestamp) : dateNow
  )

  let firstTimestamp = timestamps[0] ?? dateNow
  let maxTimestamp = firstTimestamp
  for (const timestamp of timestamps) {
    if (timestamp < firstTimestamp) {
      firstTimestamp = timestamp
    } else if (timestamp > maxTimestamp) {
      maxTimestamp = timestamp
    }
  }

  const records = messages.map((message, i) =>
    Record({
      ...message,
      offsetDelta: i,
      timestampDelta: timestamps[i] - firstTimestamp,
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
