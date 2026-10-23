const Record = require('../../recordBatch/record/v0')
const { RecordBatch } = require('../../recordBatch/v0')

/**
 * The exact int64 value of a timestamp, for a batch that holds one a `number` cannot carry. A form
 * `BigInt` rejects, such as `'1e16'`, falls back to its number; a number that is not one becomes 0,
 * as `Long` makes it.
 * @param {string|number|bigint|Date|import('../../../utils/long')} timestamp
 * @param {number} dateNow the send time, for a falsy timestamp
 * @returns {bigint}
 * @throws {RangeError} for a fractional or infinite number, as the encoders do
 */
const toExactTimestamp = (timestamp, dateNow) => {
  if (!timestamp) {
    return BigInt(dateNow)
  }

  try {
    return BigInt(timestamp)
  } catch {
    const number = Number(timestamp)
    return Number.isNaN(number) ? 0n : BigInt(number)
  }
}

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
 * the first, so the header and the records always agree. When any number is beyond
 * `Number.MAX_SAFE_INTEGER`, the batch is computed in `BigInt` from each form's exact value, so
 * int64 timestamps are kept exactly.
 *
 * @param {object} options
 * @param {import('../../message/compression').CompressionTypes} options.compression
 * @param {number} [options.compressionLevel] codec-specific compression level
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
  compressionLevel,
  messages,
  transactionalId,
  firstSequence,
  producerId,
  producerEpoch,
}) => {
  const dateNow = Date.now()
  let unsafe = false
  let timestamps = Float64Array.from(messages, ({ timestamp }) => {
    const value = timestamp ? Number(timestamp) : dateNow
    unsafe ||= Math.abs(value) > Number.MAX_SAFE_INTEGER
    return value
  })

  if (unsafe) {
    timestamps = messages.map(({ timestamp }) => toExactTimestamp(timestamp, dateNow))
  }

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
    compressionLevel,
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
