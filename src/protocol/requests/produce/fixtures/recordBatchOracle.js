const Record = require('../../../recordBatch/record/v0')
const { RecordBatch } = require('../../../recordBatch/v0')

/**
 * The golden oracle of the record batch writer: one partition's batch built the way the nested
 * `Record` and `RecordBatch` encoders build it, a record and a header at a time. A falsy timestamp
 * is the send time (`Date.now()`, taken once); the header's first and max timestamps are the
 * lowest and highest of the records; above 2^53 every timestamp is taken exactly as a `BigInt`.
 * @param {object} options as `buildRecordBatch` takes them, `messages` plain records
 * @returns {Promise<Buffer>} the batch bytes
 */
const oracleRecordBatch = async ({
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
    timestamps = messages.map(({ timestamp }) => {
      if (!timestamp) return BigInt(dateNow)
      try {
        return BigInt(timestamp)
      } catch {
        const number = Number(timestamp)
        return Number.isNaN(number) ? 0n : BigInt(number)
      }
    })
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
    Record({ ...message, offsetDelta: i, timestampDelta: timestamps[i] - firstTimestamp })
  )

  const batch = await RecordBatch({
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
  return Buffer.from(batch.buffer)
}

module.exports = { oracleRecordBatch }
