const Encoder = require('../../encoder')
const { captureEntries, RecordEntries } = require('../../recordBatch/entries')
const { writeRecordBatch } = require('../../recordBatch/v0/writer')

/**
 * Builds the record batch for one partition's messages. Identical across every Produce request
 * version — only the outer framing differs (classic INT32-length `BYTES` vs flexible
 * `COMPACT_NULLABLE_RECORDS`), which each version's own `partitionsEncoder` applies to the
 * `Encoder` this returns.
 *
 * Plain messages are taken over as owned entries first (`captureEntries`), each field read once;
 * entries `Broker#produce` already captured are used as they are. A record without a timestamp,
 * or with `0`, `0n` or `false`, has the send time: the time the entries were captured, here the
 * time of this call. The batch header's first and max timestamps are the lowest and highest of
 * the records' timestamps and every record's delta is taken from the first; int64 timestamps are
 * kept exactly. The batch is written into a buffer of exactly its size.
 *
 * @param {object} options
 * @param {import('../../message/compression').CompressionTypes} options.compression
 * @param {number} [options.compressionLevel] codec-specific compression level
 * @param {object[]|RecordEntries} options.messages
 * @param {string} [options.transactionalId]
 * @param {number} [options.firstSequence]
 * @param {import('../../../utils/long')} options.producerId
 * @param {number} options.producerEpoch
 * @returns {Promise<import('../../encoder')>} the encoded record batch; callers read
 *   `.size()`/`.buffer` to frame it for their own request version
 * @throws {import('../../../errors').KafkaJSInvalidConfigurationError} for a field that cannot be
 *   written, see `captureEntries`
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
  const entries = RecordEntries.is(messages)
    ? messages
    : captureEntries(messages, { now: Date.now() })
  const batch = await writeRecordBatch({
    entries,
    compression,
    compressionLevel,
    producerId,
    producerEpoch,
    firstSequence,
    transactional: !!transactionalId,
  })
  return Encoder.wrap(batch)
}
