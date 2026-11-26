const Encoder = require('../../../../encoder')

/**
 * A flexible Fetch response body with one topic and one partition, whose RECORDS field is the
 * bytes given, so a test can write a length that the answer does or does not hold. The partition
 * tag buffer, the topic tag buffer and the body tag buffer, each one `00` byte, follow the field.
 * @param {(encoder: Encoder) => Encoder} writeTopicKey writes the topic: its name up to v12, its id
 *   from v13
 * @param {Buffer} recordsField the COMPACT_RECORDS field as written on the wire
 * @returns {Buffer}
 */
const fetchBodyWithRecords = (writeTopicKey, recordsField) =>
  new Encoder()
    .writeInt32(0) // ThrottleTimeMs
    .writeInt16(0) // ErrorCode
    .writeInt32(0) // SessionId
    .writeUVarIntArray([
      writeTopicKey(new Encoder())
        .writeUVarIntArray([
          new Encoder()
            .writeInt32(0) // PartitionIndex
            .writeInt16(0) // ErrorCode
            .writeInt64(100) // HighWatermark
            .writeInt64(100) // LastStableOffset
            .writeInt64(0) // LogStartOffset
            .writeUVarIntArray([]) // AbortedTransactions
            .writeInt32(-1) // PreferredReadReplica
            .writeBuffer(recordsField)
            .writeTaggedFields(),
        ])
        .writeTaggedFields(),
    ])
    .writeTaggedFields().buffer

module.exports = { fetchBodyWithRecords }
