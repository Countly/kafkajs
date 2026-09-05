/**
 * Flattens a decoded produce response into the flat `RecordMetadata[]` shape `Producer#send`
 * returns. Only called on a fully successful response (`sendMessages` throws before reaching
 * here otherwise), so every partition's `errorCode` is 0.
 *
 * Reads an explicit allowlist of fields rather than spreading the decoded partition: from v8 the
 * partition also carries `recordErrors`/`errorMessage` (KIP-467), and from v10 it carries
 * `currentLeader` (KIP-951) — both describe a *failure* and belong only on the thrown error (see
 * `../protocol/requests/produce/v8/response.js` and `v10/response.js`'s `parse`), never on a
 * successful `RecordMetadata`. An allowlist, rather than naming those fields to drop, means a
 * future additive decode field can't leak here by default.
 *
 * @param {{ topics: { topicName: string, partitions: object[] }[] }} response decoded response
 * @returns {object[]} one flattened record per partition, exactly `RecordMetadata`'s shape
 */
module.exports = ({ topics }) =>
  topics.flatMap(({ topicName, partitions }) =>
    partitions.map(({ partition, errorCode, baseOffset, logAppendTime, logStartOffset }) => ({
      topicName,
      partition,
      errorCode,
      baseOffset,
      logAppendTime,
      logStartOffset,
    }))
  )
