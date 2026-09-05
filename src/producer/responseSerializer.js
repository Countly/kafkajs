/**
 * Flattens a decoded produce response into the flat `RecordMetadata[]` shape `Producer#send`
 * returns. Only called on a fully successful response (`sendMessages` throws before reaching
 * here otherwise), so every partition's `errorCode` is 0.
 *
 * From Produce v8, the decoded partition also carries `recordErrors`/`errorMessage` (KIP-467).
 * Those fields describe a *failure* and belong only on the thrown error (see
 * `../protocol/requests/produce/v8/response.js`'s `parse`), never on a successful
 * `RecordMetadata` — so they are dropped here rather than spread through.
 *
 * @param {{ topics: { topicName: string, partitions: object[] }[] }} response decoded response
 * @returns {object[]} one flattened, KIP-467-field-free record per partition
 */
module.exports = ({ topics }) =>
  topics.flatMap(({ topicName, partitions }) =>
    partitions.map(({ recordErrors, errorMessage, ...partition }) => ({
      topicName,
      ...partition,
    }))
  )
