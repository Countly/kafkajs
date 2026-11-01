const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { offsetFetchV7: expected } = require('./expected')
module.exports = {
  api: 'OffsetFetch',
  apiKey: 9,
  version: 7,
  /** Encodes the flexible single-group response from independent field literals.
   * @param {object} [fields] response values over the default
   * @returns {Buffer} response body
   */
  buildResponse(fields = {}) {
    const r = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(r.throttleTime)
      .writeUVarIntArray(
        r.topics.map((t) =>
          new Encoder()
            .writeUVarIntString(t.topic)
            .writeUVarIntArray(
              t.partitions.map((p) =>
                new Encoder()
                  .writeInt32(p.partition)
                  .writeInt64(p.offset)
                  .writeInt32(p.leaderEpoch)
                  .writeUVarIntString(p.metadata)
                  .writeInt16(p.errorCode)
                  .writeTaggedFields()
              )
            )
            .writeTaggedFields()
        )
      )
      .writeInt16(r.errorCode)
      .writeTaggedFields().buffer
  },
  /** Reads the v7 request independently, refusing unread bytes.
   * @param {Buffer} body request body
   * @returns {object} groupId, topics and requireStable
   * @throws {Error} truncated or trailing data
   */
  readRequest(body) {
    const d = new Decoder(body)
    const groupId = d.readUVarIntString()
    const topics = d.readUVarIntArray((t) => {
      const row = {
        topic: t.readUVarIntString(),
        partitions: t.readUVarIntArray((p) => ({ partition: p.readInt32() })),
      }
      t.readTaggedFields()
      return row
    })
    const requireStable = d.readBoolean()
    d.readTaggedFields()
    if (d.canReadBytes(1)) throw new Error('OffsetFetch v7 request has trailing bytes')
    return { groupId, topics, requireStable }
  },
}
