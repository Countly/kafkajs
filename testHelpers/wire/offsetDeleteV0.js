const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { offsetDeleteV0: expected } = require('./expected')
module.exports = {
  api: 'OffsetDelete',
  apiKey: 47,
  version: 0,
  /** Encodes a classic response from independent field literals.
   * @param {object} [fields] fields over the default response
   * @returns {Buffer} response body
   */
  buildResponse(fields = {}) {
    const r = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt16(r.errorCode)
      .writeInt32(r.throttleTime)
      .writeArray(
        r.topics.map((t) =>
          new Encoder()
            .writeString(t.topic)
            .writeArray(
              t.partitions.map((p) => new Encoder().writeInt32(p.partition).writeInt16(p.errorCode))
            )
        )
      ).buffer
  },
  /** Reads every byte of the classic request independently of the production codec.
   * @param {Buffer} body request body after header
   * @returns {object} literal-shaped request fields
   * @throws {Error} truncated or surplus bytes
   */
  readRequest(body) {
    const d = new Decoder(body)
    const groupId = d.readString()
    const topics = Decoder.requireArray(
      d.readArray((t) => ({
        topic: t.readString(),
        partitions: Decoder.requireArray(
          t.readArray((p) => ({ partition: p.readInt32() })),
          'OffsetDelete.Partitions'
        ),
      })),
      'OffsetDelete.Topics'
    )
    if (d.canReadBytes(1)) throw new Error('OffsetDelete request has trailing bytes')
    return { groupId, topics }
  },
}
