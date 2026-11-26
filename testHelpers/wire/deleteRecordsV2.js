const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { deleteRecordsV2: expected } = require('./expected')

/** Skips a struct's flexible tags and returns the fields read from it.
 * @param {Decoder} decoder
 * @param {object} fields
 * @returns {object}
 */
const tagged = (decoder, fields) => {
  decoder.readTaggedFields()
  return fields
}

module.exports = {
  api: 'DeleteRecords',
  apiKey: 21,
  version: 2,

  /** @param {object} [fields] response overrides @returns {Buffer} flexible response body */
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
                  .writeInt64(p.lowWatermark)
                  .writeInt16(p.errorCode)
                  .writeTaggedFields()
              )
            )
            .writeTaggedFields()
        )
      )
      .writeTaggedFields().buffer
  },

  /** @param {Buffer} body request body @returns {object} decoded fixture fields */
  readRequest(body) {
    const d = new Decoder(body)
    const fields = (() => {
      const topics = d.readUVarIntArray((topic) =>
        tagged(topic, {
          topic: topic.readUVarIntString(),
          partitions: topic.readUVarIntArray((p) =>
            tagged(p, { partition: p.readInt32(), offset: p.readInt64().toString() })
          ),
        })
      )
      const timeout = d.readInt32()
      return { topics, timeout }
    })()
    d.readTaggedFields()
    if (d.canReadBytes(1)) throw new Error('DeleteRecords request has trailing bytes')
    return fields
  },
}
