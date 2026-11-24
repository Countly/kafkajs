const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { deleteGroupsV3: expected } = require('./expected')

module.exports = {
  api: 'DeleteGroups',
  apiKey: 42,
  version: 3,

  /** @param {object} [fields] response overrides @returns {Buffer} flexible response body */
  buildResponse(fields = {}) {
    const r = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(r.throttleTime)
      .writeUVarIntArray(
        r.results.map((g) =>
          new Encoder()
            .writeUVarIntString(g.groupId)
            .writeInt16(g.errorCode)
            .writeUVarIntString(g.errorMessage ?? null)
            .writeTaggedFields()
        )
      )
      .writeTaggedFields().buffer
  },

  /** @param {Buffer} body request body @returns {object} decoded fixture fields */
  readRequest(body) {
    const d = new Decoder(body)
    const fields = (() => {
      return d.readUVarIntArray((group) => group.readUVarIntString())
    })()
    d.readTaggedFields()
    if (d.canReadBytes(1)) throw new Error('DeleteGroups request has trailing bytes')
    return fields
  },
}
