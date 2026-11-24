const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { createAclsV3: expected } = require('./expected')

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
  api: 'CreateAcls',
  apiKey: 30,
  version: 3,

  /** @param {object} [fields] response overrides @returns {Buffer} flexible response body */
  buildResponse(fields = {}) {
    const r = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(r.throttleTime)
      .writeUVarIntArray(
        r.creationResponses.map((row) =>
          new Encoder()
            .writeInt16(row.errorCode)
            .writeUVarIntString(row.errorMessage)
            .writeTaggedFields()
        )
      )
      .writeTaggedFields().buffer
  },

  /** @param {Buffer} body request body @returns {object} decoded fixture fields */
  readRequest(body) {
    const d = new Decoder(body)
    const fields = (() => {
      return {
        creations: d.readUVarIntArray((a) =>
          tagged(a, {
            resourceType: a.readInt8(),
            resourceName: a.readUVarIntString(),
            resourcePatternType: a.readInt8(),
            principal: a.readUVarIntString(),
            host: a.readUVarIntString(),
            operation: a.readInt8(),
            permissionType: a.readInt8(),
          })
        ),
      }
    })()
    d.readTaggedFields()
    if (d.canReadBytes(1)) throw new Error('CreateAcls request has trailing bytes')
    return fields
  },
}
