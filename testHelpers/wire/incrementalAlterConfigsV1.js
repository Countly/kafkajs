const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { incrementalAlterConfigsV1: expected } = require('./expected')

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
  api: 'IncrementalAlterConfigs',
  apiKey: 44,
  version: 1,

  /** @param {object} [fields] response overrides @returns {Buffer} flexible response body */
  buildResponse(fields = {}) {
    const r = { ...expected.response, ...fields }
    return new Encoder().writeInt32(r.throttleTime).writeUVarIntArray(r.resources.map((row) =>
      new Encoder().writeInt16(row.errorCode).writeUVarIntString(row.errorMessage)
        .writeInt8(row.resourceType).writeUVarIntString(row.resourceName).writeTaggedFields()
    )).writeTaggedFields().buffer
  },

  /** @param {Buffer} body request body @returns {object} decoded fixture fields */
  readRequest(body) {
    const d = new Decoder(body)
    const fields = (() => {
      const resources = d.readUVarIntArray((resource) => tagged(resource, {
        type: resource.readInt8(),
        name: resource.readUVarIntString(),
        configEntries: resource.readUVarIntArray((entry) => tagged(entry, {
          name: entry.readUVarIntString(),
          operation: entry.readInt8(),
          value: entry.readUVarIntString(),
        })),
      }))
      return { resources, validateOnly: d.readBoolean() }
    })()
    d.readTaggedFields()
    if (d.canReadBytes(1)) throw new Error('IncrementalAlterConfigs request has trailing bytes')
    return fields
  },
}
