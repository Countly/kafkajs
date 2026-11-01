const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { listConfigResourcesV0: expected } = require('./expected')
/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readBody = (d) => {
  const value = {}
  d.readTaggedFields()
  return value
}
module.exports = {
  api: 'ListConfigResources',
  apiKey: 74,
  version: 0,
  /** Builds a response from independent field literals. @param {object} fields overrides @returns {Buffer} complete body @throws {Error} invalid fields */
  buildResponse(fields = {}) {
    const value = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(value.throttleTimeMs)
      .writeInt16(value.errorCode)
      .writeUVarIntArray(
        value.configResources === null
          ? null
          : value.configResources.map((item) =>
              new Encoder().writeUVarIntString(item.resourceName).writeTaggedFields()
            )
      )
      .writeTaggedFields().buffer
  },
  /** Reads every request byte. @param {Buffer} body wire body @returns {object} fields @throws {Error} malformed or trailing bytes */
  readRequest(body) {
    const d = new Decoder(body)
    const value = readBody(d)
    if (d.canReadBytes(1)) throw new Error('Request has trailing bytes after its tag buffer')
    return value
  },
}
