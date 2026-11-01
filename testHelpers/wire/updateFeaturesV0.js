const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { updateFeaturesV0: expected } = require('./expected')
/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readBodyFeatureUpdates = (d) => {
  const value = {
    feature: d.readUVarIntString(),
    maxVersionLevel: d.readInt16(),
    allowDowngrade: d.readBoolean(),
  }
  d.readTaggedFields()
  if (value.feature === null) throw new Error('feature must not be null')
  return value
}

/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readBody = (d) => {
  const value = {
    timeoutMs: d.readInt32(),
    featureUpdates: Decoder.requireArray(
      d.readUVarIntArray(readBodyFeatureUpdates),
      'FeatureUpdates'
    ),
  }
  d.readTaggedFields()
  return value
}
module.exports = {
  api: 'UpdateFeatures',
  apiKey: 57,
  version: 0,
  /** Builds a response from independent field literals. @param {object} fields overrides @returns {Buffer} complete body @throws {Error} invalid fields */
  buildResponse(fields = {}) {
    const value = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(value.throttleTimeMs)
      .writeInt16(value.errorCode)
      .writeUVarIntString(value.errorMessage)
      .writeUVarIntArray(
        value.results === null
          ? null
          : value.results.map((item) =>
              new Encoder()
                .writeUVarIntString(item.feature)
                .writeInt16(item.errorCode)
                .writeUVarIntString(item.errorMessage)
                .writeTaggedFields()
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
