const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { apiVersionsV4: expected } = require('./expected')

/** Tags of the ApiVersions response's tagged fields. */
const SUPPORTED_FEATURES = 0
const FINALIZED_FEATURES_EPOCH = 1
const FINALIZED_FEATURES = 2

const encodeApiKey = ({ apiKey, minVersion, maxVersion }) =>
  new Encoder().writeInt16(apiKey).writeInt16(minVersion).writeInt16(maxVersion).writeTaggedFields()

const encodeFeature = ({ name, minVersion, maxVersion }) =>
  new Encoder()
    .writeUVarIntString(name)
    .writeInt16(minVersion)
    .writeInt16(maxVersion)
    .writeTaggedFields()

const encodeFinalized = ({ name, minVersionLevel, maxVersionLevel }) =>
  new Encoder()
    .writeUVarIntString(name)
    .writeInt16(maxVersionLevel)
    .writeInt16(minVersionLevel)
    .writeTaggedFields()

/**
 * ApiVersions v4 (flexible; the response keeps the classic response header). The request carries
 * the client's software name and version; the response lists the APIs the broker offers, and its
 * features in tagged fields.
 *
 * Evidence: both messages are Java's own bytes (`fixtures/v4_*_java.json`, bytes only), and
 * `./expected` holds the fields of their generator.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ApiVersionsResponse.json
 */
module.exports = {
  api: 'ApiVersions',
  apiKey: apiKeys.ApiVersions,
  version: 4,

  /**
   * @param {object} [fields] over the vector's: `errorCode`, `apiKeys` (`{ apiKey, minVersion,
   *   maxVersion }`), `throttleTime`, `supportedFeatures`, `finalizedFeaturesEpoch` and
   *   `finalizedFeatures` (`{ name, minVersionLevel, maxVersionLevel }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...expected.response, ...fields }

    return new Encoder()
      .writeInt16(response.errorCode)
      .writeUVarIntArray(response.apiKeys.map(encodeApiKey))
      .writeInt32(response.throttleTime)
      .writeTaggedFields([
        {
          tag: SUPPORTED_FEATURES,
          value: new Encoder().writeUVarIntArray(response.supportedFeatures.map(encodeFeature)),
        },
        {
          tag: FINALIZED_FEATURES_EPOCH,
          value: new Encoder().writeInt64(response.finalizedFeaturesEpoch),
        },
        {
          tag: FINALIZED_FEATURES,
          value: new Encoder().writeUVarIntArray(response.finalizedFeatures.map(encodeFinalized)),
        },
      ]).buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ clientSoftwareName: string, clientSoftwareVersion: string }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const clientSoftwareName = decoder.readUVarIntString()
    const clientSoftwareVersion = decoder.readUVarIntString()
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('ApiVersions v4 request has a byte after its tag buffer')
    }

    return { clientSoftwareName, clientSoftwareVersion }
  },
}
