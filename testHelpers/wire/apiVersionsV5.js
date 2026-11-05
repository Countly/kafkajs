const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { apiVersionsV5: expected } = require('./expected')

/** Tags of the ApiVersions response's tagged fields. */
const SUPPORTED_FEATURES = 0
const FINALIZED_FEATURES_EPOCH = 1
const FINALIZED_FEATURES = 2

/** The default of the epoch tag: Java leaves a tag out of the body when it holds its default. */
const NO_EPOCH = '-1'

/**
 * @param {{ unknownTag?: number, unknownTagBytes?: number[] }} fields the response fields
 * @returns {Array<{ tag: number, value: Buffer }>} the unknown tagged field to write into a
 *   struct, none when the fields name none
 */
const unknownTagsOf = ({ unknownTag, unknownTagBytes }) =>
  unknownTag === undefined ? [] : [{ tag: unknownTag, value: Buffer.from(unknownTagBytes) }]

/**
 * ApiVersions v5 (flexible; the response keeps the classic response header). The request carries
 * the client's software name and version and, once both are known, the cluster id and node id the
 * client means to reach (KIP-1242); the response lists the APIs the broker offers, and its features
 * in tagged fields. A tagged field that holds its default (an empty list, the epoch -1) is left
 * out of the body, as Java writes it.
 *
 * Evidence: both messages are Java's own bytes, the structured vectors
 * `apiVersionsRequest-v5-*` and `apiVersionsResponse-v5-*` of kafka-clients 4.4.0, and
 * `./expected` holds the fields of their generator. The answer an older broker gives to a v5 request
 * is the v0-shaped body of the vector `apiVersionsResponse-v0-unsupportedVersion`.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/ApiVersionsRequest.json
 */
module.exports = {
  api: 'ApiVersions',
  apiKey: apiKeys.ApiVersions,
  version: 5,

  /**
   * @param {object} [fields] over the vector's: `errorCode`, `apiKeys` (`{ apiKey, minVersion,
   *   maxVersion }`), `throttleTime`, `supportedFeatures`, `finalizedFeaturesEpoch` and
   *   `finalizedFeatures` (`{ name, minVersionLevel, maxVersionLevel }`); `unknownTag` and
   *   `unknownTagBytes` add one unknown tagged field to the body and to every struct
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...expected.response, ...fields }
    const unknown = unknownTagsOf(response)
    const struct = (...parts) => {
      const encoder = new Encoder()
      for (const write of parts) write(encoder)
      return encoder.writeTaggedFields(unknown)
    }
    const tags = []

    if (response.supportedFeatures.length > 0) {
      tags.push({
        tag: SUPPORTED_FEATURES,
        value: new Encoder().writeUVarIntArray(
          response.supportedFeatures.map(({ name, minVersion, maxVersion }) =>
            struct(
              (e) => e.writeUVarIntString(name),
              (e) => e.writeInt16(minVersion),
              (e) => e.writeInt16(maxVersion)
            )
          )
        ),
      })
    }
    if (String(response.finalizedFeaturesEpoch) !== NO_EPOCH) {
      tags.push({
        tag: FINALIZED_FEATURES_EPOCH,
        value: new Encoder().writeInt64(response.finalizedFeaturesEpoch),
      })
    }
    if (response.finalizedFeatures.length > 0) {
      tags.push({
        tag: FINALIZED_FEATURES,
        value: new Encoder().writeUVarIntArray(
          response.finalizedFeatures.map(({ name, minVersionLevel, maxVersionLevel }) =>
            struct(
              (e) => e.writeUVarIntString(name),
              (e) => e.writeInt16(maxVersionLevel),
              (e) => e.writeInt16(minVersionLevel)
            )
          )
        ),
      })
    }

    return new Encoder()
      .writeInt16(response.errorCode)
      .writeUVarIntArray(
        response.apiKeys.map(({ apiKey, minVersion, maxVersion }) =>
          struct(
            (e) => e.writeInt16(apiKey),
            (e) => e.writeInt16(minVersion),
            (e) => e.writeInt16(maxVersion)
          )
        )
      )
      .writeInt32(response.throttleTime)
      .writeTaggedFields([...tags, ...unknown]).buffer
  },

  /**
   * The answer of a broker that offers ApiVersions only up to v4 to a v5 request: a v0-shaped body
   * (an error code and a classic array of API entries, no throttle time and no tagged fields),
   * which is what lets the client read it before it knows what the broker supports.
   * @param {object} [fields] over the vector's: `errorCode` (35) and `apiKeys`, the entry for
   *   ApiVersions itself (`{ apiKey: 18, minVersion: 0, maxVersion: 4 }`)
   * @returns {Buffer} the response body
   */
  buildUnsupportedVersionResponse(fields = {}) {
    const response = { ...expected.unsupportedVersion, ...fields }

    return new Encoder().writeInt16(response.errorCode).writeArray(
      response.apiKeys.map(({ apiKey, minVersion, maxVersion }) =>
        new Encoder().writeInt16(apiKey).writeInt16(minVersion).writeInt16(maxVersion)
      ),
      'object'
    ).buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ clientSoftwareName: string, clientSoftwareVersion: string, clusterId: string|null,
   *   nodeId: number }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const clientSoftwareName = decoder.readUVarIntString()
    const clientSoftwareVersion = decoder.readUVarIntString()
    const clusterId = decoder.readUVarIntString()
    const nodeId = decoder.readInt32()
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('ApiVersions v5 request has a byte after its tag buffer')
    }

    return { clientSoftwareName, clientSoftwareVersion, clusterId, nodeId }
  },
}
