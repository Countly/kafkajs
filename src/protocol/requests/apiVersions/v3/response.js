const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * ApiVersionsResponse (Version: 3) — flexible (KIP-482) body, response header v0
 *
 * ErrorCode => INT16 (0+)
 * ApiKeys => COMPACT_ARRAY (0+)
 *   ApiKey => INT16 (0+)
 *   MinVersion => INT16 (0+)
 *   MaxVersion => INT16 (0+)
 *   TAG_BUFFER
 * ThrottleTimeMs => INT32 (1+)
 * TAG_BUFFER
 *   tag 0 SupportedFeatures, tag 1 FinalizedFeaturesEpoch (default -1),
 *   tag 2 FinalizedFeatures, tag 3 ZkMigrationReady (default false)
 *
 * The response header stays v0 for every ApiVersions version so the UNSUPPORTED_VERSION
 * downgrade is parseable before the version is known. On that error the body itself is v0-shaped
 * too, which is why decode() branches before reading anything else.
 * @see https://kafka.apache.org/43/design/protocol
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers
 * @see clients/src/main/resources/common/message/ApiVersionsResponse.json
 */

const UNSUPPORTED_VERSION = 35

const SUPPORTED_FEATURES_TAG = 0
const FINALIZED_FEATURES_EPOCH_TAG = 1
const FINALIZED_FEATURES_TAG = 2
const ZK_MIGRATION_READY_TAG = 3

const decodeApiVersion = (decoder) => {
  const apiVersion = {
    apiKey: decoder.readInt16(),
    minVersion: decoder.readInt16(),
    maxVersion: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return apiVersion
}

const decodeSupportedFeature = (decoder) => {
  const feature = {
    name: decoder.readUVarIntString(),
    minVersion: decoder.readInt16(),
    maxVersion: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return feature
}

const decodeFinalizedFeature = (decoder) => {
  const feature = {
    name: decoder.readUVarIntString(),
    maxVersionLevel: decoder.readInt16(),
    minVersionLevel: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return feature
}

/**
 * @param {Map<number, Buffer>} taggedFields
 * @param {number} tag
 * @param {(decoder: Decoder) => any} read
 * @param {any} fallback the field's default from the message definition
 */
const readTag = (taggedFields, tag, read, fallback) => {
  const value = taggedFields.get(tag)
  return value === undefined ? fallback : read(new Decoder(value))
}

/**
 * A broker that rejects the requested version answers with a v0-shaped body: INT16 error code,
 * then a classic INT32-prefixed array of (apiKey, minVersion, maxVersion) and nothing else.
 */
const decodeApiVersionV0 = (decoder) => ({
  apiKey: decoder.readInt16(),
  minVersion: decoder.readInt16(),
  maxVersion: decoder.readInt16(),
})

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const errorCode = decoder.readInt16()

  if (errorCode === UNSUPPORTED_VERSION) {
    return { errorCode, apiVersions: decoder.readArray(decodeApiVersionV0) }
  }

  const apiVersions = decoder.readUVarIntArray(decodeApiVersion) || []
  const throttleTime = decoder.readInt32()
  const taggedFields = decoder.readTaggedFields()

  return {
    errorCode,
    apiVersions,
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    supportedFeatures: readTag(
      taggedFields,
      SUPPORTED_FEATURES_TAG,
      (d) => d.readUVarIntArray(decodeSupportedFeature) || [],
      []
    ),
    finalizedFeaturesEpoch: readTag(
      taggedFields,
      FINALIZED_FEATURES_EPOCH_TAG,
      (d) => d.readInt64().toString(),
      '-1'
    ),
    finalizedFeatures: readTag(
      taggedFields,
      FINALIZED_FEATURES_TAG,
      (d) => d.readUVarIntArray(decodeFinalizedFeature) || [],
      []
    ),
    zkMigrationReady: readTag(taggedFields, ZK_MIGRATION_READY_TAG, (d) => d.readBoolean(), false),
  }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ApiVersions',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ApiKeys', type: '[]ApiVersion', versions: '0+' },
    { name: 'ApiKeys.ApiKey', type: 'int16', versions: '0+' },
    { name: 'ApiKeys.MinVersion', type: 'int16', versions: '0+' },
    { name: 'ApiKeys.MaxVersion', type: 'int16', versions: '0+' },
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'SupportedFeatures', type: '[]SupportedFeatureKey', versions: '3+', tag: 0 },
    { name: 'SupportedFeatures.Name', type: 'string', versions: '3+' },
    { name: 'SupportedFeatures.MinVersion', type: 'int16', versions: '3+' },
    { name: 'SupportedFeatures.MaxVersion', type: 'int16', versions: '3+' },
    { name: 'FinalizedFeaturesEpoch', type: 'int64', versions: '3+', tag: 1 },
    { name: 'FinalizedFeatures', type: '[]FinalizedFeatureKey', versions: '3+', tag: 2 },
    { name: 'FinalizedFeatures.Name', type: 'string', versions: '3+' },
    { name: 'FinalizedFeatures.MaxVersionLevel', type: 'int16', versions: '3+' },
    { name: 'FinalizedFeatures.MinVersionLevel', type: 'int16', versions: '3+' },
    { name: 'ZkMigrationReady', type: 'bool', versions: '3+', tag: 3 },
  ],
}
