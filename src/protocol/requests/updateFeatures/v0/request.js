const Encoder = require('../../../encoder')
const { UpdateFeatures: apiKey } = require('../../apiKeys')
/** Builds a versioned body; capability validation is owned by Broker. @param {object} options wire fields @returns {object} protocol request @throws {Error} invalid encoding */
module.exports = ({ timeoutMs = 60000, featureUpdates = [] } = {}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'UpdateFeatures',
  encode: async () =>
    new Encoder()
      .writeInt32(timeoutMs)
      .writeUVarIntArray(
        featureUpdates === null
          ? null
          : featureUpdates.map((item) =>
              new Encoder()
                .writeUVarIntString(item.feature)
                .writeInt16(item.maxVersionLevel)
                .writeBoolean(item.allowDowngrade)
                .writeTaggedFields()
            )
      )
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'UpdateFeatures',
  version: 0,
  direction: 'request',
  fields: [
    {
      name: 'timeoutMs',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'FeatureUpdates',
      type: '[]FeatureUpdateKey',
      versions: '0+',
    },
    {
      name: 'FeatureUpdates.Feature',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'FeatureUpdates.MaxVersionLevel',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'FeatureUpdates.AllowDowngrade',
      type: 'bool',
      versions: '0',
    },
  ],
}
