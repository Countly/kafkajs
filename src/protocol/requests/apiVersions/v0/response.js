const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * ApiVersionResponse => ApiVersions
 *   ErrorCode = INT16
 *   ApiVersions = [ApiVersion]
 *     ApiVersion = ApiKey MinVersion MaxVersion
 *       ApiKey = INT16
 *       MinVersion = INT16
 *       MaxVersion = INT16
 */

const apiVersion = (decoder) => ({
  apiKey: decoder.readInt16(),
  minVersion: decoder.readInt16(),
  maxVersion: decoder.readInt16(),
})

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  return {
    errorCode,
    apiVersions: decoder.readArray(apiVersion),
  }
}

const parse = async (data) => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ApiVersions',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ApiKeys', type: '[]ApiVersion', versions: '0+' },
    { name: 'ApiKeys.ApiKey', type: 'int16', versions: '0+' },
    { name: 'ApiKeys.MinVersion', type: 'int16', versions: '0+' },
    { name: 'ApiKeys.MaxVersion', type: 'int16', versions: '0+' },
  ],
}
