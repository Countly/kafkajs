const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

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

/**
 * Unlike v1+, this decoder does not call `failIfVersionNotSupported`: doing so before
 * `api_keys` is read would discard the KIP-511 hint the broker sends alongside
 * UNSUPPORTED_VERSION (35), which `Broker#apiVersions()` needs for its retry.
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const errorCode = decoder.readInt16()

  return {
    errorCode,
    apiVersions: decoder.readArray(apiVersion),
  }
}

/**
 * @param {{ errorCode: number, apiVersions: { apiKey: number, minVersion: number, maxVersion: number }[] }} data
 * @throws {Error} carrying the decoded `apiVersions` as `error.apiVersions` (KIP-511) so a caller
 *         can retry at the version the broker actually supports without a second round trip.
 */
const parse = async (data) => {
  if (failure(data.errorCode)) {
    const error = createErrorFromCode(data.errorCode)
    // KIP-511: the broker sends the ApiVersions api_keys entry alongside UNSUPPORTED_VERSION so
    // the client can retry at the right version. Carried on the error, not as a new error class.
    error.apiVersions = data.apiVersions
    throw error
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
