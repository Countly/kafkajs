const { KafkaJSPartialMessageError } = require('../../../../errors')
const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')
/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readResponseResults = (d) => {
  const value = {
    feature: d.readUVarIntString(),
    errorCode: d.readInt16(),
    errorMessage: d.readUVarIntString(),
  }
  d.readTaggedFields()
  if (value.feature === null) throw new KafkaJSPartialMessageError('feature must not be null')
  return value
}

/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readResponse = (d) => {
  const value = {
    throttleTimeMs: d.readInt32(),
    errorCode: d.readInt16(),
    errorMessage: d.readUVarIntString(),
    results: Decoder.requireArray(d.readUVarIntArray(readResponseResults), 'Results'),
  }
  d.readTaggedFields()
  return value
}
/** Decodes the whole body with strict framing and client throttling. @param {Buffer} raw body @returns {Promise<object>} fields @throws {Error} malformed body */
const decode = async (raw) => {
  const d = new Decoder(raw)
  const response = readResponse(d)
  if (d.canReadBytes(1))
    throw new KafkaJSPartialMessageError('UpdateFeatures response has trailing bytes')
  return { ...response, throttleTime: 0, clientSideThrottleTime: response.throttleTimeMs }
}
/** Preserves item outcomes and throws a typed call-level error. @param {object} response decoded body @returns {Promise<object>} fields @throws {Error} broker-level error */
const parse = async (response) => {
  if (response.errorCode) {
    const error = createErrorFromCode(response.errorCode)
    error.brokerErrorMessage = response.errorMessage ?? null
    throw error
  }
  return response
}
module.exports = { decode, parse }

module.exports.schema = {
  api: 'UpdateFeatures',
  version: 1,
  direction: 'response',
  fields: [
    {
      name: 'ThrottleTimeMs',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'ErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'Results',
      type: '[]UpdatableFeatureResult',
      versions: '0-1',
    },
    {
      name: 'Results.Feature',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Results.ErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'Results.ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
  ],
}
