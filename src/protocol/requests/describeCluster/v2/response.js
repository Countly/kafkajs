const { KafkaJSPartialMessageError } = require('../../../../errors')
const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')
/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readResponseBrokers = (d) => {
  const value = {
    brokerId: d.readInt32(),
    host: d.readUVarIntString(),
    port: d.readInt32(),
    rack: d.readUVarIntString(),
    isFenced: d.readBoolean(),
  }
  d.readTaggedFields()
  if (value.host === null) throw new KafkaJSPartialMessageError('host must not be null')
  return value
}

/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readResponse = (d) => {
  const value = {
    throttleTimeMs: d.readInt32(),
    errorCode: d.readInt16(),
    errorMessage: d.readUVarIntString(),
    endpointType: d.readInt8(),
    clusterId: d.readUVarIntString(),
    controllerId: d.readInt32(),
    brokers: Decoder.requireArray(d.readUVarIntArray(readResponseBrokers), 'Brokers'),
    clusterAuthorizedOperations: d.readInt32(),
  }
  d.readTaggedFields()
  if (value.clusterId === null) throw new KafkaJSPartialMessageError('clusterId must not be null')
  return value
}
/** Decodes the whole body with strict framing and client throttling. @param {Buffer} raw body @returns {Promise<object>} fields @throws {Error} malformed body */
const decode = async (raw) => {
  const d = new Decoder(raw)
  const response = readResponse(d)
  if (d.canReadBytes(1))
    throw new KafkaJSPartialMessageError('DescribeCluster response has trailing bytes')
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
  api: 'DescribeCluster',
  version: 2,
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
      name: 'EndpointType',
      type: 'int8',
      versions: '1+',
    },
    {
      name: 'ClusterId',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'ControllerId',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Brokers',
      type: '[]DescribeClusterBroker',
      versions: '0+',
    },
    {
      name: 'Brokers.BrokerId',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Brokers.Host',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Brokers.Port',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Brokers.Rack',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'Brokers.IsFenced',
      type: 'bool',
      versions: '2+',
    },
    {
      name: 'ClusterAuthorizedOperations',
      type: 'int32',
      versions: '0+',
    },
  ],
}
