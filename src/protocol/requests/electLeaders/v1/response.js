const { KafkaJSPartialMessageError } = require('../../../../errors')
const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')
/** Reads a nonnullable classic array without accepting the null sentinel, bounded by the decoder's count limit. @param {Decoder} d cursor @param {Function} read entry reader @returns {Array} entries @throws {Error} invalid, over-limit or truncated length */
const readClassicArray = (d, read) => {
  const length = d.readInt32()
  d.assertCount(length, 'array')
  d.ensureReadable(length, 'array')
  const values = []
  for (let i = 0; i < length; i++) values.push(read(d))
  return values
}
/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readResponseReplicaElectionResultsPartitionResult = (d) => {
  const value = {
    partitionId: d.readInt32(),
    errorCode: d.readInt16(),
    errorMessage: d.readString(),
  }
  return value
}

/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readResponseReplicaElectionResults = (d) => {
  const value = {
    topic: d.readString(),
    partitionResult: Decoder.requireArray(
      readClassicArray(d, readResponseReplicaElectionResultsPartitionResult),
      'PartitionResult'
    ),
  }
  if (value.topic === null) throw new KafkaJSPartialMessageError('topic must not be null')
  return value
}

/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readResponse = (d) => {
  const value = {
    throttleTimeMs: d.readInt32(),
    errorCode: d.readInt16(),
    replicaElectionResults: Decoder.requireArray(
      readClassicArray(d, readResponseReplicaElectionResults),
      'ReplicaElectionResults'
    ),
  }
  return value
}
/** Decodes the whole body with strict framing and client throttling. @param {Buffer} raw body @returns {Promise<object>} fields @throws {Error} malformed body */
const decode = async (raw) => {
  const d = new Decoder(raw)
  const response = readResponse(d)
  if (d.canReadBytes(1))
    throw new KafkaJSPartialMessageError('ElectLeaders response has trailing bytes')
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
  api: 'ElectLeaders',
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
      versions: '1+',
    },
    {
      name: 'ReplicaElectionResults',
      type: '[]ReplicaElectionResult',
      versions: '0+',
    },
    {
      name: 'ReplicaElectionResults.Topic',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'ReplicaElectionResults.PartitionResult',
      type: '[]PartitionResult',
      versions: '0+',
    },
    {
      name: 'ReplicaElectionResults.PartitionResult.PartitionId',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'ReplicaElectionResults.PartitionResult.ErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'ReplicaElectionResults.PartitionResult.ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
  ],
}
