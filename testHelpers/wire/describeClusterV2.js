const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { describeClusterV2: expected } = require('./expected')
/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readBody = (d) => {
  const value = {
    includeClusterAuthorizedOperations: d.readBoolean(),
    endpointType: d.readInt8(),
    includeFencedBrokers: d.readBoolean(),
  }
  d.readTaggedFields()
  return value
}
module.exports = {
  api: 'DescribeCluster',
  apiKey: 60,
  version: 2,
  /** Builds a response from independent field literals. @param {object} fields overrides @returns {Buffer} complete body @throws {Error} invalid fields */
  buildResponse(fields = {}) {
    const value = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(value.throttleTimeMs)
      .writeInt16(value.errorCode)
      .writeUVarIntString(value.errorMessage)
      .writeInt8(value.endpointType)
      .writeUVarIntString(value.clusterId)
      .writeInt32(value.controllerId)
      .writeUVarIntArray(
        value.brokers === null
          ? null
          : value.brokers.map((item) =>
              new Encoder()
                .writeInt32(item.brokerId)
                .writeUVarIntString(item.host)
                .writeInt32(item.port)
                .writeUVarIntString(item.rack)
                .writeBoolean(item.isFenced)
                .writeTaggedFields()
            )
      )
      .writeInt32(value.clusterAuthorizedOperations)
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
