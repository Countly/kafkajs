const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { electLeadersV2: expected } = require('./expected')
/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readBodyTopicPartitions = (d) => {
  const value = {
    topic: d.readUVarIntString(),
    partitions: Decoder.requireArray(
      d.readUVarIntArray((item) => item.readInt32()),
      'Partitions'
    ),
  }
  d.readTaggedFields()
  if (value.topic === null) throw new Error('topic must not be null')
  return value
}

/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readBody = (d) => {
  const value = {
    electionType: d.readInt8(),
    topicPartitions: d.readUVarIntArray(readBodyTopicPartitions),
    timeoutMs: d.readInt32(),
  }
  d.readTaggedFields()
  return value
}
module.exports = {
  api: 'ElectLeaders',
  apiKey: 43,
  version: 2,
  /** Builds a response from independent field literals. @param {object} fields overrides @returns {Buffer} complete body @throws {Error} invalid fields */
  buildResponse(fields = {}) {
    const value = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(value.throttleTimeMs)
      .writeInt16(value.errorCode)
      .writeUVarIntArray(
        value.replicaElectionResults === null
          ? null
          : value.replicaElectionResults.map((item) =>
              new Encoder()
                .writeUVarIntString(item.topic)
                .writeUVarIntArray(
                  item.partitionResult === null
                    ? null
                    : item.partitionResult.map((item) =>
                        new Encoder()
                          .writeInt32(item.partitionId)
                          .writeInt16(item.errorCode)
                          .writeUVarIntString(item.errorMessage)
                          .writeTaggedFields()
                      )
                )
                .writeTaggedFields()
            )
      )
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
