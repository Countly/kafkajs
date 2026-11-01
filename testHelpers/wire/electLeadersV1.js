const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { electLeadersV1: expected } = require('./expected')
/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readBodyTopicPartitions = (d) => {
  const value = {
    topic: d.readString(),
    partitions: Decoder.requireArray(
      readArray(d, (item) => item.readInt32()),
      'Partitions'
    ),
  }
  if (value.topic === null) throw new Error('topic must not be null')
  return value
}

/** Reads one complete wire structure. @param {Decoder} d cursor @returns {object} fields @throws {Error} malformed bytes */
const readBody = (d) => {
  const value = {
    electionType: d.readInt8(),
    topicPartitions: readArray(d, readBodyTopicPartitions, true),
    timeoutMs: d.readInt32(),
  }
  return value
}
/** Reads classic arrays without erasing the nullable list sentinel. @param {Decoder} d cursor @param {Function} read entry reader @param {boolean} [nullable=false] sentinel allowed @returns {Array|null} entries @throws {Error} malformed length */
const readArray = (d, read, nullable = false) => {
  const length = d.readInt32()
  if (length === -1 && nullable) return null
  d.ensureReadable(length, 'array')
  return Array.from({ length }, () => read(d))
}
module.exports = {
  api: 'ElectLeaders',
  apiKey: 43,
  version: 1,
  /** Builds a response from independent field literals. @param {object} fields overrides @returns {Buffer} complete body @throws {Error} invalid fields */
  buildResponse(fields = {}) {
    const value = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(value.throttleTimeMs)
      .writeInt16(value.errorCode)
      .writeArray(
        value.replicaElectionResults === null
          ? null
          : value.replicaElectionResults.map((item) =>
              new Encoder()
                .writeString(item.topic)
                .writeArray(
                  item.partitionResult === null
                    ? null
                    : item.partitionResult.map((item) =>
                        new Encoder()
                          .writeInt32(item.partitionId)
                          .writeInt16(item.errorCode)
                          .writeString(item.errorMessage)
                      )
                )
            )
      ).buffer
  },
  /** Reads every request byte. @param {Buffer} body wire body @returns {object} fields @throws {Error} malformed or trailing bytes */
  readRequest(body) {
    const d = new Decoder(body)
    const value = readBody(d)
    if (d.canReadBytes(1)) throw new Error('Request has trailing bytes after its tag buffer')
    return value
  },
}
