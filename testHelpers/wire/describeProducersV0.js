const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const expected = require('./expected').describeProducersV0

/** Reads a required compact string. @param {Decoder} decoder source @returns {string} value @throws {KafkaJSPartialMessageError} null. */
const readString = (decoder) => {
  const value = decoder.readUVarIntString()
  if (value === null) throw new KafkaJSPartialMessageError('Required transaction string is null')
  return value
}

/** Reads fields and struct tags. @param {Decoder} decoder source @param {Function} read fields @returns {object} fields @throws {Error} malformed bytes. */
const readStruct = (decoder, read) => {
  const value = read()
  decoder.readTaggedFields()
  return value
}

module.exports = {
  api: 'DescribeProducers',
  apiKey: apiKeys.DescribeProducers,
  version: 0,
  /** Builds fixture response fields. @param {object} fields overrides @returns {Buffer} body @throws {Error} invalid fixture. */
  buildResponse(fields = {}) {
    const data = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(data.throttleTime)
      .writeUVarIntArray(
        data.topics.map((cell0) =>
          new Encoder()
            .writeUVarIntString(cell0.topic)
            .writeUVarIntArray(
              cell0.partitions.map((cell1) =>
                new Encoder()
                  .writeInt32(cell1.partition)
                  .writeInt16(cell1.errorCode)
                  .writeUVarIntString(cell1.errorMessage)
                  .writeUVarIntArray(
                    cell1.activeProducers.map((cell2) =>
                      new Encoder()
                        .writeInt64(cell2.producerId)
                        .writeInt32(cell2.producerEpoch)
                        .writeInt32(cell2.lastSequence)
                        .writeInt64(cell2.lastTimestamp)
                        .writeInt32(cell2.coordinatorEpoch)
                        .writeInt64(cell2.currentTxnStartOffset)
                        .writeTaggedFields()
                    )
                  )
                  .writeTaggedFields()
              )
            )
            .writeTaggedFields()
        )
      )
      .writeTaggedFields().buffer
  },
  /** Reads a flexible request. @param {Buffer} body wire body @returns {object} fields @throws {Error} malformed body. */
  readRequest(body) {
    const decoder = new Decoder(body)
    const data = readStruct(decoder, () => ({
      topics: Decoder.requireArray(
        decoder.readUVarIntArray((cell0) =>
          readStruct(cell0, () => ({
            topic: readString(cell0),
            partitions: Decoder.requireArray(
              cell0.readUVarIntArray((cell1) => cell1.readInt32()),
              'DescribeProducersRequest.Topics.PartitionIndexes'
            ),
          }))
        ),
        'DescribeProducersRequest.Topics'
      ),
    }))
    if (decoder.canReadBytes(1)) throw new Error('Request has a byte after its tag buffer')
    return data
  },
}
