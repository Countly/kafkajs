const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const expected = require('./expected').writeTxnMarkersV1

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
  api: 'WriteTxnMarkers',
  apiKey: apiKeys.WriteTxnMarkers,
  version: 1,
  /** Builds fixture response fields. @param {object} fields overrides @returns {Buffer} body @throws {Error} invalid fixture. */
  buildResponse(fields = {}) {
    const data = { ...expected.response, ...fields }
    return new Encoder()
      .writeUVarIntArray(
        data.markers.map((cell0) =>
          new Encoder()
            .writeInt64(cell0.producerId)
            .writeUVarIntArray(
              cell0.topics.map((cell1) =>
                new Encoder()
                  .writeUVarIntString(cell1.topic)
                  .writeUVarIntArray(
                    cell1.partitions.map((cell2) =>
                      new Encoder()
                        .writeInt32(cell2.partition)
                        .writeInt16(cell2.errorCode)
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
      markers: Decoder.requireArray(
        decoder.readUVarIntArray((cell0) =>
          readStruct(cell0, () => ({
            producerId: cell0.readInt64().toString(),
            producerEpoch: cell0.readInt16(),
            transactionResult: cell0.readBoolean(),
            topics: Decoder.requireArray(
              cell0.readUVarIntArray((cell1) =>
                readStruct(cell1, () => ({
                  topic: readString(cell1),
                  partitions: Decoder.requireArray(
                    cell1.readUVarIntArray((cell2) => cell2.readInt32()),
                    'WriteTxnMarkersRequest.Markers.Topics.PartitionIndexes'
                  ),
                }))
              ),
              'WriteTxnMarkersRequest.Markers.Topics'
            ),
            coordinatorEpoch: cell0.readInt32(),
          }))
        ),
        'WriteTxnMarkersRequest.Markers'
      ),
    }))
    if (decoder.canReadBytes(1)) throw new Error('Request has a byte after its tag buffer')
    return data
  },
}
