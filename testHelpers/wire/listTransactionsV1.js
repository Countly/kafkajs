const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const expected = require('./expected').listTransactionsV1

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
  api: 'ListTransactions',
  apiKey: apiKeys.ListTransactions,
  version: 1,
  /** Builds fixture response fields. @param {object} fields overrides @returns {Buffer} body @throws {Error} invalid fixture. */
  buildResponse(fields = {}) {
    const data = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(data.throttleTime)
      .writeInt16(data.errorCode)
      .writeUVarIntArray(
        data.unknownStateFilters.map((value) => new Encoder().writeUVarIntString(value))
      )
      .writeUVarIntArray(
        data.transactions.map((cell0) =>
          new Encoder()
            .writeUVarIntString(cell0.transactionalId)
            .writeInt64(cell0.producerId)
            .writeUVarIntString(cell0.state)
            .writeTaggedFields()
        )
      )
      .writeTaggedFields().buffer
  },
  /** Reads a flexible request. @param {Buffer} body wire body @returns {object} fields @throws {Error} malformed body. */
  readRequest(body) {
    const decoder = new Decoder(body)
    const data = readStruct(decoder, () => ({
      states: Decoder.requireArray(
        decoder.readUVarIntArray((cell0) => readString(cell0)),
        'ListTransactionsRequest.StateFilters'
      ),
      producerIds: Decoder.requireArray(
        decoder.readUVarIntArray((cell0) => cell0.readInt64().toString()),
        'ListTransactionsRequest.ProducerIdFilters'
      ),
      durationFilter: decoder.readInt64().toString(),
    }))
    if (decoder.canReadBytes(1)) throw new Error('Request has a byte after its tag buffer')
    return data
  },
}
