const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const expected = require('./expected').describeTransactionsV0

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
  api: 'DescribeTransactions',
  apiKey: apiKeys.DescribeTransactions,
  version: 0,
  /** Builds fixture response fields. @param {object} fields overrides @returns {Buffer} body @throws {Error} invalid fixture. */
  buildResponse(fields = {}) {
    const data = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(data.throttleTime)
      .writeUVarIntArray(
        data.transactions.map((cell0) =>
          new Encoder()
            .writeInt16(cell0.errorCode)
            .writeUVarIntString(cell0.transactionalId)
            .writeUVarIntString(cell0.state)
            .writeInt32(cell0.timeoutMs)
            .writeInt64(cell0.startTimeMs)
            .writeInt64(cell0.producerId)
            .writeInt16(cell0.producerEpoch)
            .writeUVarIntArray(
              cell0.topics.map((cell1) =>
                new Encoder()
                  .writeUVarIntString(cell1.topic)
                  .writeUVarIntArray(
                    cell1.partitions.map((value) => new Encoder().writeInt32(value))
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
      transactionalIds: Decoder.requireArray(
        decoder.readUVarIntArray((cell0) => readString(cell0)),
        'DescribeTransactionsRequest.TransactionalIds'
      ),
    }))
    if (decoder.canReadBytes(1)) throw new Error('Request has a byte after its tag buffer')
    return data
  },
}
