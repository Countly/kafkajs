const Decoder = require('../../../decoder')
const { KafkaJSPartialMessageError } = require('../../../../errors')
const { createErrorFromCode } = require('../../../error')

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

/** Decodes ListTransactions v1; item codes remain data.
 * @param {Buffer} rawData body after header v1
 * @returns {Promise<object>} lossless fields and client-side throttle
 * @throws {KafkaJSPartialMessageError} truncated, forbidden-null or trailing bytes
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const data = readStruct(decoder, () => ({
    throttleTime: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    unknownStateFilters: Decoder.requireArray(
      decoder.readUVarIntArray((cell0) => readString(cell0)),
      'ListTransactionsResponse.UnknownStateFilters'
    ),
    transactions: Decoder.requireArray(
      decoder.readUVarIntArray((cell0) =>
        readStruct(cell0, () => ({
          transactionalId: readString(cell0),
          producerId: cell0.readInt64().toString(),
          state: readString(cell0),
        }))
      ),
      'ListTransactionsResponse.TransactionStates'
    ),
  }))
  if (decoder.canReadBytes(1))
    throw new KafkaJSPartialMessageError('Trailing transaction response bytes')
  return { ...data, throttleTime: 0, clientSideThrottleTime: data.throttleTime }
}

/** Preserves item errors; aggregate errors reject. @param {object} data fields @returns {Promise<object>} fields @throws {KafkaJSProtocolError} aggregate failure. */
const parse = async (data) => {
  if (data.errorCode !== 0) throw createErrorFromCode(data.errorCode)
  return data
}

module.exports = { decode, parse }
module.exports.schema = {
  api: 'ListTransactions',
  version: 1,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'UnknownStateFilters', type: '[]string', versions: '0+' },
    { name: 'TransactionStates', type: '[]TransactionState', versions: '0+' },
    { name: 'TransactionStates.TransactionalId', type: 'string', versions: '0+' },
    { name: 'TransactionStates.ProducerId', type: 'int64', versions: '0+' },
    { name: 'TransactionStates.TransactionState', type: 'string', versions: '0+' },
  ],
}
