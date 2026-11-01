const Decoder = require('../../../decoder')
const { KafkaJSPartialMessageError } = require('../../../../errors')

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

/** Decodes DescribeTransactions v0; item codes remain data.
 * @param {Buffer} rawData body after header v1
 * @returns {Promise<object>} lossless fields and client-side throttle
 * @throws {KafkaJSPartialMessageError} truncated, forbidden-null or trailing bytes
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const data = readStruct(decoder, () => ({
    throttleTime: decoder.readInt32(),
    transactions: Decoder.requireArray(
      decoder.readUVarIntArray((cell0) =>
        readStruct(cell0, () => ({
          errorCode: cell0.readInt16(),
          transactionalId: readString(cell0),
          state: readString(cell0),
          timeoutMs: cell0.readInt32(),
          startTimeMs: cell0.readInt64().toString(),
          producerId: cell0.readInt64().toString(),
          producerEpoch: cell0.readInt16(),
          topics: Decoder.requireArray(
            cell0.readUVarIntArray((cell1) =>
              readStruct(cell1, () => ({
                topic: readString(cell1),
                partitions: Decoder.requireArray(
                  cell1.readUVarIntArray((cell2) => cell2.readInt32()),
                  'DescribeTransactionsResponse.TransactionStates.Topics.Partitions'
                ),
              }))
            ),
            'DescribeTransactionsResponse.TransactionStates.Topics'
          ),
        }))
      ),
      'DescribeTransactionsResponse.TransactionStates'
    ),
  }))
  if (decoder.canReadBytes(1))
    throw new KafkaJSPartialMessageError('Trailing transaction response bytes')
  return { ...data, throttleTime: 0, clientSideThrottleTime: data.throttleTime }
}

/** Preserves item errors; aggregate errors reject. @param {object} data fields @returns {Promise<object>} fields @throws {KafkaJSProtocolError} aggregate failure. */
const parse = async (data) => {
  return data
}

module.exports = { decode, parse }
module.exports.schema = {
  api: 'DescribeTransactions',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'TransactionStates', type: '[]TransactionState', versions: '0+' },
    { name: 'TransactionStates.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'TransactionStates.TransactionalId', type: 'string', versions: '0+' },
    { name: 'TransactionStates.TransactionState', type: 'string', versions: '0+' },
    { name: 'TransactionStates.TransactionTimeoutMs', type: 'int32', versions: '0+' },
    { name: 'TransactionStates.TransactionStartTimeMs', type: 'int64', versions: '0+' },
    { name: 'TransactionStates.ProducerId', type: 'int64', versions: '0+' },
    { name: 'TransactionStates.ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'TransactionStates.Topics', type: '[]TopicData', versions: '0+' },
    { name: 'TransactionStates.Topics.Topic', type: 'string', versions: '0+' },
    { name: 'TransactionStates.Topics.Partitions', type: '[]int32', versions: '0+' },
  ],
}
