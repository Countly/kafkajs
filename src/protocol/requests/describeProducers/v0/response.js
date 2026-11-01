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

/** Decodes DescribeProducers v0; item codes remain data.
 * @param {Buffer} rawData body after header v1
 * @returns {Promise<object>} lossless fields and client-side throttle
 * @throws {KafkaJSPartialMessageError} truncated, forbidden-null or trailing bytes
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const data = readStruct(decoder, () => ({
    throttleTime: decoder.readInt32(),
    topics: Decoder.requireArray(
      decoder.readUVarIntArray((cell0) =>
        readStruct(cell0, () => ({
          topic: readString(cell0),
          partitions: Decoder.requireArray(
            cell0.readUVarIntArray((cell1) =>
              readStruct(cell1, () => ({
                partition: cell1.readInt32(),
                errorCode: cell1.readInt16(),
                errorMessage: cell1.readUVarIntString(),
                activeProducers: Decoder.requireArray(
                  cell1.readUVarIntArray((cell2) =>
                    readStruct(cell2, () => ({
                      producerId: cell2.readInt64().toString(),
                      producerEpoch: cell2.readInt32(),
                      lastSequence: cell2.readInt32(),
                      lastTimestamp: cell2.readInt64().toString(),
                      coordinatorEpoch: cell2.readInt32(),
                      currentTxnStartOffset: cell2.readInt64().toString(),
                    }))
                  ),
                  'DescribeProducersResponse.Topics.Partitions.ActiveProducers'
                ),
              }))
            ),
            'DescribeProducersResponse.Topics.Partitions'
          ),
        }))
      ),
      'DescribeProducersResponse.Topics'
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
  api: 'DescribeProducers',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Topics', type: '[]TopicResponse', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]PartitionResponse', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Partitions.ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Topics.Partitions.ActiveProducers', type: '[]ProducerState', versions: '0+' },
    { name: 'Topics.Partitions.ActiveProducers.ProducerId', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.ActiveProducers.ProducerEpoch', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ActiveProducers.LastSequence', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ActiveProducers.LastTimestamp', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.ActiveProducers.CoordinatorEpoch', type: 'int32', versions: '0+' },
    {
      name: 'Topics.Partitions.ActiveProducers.CurrentTxnStartOffset',
      type: 'int64',
      versions: '0+',
    },
  ],
}
