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

/** Decodes WriteTxnMarkers v2; item codes remain data.
 * @param {Buffer} rawData body after header v1
 * @returns {Promise<object>} lossless fields and client-side throttle
 * @throws {KafkaJSPartialMessageError} truncated, forbidden-null or trailing bytes
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const data = readStruct(decoder, () => ({
    markers: Decoder.requireArray(
      decoder.readUVarIntArray((cell0) =>
        readStruct(cell0, () => ({
          producerId: cell0.readInt64().toString(),
          topics: Decoder.requireArray(
            cell0.readUVarIntArray((cell1) =>
              readStruct(cell1, () => ({
                topic: readString(cell1),
                partitions: Decoder.requireArray(
                  cell1.readUVarIntArray((cell2) =>
                    readStruct(cell2, () => ({
                      partition: cell2.readInt32(),
                      errorCode: cell2.readInt16(),
                    }))
                  ),
                  'WriteTxnMarkersResponse.Markers.Topics.Partitions'
                ),
              }))
            ),
            'WriteTxnMarkersResponse.Markers.Topics'
          ),
        }))
      ),
      'WriteTxnMarkersResponse.Markers'
    ),
  }))
  if (decoder.canReadBytes(1))
    throw new KafkaJSPartialMessageError('Trailing transaction response bytes')
  return data
}

/** Preserves item errors; aggregate errors reject. @param {object} data fields @returns {Promise<object>} fields @throws {KafkaJSProtocolError} aggregate failure. */
const parse = async (data) => {
  return data
}

module.exports = { decode, parse }
module.exports.schema = {
  api: 'WriteTxnMarkers',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'Markers', type: '[]WritableTxnMarkerResult', versions: '0+' },
    { name: 'Markers.ProducerId', type: 'int64', versions: '0+' },
    { name: 'Markers.Topics', type: '[]WritableTxnMarkerTopicResult', versions: '0+' },
    { name: 'Markers.Topics.Name', type: 'string', versions: '0+' },
    {
      name: 'Markers.Topics.Partitions',
      type: '[]WritableTxnMarkerPartitionResult',
      versions: '0+',
    },
    { name: 'Markers.Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Markers.Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
