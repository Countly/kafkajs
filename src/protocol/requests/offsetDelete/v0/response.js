const Decoder = require('../../../decoder')
/** Decodes OffsetDelete v0 with errors retained as data for family routing.
 * @param {Buffer} rawData classic response body, without its header
 * @returns {Promise<object>} throttle and group/partition error codes
 * @throws {Error} truncated or invalid required arrays
 */
const decode = async (rawData) => {
  const d = new Decoder(rawData)
  const errorCode = d.readInt16()
  const throttleTime = d.readInt32()
  const topics = Decoder.requireArray(
    d.readArray((t) => ({
      topic: t.readString(),
      partitions: Decoder.requireArray(
        t.readArray((q) => ({ partition: q.readInt32(), errorCode: q.readInt16() })),
        'OffsetDelete.Partitions'
      ),
    })),
    'OffsetDelete.Topics'
  )
  return { errorCode, throttleTime: 0, clientSideThrottleTime: throttleTime, topics }
}
/** Leaves result classification to the admin family; no partition error is thrown here.
 * @param {object} data decoded response
 * @returns {Promise<object>} original response
 */
const parse = async (data) => data
module.exports = { decode, parse }
module.exports.schema = {
  api: 'OffsetDelete',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Topics', type: '[]OffsetDeleteResponseTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]OffsetDeleteResponsePartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
