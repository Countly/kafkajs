const Encoder = require('../../../encoder')
const { WriteTxnMarkers: apiKey } = require('../../apiKeys')

/** Encodes WriteTxnMarkers v2 with compact collections and struct tags.
 * @param {object} options validated fields; decimal strings preserve int64 values
 * @returns {object} request with header v2 and lazy body encoder
 * @throws {Error} invalid internal fields during encoding
 */
module.exports = (options = {}) => {
  options = { ...{}, ...options }
  return {
    apiKey,
    apiVersion: 2,
    apiName: 'WriteTxnMarkers',
    encode: async () =>
      new Encoder()
        .writeUVarIntArray(
          options.markers.map((cell0) =>
            new Encoder()
              .writeInt64(cell0.producerId)
              .writeInt16(cell0.producerEpoch)
              .writeBoolean(cell0.transactionResult)
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
              .writeInt32(cell0.coordinatorEpoch)
              .writeInt8(cell0.transactionVersion ?? 0)
              .writeTaggedFields()
          )
        )
        .writeTaggedFields(),
  }
}

module.exports.schema = {
  api: 'WriteTxnMarkers',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'Markers', type: '[]WritableTxnMarker', versions: '0+' },
    { name: 'Markers.ProducerId', type: 'int64', versions: '0+' },
    { name: 'Markers.ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'Markers.TransactionResult', type: 'bool', versions: '0+' },
    { name: 'Markers.Topics', type: '[]WritableTxnMarkerTopic', versions: '0+' },
    { name: 'Markers.Topics.Name', type: 'string', versions: '0+' },
    { name: 'Markers.Topics.PartitionIndexes', type: '[]int32', versions: '0+' },
    { name: 'Markers.CoordinatorEpoch', type: 'int32', versions: '0+' },
    { name: 'Markers.TransactionVersion', type: 'int8', versions: '2+' },
  ],
}
