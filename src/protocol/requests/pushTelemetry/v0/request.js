const Encoder = require('../../../encoder')
const { PushTelemetry: apiKey } = require('../../apiKeys')
const { KafkaJSNonRetriableError } = require('../../../../errors')

/**
 * Builds a flexible PushTelemetry v0 request. Metrics is non-null compact bytes; an empty
 * Buffer sends an empty payload. The caller owns subscription identity, compression and limits.
 * @param {object} [options] body fields
 * @param {string|Buffer|null} [options.clientInstanceId=null] UUID, with null meaning all zero
 * @param {number} options.subscriptionId opaque signed int32, including zero and negatives
 * @param {boolean} [options.terminating=false] whether this is the bounded final push
 * @param {number} [options.compressionType=0] signed int8 compression identifier
 * @param {Buffer} options.metrics already encoded and compressed bytes, never null
 * @returns {object} metadata and asynchronous encoder; encoding does not retry or compress
 * @throws {KafkaJSNonRetriableError} from encode when Metrics is not a Buffer
 * @throws {Error} from encode for invalid UUIDs or integer fields
 */
module.exports = ({
  clientInstanceId = null,
  subscriptionId,
  terminating = false,
  compressionType = 0,
  metrics,
} = {}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'PushTelemetry',
  /**
   * @returns {Promise<Encoder>} the body, without a request header
   * @throws {KafkaJSNonRetriableError} when Metrics is null, omitted or not a Buffer
   */
  encode: async () => {
    if (!Buffer.isBuffer(metrics)) {
      throw new KafkaJSNonRetriableError('PushTelemetry Metrics must be a non-null Buffer')
    }
    return new Encoder()
      .writeUuid(clientInstanceId)
      .writeInt32(subscriptionId)
      .writeBoolean(terminating)
      .writeInt8(compressionType)
      .writeUVarIntBytes(metrics)
      .writeTaggedFields()
  },
})
module.exports.schema = {
  api: 'PushTelemetry',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'ClientInstanceId', type: 'uuid', versions: '0+' },
    { name: 'SubscriptionId', type: 'int32', versions: '0+' },
    { name: 'Terminating', type: 'bool', versions: '0+' },
    { name: 'CompressionType', type: 'int8', versions: '0+' },
    { name: 'Metrics', type: 'bytes', versions: '0+' },
  ],
}
