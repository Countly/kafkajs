const { decode, parse } = require('../v3/response')

/**
 * AddOffsetsToTxn Response (Version: 4) — flexible (KIP-482), response header v1
 *
 * The v3 body and decoded shape. v4 adds TRANSACTION_ABORTABLE to the codes the broker may answer.
 * @see clients/src/main/resources/common/message/AddOffsetsToTxnResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = {
  api: 'AddOffsetsToTxn',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
  ],
}
