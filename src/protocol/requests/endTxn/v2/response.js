const { decode, parse } = require('../v1/response')

/**
 * EndTxn Response (Version: 2) => throttle_time_ms error_code
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *
 * The v1 body, decoded into v1's shape, `{ throttleTime: 0, clientSideThrottleTime, errorCode }`.
 * v2 adds PRODUCER_FENCED to the codes the broker may answer.
 * @see clients/src/main/resources/common/message/EndTxnResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = {
  api: 'EndTxn',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
  ],
}
