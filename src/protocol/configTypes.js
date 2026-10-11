/**
 * The value type DescribeConfigs reports for a config entry (v3 and later).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/requests/DescribeConfigsResponse.java
 * @typedef {number} ConfigTypes
 *
 * @readonly
 * @enum {ConfigTypes}
 */
module.exports = Object.freeze({
  UNKNOWN: 0,
  BOOLEAN: 1,
  STRING: 2,
  INT: 3,
  SHORT: 4,
  LONG: 5,
  DOUBLE: 6,
  LIST: 7,
  CLASS: 8,
  PASSWORD: 9,
})
