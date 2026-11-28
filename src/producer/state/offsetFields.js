const Long = require('../../utils/long')

/**
 * Captures the number the int32 encoder writes, including its range and coercion rules.
 * @param {*} value
 * @returns {number}
 */
const int32 = (value) => {
  const number = +value
  if (number < -2147483648 || number > 2147483647) {
    throw new RangeError('Offset request field is outside the int32 range')
  }
  return Math.trunc(number) || 0
}

/**
 * Captures a nullable Kafka string. The string encoder accepts strings without coercion.
 * @param {*} value
 * @returns {string|null|undefined}
 */
const text = (value) => {
  if (value != null && typeof value !== 'string') {
    throw new TypeError('Offset request field must be a string')
  }
  return value
}

/**
 * Captures an int64 as its signed wire value in decimal, reading the payload of a Long once.
 * @param {*} value
 * @returns {string}
 */
const int64 = (value) => {
  let number
  if (value != null && (typeof value === 'object' || typeof value === 'function')) {
    const payload = value.value
    number = typeof payload === 'bigint' ? payload : BigInt(value)
  } else {
    number = Long.fromValue(value).value
  }
  return String(BigInt.asIntN(64, number))
}

module.exports = { int32, int64, text }
