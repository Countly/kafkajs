/**
 * Whether reading a value can run the caller's code (a getter, `valueOf`, `toString`, a Proxy).
 * @param {*} value
 * @returns {boolean}
 */
const isObject = (value) =>
  value !== null && (typeof value === 'object' || typeof value === 'function')

/**
 * Captures an int32 field. A primitive is kept as given, for the encoder to check as it always
 * has; an object is converted once here, with the encoder's range and coercion rules.
 * @param {*} value
 * @returns {*} the primitive as given, or the converted number
 * @throws {RangeError} for an object whose number is outside the int32 range
 */
const int32 = (value) => {
  if (!isObject(value)) return value
  const number = +value
  if (number < -2147483648 || number > 2147483647) {
    throw new RangeError('Offset request field is outside the int32 range')
  }
  return Math.trunc(number) || 0
}

/**
 * Captures a nullable Kafka string field. A primitive is kept as given; an object is refused,
 * since the string encoder writes no object.
 * @param {*} value
 * @returns {*} the primitive as given
 * @throws {TypeError} for an object
 */
const text = (value) => {
  if (isObject(value)) throw new TypeError('Offset request field must be a string')
  return value
}

/**
 * Captures an int64 field. A primitive is kept as given; an object (a `Long` or a value with a
 * conversion of its own) is read once and kept as its signed wire value in decimal.
 * @param {*} value
 * @returns {*} the primitive as given, or the decimal string
 */
const int64 = (value) => {
  if (!isObject(value)) return value
  const payload = value.value
  const number = typeof payload === 'bigint' ? payload : BigInt(value)
  return String(BigInt.asIntN(64, number))
}

module.exports = { int32, int64, text }
