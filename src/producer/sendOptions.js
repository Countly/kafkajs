const { KafkaJSInvalidConfigurationError, KafkaJSNotImplemented } = require('../errors')
const {
  Codecs,
  COMPRESSION_CODEC_MASK,
  unsupportedReason,
} = require('../protocol/message/compression')

/**
 * The texts whose value is a zero: a decimal literal of zero digits with an optional sign and
 * exponent, or a hex, octal or binary literal of zero digits (unsigned, as `Number` reads them).
 */
const ZERO_LITERAL = /^(?:[+-]?(?:0+(?:\.0*)?|\.0+)(?:[eE][+-]?[0-9]+)?|0[xX]0+|0[oO]0+|0[bB]0+)$/

/** No compression: shared, so a send without compression allocates nothing for it. */
const NONE = Object.freeze({ bits: 0, factory: null })

/**
 * @param {*} value
 * @returns {boolean} whether `value` is an object or a function
 */
const isObject = (value) =>
  value !== null && (typeof value === 'object' || typeof value === 'function')

/**
 * Names an option value without converting it, so no caller code runs for the message.
 * @param {*} value
 * @returns {string}
 */
const describe = (value) => {
  switch (typeof value) {
    case 'string':
      return JSON.stringify(value)
    case 'bigint':
      return `${value}n`
    case 'symbol':
      return 'a symbol'
    case 'function':
      return 'a function'
    case 'object':
      return value === null ? 'null' : 'an object'
    default:
      return String(value)
  }
}

/**
 * @param {string} option
 * @param {*} value the value as given
 * @param {string} reason
 * @param {*} [cause] what a conversion threw
 * @returns {KafkaJSInvalidConfigurationError}
 */
const refusal = (option, value, reason, cause) =>
  new KafkaJSInvalidConfigurationError(
    `Invalid send option ${option} ${describe(value)}: ${reason}`,
    cause === undefined ? {} : { cause }
  )

/**
 * The language's `ToPrimitive` of an object with hint `'number'`, as unary `+` performs it before
 * converting the result: `Symbol.toPrimitive`, else `valueOf`, then `toString`. Each method is
 * invoked with the language's own call operation, never through a `call` property the value could
 * define or shadow.
 * @param {object} object
 * @returns {*} a primitive
 * @throws {TypeError} when no method gives a primitive; whatever a method throws
 */
const toPrimitive = (object) => {
  const exotic = object[Symbol.toPrimitive]
  if (exotic !== undefined && exotic !== null) {
    if (typeof exotic !== 'function') throw new TypeError('Symbol.toPrimitive is not a function')
    const result = Reflect.apply(exotic, object, ['number'])
    if (!isObject(result)) return result
  } else {
    const valueOf = object.valueOf
    const fromValue = typeof valueOf === 'function' ? Reflect.apply(valueOf, object, []) : object
    if (!isObject(fromValue)) return fromValue
    const toString = object.toString
    const fromString = typeof toString === 'function' ? Reflect.apply(toString, object, []) : object
    if (!isObject(fromString)) return fromString
  }
  throw new TypeError('Cannot convert object to primitive value')
}

/**
 * @param {number} number a converted `acks`
 * @param {*} value the value as given
 * @returns {-1|0|1} ±0 is 0; otherwise truncated toward zero, as the wire writes it, and -1 or 1
 * @throws {KafkaJSInvalidConfigurationError} any other number
 */
const acksOfNumber = (number, value) => {
  if (number === 0) return 0
  const acks = Math.trunc(number)
  if (acks === 1 || acks === -1) return acks
  throw refusal('acks', value, 'acks must be -1 (all replicas), 1 (the leader) or 0 (none)')
}

/**
 * Converts a send's `acks` once. Absent is -1. A string gives 0 only when its trimmed text is a
 * zero literal; a non-zero literal that rounds to zero is refused. An object is converted once to
 * a primitive, as unary `+` does, and that primitive is classified.
 * @param {*} value the option as given
 * @returns {-1|0|1}
 * @throws {KafkaJSInvalidConfigurationError} `null`, `false`, an empty or blank string, a bigint, a
 *   symbol, `undefined` from a conversion, anything not -1, 0 or 1 after truncation; a throwing
 *   conversion as `cause`
 */
const classifyAcks = (value) => {
  if (value === undefined) return -1
  if (typeof value === 'number') return acksOfNumber(value, value)

  let primitive = value
  if (isObject(value)) {
    try {
      primitive = toPrimitive(value)
    } catch (error) {
      throw refusal('acks', value, 'it cannot be converted to a primitive', error)
    }
  }

  switch (typeof primitive) {
    case 'number':
      return acksOfNumber(primitive, value)
    case 'boolean':
      if (primitive) return 1
      break
    case 'string': {
      const text = primitive.trim()
      if (text === '') break
      if (ZERO_LITERAL.test(text)) return 0
      const number = Number(text)
      if (number === 0) {
        throw refusal('acks', value, 'a non-zero number that rounds to zero is not a zero')
      }
      return acksOfNumber(number, value)
    }
  }
  throw refusal('acks', value, 'acks must be -1 (all replicas), 1 (the leader) or 0 (none)')
}

/**
 * Converts a send's `timeout` once, with the language's `ToNumber` as the wire did: `NaN` is 0,
 * the value must lie in int32 before truncation, then it is truncated toward zero.
 * @param {*} value the option as given
 * @returns {number|undefined} undefined when absent, so the broker default of 30000 is sent
 * @throws {KafkaJSInvalidConfigurationError} a bigint, a symbol, a value outside int32; a throwing
 *   conversion as `cause`
 */
const classifyTimeout = (value) => {
  if (value === undefined) return undefined
  let number
  try {
    number = +value
  } catch (error) {
    throw refusal('timeout', value, 'it cannot be converted to a number', error)
  }
  if (Number.isNaN(number)) return 0
  if (!(number >= -2147483648 && number <= 2147483647)) {
    throw refusal('timeout', value, 'timeout must lie between -2147483648 and 2147483647 ms')
  }
  return Math.trunc(number) || 0
}

/**
 * The registry key an option names: the language's `ToPropertyKey`, so a string is its own key, a
 * number its text, and an object the primitive of its string-hint conversion, a symbol kept as it
 * is.
 * @param {*} value a string, a number or an object
 * @returns {string|symbol}
 * @throws {*} whatever the conversion throws
 */
const registryKey = (value) => {
  switch (typeof value) {
    case 'string':
      return value
    case 'number':
      return String(value)
    default:
      return Reflect.ownKeys({ [value]: 0 })[0]
  }
}

/**
 * Converts a send's `compression` once per kind, in kafkajs 2.2.4's order: the batch attribute
 * bits `ToNumber(value) & 7`, then the registry key (`ToPropertyKey(value)`, so an object is
 * converted a second time, with the string hint, and a symbol alias works). The registry is read
 * at the key and, when the key is not the canonical one of those bits, at that canonical key. The
 * value is accepted only when both name the same factory (a registered alias included), so the
 * batch header and its compressed bytes always agree. The factory is pinned to the send, never
 * called. No compression is bits 0 together with the key `'0'` and no codec registered under it.
 * @param {*} value the option as given
 * @returns {{ bits: number, factory: ?Function }} `factory` null for no compression
 * @throws {KafkaJSInvalidConfigurationError} `null`, a boolean, a bigint, a symbol, a key with no
 *   registered factory, bits 0 or 5-7 on a registered factory, or a factory that is not the one
 *   of its bits' canonical key; a throwing conversion as `cause`
 * @throws {KafkaJSNotImplemented} a built-in LZ4 or ZSTD codec this platform cannot run
 */
const classifyCompression = (value) => {
  if (value === undefined || value === 0) return NONE
  const type = typeof value
  if (value === null || type === 'boolean' || type === 'bigint' || type === 'symbol') {
    throw refusal('compression', value, 'compression must be one of CompressionTypes')
  }

  let bits, key
  try {
    bits = value & COMPRESSION_CODEC_MASK
    key = registryKey(value)
  } catch (error) {
    throw refusal('compression', value, 'it cannot be converted', error)
  }

  const factory = Codecs[key]
  if (typeof factory !== 'function') {
    if (bits === 0 && key === '0') return NONE
    throw refusal(
      'compression',
      value,
      `no codec is registered as CompressionCodecs[${String(key)}]`
    )
  }
  if (bits === 0 || bits > 4) {
    throw refusal('compression', value, `its batch attribute bits ${bits} name no codec`)
  }
  const canonical = key === String(bits) ? factory : Codecs[bits]
  if (factory !== canonical) {
    throw refusal(
      'compression',
      value,
      `CompressionCodecs[${String(key)}] is not the codec its batch header names, CompressionCodecs[${bits}]`
    )
  }
  const unsupported = unsupportedReason(factory)
  if (unsupported !== null) throw new KafkaJSNotImplemented(unsupported)
  return { bits, factory }
}

module.exports = { classifyAcks, classifyTimeout, classifyCompression }
