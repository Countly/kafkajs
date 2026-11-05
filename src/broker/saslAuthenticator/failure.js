const { isNativeError } = require('util').types
const errors = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const { KafkaJSSASLAuthenticationError, KafkaJSNonRetriableError, KafkaJSProtocolError } = errors

const authoredErrors = new WeakSet()

/** Classes a failure may be named by: this library's errors and the built-in programming errors. */
const NAMEABLE_CLASSES = [
  ...Object.values(errors).filter(
    (value) => typeof value === 'function' && value.prototype instanceof Error
  ),
  TypeError,
  RangeError,
  ReferenceError,
  SyntaxError,
]

const localReasons = new Set([
  'Buffers must be of the same length',
  'The server final message reports an error',
  'Invalid server signature in server final message',
  'The server first message reports an error',
  'Invalid server nonce, it does not start with the client nonce',
  'Invalid server nonce, it adds no server part',
  'Missing salt in the server first message',
  'Invalid salt in the server first message',
  'Invalid iteration count in the server first message',
  'Malformed SCRAM server message',
  'The SCRAM server message requires an unsupported mandatory extension',
  'The SCRAM server message has an unknown or repeated attribute',
])

/**
 * Constructs an immutable local diagnostic from the closed validation vocabulary.
 * Copies cannot transfer identity membership. Unapproved inputs produce an untrusted fallback.
 * @param {Function} ErrorClass one of the two supported library constructors
 * @param {string} message a fixed reason or canonical numeric iteration reason
 * @returns {Error} a local diagnostic, or a generic authentication error
 */
const trustedError = (ErrorClass, message) => {
  const match =
    typeof message === 'string'
      ? /^Requested iterations (\S+) is (less than the minimum 4096|more than the maximum 1048576)$/.exec(
          message
        )
      : null
  const count = match ? Number(match[1]) : NaN
  const numericReason =
    match &&
    String(count) === match[1] &&
    (Number.isInteger(count) || count === Infinity) &&
    count >= 0 &&
    (match[2] === 'less than the minimum 4096' ? count < 4096 : count > 1048576)

  if (
    (ErrorClass !== KafkaJSSASLAuthenticationError && ErrorClass !== KafkaJSNonRetriableError) ||
    (!localReasons.has(message) && !numericReason)
  ) {
    return new KafkaJSSASLAuthenticationError('Error')
  }

  const error = new ErrorClass(message)
  Object.defineProperty(error, 'message', { value: message, writable: false, configurable: false })
  authoredErrors.add(error)
  return error
}

/**
 * Returns an immutable local reason, a canonical numeric-code description, or a fallback.
 * Does not execute supplier property accessors or Proxy traps.
 * @param {*} error the exchange failure
 * @returns {string} library-authored diagnostic text
 */
const failureDetail = (error) => {
  if (authoredErrors.has(error)) return Object.getOwnPropertyDescriptor(error, 'message').value
  if (!isNativeError(error)) return 'Error'
  const descriptor = Object.getOwnPropertyDescriptor(error, 'code')
  if (descriptor && Number.isInteger(descriptor.value)) {
    return createErrorFromCode(descriptor.value).message
  }
  return 'Error'
}

/**
 * Finds the nearest class in `NAMEABLE_CLASSES` on a native error's prototype chain.
 * @param {Error} error a native error, never a Proxy itself
 * @returns {Function|undefined} the class, or undefined when the chain holds none
 */
const nameableClass = (error) => {
  for (let proto = Object.getPrototypeOf(error); proto; proto = Object.getPrototypeOf(proto)) {
    const Class = NAMEABLE_CLASSES.find((candidate) => candidate.prototype === proto)
    if (Class) return Class
  }
  return undefined
}

/**
 * Describes a failure by its class alone, for a message that must not carry the failure's text.
 * The class is found by prototype identity in a closed list, never by reading `name`, and a
 * protocol error adds the `type` and `code` the protocol table holds for its own integer `code`.
 * Does not execute supplier property accessors or Proxy traps. A plain `Error`, a non-error, or a
 * class outside the list names nothing, since it says nothing a reader can act on.
 * @param {*} error the failure
 * @returns {string} '' or ` cause=<Class>` with ` type=<TYPE> code=<n>` for a protocol error
 */
const failureCause = (error) => {
  const Class = isNativeError(error) ? nameableClass(error) : undefined
  if (!Class) return ''
  const code = Object.getOwnPropertyDescriptor(error, 'code')?.value
  const protocol =
    error instanceof KafkaJSProtocolError && Number.isInteger(code)
      ? ` type=${createErrorFromCode(code).type} code=${code}`
      : ''
  return ` cause=${Class.name}${protocol}`
}

module.exports = { failureDetail, failureCause, trustedError }
