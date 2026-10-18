const FAILURE_SOURCE = Symbol('kafkajs:failureSource')

/**
 * Notes on an error which broker sent the request that failed, and which connection generation of
 * that broker it was sent on. The note is a non-enumerable property keyed by a private symbol, so
 * it is no part of the error's public shape; the first note on an error stays.
 * @param {*} error whatever was thrown; only an object can carry the note
 * @param {{ broker: object, generation: number }} source
 * @returns {void}
 */
const attachFailureSource = (error, source) => {
  try {
    if (error !== null && typeof error === 'object' && !(FAILURE_SOURCE in error)) {
      Object.defineProperty(error, FAILURE_SOURCE, { value: source })
    }
  } catch {}
}

/**
 * @param {*} error
 * @returns {{ broker: object, generation: number }|undefined} the broker and pool generation a
 *   request failed on, when `attachFailureSource` noted them on this error
 */
const failureSourceOf = (error) => {
  try {
    return error !== null && typeof error === 'object' ? error[FAILURE_SOURCE] : undefined
  } catch {
    return undefined
  }
}

module.exports = { attachFailureSource, failureSourceOf }
