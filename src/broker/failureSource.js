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

const RESPONSE_SOURCE = Symbol('kafkajs:responseSource')

/**
 * Notes on an error that it ended the handling of a response, and the API key of the request that
 * response answered. Setup requests (ApiVersions, SASL) note their own keys, so a caller can tell
 * its request's broker reply from a failure on the way to sending it. Private and first-note-stays,
 * like `attachFailureSource`.
 * @param {*} error whatever decoding, parsing or the response guard threw
 * @param {number} apiKey the answered request's API key
 * @returns {void}
 */
const attachResponseSource = (error, apiKey) => {
  try {
    if (error !== null && typeof error === 'object' && !(RESPONSE_SOURCE in error)) {
      Object.defineProperty(error, RESPONSE_SOURCE, { value: apiKey })
    }
  } catch {}
}

/**
 * @param {*} error
 * @returns {number|undefined} the API key of the request whose response handling raised the
 *   error, when `attachResponseSource` noted it
 */
const responseSourceOf = (error) => {
  try {
    return error !== null && typeof error === 'object' ? error[RESPONSE_SOURCE] : undefined
  } catch {
    return undefined
  }
}

module.exports = { attachFailureSource, failureSourceOf, attachResponseSource, responseSourceOf }
