const MAX_BODY = 40

/**
 * Derives a gate nonce that satisfies the live guard's `^[a-zA-Z0-9_-]{1,64}$` limit.
 * The id is reduced to its first 40 alphanumeric characters, so the result is at most
 * 41 + suffix characters and stays unique per case through the id's random prefix.
 * @param {string} id a case-unique id, for example from `secureRandom()`
 * @param {string} [suffix] a short alphanumeric tag that tells sibling nonces apart
 * @returns {string} the bounded nonce
 */
const gateNonce = (id, suffix = '') =>
  `n${id.replace(/[^a-zA-Z0-9]/g, '').slice(0, MAX_BODY)}${suffix}`

module.exports = { gateNonce }
