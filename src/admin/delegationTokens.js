const { KafkaJSInvalidConfigurationError, KafkaJSInvariantViolation } = require('../errors')
const { retrierOf, waitOf, checked } = require('./deadline')

/** Refuses an invalid field without placing input or credential bytes in its error.
 * @param {boolean} condition accepted shape
 * @param {string} field fixed field name
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} invalid shape
 */
const valid = (condition, field) => {
  if (!condition) throw new KafkaJSInvalidConfigurationError(`Invalid ${field}`)
}
/** Copies and validates a unique principal list.
 * @param {object[]} principals supplied principals
 * @returns {object[]} normalized principal pairs
 * @throws {KafkaJSInvalidConfigurationError} invalid or duplicate principal
 */
const principals = (principals) => {
  valid(Array.isArray(principals), 'principals')
  const seen = new Set()
  return Array.from(principals, (principal) => {
    valid(
      principal &&
        typeof principal.principalType === 'string' &&
        principal.principalType.length > 0 &&
        typeof principal.principalName === 'string' &&
        principal.principalName.length > 0,
      'principal'
    )
    const { principalType, principalName } = principal
    const key = JSON.stringify([principalType, principalName])
    valid(!seen.has(key), 'duplicate principal')
    seen.add(key)
    return { principalType, principalName }
  })
}
/** Normalizes a duration once, preserving signed int64 precision and sentinel semantics.
 * @param {number|string|undefined} value milliseconds
 * @param {boolean} immediate whether zero expires immediately
 * @returns {string} canonical decimal duration
 * @throws {KafkaJSInvalidConfigurationError} unsafe, noncanonical or invalid duration
 */
const duration = (value = -1, immediate = false) => {
  valid(
    typeof value === 'string' || (typeof value === 'number' && Number.isSafeInteger(value)),
    'token duration'
  )
  const text = String(value)
  valid(/^(0|[1-9][0-9]*|-[1-9][0-9]*)$/.test(text), 'token duration')
  const number = BigInt(text)
  valid(
    number <= 9223372036854775807n && (number === -1n || number >= (immediate ? 0n : 1n)),
    'token duration'
  )
  return text
}
/** Maps the wire token to the public token without retaining or logging the credential.
 * @param {object} token decoded wire token
 * @param {object[]} [renewers] create request renewers, absent in its response
 * @returns {object} token with a Buffer HMAC and decimal timestamps
 */
const tokenOf = (token, renewers = token.renewers) => {
  if (
    !Buffer.isBuffer(token.hmac) ||
    token.hmac.length === 0 ||
    typeof token.tokenId !== 'string' ||
    token.tokenId.length === 0
  )
    throw new KafkaJSInvariantViolation('Invalid token result')
  return {
    tokenId: token.tokenId,
    hmac: token.hmac,
    owner: { principalType: token.principalType, principalName: token.principalName },
    renewers,
    issueTimestampMs: token.issueTimestampMs ?? token.issueTimestamp,
    expiryTimestampMs: token.expiryTimestampMs ?? token.expiryTimestamp,
    maxTimestampMs: token.maxTimestampMs ?? token.maxTimestamp,
  }
}

/** Token administration on any connected broker, using the original API deadline. */
class DelegationTokens {
  /** Binds the existing cluster and retry policy; call state stays local to each method.
   * @param {object} options shared cluster and unchanged retry policy
   */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /** Sends only a normalized wire request; no payload or caught error is logged here.
   * @param {string} method broker method
   * @param {object} fields normalized fields
   * @returns {Promise<object>} typed response
   * @throws {Error} capability, fixed broker error, transport or deadline
   */
  async send(method, fields) {
    return retrierOf(this.retry)(async (bail) => {
      try {
        const broker = checked(await waitOf(() => this.cluster.findConnectedBroker()))
        return checked(await waitOf(() => broker[method](fields)))
      } catch (error) {
        if (error.retriable === false) bail(error)
        throw error
      }
    })
  }

  /** Creates a token; an explicit owner requires v3 and is never silently omitted.
   * @param {object} [options] renewers, owner and maxLifetimeMs
   * @returns {Promise<object>} token including its HMAC for the authorized caller
   * @throws {Error} invalid options, unsupported owner, broker refusal or deadline
   */
  async createDelegationToken(options = {}) {
    valid(options != null && typeof options === 'object' && !Array.isArray(options), 'options')
    const { renewers = [], maxLifetimeMs, owner } = options
    const normalized = principals(renewers)
    const principal = owner === undefined ? null : principals([owner])[0]
    const wire = {
      renewers: normalized,
      maxLifetimeMs: duration(maxLifetimeMs),
      ownerPrincipalType: principal?.principalType ?? null,
      ownerPrincipalName: principal?.principalName ?? null,
    }
    return tokenOf(await this.send('createDelegationToken', wire), normalized)
  }

  /** Renews a token by HMAC using -1 for the broker default.
   * @param {object} options nonempty Buffer hmac and renewPeriodMs
   * @returns {Promise<{expiryTimestampMs:string}>} new expiry
   * @throws {Error} invalid input or typed request failure
   */
  async renewDelegationToken(options = {}) {
    valid(options != null && typeof options === 'object' && !Array.isArray(options), 'options')
    const { hmac, renewPeriodMs } = options
    valid(Buffer.isBuffer(hmac) && hmac.length > 0, 'hmac')
    const response = await this.send('renewDelegationToken', {
      hmac,
      renewPeriodMs: duration(renewPeriodMs),
    })
    return { expiryTimestampMs: response.expiryTimestampMs }
  }

  /** Expires a token immediately at zero, or uses -1 for the broker default.
   * @param {object} options nonempty Buffer hmac and expiryTimePeriodMs
   * @returns {Promise<{expiryTimestampMs:string}>} expiry
   * @throws {Error} invalid input or typed request failure
   */
  async expireDelegationToken(options = {}) {
    valid(options != null && typeof options === 'object' && !Array.isArray(options), 'options')
    const { hmac, expiryTimePeriodMs } = options
    valid(Buffer.isBuffer(hmac) && hmac.length > 0, 'hmac')
    const response = await this.send('expireDelegationToken', {
      hmac,
      expiryTimePeriodMs: duration(expiryTimePeriodMs, true),
    })
    return { expiryTimestampMs: response.expiryTimestampMs }
  }

  /** Describes all owners when omitted and no owners when explicitly empty.
   * @param {object} [options] owners
   * @returns {Promise<object[]>} tokens with Buffer HMACs for the authorized caller
   * @throws {Error} invalid principals or typed request failure
   */
  async describeDelegationToken(options = {}) {
    valid(options != null && typeof options === 'object' && !Array.isArray(options), 'options')
    const { owners } = options
    const normalized = owners === undefined ? null : principals(owners)
    if (normalized?.length === 0) return []
    const response = await this.send('describeDelegationToken', { owners: normalized })
    const ids = new Set()
    return response.tokens.map((token) => {
      if (ids.has(token.tokenId)) throw new KafkaJSInvariantViolation('Duplicate token result')
      ids.add(token.tokenId)
      return tokenOf(token)
    })
  }
}
/** Creates the token family, with no test-only exports or additional public wrapper.
 * @param {object} options cluster and retry
 * @returns {object} four bound public methods
 */
module.exports = (options) => {
  const family = new DelegationTokens(options)
  return {
    createDelegationToken: family.createDelegationToken.bind(family),
    renewDelegationToken: family.renewDelegationToken.bind(family),
    expireDelegationToken: family.expireDelegationToken.bind(family),
    describeDelegationToken: family.describeDelegationToken.bind(family),
  }
}
