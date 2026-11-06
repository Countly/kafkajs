const crypto = require('node:crypto')
const { SCRAM, DIGESTS } = require('../broker/saslAuthenticator/scram')
const {
  KafkaJSProtocolError,
  KafkaJSInvalidConfigurationError,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
} = require('../errors')
const { itemRetry } = require('./itemRetry')
const apiKeys = require('../protocol/requests/apiKeys')
const { responseSourceOf } = require('../broker/failureSource')
const { retrierOf, currentDeadline, checked, waitOf } = require('./deadline')
const MECHANISMS = { 'SCRAM-SHA-256': 1, 'SCRAM-SHA-512': 2 }
const DIGEST = { 'SCRAM-SHA-256': DIGESTS.SHA256, 'SCRAM-SHA-512': DIGESTS.SHA512 }

/** Refuses a field with stable text that never interpolates supplied credentials.
 * @param {boolean} condition accepted input
 * @param {string} field fixed field name
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} invalid input
 */
const valid = (condition, field) => {
  if (!condition) throw new KafkaJSInvalidConfigurationError(`Invalid ${field}`)
}
/** Validates a nonempty user name.
 * @param {unknown} user user name
 * @returns {boolean} accepted name
 */
const name = (user) => typeof user === 'string' && user.length > 0
/** Whether a failure rejects the complete additive call.
 * @param {Error} error operation failure
 * @returns {boolean} capability, validation, invariant or original deadline refusal
 */
const callFailure = (error) =>
  error instanceof KafkaJSServerDoesNotSupportApiKey ||
  error instanceof KafkaJSInvalidConfigurationError ||
  error instanceof KafkaJSInvariantViolation ||
  error === currentDeadline()?.refusal()

/** Credential administration; credential bytes exist only in a single call's local work. */
class ScramCredentials {
  /** Binds the existing cluster and retry policy; call state stays local to each method.
   * @param {object} options shared cluster and unchanged retry policy
   */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /** Describes credentials without password, salt or derived bytes in returned diagnostics.
   * @param {object} [options] users omitted/null selects all; [] returns no work
   * @returns {Promise<object[]>} per-user results, sanitized messages and supported mechanisms
   * @throws {Error} invalid filter, capability, message-level error or deadline
   */
  async describeUserScramCredentials(options = {}) {
    valid(options != null && typeof options === 'object' && !Array.isArray(options), 'options')
    const { users } = options
    valid(users === undefined || users === null || Array.isArray(users), 'users')
    if (users != null) {
      const seen = new Set()
      for (const user of users) {
        valid(name(user) && !seen.has(user), 'user')
        seen.add(user)
      }
      if (users.length === 0) return []
    }
    /** Reads one pending user subset without starting another deadline.
     * @param {string[]|null} names null selects all on the first request
     * @returns {Promise<object[]>} raw per-user broker results
     * @throws {Error} transport, capability, top-level protocol or original deadline failure
     */
    const read = async (names) => {
      const broker = checked(await waitOf(() => this.cluster.findConnectedBroker()))
      const response = checked(
        await waitOf(() =>
          broker.describeUserScramCredentials({
            users: names == null ? null : names.map((name) => ({ name })),
          })
        )
      )
      return response.results
    }
    let initial = null
    if (users == null) {
      initial = await retrierOf(this.retry)(async (bail) => {
        try {
          return await read(null)
        } catch (error) {
          if (error.retriable === false) bail(error)
          throw error
        }
      })
      if (
        !Array.isArray(initial) ||
        initial.some((row) => !name(row.user)) ||
        new Set(initial.map(({ user }) => user)).size !== initial.length
      )
        throw new KafkaJSInvariantViolation('Invalid SCRAM result identity')
      if (initial.length === 0) return []
    }
    const items = (users ?? initial.map(({ user }) => user)).map((user) => ({ user }))
    return itemRetry(
      {
        items,
        keyOf: ({ user }) => user,
        retry: this.retry,
        retryable: (row) => row.errorCode === 7,
        callFailure: (error) =>
          callFailure(error) ||
          (error instanceof KafkaJSProtocolError &&
            responseSourceOf(error) === apiKeys.DescribeUserScramCredentials),
        sanitize: () => null,
      },
      async (pending) => {
        const rows = initial ?? (await read(pending.map(({ user }) => user)))
        initial = null
        return rows.map((row) => {
          if (!row || !Array.isArray(row.credentialInfos))
            throw new KafkaJSInvariantViolation('Invalid SCRAM credentials')
          const mechanisms = new Set()
          const credentials = row.credentialInfos.map(({ mechanism, iterations }) => {
            const value = Object.keys(MECHANISMS).find((key) => MECHANISMS[key] === mechanism)
            if (!value || mechanisms.has(mechanism))
              throw new KafkaJSInvariantViolation('Invalid SCRAM mechanism')
            mechanisms.add(mechanism)
            return { mechanism: value, iterations }
          })
          return { user: row.user, errorCode: row.errorCode, errorMessage: null, credentials }
        })
      }
    )
  }

  /** Validates the whole mutation, derives once, and retries only unresolved user groups.
   * @param {object} options upsertions and deletions
   * @returns {Promise<object[]>} one caller-ordered result per affected user
   * @throws {Error} invalid input, derivation, capability, invariant or deadline refusal
   */
  async alterUserScramCredentials(options = {}) {
    valid(options != null && typeof options === 'object' && !Array.isArray(options), 'options')
    const { upsertions = [], deletions = [] } = options
    valid(
      Array.isArray(upsertions) &&
        Array.isArray(deletions) &&
        upsertions.length + deletions.length > 0,
      'credential changes'
    )
    const identities = new Set()
    const users = new Map()
    for (const [list, upsert] of [
      [upsertions, true],
      [deletions, false],
    ]) {
      for (const entry of list) {
        valid(entry && name(entry.user) && Object.hasOwn(MECHANISMS, entry.mechanism), 'credential')
        const identity = JSON.stringify([entry.user, entry.mechanism])
        valid(!identities.has(identity), 'duplicate credential')
        identities.add(identity)
        users.set(entry.user, { user: entry.user })
        if (upsert) {
          valid(
            Number.isInteger(entry.iterations) &&
              entry.iterations >= 4096 &&
              entry.iterations <= 16384,
            'iterations'
          )
          valid(typeof entry.password === 'string' && entry.password.length > 0, 'password')
          valid(
            entry.salt === undefined ||
              (Buffer.isBuffer(entry.salt) && entry.salt.length >= 1 && entry.salt.length <= 1024),
            'salt'
          )
        }
      }
    }
    const scope = currentDeadline()
    const wireUpsertions = []
    const owned = new Set()
    /** Erases only internally owned buffers, including a late derivation after expiry.
     * @returns {void}
     */
    const erase = () => {
      for (const bytes of owned) bytes.fill(0)
      owned.clear()
      wireUpsertions.length = 0
    }
    const unown = scope?.own(erase)
    try {
      scope?.check()
      for (const { user, mechanism, iterations, password, salt: inputSalt } of upsertions) {
        const salt = inputSalt === undefined ? crypto.randomBytes(32) : Buffer.from(inputSalt)
        owned.add(salt)
        const saltedPassword = checked(
          await waitOf(async () => {
            let derived
            try {
              derived = await SCRAM.hi(password, salt, iterations, DIGEST[mechanism])
            } catch (_) {
              scope?.check()
              throw new KafkaJSNonRetriableError('SCRAM credential derivation failed')
            }
            if (scope?.refusal()) {
              derived.fill(0)
              scope.check()
            }
            owned.add(derived)
            return derived
          })
        )
        wireUpsertions.push({
          name: user,
          mechanism: MECHANISMS[mechanism],
          iterations,
          salt,
          saltedPassword,
        })
      }
      const wireDeletions = deletions.map(({ user, mechanism }) => ({
        name: user,
        mechanism: MECHANISMS[mechanism],
      }))
      return await itemRetry(
        {
          items: [...users.values()],
          keyOf: ({ user }) => user,
          retry: this.retry,
          retryable: (row) => row.errorCode === 41 || row.errorCode === 7,
          callFailure,
          sanitize: () => null,
        },
        async (pending) => {
          const selected = new Set(pending.map(({ user }) => user))
          checked(await waitOf(() => this.cluster.refreshMetadata()))
          const broker = checked(await waitOf(() => this.cluster.findControllerBroker()))
          const response = checked(
            await waitOf(() =>
              broker.alterUserScramCredentials({
                upsertions: wireUpsertions.filter(({ name }) => selected.has(name)),
                deletions: wireDeletions.filter(({ name }) => selected.has(name)),
              })
            )
          )
          return response.results.map(({ user, errorCode }) => ({
            user,
            errorCode,
            errorMessage: null,
          }))
        }
      )
    } finally {
      unown?.()
      erase()
    }
  }
}

/** Creates credential methods inside the existing admin deadline wrapper.
 * @param {object} options cluster and retry policy
 * @returns {object} two bound public methods, with no test-only exports
 */
module.exports = (options) => {
  const family = new ScramCredentials(options)
  return {
    describeUserScramCredentials: family.describeUserScramCredentials.bind(family),
    alterUserScramCredentials: family.alterUserScramCredentials.bind(family),
  }
}
