const crypto = require('node:crypto')
const fs = require('node:fs')
const { or, invalid, isRecord, text, integer, failure, fileVersion } = require('./tokenCache')

/** The JOSE algorithms of RFC 7518 §3.3 and §3.4 a signer accepts, with the key each needs. */
const ALGORITHMS = {
  RS256: { digest: 'sha256', type: 'rsa' },
  RS384: { digest: 'sha384', type: 'rsa' },
  RS512: { digest: 'sha512', type: 'rsa' },
  ES256: { digest: 'sha256', type: 'ec', curve: 'prime256v1' },
  ES384: { digest: 'sha384', type: 'ec', curve: 'secp384r1' },
  ES512: { digest: 'sha512', type: 'ec', curve: 'secp521r1' },
}
/** Claims only the signer's own options set. */
const RESERVED = ['iat', 'nbf', 'exp', 'jti']

/**
 * @param {object} value
 * @returns {string} the value as base64url JSON, without padding
 */
const encoded = (value) => Buffer.from(JSON.stringify(value)).toString('base64url')

/**
 * Checks that a claim value survives JSON unchanged: null, strings, booleans, finite numbers, and
 * arrays and plain objects of them, without holes, accessors or cycles.
 * @param {*} value
 * @param {Set<object>} ancestors the objects being checked above this one
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} naming `claims`
 */
const checkJson = (value, ancestors) => {
  if (value === null || ['string', 'boolean'].includes(typeof value) || Number.isFinite(value)) {
    return
  }
  const prototype = typeof value === 'object' ? Object.getPrototypeOf(value) : undefined
  const array = Array.isArray(value)
  if (ancestors.has(value) || !(array || prototype === Object.prototype || prototype === null)) {
    invalid('claims')
  }
  const keys = Object.keys(value)
  if (array && keys.length !== value.length) invalid('claims')
  ancestors.add(value)
  for (const key of keys) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key)
    if (descriptor.get || descriptor.set) invalid('claims')
    checkJson(descriptor.value, ancestors)
  }
  ancestors.delete(value)
}

/**
 * @param {*} claims the `claims` option
 * @returns {object} a deep copy, taken now so later changes to the caller's object have no effect
 * @throws {KafkaJSInvalidConfigurationError} on a value JSON would change, a reserved claim, a
 *   non-string `iss` or `sub`, or an `aud` that is neither a string nor a nonempty array of unique
 *   strings
 */
const claimsOf = (claims) => {
  if (!isRecord(claims) || RESERVED.some((name) => Object.hasOwn(claims, name))) invalid('claims')
  checkJson(claims, new Set())
  const { iss, sub, aud } = claims
  const audience =
    typeof aud === 'string' ||
    (Array.isArray(aud) &&
      aud.length > 0 &&
      aud.every((item) => typeof item === 'string') &&
      new Set(aud).size === aud.length)
  if (
    (Object.hasOwn(claims, 'iss') && typeof iss !== 'string') ||
    (Object.hasOwn(claims, 'sub') && typeof sub !== 'string') ||
    (Object.hasOwn(claims, 'aud') && !audience)
  ) {
    invalid('claims')
  }
  return JSON.parse(JSON.stringify(claims))
}

/**
 * Signs JWT assertions locally with `node:crypto`, for `clientAssertion` or `assertion`. The key
 * is validated against the algorithm at construction. A `{ file }` key is stat'ed on every call
 * and read again when the file's identity changed; the new key replaces the old one only once it
 * validates, and a call never signs with the old key after a failed reload.
 */
class AssertionSigner {
  /**
   * @param {*} input see `oauthBearer.signAssertion` in the type declarations
   * @throws {KafkaJSInvalidConfigurationError} naming the first invalid option, including a key
   *   that is unreadable, not private, or of the wrong type or curve for the algorithm
   */
  constructor(input) {
    if (!isRecord(input)) invalid('options')
    const algorithm = or(input.algorithm, 'RS256')
    if (typeof algorithm !== 'string' || !Object.hasOwn(ALGORITHMS, algorithm)) invalid('algorithm')
    this.algorithm = ALGORITHMS[algorithm]
    const header = { alg: algorithm, typ: 'JWT' }
    if (input.keyId !== undefined) header.kid = text(input.keyId, 'keyId', true)
    this.header = encoded(header)
    this.expiresIn = integer(or(input.expiresIn, 300), 'expiresIn', 1)
    this.notBefore = integer(or(input.notBefore, 60), 'notBefore', 0)
    this.jti = or(input.jti, false)
    if (typeof this.jti !== 'boolean') invalid('jti')
    this.passphrase = input.passphrase
    if (this.passphrase !== undefined && typeof this.passphrase !== 'string') invalid('passphrase')
    this.claims = claimsOf(or(input.claims, {}))
    const { privateKey } = input
    const material =
      typeof privateKey === 'string' ||
      Buffer.isBuffer(privateKey) ||
      privateKey instanceof crypto.KeyObject
    if (!material && !(isRecord(privateKey) && Object.keys(privateKey).join() === 'file')) {
      invalid('privateKey')
    }
    this.file = material ? undefined : text(privateKey.file, 'privateKey.file')
    try {
      if (this.file) this.version = fileVersion(fs.statSync(this.file, { bigint: true }))
      this.key = this.load(this.file ? fs.readFileSync(this.file) : privateKey)
    } catch {
      invalid('privateKey')
    }
  }

  /**
   * @param {string|Buffer|crypto.KeyObject} material PEM text, a PEM Buffer or a key object
   * @returns {crypto.KeyObject} the private key
   * @throws {Error} when it does not parse, is not private, or does not fit the algorithm; an RSA
   *   key needs at least 2048 bits (RFC 7518 §3.3)
   */
  load(material) {
    const key =
      material instanceof crypto.KeyObject
        ? material
        : crypto.createPrivateKey({ key: material, passphrase: this.passphrase })
    const { type, curve } = this.algorithm
    if (
      key.type !== 'private' ||
      key.asymmetricKeyType !== type ||
      key.asymmetricKeyDetails.namedCurve !== curve ||
      (type === 'rsa' && key.asymmetricKeyDetails.modulusLength < 2048)
    ) {
      throw new Error('The key does not fit the algorithm')
    }
    return key
  }

  /**
   * Reloads a `{ file }` key when the file's identity changed. The identity is taken before the
   * read, so a change during the read is seen again at the next call.
   * @returns {Promise<void>}
   * @throws {KafkaJSSASLAuthenticationError} when the file cannot be read or holds an unfit key;
   *   no file path or cause enters it
   */
  async reload() {
    try {
      const version = fileVersion(await fs.promises.stat(this.file, { bigint: true }))
      if (version === this.version) return
      this.key = this.load(await fs.promises.readFile(this.file))
      this.version = version
    } catch {
      throw failure('key')
    }
  }

  /**
   * Signs a new assertion: `iat` now, `nbf` now minus `notBefore`, `exp` now plus `expiresIn`, a
   * fresh UUID `jti` when asked, and the configured claims. ECDSA signatures use the JOSE
   * `ieee-p1363` encoding.
   * @returns {Promise<string>} the compact JWT
   * @throws {KafkaJSSASLAuthenticationError} when a `{ file }` key cannot be reloaded
   */
  async sign() {
    if (this.file) await this.reload()
    const now = Math.floor(Date.now() / 1000)
    const claims = {
      ...this.claims,
      iat: now,
      nbf: now - this.notBefore,
      exp: now + this.expiresIn,
    }
    if (this.jti) claims.jti = crypto.randomUUID()
    const content = `${this.header}.${encoded(claims)}`
    const signature = crypto.sign(this.algorithm.digest, Buffer.from(content), {
      key: this.key,
      dsaEncoding: 'ieee-p1363',
    })
    return `${content}.${signature.toString('base64url')}`
  }
}

/**
 * Builds an assertion supplier that signs a new JWT on every call (RFC 7523 §3), for the
 * `clientAssertion` of `clientCredentials` or the `assertion` of `jwtBearer`. It is not a token
 * provider and has no cache or `close()`.
 * @param {object} input see `oauthBearer.signAssertion` in the type declarations
 * @returns {() => Promise<string>} the supplier
 * @throws {KafkaJSInvalidConfigurationError} naming the first invalid option, before any signing
 */
const signAssertion = (input) => {
  const signer = new AssertionSigner(input)
  return () => signer.sign()
}

module.exports = { signAssertion }
