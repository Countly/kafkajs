const crypto = require('crypto')
const scram = require('../../protocol/sasl/scram')
const { KafkaJSSASLAuthenticationError, KafkaJSNonRetriableError } = require('../../errors')
const { failureDetail, trustedError } = require('./failure')

const GS2_HEADER = 'n,,'

const EQUAL_SIGN_REGEX = /=/g
const COMMA_SIGN_REGEX = /,/g

const URLSAFE_BASE64_PLUS_REGEX = /\+/g
const URLSAFE_BASE64_SLASH_REGEX = /\//g
const URLSAFE_BASE64_TRAILING_EQUAL_REGEX = /=+$/

/**
 * The largest iteration count a server may ask for. Kafka's own limit for a stored credential is
 * 16384; the margin keeps other servers working while a hostile count cannot stall the client.
 */
const MAX_ITERATIONS = 1048576
const ITERATIONS_REGEX = /^[0-9]+$/

const HMAC_CLIENT_KEY = 'Client Key'
const HMAC_SERVER_KEY = 'Server Key'

const DIGESTS = {
  SHA256: {
    length: 32,
    type: 'sha256',
    minIterations: 4096,
  },
  SHA512: {
    length: 64,
    type: 'sha512',
    minIterations: 4096,
  },
}

const encode64 = (str) => Buffer.from(str).toString('base64')

class SCRAM {
  /**
   * From https://tools.ietf.org/html/rfc5802#section-5.1
   *
   * The characters ',' or '=' in usernames are sent as '=2C' and
   * '=3D' respectively.  If the server receives a username that
   * contains '=' not followed by either '2C' or '3D', then the
   * server MUST fail the authentication.
   *
   * @returns {String}
   */
  static sanitizeString(str) {
    return str.replace(EQUAL_SIGN_REGEX, '=3D').replace(COMMA_SIGN_REGEX, '=2C')
  }

  /**
   * In cryptography, a nonce is an arbitrary number that can be used just once.
   * It is similar in spirit to a nonce * word, hence the name. It is often a random or pseudo-random
   * number issued in an authentication protocol to * ensure that old communications cannot be reused
   * in replay attacks.
   *
   * @returns {String}
   */
  static nonce() {
    return crypto
      .randomBytes(16)
      .toString('base64')
      .replace(URLSAFE_BASE64_PLUS_REGEX, '-') // make it url safe
      .replace(URLSAFE_BASE64_SLASH_REGEX, '_')
      .replace(URLSAFE_BASE64_TRAILING_EQUAL_REGEX, '')
      .toString('ascii')
  }

  /**
   * Hi() is, essentially, PBKDF2 [RFC2898] with HMAC() as the
   * pseudorandom function (PRF) and with dkLen == output length of
   * HMAC() == output length of H()
   *
   * @returns {Promise<Buffer>}
   */
  static hi(password, salt, iterations, digestDefinition) {
    return new Promise((resolve, reject) => {
      crypto.pbkdf2(
        password,
        salt,
        iterations,
        digestDefinition.length,
        digestDefinition.type,
        (err, derivedKey) => (err ? reject(err) : resolve(derivedKey))
      )
    })
  }

  /**
   * Apply the exclusive-or operation to combine the octet string
   * on the left of this operator with the octet string on the right of
   * this operator.  The length of the output and each of the two
   * inputs will be the same for this use
   *
   * @returns {Buffer}
   */
  static xor(left, right) {
    const bufferA = Buffer.from(left)
    const bufferB = Buffer.from(right)
    const length = Buffer.byteLength(bufferA)

    if (length !== Buffer.byteLength(bufferB)) {
      throw trustedError(KafkaJSNonRetriableError, 'Buffers must be of the same length')
    }

    const result = []
    for (let i = 0; i < length; i++) {
      result.push(bufferA[i] ^ bufferB[i])
    }

    return Buffer.from(result)
  }

  /**
   * @param {SASLOptions} sasl
   * @param {Logger} logger
   * @param {Function} saslAuthenticate
   * @param {DigestDefinition} digestDefinition
   */
  constructor(sasl, host, port, logger, saslAuthenticate, digestDefinition) {
    this.sasl = sasl
    this.host = host
    this.port = port
    this.logger = logger
    this.saslAuthenticate = saslAuthenticate
    this.digestDefinition = digestDefinition

    const digestType = digestDefinition.type.toUpperCase()
    this.PREFIX = `SASL SCRAM ${digestType} authentication`

    this.currentNonce = SCRAM.nonce()
  }

  /**
   * Runs the exchange (RFC 5802): the client-first message, then the client-final message once
   * the server-first message passed its checks, then the server signature, compared in constant
   * time. Every failure is one `KafkaJSSASLAuthenticationError` with this mechanism's prefix and
   * library-written text only.
   * @returns {Promise<void>}
   * @throws {KafkaJSSASLAuthenticationError}
   */
  async authenticate() {
    const { PREFIX } = this
    const broker = `${this.host}:${this.port}`

    if (this.sasl.username == null || this.sasl.password == null) {
      throw new KafkaJSSASLAuthenticationError(`${this.PREFIX}: Invalid username or password`)
    }

    try {
      this.logger.debug('Exchanging first client message', { broker })
      const clientMessageResponse = await this.sendClientFirstMessage()

      this.logger.debug('Sending final message', { broker })
      const finalResponse = await this.sendClientFinalMessage(clientMessageResponse)

      if (finalResponse.e !== undefined) {
        throw trustedError(
          KafkaJSSASLAuthenticationError,
          'The server final message reports an error'
        )
      }

      const serverKey = await this.serverKey(clientMessageResponse)
      const expected = this.serverSignature(serverKey, clientMessageResponse)
      const verifier = finalResponse.v
      const received = Buffer.from(typeof verifier === 'string' ? verifier : '', 'base64')

      if (
        typeof verifier !== 'string' ||
        received.toString('base64') !== verifier ||
        received.length !== expected.length ||
        !crypto.timingSafeEqual(received, expected)
      ) {
        throw trustedError(
          KafkaJSSASLAuthenticationError,
          'Invalid server signature in server final message'
        )
      }

      this.logger.debug(`${PREFIX} successful`, { broker })
    } catch (e) {
      const error = new KafkaJSSASLAuthenticationError(`${PREFIX} failed: ${failureDetail(e)}`)
      this.logger.error(error.message, { broker })
      throw error
    }
  }

  /**
   * @private
   */
  async sendClientFirstMessage() {
    const clientFirstMessage = `${GS2_HEADER}${this.firstMessageBare()}`
    const request = scram.firstMessage.request({ clientFirstMessage })
    const response = scram.firstMessage.response

    return this.saslAuthenticate({
      request,
      response,
    })
  }

  /**
   * Checks the server-first message before any key is derived from it: the server nonce must
   * extend the client nonce with a part of its own, the salt must be present and the iteration
   * count must be an integer between the digest's minimum and 1048576. A server error attribute
   * fails the exchange. Messages name the check, never the received text.
   * @private
   * @param {{ r?: string, s?: string, i?: string, e?: string }} serverFirst the parsed message
   * @returns {{ nonce: string, salt: Buffer, iterations: number }}
   * @throws {KafkaJSSASLAuthenticationError} when a check fails
   */
  serverFirst({ r: nonce, s: salt, i, e }) {
    if (e !== undefined) {
      throw trustedError(
        KafkaJSSASLAuthenticationError,
        'The server first message reports an error'
      )
    }

    if (typeof nonce !== 'string' || !nonce.startsWith(this.currentNonce)) {
      throw trustedError(
        KafkaJSSASLAuthenticationError,
        'Invalid server nonce, it does not start with the client nonce'
      )
    }

    if (nonce.length === this.currentNonce.length) {
      throw trustedError(
        KafkaJSSASLAuthenticationError,
        'Invalid server nonce, it adds no server part'
      )
    }

    if (!salt) {
      throw trustedError(KafkaJSSASLAuthenticationError, 'Missing salt in the server first message')
    }

    const decodedSalt = Buffer.from(salt, 'base64')
    if (decodedSalt.length === 0 || decodedSalt.toString('base64') !== salt) {
      throw trustedError(KafkaJSSASLAuthenticationError, 'Invalid salt in the server first message')
    }

    return { nonce, salt: decodedSalt, iterations: this.iterationCount(i) }
  }

  /**
   * @private
   * @param {string} [value] the iteration count the server asked for
   * @returns {number} the count, an integer between the digest's minimum and 1048576
   * @throws {KafkaJSSASLAuthenticationError} for any other value
   */
  iterationCount(value) {
    const { minIterations } = this.digestDefinition
    if (typeof value !== 'string' || !ITERATIONS_REGEX.test(value)) {
      throw trustedError(
        KafkaJSSASLAuthenticationError,
        'Invalid iteration count in the server first message'
      )
    }

    const iterations = Number(value)
    if (iterations < minIterations) {
      throw trustedError(
        KafkaJSSASLAuthenticationError,
        `Requested iterations ${iterations} is less than the minimum ${minIterations}`
      )
    }

    if (iterations > MAX_ITERATIONS) {
      throw trustedError(
        KafkaJSSASLAuthenticationError,
        `Requested iterations ${iterations} is more than the maximum ${MAX_ITERATIONS}`
      )
    }

    return iterations
  }

  /**
   * @private
   */
  async sendClientFinalMessage(clientMessageResponse) {
    this.serverFirst(clientMessageResponse)
    const finalMessageWithoutProof = this.finalMessageWithoutProof(clientMessageResponse)
    const clientProof = await this.clientProof(clientMessageResponse)
    const finalMessage = `${finalMessageWithoutProof},p=${clientProof}`
    const request = scram.finalMessage.request({ finalMessage })
    const response = scram.finalMessage.response

    return this.saslAuthenticate({
      request,
      response,
    })
  }

  /**
   * @private
   */
  async clientProof(clientMessageResponse) {
    const clientKey = await this.clientKey(clientMessageResponse)
    const storedKey = this.H(clientKey)
    const clientSignature = this.clientSignature(storedKey, clientMessageResponse)
    return encode64(SCRAM.xor(clientKey, clientSignature))
  }

  /**
   * @private
   */
  async clientKey(clientMessageResponse) {
    const saltedPassword = await this.saltPassword(clientMessageResponse)
    return this.HMAC(saltedPassword, HMAC_CLIENT_KEY)
  }

  /**
   * @private
   */
  async serverKey(clientMessageResponse) {
    const saltedPassword = await this.saltPassword(clientMessageResponse)
    return this.HMAC(saltedPassword, HMAC_SERVER_KEY)
  }

  /**
   * @private
   */
  clientSignature(storedKey, clientMessageResponse) {
    return this.HMAC(storedKey, this.authMessage(clientMessageResponse))
  }

  /**
   * @private
   */
  serverSignature(serverKey, clientMessageResponse) {
    return this.HMAC(serverKey, this.authMessage(clientMessageResponse))
  }

  /**
   * @private
   */
  authMessage(clientMessageResponse) {
    return [
      this.firstMessageBare(),
      clientMessageResponse.original,
      this.finalMessageWithoutProof(clientMessageResponse),
    ].join(',')
  }

  /**
   * @private
   */
  async saltPassword(clientMessageResponse) {
    const salt = Buffer.from(clientMessageResponse.s, 'base64')
    const iterations = this.iterationCount(clientMessageResponse.i)
    return SCRAM.hi(this.encodedPassword(), salt, iterations, this.digestDefinition)
  }

  /**
   * @private
   */
  firstMessageBare() {
    return `n=${this.encodedUsername()},r=${this.currentNonce}`
  }

  /**
   * @private
   */
  finalMessageWithoutProof(clientMessageResponse) {
    const rnonce = clientMessageResponse.r
    return `c=${encode64(GS2_HEADER)},r=${rnonce}`
  }

  /**
   * @private
   */
  encodedUsername() {
    const { username } = this.sasl
    return SCRAM.sanitizeString(username).toString('utf-8')
  }

  /**
   * @private
   */
  encodedPassword() {
    const { password } = this.sasl
    return password.toString('utf-8')
  }

  /**
   * @private
   */
  H(data) {
    return crypto.createHash(this.digestDefinition.type).update(data).digest()
  }

  /**
   * @private
   */
  HMAC(key, data) {
    return crypto.createHmac(this.digestDefinition.type, key).update(data).digest()
  }
}

module.exports = {
  DIGESTS,
  SCRAM,
}
