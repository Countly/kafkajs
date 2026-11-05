/* eslint n/no-missing-require: ["error", { "allowModules": ["kerberos"] }] */
const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')
const {
  KafkaJSNotImplemented,
  KafkaJSSASLAuthenticationError,
  KafkaJSInvalidConfigurationError,
} = require('../../errors')

let kerberos

/** Loads the optional native peer on first use; failed loads are never cached.
 * @returns {object} native provider
 * @throws {KafkaJSNotImplemented} when the peer is absent or not version 7.0.0
 * @throws {Error} when an installed native binding cannot load
 */
const loadKerberos = () => {
  if (kerberos) return kerberos
  try {
    if (require('kerberos/package.json').version !== '7.0.0') {
      throw new KafkaJSNotImplemented(
        'SASL GSSAPI requires the optional peer "kerberos" at version 7.0.0. Install it with `npm install kerberos@7.0.0`.'
      )
    }
    kerberos = require('kerberos')
    return kerberos
  } catch (error) {
    if (
      error.code === 'MODULE_NOT_FOUND' &&
      ["'kerberos'", "'kerberos/package.json'"].some((name) => error.message.includes(name))
    ) {
      throw new KafkaJSNotImplemented(
        'SASL GSSAPI requires the optional peer "kerberos". Install it with `npm install kerberos@7.0.0`.'
      )
    }
    throw error
  }
}

/** Exchanges RFC 4752 tokens once per authentication; no native context is shared.
 * Credentials are obtained by the native provider from the process environment.
 * No subprocess, ticket renewal, DNS canonicalization, or security layer is added.
 */
class GssapiAuthenticator {
  /** Stores connection-local collaborators without loading the native peer.
   * @param {object} options serviceName and optional principal
   * @param {object} connection host, port and guarded saslAuthenticate
   */
  constructor(options, { host, port, saslAuthenticate }) {
    this.options = options
    this.host = host
    this.broker = `${host}:${port}`
    this.saslAuthenticate = saslAuthenticate
  }

  /** Exchanges opaque mechanism bytes through the existing INT32-prefixed boundary.
   * @param {string} token base64 native token
   * @returns {Promise<string>} base64 server token
   * @throws {Error} original guarded transport refusal
   */
  async exchange(token) {
    return this.saslAuthenticate({
      request: {
        /** @returns {Promise<Buffer>} one INT32-prefixed token */
        encode: async () => new Encoder().writeBytes(Buffer.from(token, 'base64')).buffer,
      },
      response: {
        /** @param {Buffer} bytes INT32-prefixed mechanism response @returns {Promise<Buffer>} token */
        decode: async (bytes) => new Decoder(bytes).readBytes(),
        /** @param {Buffer} bytes mechanism token @returns {Promise<string>} native representation */
        parse: async (bytes) => bytes.toString('base64'),
      },
    })
  }

  /** Authenticates with a fresh context and selects the RFC 4752 no-security-layer bit.
   * Native context completion precedes SASL layer negotiation. Empty output tokens
   * still travel through SaslAuthenticate; success awaits the final broker status.
   * @returns {Promise<void>} completed authentication
   * @throws {KafkaJSInvalidConfigurationError} invalid serviceName or principal
   * @throws {KafkaJSNotImplemented} missing or unsupported optional peer
   * @throws {KafkaJSSASLAuthenticationError} sanitized acquisition or exchange failure
   */
  async authenticate() {
    const { serviceName = 'kafka', principal } = this.options
    if (typeof serviceName !== 'string' || !serviceName || /[@/\0]/.test(serviceName)) {
      throw new KafkaJSInvalidConfigurationError(
        'SASL GSSAPI serviceName must be a nonempty name without @, slash or NUL'
      )
    }
    if (
      principal !== undefined &&
      (typeof principal !== 'string' || !principal || principal.includes('\0'))
    ) {
      throw new KafkaJSInvalidConfigurationError(
        'SASL GSSAPI principal must be a nonempty string without NUL'
      )
    }
    let stage = 'load'
    try {
      const peer = loadKerberos()
      stage = 'initialize'
      const client = await peer.initializeClient(`${serviceName}@${this.host}`, {
        principal,
        mechOID: peer.GSS_MECH_OID_KRB5,
      })
      stage = 'step'
      let token = await client.step('')
      stage = 'exchange'
      let challenge = await this.exchange(token)
      while (!client.contextComplete) {
        stage = 'step'
        token = await client.step(challenge)
        stage = 'exchange'
        challenge = await this.exchange(token)
      }
      stage = 'unwrap'
      const offer = Buffer.from(await client.unwrap(challenge), 'base64')
      if (offer.length !== 4 || !(offer[0] & 1)) throw new Error('Unsupported security layer')
      const identity = principal === undefined ? client.username : principal
      if (typeof identity !== 'string' || !identity || identity.includes('\0')) {
        throw new Error('Missing authorization identity')
      }
      const reply = Buffer.concat([Buffer.from([1, 0, 0, 0]), Buffer.from(identity, 'utf8')])
      stage = 'wrap'
      token = await client.wrap(reply.toString('base64'), { protect: 0 })
      stage = 'exchange'
      await this.exchange(token)
    } catch (error) {
      if (stage === 'load' && error instanceof KafkaJSNotImplemented) throw error
      throw new KafkaJSSASLAuthenticationError(
        `SASL GSSAPI authentication failed: broker=${this.broker} stage=${stage}`
      )
    }
  }
}

/** Creates a mechanism provider. Each authenticate call obtains a new native context.
 * @param {object} options serviceName and optional principal; no password option
 * @returns {function(object): GssapiAuthenticator} connection-local provider
 */
module.exports = (options) => (connection) => new GssapiAuthenticator(options, connection)
