const { requests, lookup } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const plainAuthenticatorProvider = require('./plain')
const scram256AuthenticatorProvider = require('./scram256')
const scram512AuthenticatorProvider = require('./scram512')
const awsIAMAuthenticatorProvider = require('./awsIam')
const oauthBearerAuthenticatorProvider = require('./oauthBearer')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

const BUILT_IN_AUTHENTICATION_PROVIDERS = {
  AWS: awsIAMAuthenticatorProvider,
  PLAIN: plainAuthenticatorProvider,
  OAUTHBEARER: oauthBearerAuthenticatorProvider,
  'SCRAM-SHA-256': scram256AuthenticatorProvider,
  'SCRAM-SHA-512': scram512AuthenticatorProvider,
}

const UNLIMITED_SESSION_LIFETIME = '0'

// src/protocol/error.js: SASL_AUTHENTICATION_FAILED
const SASL_AUTHENTICATION_FAILED_CODE = 58

module.exports = class SASLAuthenticator {
  constructor(connection, logger, versions) {
    this.connection = connection
    this.logger = logger
    this.sessionLifetime = UNLIMITED_SESSION_LIFETIME

    const lookupRequest = lookup(versions)
    this.saslHandshake = lookupRequest(apiKeys.SaslHandshake, requests.SaslHandshake)
    // Kafka 4.x always supports SaslAuthenticate (key 36, versions 0-2); the pre-KIP-152
    // raw-token path was removed with the rest of the pre-4.0 code.
    this.protocolAuthentication = lookupRequest(apiKeys.SaslAuthenticate, requests.SaslAuthenticate)
  }

  async authenticate() {
    const mechanism = this.connection.sasl.mechanism.toUpperCase()
    const handshake = await this.connection.send(this.saslHandshake({ mechanism }))
    if (!handshake.enabledMechanisms.includes(mechanism)) {
      throw new KafkaJSSASLAuthenticationError(
        `SASL ${mechanism} mechanism is not supported by the server`
      )
    }

    const saslAuthenticate = async ({ request, response }) => {
      const requestAuthBytes = await request.encode()

      let authResponse
      try {
        authResponse = await this.connection.send(
          this.protocolAuthentication({ authBytes: requestAuthBytes, mechanism })
        )
      } catch (e) {
        // Only the raw protocol error carries the broker's own error_message and code;
        // built-in mechanisms (plain/scram/oauthBearer/awsIam) re-wrap it into
        // KafkaJSSASLAuthenticationError with a plain string, losing both. Report here,
        // before that happens, then re-throw unchanged so the thrown class is unaffected.
        if (e.code === SASL_AUTHENTICATION_FAILED_CODE) {
          this.connection.reportAuthenticationFailure(mechanism, e.message)
        }
        throw e
      }

      // `0` is a string because `sessionLifetimeMs` is an int64 encoded as string.
      // This is not present in SaslAuthenticateV0, so we default to `"0"`
      this.sessionLifetime = authResponse.sessionLifetimeMs || UNLIMITED_SESSION_LIFETIME

      if (!response) {
        return
      }

      const { authBytes: responseAuthBytes } = authResponse
      const payloadDecoded = await response.decode(responseAuthBytes)
      return response.parse(payloadDecoded)
    }

    if (
      !this.connection.sasl.authenticationProvider &&
      Object.keys(BUILT_IN_AUTHENTICATION_PROVIDERS).includes(mechanism)
    ) {
      this.connection.sasl.authenticationProvider = BUILT_IN_AUTHENTICATION_PROVIDERS[mechanism](
        this.connection.sasl
      )
    }
    await this.connection.sasl
      .authenticationProvider({
        host: this.connection.host,
        port: this.connection.port,
        logger: this.logger.namespace(`SaslAuthenticator-${mechanism}`),
        saslAuthenticate,
      })
      .authenticate()
  }
}
