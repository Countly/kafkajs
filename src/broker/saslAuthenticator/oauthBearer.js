/**
 * The sasl object must include a property named oauthBearerProvider, an
 * async function that is used to return the OAuth bearer token.
 *
 * The OAuth bearer token must be an object with properties value and
 * (optionally) extensions, that will be sent during the SASL/OAUTHBEARER
 * request.
 *
 * The implementation of the oauthBearerProvider must take care that tokens are
 * reused and refreshed when appropriate.
 */

const { request, response } = require('../../protocol/sasl/oauthBearer')
const { KafkaJSSASLAuthenticationError } = require('../../errors')
const { failureDetail, trustedError } = require('./failure')

/**
 * Built-in SASL OAUTHBEARER mechanism.
 * @param {{ oauthBearerProvider: Function, authorizationIdentity?: string }} sasl the SASL config
 * @returns {(context: object) => { authenticate: () => Promise<void> }} the provider the
 *   authenticator calls with the connection's `saslAuthenticate`
 */
const oauthBearerAuthenticatorProvider =
  (sasl) =>
  ({ host, port, logger, saslAuthenticate, passesThrough = () => false }) => {
    return {
      /**
       * Sends the token. A broker that refuses it answers with an error challenge in a response
       * that carries no error code (RFC 7628 section 3.2.3); the challenge is answered with a lone
       * %x01 so the broker ends the exchange with its own rejection, and the login fails whatever
       * that last reply is. The challenge text is never logged or put in the error.
       * @returns {Promise<void>}
       * @throws {KafkaJSSASLAuthenticationError} for a missing provider or token, a broker
       *   rejection or an error challenge
       * @throws {Error} the connection's own refusal when it ended the exchange
       */
      authenticate: async () => {
        const { oauthBearerProvider } = sasl

        if (oauthBearerProvider == null) {
          throw new KafkaJSSASLAuthenticationError(
            'SASL OAUTHBEARER: Missing OAuth bearer token provider'
          )
        }

        const oauthBearerToken = await oauthBearerProvider()

        if (oauthBearerToken.value == null) {
          throw new KafkaJSSASLAuthenticationError('SASL OAUTHBEARER: Invalid OAuth bearer token')
        }

        const broker = `${host}:${port}`

        try {
          logger.debug('Authenticate with SASL OAUTHBEARER', { broker })
          const challenged = await saslAuthenticate({
            request: await request(sasl, oauthBearerToken),
            response,
          })
          if (challenged) {
            await saslAuthenticate({ request: request.acknowledgeError() })
            throw trustedError(
              KafkaJSSASLAuthenticationError,
              'The server answered the token with an error challenge'
            )
          }
          logger.debug('SASL OAUTHBEARER authentication successful', { broker })
        } catch (e) {
          if (passesThrough(e)) throw e
          const error = new KafkaJSSASLAuthenticationError(
            `SASL OAUTHBEARER authentication failed: ${failureDetail(e)}`
          )
          logger.error(error.message, { broker })
          throw error
        }
      },
    }
  }

module.exports = oauthBearerAuthenticatorProvider
