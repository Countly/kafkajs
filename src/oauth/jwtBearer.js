const { invalid, optionsOf } = require('./tokenCache')
const { tokenRequest, assertionOf } = require('./clientCredentials')

/** RFC 7523 §2.1: a JWT is the authorization grant. */
const JWT_BEARER = 'urn:ietf:params:oauth:grant-type:jwt-bearer'

/**
 * Builds the existing `oauthBearerProvider` callback for the JWT-bearer grant (RFC 7523 §2.1,
 * KIP-1139), over one single-flight token cache. The request carries no HTTP authentication: the
 * supplied assertion is the grant, and the supplier is called once per retrieval attempt.
 * @param {object} input see `oauthBearer.jwtBearer` in the type declarations
 * @returns {import('../../types').OAuthBearerTokenProvider} the callback, with `close()`
 * @throws {KafkaJSInvalidConfigurationError} naming the first invalid option, before any work
 */
const jwtBearer = (input) => {
  const options = optionsOf(input)
  const { assertion } = input
  if (typeof assertion !== 'function') invalid('assertion')
  return tokenRequest(
    input,
    options,
    async () => ({ grant_type: JWT_BEARER, assertion: await assertionOf(assertion) }),
    {},
    undefined
  )
}

module.exports = { jwtBearer }
