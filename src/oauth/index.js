const { clientCredentials } = require('./clientCredentials')
const { jwtBearer } = require('./jwtBearer')
const { fileToken } = require('./fileToken')
const { signAssertion } = require('./signAssertion')

/**
 * The `oauthBearer` namespace of the package root: three factories of the existing
 * `oauthBearerProvider` callback and one assertion signer.
 */
module.exports = { clientCredentials, jwtBearer, fileToken, signAssertion }
