const SASLAuthenticator = require('./index')
const Connection = require('../../network/connection')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')

const versionsWith = (overrides) => ({
  [apiKeys.SaslHandshake]: { minVersion: 0, maxVersion: 1 },
  [apiKeys.SaslAuthenticate]: { minVersion: 0, maxVersion: 1 },
  ...overrides,
})

describe('Broker > SASLAuthenticator > protocol authentication only', () => {
  it('always wraps SASL tokens in SaslAuthenticate requests', () => {
    const authenticator = new SASLAuthenticator({}, newLogger(), versionsWith({}))
    expect(authenticator.protocolAuthentication).not.toBeNull()
  })

  // Structural, not behavioral — but it is the cheapest red-first signal for this refactor
  // (arity is 4 today) and it pins the constructor contract the connection depends on.
  it('no longer accepts a supportAuthenticationProtocol argument', () => {
    expect(SASLAuthenticator.length).toBe(3)
  })

  it('Connection no longer exposes the raw-token write path', () => {
    expect(Connection.prototype.sendAuthRequest).toBeUndefined()
    expect(Connection.prototype.getSupportAuthenticationProtocol).toBeUndefined()
    expect(Connection.prototype.setSupportAuthenticationProtocol).toBeUndefined()
  })
})
