const { SCRAM, DIGESTS } = require('./scram')

const scram256AuthenticatorProvider =
  (sasl) =>
  ({ host, port, logger, saslAuthenticate, passesThrough }) => {
    const scram = new SCRAM(
      sasl,
      host,
      port,
      logger,
      saslAuthenticate,
      DIGESTS.SHA256,
      passesThrough
    )
    return {
      authenticate: async () => await scram.authenticate(),
    }
  }

module.exports = scram256AuthenticatorProvider
