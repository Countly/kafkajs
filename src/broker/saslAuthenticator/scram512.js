const { SCRAM, DIGESTS } = require('./scram')

const scram512AuthenticatorProvider =
  (sasl) =>
  ({ host, port, logger, saslAuthenticate, passesThrough }) => {
    const scram = new SCRAM(
      sasl,
      host,
      port,
      logger,
      saslAuthenticate,
      DIGESTS.SHA512,
      passesThrough
    )
    return {
      authenticate: async () => await scram.authenticate(),
    }
  }

module.exports = scram512AuthenticatorProvider
