const { ConnectionSetupPolicy, SetupAttempt } = require('../src/network/connectionSetup')

/**
 * Adds setup collaborators to a legacy test pool without changing its request oracle.
 * @param {object} pool fake pool with its original dial and dispatch behavior
 * @returns {object} the same pool, with a managed control connection
 */
module.exports = (pool) => {
  pool.setupPolicy = ConnectionSetupPolicy.resolve({ connectionTimeout: pool.connectionTimeout })
  pool.authenticationBlackoutEntry = () => null
  pool.beginSetup = () =>
    new SetupAttempt({ address: `${pool.host}:${pool.port}`, policy: pool.setupPolicy })
  let established
  pool.pool = [
    {
      isConnected: () => pool.isConnected(),
      isAuthenticated: () => pool.isAuthenticated(),
      connect: async ({ attempt }) => {
        attempt.arm(() => {})
        established = await pool.getConnection()
      },
      send: (request) => pool.send(request),
      authenticate: () => established.authenticate(),
      finishSetup: (attempt) => attempt.ready(),
    },
  ]
  return pool
}
