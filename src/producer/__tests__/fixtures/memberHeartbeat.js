const { REBALANCE_PATHS, rebalanceAction } = require('../../../consumer/rebalanceErrorContract')

/** What the consumer's own contract calls a membership change: the group moved on without us. */
const MEMBERSHIP_CHANGES = new Set(['revoked', 'lost'])

/**
 * Heartbeats from inside a long `eachBatch` handler, the only way such a handler learns of a
 * rebalance: a classic consumer rejoins a rebalancing group only once its handlers return, and
 * neither `isRunning` nor `isStale` says that one is pending. An error the consumer's contract
 * classifies as a rebalance or a lost membership is thrown on, which ends the batch the way the
 * runner expects; any other failure, a lost connection or a moved coordinator, is left to the
 * consumer's own heartbeat and fetch loop, which ride it out, and would otherwise count against
 * the consumer's retries.
 * @param {() => Promise<void>} heartbeat the `heartbeat` of the handler's payload
 * @returns {Promise<void>}
 * @throws {Error} what `heartbeat` threw, when it is a rebalance or a lost membership
 */
const heartbeatInBatch = async (heartbeat) => {
  try {
    await heartbeat()
  } catch (error) {
    if (MEMBERSHIP_CHANGES.has(rebalanceAction(error, REBALANCE_PATHS.HEARTBEAT).action))
      throw error
  }
}

module.exports = { heartbeatInBatch }
