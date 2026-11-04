const { execFile } = require('child_process')
const { promisify } = require('util')
const { composeArgs } = require('./leaderElection')

const execFileAsync = promisify(execFile)

/**
 * Creates a broker runner with per-operation budgets. Calls are awaited and are not retried.
 * @param {Record<string, number>} GUARD_MS operation budgets in milliseconds
 * @returns {(operation: string, service: string, options?: {signal?: AbortSignal}) => Promise<object>}
 *   a runner that rejects incomplete ownership environments and operations without budgets
 */
const createBrokerRun = (GUARD_MS) => {
  /**
   * Runs one broker action through the guard, or Compose on a standalone cluster.
   * The child is bounded by the action's budget and is killed when the phase aborts.
   * @param {string} operation broker action with a configured budget
   * @param {string} service Compose broker service
   * @param {object} [options]
   * @param {AbortSignal} [options.signal] phase cancellation signal
   * @returns {Promise<{stdout: string, stderr: string}>} child output
   * @throws {Error} incomplete ownership environment, missing budget or child failure
   */
  const run = (operation, service, { signal } = {}) => {
    const { P9_CLUSTER_GUARD: guard, P9_CLUSTER_MANIFEST: manifest } = process.env
    if (!guard && !manifest) {
      if (GUARD_MS[operation] === undefined) throw new Error(`No guard budget for ${operation}`)
      return execFileAsync('docker', composeArgs([operation, service]), {
        signal,
        timeout: GUARD_MS[operation],
        killSignal: 'SIGKILL',
      })
    }
    if (!guard || !manifest) throw new Error('Incomplete cluster ownership environment')
    if (GUARD_MS[operation] === undefined) throw new Error(`No guard budget for ${operation}`)
    return execFileAsync(
      'timeout',
      [`${GUARD_MS[operation] / 1000}s`, 'node', guard, 'control', manifest, operation, service],
      { signal }
    )
  }
  return run
}

/**
 * Selects the state fixture controller without changing guarded operands.
 * @param {string} action gate action
 * @param {string} groupId exact group
 * @param {string} nonce case identity
 * @returns {string[]} Node CLI operands for the guard or standalone fixture controller
 * @throws {Error} when only one ownership variable is present
 */
const gateCommand = (action, groupId, nonce) => {
  const { P9_CLUSTER_GUARD: guard, P9_CLUSTER_MANIFEST: manifest } = process.env
  if (!guard && !manifest) {
    return [require.resolve('./consumerGroupStates/gate'), action, groupId, nonce]
  }
  if (!guard || !manifest) throw new Error('Incomplete cluster ownership environment')
  return [guard, 'gate', manifest, action, groupId, nonce]
}

module.exports = { createBrokerRun, gateCommand }
