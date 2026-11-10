const { _load } = require('module')

// Jest substitutes a per-file process object, even for require('process').
const workerProcess = _load('process')
const registryKey = Symbol.for('kafkajs.test.socketOwners')

/**
 * Opts this Jest file into one process-wide client-connect registry; disabled runs patch nothing.
 * @param {() => { testPath?: string, currentTestName?: string }} getState Current Jest state.
 * @returns {void}
 */
const installSocketOwners = (getState) => {
  if (process.env.KAFKAJS_SOCKET_OWNERS !== '1') return
  if (!workerProcess[registryKey]) {
    // The native loader keeps the hook, class and containers outside Jest's file realms.
    const SocketOwners = _load(require.resolve('./socketOwnerRegistry'))
    workerProcess[registryKey] = new SocketOwners()
  }
  workerProcess[registryKey].enterFile(getState)
}

/**
 * Reads failure-only diagnostics from the worker registry without installing any hooks.
 * @returns {string} Worker/socket lines, or an empty string when tracking is inactive.
 */
const describeSocketOwners = () => workerProcess[registryKey]?.describe() ?? ''

module.exports = { installSocketOwners, describeSocketOwners }
