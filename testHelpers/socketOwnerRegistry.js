const net = require('net')
const path = require('path')
const workerProcess = require('process')

const root = path.resolve(__dirname, '..') + path.sep

/** Attributes client connects with native-realm data and weak socket keys. */
class SocketOwners {
  /**
   * Installs one worker-wide hook, also inherited by TLSSocket.
   * @returns {SocketOwners} The registry with weak socket keys and bounded file history.
   */
  constructor() {
    this.owners = new WeakMap()
    this.testFiles = []
    this.getState = null
    const registry = this
    const original = net.Socket.prototype.connect
    /**
     * Attributes the first connect without changing arguments, receiver or return value.
     * @param {...*} args Original Socket.connect arguments.
     * @returns {import('net').Socket} The original connect result.
     * @throws {Error} Any error from the original connect.
     */
    net.Socket.prototype.connect = function connect(...args) {
      registry.record(this, connect)
      return Reflect.apply(original, this, args)
    }
  }

  /**
   * Replaces the only file-realm callback so completed files can be collected.
   * @param {() => { testPath?: string, currentTestName?: string }} getState Current Jest state.
   * @returns {void}
   */
  enterFile(getState) {
    this.getState = getState
    const file = getState().testPath
    if (file && this.testFiles[this.testFiles.length - 1] !== file) {
      this.testFiles.push(file)
      if (this.testFiles.length > 200) this.testFiles.shift()
    }
  }

  /**
   * Records only the first connect, with at most eight repository frames and no socket reference.
   * @param {import('net').Socket} socket Connecting TCP or TLS client.
   * @param {Function} connect Hook frame to omit from the creation stack.
   * @returns {void}
   */
  record(socket, connect) {
    if (this.owners.has(socket)) return
    const state = this.getState()
    const trace = {}
    Error.captureStackTrace(trace, connect)
    this.owners.set(socket, {
      file: state.testPath,
      test: state.currentTestName,
      time: Date.now(),
      stack: trace.stack
        .split('\n')
        .filter((frame) => frame.includes(root) && !frame.includes('node_modules'))
        .slice(0, 8)
        .map((frame) => frame.trim().replace(root, '')),
    })
  }

  /**
   * Reports every active Socket, including unregistered accepted sockets and worker stdio.
   * File history is ordered oldest first, with the current file last (at most 200 entries).
   * @returns {string} Worker identity and one compact line per live socket.
   */
  describe() {
    const files = this.testFiles.map((file) => path.relative(root, file))
    const lines = [
      `; socket owners JEST_WORKER_ID=${workerProcess.env.JEST_WORKER_ID ?? 'unknown'} pid=${workerProcess.pid} testFiles=${JSON.stringify(files)}`,
    ]
    for (const socket of workerProcess._getActiveHandles()) {
      if (!(socket instanceof net.Socket)) continue
      const owner = this.owners.get(socket)
      const identity = owner
        ? `file=${JSON.stringify(owner.file ? path.relative(root, owner.file) : null)} test=${JSON.stringify(owner.test ?? null)} time=${owner.time} stack=${JSON.stringify(owner.stack)}`
        : 'unregistered'
      lines.push(
        `socket local=${socket.localAddress ?? '-'}:${socket.localPort ?? '-'} remote=${socket.remoteAddress ?? '-'}:${socket.remotePort ?? '-'} destroyed=${socket.destroyed} readyState=${socket.readyState} ${identity}`
      )
    }
    return lines.join('\n')
  }
}

module.exports = SocketOwners
