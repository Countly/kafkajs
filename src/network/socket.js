/** @type {WeakMap<object, () => void>} each socket's close handler, removed on a deliberate close */
const closeHandlers = new WeakMap()

/**
 * @param {Object} options
 * @param {import("../../types").ISocketFactory} options.socketFactory
 * @param {string} options.host
 * @param {number} options.port
 * @param {Object} options.ssl
 * @param {() => void} options.onConnect
 * @param {(data: Buffer) => void} options.onData
 * @param {() => void} options.onEnd Handles both end and close events.
 * @param {(err: Error) => void} options.onError
 * @param {() => void} options.onTimeout
 */
module.exports = ({
  socketFactory,
  host,
  port,
  ssl,
  onConnect,
  onData,
  onEnd,
  onError,
  onTimeout,
}) => {
  const socket = socketFactory({ host, port, ssl, onConnect })

  socket.on('data', onData)
  socket.on('end', onEnd)
  socket.on('close', onEnd)
  socket.on('error', onError)
  socket.on('timeout', onTimeout)
  closeHandlers.set(socket, onEnd)

  return socket
}

/**
 * Detaches this module's data, end, timeout and close handlers before a deliberate destroy.
 * Only our own close handler is removed: a TLS socket keeps its internal close cleanup. The
 * error handler stays attached, so a late error on the old socket is not uncaught.
 * @param {import("net").Socket} socket
 * @returns {void}
 */
module.exports.detachSocket = (socket) => {
  if (typeof socket.removeAllListeners === 'function') {
    socket.removeAllListeners('data')
    socket.removeAllListeners('end')
    socket.removeAllListeners('timeout')
  }
  const onClose = closeHandlers.get(socket)
  if (onClose && typeof socket.removeListener === 'function')
    socket.removeListener('close', onClose)
  closeHandlers.delete(socket)
}
