const { EventEmitter } = require('events')
const Encoder = require('../src/protocol/encoder')

/**
 * A socket that records everything written and lets a test push response frames back. It matches
 * the surface `src/network/socket.js` and `Connection` use: an EventEmitter with write/end/unref/
 * destroy, built by a factory that calls onConnect.
 *
 * Every socket remembers the endpoint the product dialled (`socket.endpoint`) and what was written
 * to it (`socket.writes`), so a scenario of several nodes can tell them apart; `state.writes` is
 * every socket's writes in one list, `state.sockets` every socket ever built, oldest first, and
 * `state.socket` the newest. `destroy()` behaves as `net.Socket#destroy` does, minus the kernel: it
 * marks the socket `destroyed` at once and emits `error` (when given one) and `close` a microtask
 * later, once. A write after that is still recorded, so a test can see it.
 * @returns {{ factory: Function, state: { writes: Buffer[], socket: EventEmitter,
 *   sockets: EventEmitter[] } }}
 */
const createFakeSocketFactory = () => {
  const state = { writes: [], socket: null, sockets: [] }

  const factory = ({ host, port, onConnect }) => {
    const socket = new EventEmitter()
    socket.endpoint = { host, port }
    socket.destroyed = false
    socket.writes = []
    socket.write = (buffer) => {
      const written = Buffer.from(buffer)
      socket.writes.push(written)
      state.writes.push(written)
      return true
    }
    socket.end = () => {}
    socket.unref = () => {}
    socket.destroy = (error) => {
      if (!socket.destroyed) {
        socket.destroyed = true
        queueMicrotask(() => {
          if (error) {
            socket.emit('error', error)
          }
          socket.emit('close', Boolean(error))
        })
      }

      return socket
    }
    state.socket = socket
    state.sockets.push(socket)
    setImmediate(onConnect)
    return socket
  }

  return { factory, state }
}

/**
 * @param {{ sockets: Array<{ destroyed: boolean }> }} state a fake socket factory's state
 * @returns {{ created: number, destroyed: number }} how many sockets the product has opened, and
 *   how many of them it has destroyed
 */
const socketCounts = ({ sockets }) => ({
  created: sockets.length,
  destroyed: sockets.filter((socket) => socket.destroyed).length,
})

/**
 * Frames a response the way a broker does: size, correlation id, optional header tag buffer,
 * then the body.
 * @param {{ socket: EventEmitter }} state
 * @param {number} correlationId
 * @param {0|1} headerVersion 0 = classic, 1 = flexible (ApiVersions is always 0)
 * @param {Buffer} body
 * @param {{ tag: number, value: Buffer }[]} [headerTags] tagged fields to put in a flexible
 *        response header; omit for the empty section every real broker sends today
 */
const respondWithFrame = (state, correlationId, headerVersion, body, headerTags) => {
  const header = new Encoder().writeInt32(correlationId)
  if (headerVersion === 1) {
    header.writeTaggedFields(headerTags)
  }

  const framed = new Encoder().writeEncoder(header).writeBuffer(body)
  state.socket.emit('data', new Encoder().writeInt32(framed.size()).writeEncoder(framed).buffer)
}

/**
 * Reads the request header off a recorded write.
 *
 * `byteLength` is where the header ends and the codec's own body begins, which is what a
 * byte-for-byte frame comparison needs: the correlation id and the client id live in the header
 * and move with call order. The header length is version-dependent — a flexible request header
 * carries a tagged-field buffer after the client id, a classic one does not.
 * @see https://kafka.apache.org/43/design/protocol
 * @param {Buffer} frame
 * @returns {{ apiKey: number, apiVersion: number, correlationId: number, clientId: string,
 *   byteLength: number }}
 */
const readRequestHeader = (frame) => {
  const Decoder = require('../src/protocol/decoder')
  const { requestHeaderVersion, REQUEST_HEADER_V2 } = require('../src/protocol/headers')
  const decoder = new Decoder(frame)
  decoder.readInt32()
  const apiKey = decoder.readInt16()
  const apiVersion = decoder.readInt16()
  const correlationId = decoder.readInt32()
  const clientId = decoder.readString()

  if (requestHeaderVersion(apiKey, apiVersion) === REQUEST_HEADER_V2) {
    decoder.readTaggedFields()
  }

  return { apiKey, apiVersion, correlationId, clientId, byteLength: decoder.offset }
}

module.exports = { createFakeSocketFactory, socketCounts, respondWithFrame, readRequestHeader }
