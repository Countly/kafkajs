const { EventEmitter } = require('events')
const Encoder = require('../src/protocol/encoder')

/**
 * A socket that records everything written and lets a test push response frames back. It matches
 * the surface `src/network/socket.js` and `Connection` use: an EventEmitter with write/end/unref,
 * built by a factory that calls onConnect.
 * @returns {{ factory: Function, state: { writes: Buffer[], socket: EventEmitter } }}
 */
const createFakeSocketFactory = () => {
  const state = { writes: [], socket: null }

  const factory = ({ onConnect }) => {
    const socket = new EventEmitter()
    socket.write = (buffer) => {
      state.writes.push(Buffer.from(buffer))
      return true
    }
    socket.end = () => {}
    socket.unref = () => {}
    state.socket = socket
    setImmediate(onConnect)
    return socket
  }

  return { factory, state }
}

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
 * @param {Buffer} frame
 * @returns {{ apiKey: number, apiVersion: number, correlationId: number }}
 */
const readRequestHeader = (frame) => {
  const Decoder = require('../src/protocol/decoder')
  const decoder = new Decoder(frame)
  decoder.readInt32()
  return {
    apiKey: decoder.readInt16(),
    apiVersion: decoder.readInt16(),
    correlationId: decoder.readInt32(),
  }
}

module.exports = { createFakeSocketFactory, respondWithFrame, readRequestHeader }
