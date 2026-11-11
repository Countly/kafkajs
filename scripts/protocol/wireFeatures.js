#!/usr/bin/env node
'use strict'

/**
 * Standalone ApiVersions v4 probe for independent broker captures.
 *
 * It uses Node built-in modules only and loads nothing from the client, so what it prints cannot
 * come from the codecs under test. It sends one ApiVersions v4 request, reads the answer and prints
 * one JSON line:
 *
 *   {"correlation","errorCode","apiKeyCount","throttle","apiRanges","shareApiRanges",
 *    "supportedFeatures","finalizedFeaturesEpoch","finalizedFeatures","tags"}
 *
 * A field the broker did not send is left out, never defaulted. `tags` lists the response tag ids in
 * the order they came; `finalizedFeaturesEpoch` is the int64 as a decimal string.
 *
 * Usage: node scripts/protocol/wireFeatures.js <host> <port>
 * @see https://kafka.apache.org/43/design/protocol
 */
const net = require('net')

const API_VERSIONS = 18
const REQUEST_VERSION = 4
const CORRELATION_ID = 1
const CLIENT_NAME = 'wire-features'
const MAX_FRAME_BYTES = 1 << 20
const DEFAULT_TIMEOUT_MS = 10000
const SHARE_API_KEYS = [76, 77, 78, 79, 90, 91, 92]

/**
 * @param {number} value a non-negative integer below 2^31
 * @returns {Buffer} its unsigned varint encoding
 */
const uvarint = (value) => {
  const bytes = []
  let rest = value
  while (rest >= 0x80) {
    bytes.push((rest & 0x7f) | 0x80)
    rest >>>= 7
  }
  bytes.push(rest)
  return Buffer.from(bytes)
}

/**
 * @param {string} text
 * @returns {Buffer} a non-null compact string: its length plus one, then the UTF-8 bytes
 */
const compactString = (text) => {
  const data = Buffer.from(text, 'utf8')
  return Buffer.concat([uvarint(data.length + 1), data])
}

/**
 * Frames the request: header v2 with the classic INT16-prefixed client id, then the v4 body with
 * the software name and version and empty tag buffers.
 * @returns {Buffer} the size-prefixed frame
 */
const buildRequest = () => {
  const clientId = Buffer.from(CLIENT_NAME, 'utf8')
  const header = Buffer.alloc(10)
  header.writeInt16BE(API_VERSIONS, 0)
  header.writeInt16BE(REQUEST_VERSION, 2)
  header.writeInt32BE(CORRELATION_ID, 4)
  header.writeInt16BE(clientId.length, 8)
  const payload = Buffer.concat([
    header,
    clientId,
    Buffer.from([0]),
    compactString(CLIENT_NAME),
    compactString('0'),
    Buffer.from([0]),
  ])
  const size = Buffer.alloc(4)
  size.writeInt32BE(payload.length)
  return Buffer.concat([size, payload])
}

/** A bounds-checked reader over one response payload. */
class Cursor {
  /** @param {Buffer} buffer */
  constructor(buffer) {
    this.buffer = buffer
    this.offset = 0
  }

  /** @returns {number} the bytes not read yet */
  remaining() {
    return this.buffer.length - this.offset
  }

  /**
   * @param {number} length
   * @returns {Buffer} the next `length` bytes
   * @throws {Error} when fewer remain
   */
  take(length) {
    if (length < 0 || length > this.remaining()) {
      throw new Error('ApiVersions answer is truncated')
    }
    const slice = this.buffer.subarray(this.offset, this.offset + length)
    this.offset += length
    return slice
  }

  /** @returns {number} */
  int16() {
    return this.take(2).readInt16BE()
  }

  /** @returns {number} */
  int32() {
    return this.take(4).readInt32BE()
  }

  /** @returns {string} the int64 as a decimal string */
  int64() {
    return this.take(8).readBigInt64BE().toString()
  }

  /** @returns {number} an unsigned varint of at most five bytes */
  uvarint() {
    let value = 0
    for (let shift = 0; shift <= 28; shift += 7) {
      const byte = this.take(1)[0]
      value += (byte & 0x7f) * 2 ** shift
      if (!(byte & 0x80)) return value
    }
    throw new Error('ApiVersions answer holds an over-long varint')
  }

  /** @returns {string} a non-null compact string */
  compactString() {
    const length = this.uvarint()
    if (length === 0) throw new Error('ApiVersions answer holds an unexpected null string')
    return this.take(length - 1).toString('utf8')
  }

  /**
   * Reads a non-null compact array; each element takes at least one byte, which bounds the count.
   * @template T
   * @param {(cursor: Cursor) => T} read reads one element
   * @returns {T[]}
   */
  compactArray(read) {
    const count = this.uvarint() - 1
    if (count < 0 || count > this.remaining()) {
      throw new Error('ApiVersions answer holds an invalid array length')
    }
    const items = []
    for (let i = 0; i < count; i++) items.push(read(this))
    return items
  }

  /** Skips an element's empty-or-unknown tag buffer. */
  skipTags() {
    for (let count = this.uvarint(); count > 0; count--) {
      this.uvarint()
      this.take(this.uvarint())
    }
  }

  /** @throws {Error} when bytes remain, naming `what` */
  done(what) {
    if (this.remaining() !== 0) throw new Error(`${what} has trailing bytes`)
  }
}

/** The readers of the tagged fields of a v4 response, by tag id. */
const TAGGED = {
  0: (cursor) => ({
    supportedFeatures: cursor.compactArray((item) => {
      const feature = { name: item.compactString(), min: item.int16(), max: item.int16() }
      item.skipTags()
      return feature
    }),
  }),
  1: (cursor) => ({ finalizedFeaturesEpoch: cursor.int64() }),
  2: (cursor) => ({
    finalizedFeatures: cursor.compactArray((item) => {
      const feature = {
        name: item.compactString(),
        maxVersionLevel: item.int16(),
        minVersionLevel: item.int16(),
      }
      item.skipTags()
      return feature
    }),
  }),
  3: (cursor) => ({ zkMigrationReady: cursor.take(1)[0] !== 0 }),
}

/**
 * Parses one response payload (header v0 and the v4 body).
 * @param {Buffer} payload the frame without its size prefix
 * @returns {object} the line this script prints
 * @throws {Error} on another correlation id, a broker error code, a malformed or truncated body
 */
const parseAnswer = (payload) => {
  const cursor = new Cursor(payload)
  const correlation = cursor.int32()
  if (correlation !== CORRELATION_ID) {
    throw new Error(`ApiVersions answered correlation ${correlation}, expected ${CORRELATION_ID}`)
  }

  const errorCode = cursor.int16()
  if (errorCode !== 0) throw new Error(`ApiVersions answered error code ${errorCode}`)

  const apiRanges = {}
  for (const [apiKey, min, max] of cursor.compactArray((item) => {
    const entry = [item.int16(), item.int16(), item.int16()]
    item.skipTags()
    return entry
  })) {
    if (Object.hasOwn(apiRanges, apiKey)) throw new Error(`ApiVersions lists API ${apiKey} twice`)
    apiRanges[apiKey] = [min, max]
  }

  const throttle = cursor.int32()
  const tagged = {}
  const tags = []
  for (let count = cursor.uvarint(); count > 0; count--) {
    const tag = cursor.uvarint()
    const field = new Cursor(cursor.take(cursor.uvarint()))
    tags.push(tag)
    if (TAGGED[tag]) {
      Object.assign(tagged, TAGGED[tag](field))
      field.done(`tag ${tag}`)
    }
  }
  cursor.done('the ApiVersions answer')

  const shareApiRanges = Object.fromEntries(
    SHARE_API_KEYS.filter((apiKey) => Object.hasOwn(apiRanges, apiKey)).map((apiKey) => [
      apiKey,
      apiRanges[apiKey],
    ])
  )

  return {
    correlation,
    errorCode,
    apiKeyCount: Object.keys(apiRanges).length,
    throttle,
    apiRanges,
    shareApiRanges,
    ...(tagged.supportedFeatures && { supportedFeatures: tagged.supportedFeatures }),
    ...(tagged.finalizedFeaturesEpoch !== undefined && {
      finalizedFeaturesEpoch: tagged.finalizedFeaturesEpoch,
    }),
    ...(tagged.finalizedFeatures && { finalizedFeatures: tagged.finalizedFeatures }),
    ...(tagged.zkMigrationReady !== undefined && { zkMigrationReady: tagged.zkMigrationReady }),
    tags,
  }
}

/**
 * Sends one ApiVersions v4 request and returns the parsed answer. The connection is destroyed on
 * every outcome.
 * @param {object} options
 * @param {string} options.host
 * @param {number} options.port
 * @param {number} [options.timeoutMs] idle time allowed on the socket, connecting included
 * @returns {Promise<object>} the line this script prints
 * @throws {Error} on a connection error, a timeout, a close before the frame ends, a frame size
 *   above 1 MiB, or anything `parseAnswer` rejects
 */
const capture = ({ host, port, timeoutMs = DEFAULT_TIMEOUT_MS }) =>
  new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port })
    const chunks = []
    let received = 0
    let size = null
    let settled = false

    const settle = (error, value) => {
      if (settled) return
      settled = true
      socket.destroy()
      if (error) reject(error)
      else resolve(value)
    }

    socket.setTimeout(timeoutMs, () =>
      settle(new Error(`ApiVersions timed out after ${timeoutMs} ms`))
    )
    socket.once('error', settle)
    socket.once('connect', () => socket.write(buildRequest()))
    socket.once('close', () =>
      settle(new Error('ApiVersions connection closed before the frame was complete'))
    )
    socket.on('data', (chunk) => {
      chunks.push(chunk)
      received += chunk.length
      if (size === null && received >= 4) {
        size = Buffer.concat(chunks).readInt32BE(0)
        if (size < 4 || size > MAX_FRAME_BYTES) {
          settle(new Error(`ApiVersions frame size ${size} is outside 4..${MAX_FRAME_BYTES}`))
        }
      }
      if (!settled && size !== null && received >= 4 + size) {
        try {
          settle(null, parseAnswer(Buffer.concat(chunks).subarray(4, 4 + size)))
        } catch (error) {
          settle(error)
        }
      }
    })
  })

/**
 * Runs the script: prints the answer as one JSON line.
 * @param {string[]} argv the arguments after the script name: host and port
 * @returns {Promise<void>}
 * @throws {Error} with the usage for a missing host or an invalid port; else what `capture` throws
 */
const main = async (argv) => {
  const [host, portText] = argv
  const port = Number(portText)
  if (!host || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('Usage: node scripts/protocol/wireFeatures.js <host> <port>')
  }

  process.stdout.write(`${JSON.stringify(await capture({ host, port }))}\n`)
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(error.message)
    process.exit(1)
  })
}

module.exports = { capture, buildRequest, parseAnswer, main }
