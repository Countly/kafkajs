#!/usr/bin/env node
'use strict'

/**
 * Broker-capture fixtures: records the raw bytes a live broker exchanges over a real
 * connection, so the flexible codecs (ApiVersions v4, Metadata v12, and the two admin
 * "hazard" codecs) have a byte-golden vector nothing hand-built or Java-generated can
 * substitute for.
 *
 * It wraps the public `socketFactory` option — the only extension point a real client is meant
 * to use — so it needs no access to internals: `KAFKAJS_DEBUG_PROTOCOL_BUFFERS`
 * (`src/network/connection.js`) only unmasks the response payload inside `send()`'s error
 * branch, and no request bytes are ever logged there, so it cannot serve as a capture hook.
 *
 * Usage: node scripts/protocol/capture.js --brokers localhost:9092 --tag 431
 *
 * Development-only script; never imported by `src/`.
 */

const fs = require('fs')
const path = require('path')
const { Kafka, logLevel } = require('../../index')
const defaultSocketFactory = require('../../src/network/socketFactory')
const Decoder = require('../../src/protocol/decoder')
const headers = require('../../src/protocol/headers')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { decode: decodeMetadataV12 } = require('../../src/protocol/requests/metadata/v12/response')

const SIZE_PREFIX = 4

const CAPTURE_TOPIC = 'kafkajs-capture-topic'
const TOPIC_ID_SENTINEL = '01020304-0506-0708-090a-0b0c0d0e0f10'
// Base64-encoded 16-byte cluster ids (the format every KRaft cluster uses) are always 22
// characters, so the sentinel below has to be exactly as long — see capture.spec.js.
const CLUSTER_ID_SENTINEL = 'kafkajs-capture-clstr0'

const FIXTURES_ROOT = path.join(__dirname, '..', '..', 'src', 'protocol', 'requests')

/**
 * The exact set of wire exchanges this script produces fixtures for. Anything else the client
 * happens to send during the run (SASL, other admin calls, metadata refreshes at other
 * versions) is recorded internally for matching purposes but never written to disk.
 * @type {{ apiKey: number, apiVersion: number, dir: string, directions: ('request'|'response')[] }[]}
 */
const CAPTURE_LIST = [
  { apiKey: apiKeys.ApiVersions, apiVersion: 4, dir: 'apiVersions', directions: ['response'] },
  { apiKey: apiKeys.Metadata, apiVersion: 12, dir: 'metadata', directions: ['response'] },
  {
    apiKey: apiKeys.AlterPartitionReassignments,
    apiVersion: 0,
    dir: 'alterPartitionReassignments',
    directions: ['request', 'response'],
  },
  {
    apiKey: apiKeys.ListPartitionReassignments,
    apiVersion: 0,
    dir: 'listPartitionReassignments',
    directions: ['request', 'response'],
  },
]

/**
 * Splits a byte stream into complete size-prefixed Kafka frames (a 4-byte big-endian length
 * followed by that many bytes), the same framing `src/network/socket.js` reassembles on the
 * client side. Bytes that do not yet form a complete frame are returned as `remainder` so the
 * caller can prepend them to the next chunk.
 * @param {Buffer} stream
 * @returns {{ frames: Buffer[], remainder: Buffer }}
 */
const splitFrames = (stream) => {
  const frames = []
  let offset = 0

  while (stream.length - offset >= SIZE_PREFIX) {
    const size = stream.readInt32BE(offset)
    if (stream.length - offset - SIZE_PREFIX < size) {
      break
    }

    frames.push(stream.slice(offset + SIZE_PREFIX, offset + SIZE_PREFIX + size))
    offset += SIZE_PREFIX + size
  }

  return { frames, remainder: stream.slice(offset) }
}

/**
 * Strips a request or response header off one frame, using `src/protocol/headers.js` to decide
 * whether the header is flexible. This does not reuse `testHelpers/fakeSocket.js`'s
 * `readRequestHeader`: that helper assumes its input still carries the 4-byte size prefix
 * (it does a leading `readInt32()` to consume it, whereas `frame` here already has it stripped
 * by `splitFrames`), only extracts apiKey/apiVersion/correlationId, and never skips `client_id`
 * or a flexible header's tag buffer or returns where the body starts — all of which this needs.
 * @param {'request'|'response'} direction
 * @param {Buffer} frame the frame `splitFrames` produced, header and body together
 * @param {{ apiKey: number, apiVersion: number }} [meta] required for `'response'`: the
 *        `(apiKey, apiVersion)` of the request this response answers, learned by the caller
 *        from a prior `'request'` call sharing the same correlation id
 * @returns {{ apiKey: number, apiVersion: number, correlationId: number, body: Buffer }|
 *           { correlationId: number, body: Buffer }}
 */
const stripHeader = (direction, frame, meta) => {
  const decoder = new Decoder(frame)

  if (direction === 'request') {
    const apiKey = decoder.readInt16()
    const apiVersion = decoder.readInt16()
    const correlationId = decoder.readInt32()
    decoder.readString() // client_id: classic INT16-prefixed string even in header v2

    if (headers.requestHeaderVersion(apiKey, apiVersion) === headers.REQUEST_HEADER_V2) {
      decoder.readTaggedFields()
    }

    return { apiKey, apiVersion, correlationId, body: frame.slice(decoder.offset) }
  }

  if (!meta) {
    throw new Error('stripHeader("response", ...) requires meta: { apiKey, apiVersion }')
  }

  const correlationId = decoder.readInt32()
  if (headers.responseHeaderVersion(meta.apiKey, meta.apiVersion) === headers.RESPONSE_HEADER_V1) {
    decoder.readTaggedFields()
  }

  return { correlationId, body: frame.slice(decoder.offset) }
}

/**
 * Overwrites every occurrence of `search` with `replacement` (same length, so no other byte
 * shifts). Buffer-length-preserving substitution only — never a re-encode.
 * @param {Buffer} buffer
 * @param {Buffer} search
 * @param {Buffer} replacement same length as `search`
 * @returns {Buffer} a copy of `buffer` with every match overwritten
 */
const replaceAll = (buffer, search, replacement) => {
  if (search.length === 0 || search.length !== replacement.length) {
    return buffer
  }

  const out = Buffer.from(buffer)
  let index = out.indexOf(search)
  while (index !== -1) {
    replacement.copy(out, index)
    index = out.indexOf(search, index + replacement.length)
  }

  return out
}

/**
 * Replaces the values a capture run cannot pin down in advance — the broker-assigned topic
 * uuid(s) and the cluster id — with fixed sentinels, byte-length-preserving so the fixture
 * stays a faithful capture rather than a re-encoded approximation. The broker `host` is
 * deliberately left alone (whatever the compose network reports).
 * @param {Buffer} body
 * @param {{ topicIds?: string[], clusterId?: string }} values
 * @returns {Buffer} a copy of `body` with every match substituted
 */
const normalise = (body, { topicIds = [], clusterId } = {}) => {
  let out = body

  const sentinelBytes = Buffer.from(TOPIC_ID_SENTINEL.replace(/-/g, ''), 'hex')
  for (const topicId of topicIds) {
    const raw = Buffer.from(topicId.replace(/-/g, ''), 'hex')
    out = replaceAll(out, raw, sentinelBytes)
  }

  if (clusterId) {
    out = replaceAll(out, Buffer.from(clusterId, 'utf8'), Buffer.from(CLUSTER_ID_SENTINEL, 'utf8'))
  }

  return out
}

/**
 * Records every request written and response received on one broker connection, matching
 * requests to responses by correlation id so a response frame — which carries no api key or
 * version of its own — can still have its header stripped correctly. Reports each captured
 * `(apiKey, apiVersion, direction)` body to `onCapture`, last-write-wins: a script run drives
 * the client through a deliberate sequence (connect, create the topic, re-fetch metadata,
 * reassign), and the final exchange for a given key is always the most complete one.
 */
class CaptureSession {
  /** @param {(apiKey: number, apiVersion: number, direction: 'request'|'response', body: Buffer) => void} onCapture */
  constructor(onCapture) {
    this._onCapture = onCapture
    this._writeBuffer = Buffer.alloc(0)
    this._dataBuffer = Buffer.alloc(0)
    /** @type {Map<number, { apiKey: number, apiVersion: number }>} correlationId -> request meta */
    this._pending = new Map()
  }

  /**
   * Feed bytes the client wrote to the socket (a request, or a fragment of one).
   * @param {Buffer} chunk
   * @returns {void}
   */
  onWrite(chunk) {
    this._writeBuffer = Buffer.concat([this._writeBuffer, chunk])
    const { frames, remainder } = splitFrames(this._writeBuffer)
    this._writeBuffer = remainder

    for (const frame of frames) {
      const { apiKey, apiVersion, correlationId, body } = stripHeader('request', frame)
      this._pending.set(correlationId, { apiKey, apiVersion })
      this._onCapture(apiKey, apiVersion, 'request', body)
    }
  }

  /**
   * Feed bytes the socket received from the broker (a response, or a fragment of one).
   * @param {Buffer} chunk
   * @returns {void}
   */
  onData(chunk) {
    this._dataBuffer = Buffer.concat([this._dataBuffer, chunk])
    const { frames, remainder } = splitFrames(this._dataBuffer)
    this._dataBuffer = remainder

    for (const frame of frames) {
      const correlationId = frame.readInt32BE(0)
      const meta = this._pending.get(correlationId)
      if (!meta) {
        continue // a response this session never saw the request for (e.g. from a stale epoch)
      }

      const { body } = stripHeader('response', frame, meta)
      this._onCapture(meta.apiKey, meta.apiVersion, 'response', body)
    }
  }
}

/**
 * Wraps the default `net`/`tls` socket factory so every connection it opens is teed into a
 * fresh {@link CaptureSession}. One session per socket keeps request/response framing correct
 * per-connection; all sessions report into the same `onCapture` callback.
 * @param {(apiKey: number, apiVersion: number, direction: 'request'|'response', body: Buffer) => void} onCapture
 * @returns {import("../../types").ISocketFactory}
 */
const createCapturingSocketFactory = (onCapture) => {
  const factory = defaultSocketFactory()

  return (options) => {
    const socket = factory(options)
    const session = new CaptureSession(onCapture)

    const originalWrite = socket.write.bind(socket)
    socket.write = (chunk, ...rest) => {
      session.onWrite(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
      return originalWrite(chunk, ...rest)
    }

    socket.on('data', (chunk) => session.onData(chunk))

    return socket
  }
}

/** @param {string[]} argv @returns {{ [name: string]: string }} */
const parseArgs = (argv) => {
  const args = {}
  for (let i = 0; i < argv.length; i += 2) {
    args[argv[i].replace(/^--/, '')] = argv[i + 1]
  }
  return args
}

/**
 * Drives an admin client through a fixed sequence of real broker calls — connect (ApiVersions),
 * list topics (Metadata), create the capture topic if missing, list topics again (Metadata with
 * the topic present), alter and list partition reassignments — while every request and response
 * is captured, normalised, and written to the matching codec's `fixtures/` directory as
 * `v<version>_<request|response>_kafka<tag>.json`.
 * @returns {Promise<string[]>} the fixture files written, in write order
 */
const run = async () => {
  const args = parseArgs(process.argv.slice(2))
  const brokers = (args.brokers || 'localhost:9092').split(',')
  const tag = args.tag

  if (!tag) {
    throw new Error('usage: node scripts/protocol/capture.js --brokers <host:port,...> --tag <tag>')
  }

  /** @type {Map<string, Buffer>} `${apiKey}/${apiVersion}/${direction}` -> last captured body */
  const captures = new Map()
  const onCapture = (apiKey, apiVersion, direction, body) => {
    captures.set(`${apiKey}/${apiVersion}/${direction}`, body)
  }

  const kafka = new Kafka({
    clientId: 'kafkajs-capture',
    brokers,
    logLevel: logLevel.NOTHING,
    socketFactory: createCapturingSocketFactory(onCapture),
  })

  const admin = kafka.admin()
  await admin.connect()

  let topics = await admin.listTopics()
  if (!topics.includes(CAPTURE_TOPIC)) {
    await admin.createTopics({ topics: [{ topic: CAPTURE_TOPIC }], waitForLeaders: true })
  }
  // Re-fetch so the captured Metadata response reflects the topic, with a leader elected.
  topics = await admin.listTopics()
  if (!topics.includes(CAPTURE_TOPIC)) {
    throw new Error(`${CAPTURE_TOPIC} was not created`)
  }

  const metadataBody = captures.get(`${apiKeys.Metadata}/12/response`)
  const metadata = await decodeMetadataV12(metadataBody)
  const clusterId = metadata.clusterId
  const topicMetadata = metadata.topicMetadata.find((entry) => entry.topic === CAPTURE_TOPIC)
  if (!topicMetadata) {
    throw new Error(`captured Metadata response has no entry for ${CAPTURE_TOPIC}`)
  }
  const topicIds = [topicMetadata.topicId]
  // Metadata does not guarantee partitions come back in partition-id order, so read both the
  // id and its replicas off the same entry rather than assuming index 0 is partition 0 — a
  // mismatched pair here would request a genuine reassignment instead of the intended no-op.
  const { partitionId, replicas } = topicMetadata.partitionMetadata[0]

  await admin.alterPartitionReassignments({
    topics: [{ topic: CAPTURE_TOPIC, partitionAssignment: [{ partition: partitionId, replicas }] }],
  })
  await admin.listPartitionReassignments({ topics: null })

  await admin.disconnect()

  const written = []
  for (const { apiKey, apiVersion, dir, directions } of CAPTURE_LIST) {
    for (const direction of directions) {
      const body = captures.get(`${apiKey}/${apiVersion}/${direction}`)
      if (!body) {
        throw new Error(`no ${direction} captured for apiKey ${apiKey} v${apiVersion}`)
      }

      const normalised = normalise(body, { topicIds, clusterId })
      const filePath = path.join(
        FIXTURES_ROOT,
        dir,
        'fixtures',
        `v${apiVersion}_${direction}_kafka${tag}.json`
      )
      fs.writeFileSync(filePath, `${JSON.stringify(normalised)}\n`)
      written.push(filePath)
    }
  }

  for (const filePath of written) {
    console.log(`wrote ${path.relative(process.cwd(), filePath)}`)
  }

  return written
}

if (require.main === module) {
  run().catch((error) => {
    console.error(error)
    process.exit(1)
  })
}

module.exports = { splitFrames, stripHeader, normalise, run }
