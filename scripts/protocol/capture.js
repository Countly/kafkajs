#!/usr/bin/env node
'use strict'

/**
 * Broker-capture fixtures: records the raw bytes a live broker exchanges over a real
 * connection, so the flexible codecs have a byte-golden vector nothing hand-built or
 * Java-generated can substitute for.
 *
 * It wraps the public `socketFactory` option — the only extension point a real client is meant
 * to use — so it needs no access to internals: `KAFKAJS_DEBUG_PROTOCOL_BUFFERS`
 * (`src/network/connection.js`) only unmasks the response payload inside `send()`'s error
 * branch, and no request bytes are ever logged there, so it cannot serve as a capture hook.
 *
 * Every value a fixture would otherwise pin to one run is fixed up front — the topic, group and
 * transactional id are constants below — and the two the broker chooses (the topic uuid and the
 * cluster id) are substituted byte-length-preservingly by `normalise`. Nothing is ever
 * re-encoded, so a fixture stays a faithful record of the wire. Two values are deliberately left
 * as captured: the broker `host`, and the coordinator-assigned `memberId` an OffsetCommit request
 * carries — neither is asserted on, and rewriting them would be a re-encode in all but name.
 *
 * The Produce **request** is deliberately not captured: it carries a record batch with a
 * wall-clock timestamp and a CRC over it, which no length-preserving substitution can normalise
 * and no second encoder can reproduce. Only the Produce response is written.
 *
 * The Fetch **request** is captured: its only run-varying values are the fetch offset and the
 * cached leader epoch, fixed-width integers that shift no other byte, and its topic uuid is
 * substituted like any other. They are left as captured for the same reason the `memberId` is —
 * rewriting them would be a re-encode — and no spec asserts on them.
 *
 * Usage: node scripts/protocol/capture.js --brokers localhost:9092 --tag 431
 *        node scripts/protocol/capture.js --brokers localhost:9092 --print-versions
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
const { decode: decodeApiVersions } = require('../../src/protocol/requests/apiVersions/v4/response')
const { decode: decodeMetadataV12 } = require('../../src/protocol/requests/metadata/v12/response')
const { decode: decodeMetadataV13 } = require('../../src/protocol/requests/metadata/v13/response')

const SIZE_PREFIX = 4

const CAPTURE_TOPIC = 'kafkajs-capture-topic'
const CAPTURE_GROUP = 'kafkajs-capture-group'
const CAPTURE_TRANSACTIONAL_ID = 'kafkajs-capture-txn'
const TOPIC_ID_SENTINEL = '01020304-0506-0708-090a-0b0c0d0e0f10'
// Base64-encoded 16-byte cluster ids (the format every KRaft cluster uses) are always 22
// characters, so the sentinel below has to be exactly as long — see capture.spec.js.
const CLUSTER_ID_SENTINEL = 'kafkajs-capture-clstr0'

const FIXTURES_ROOT = path.join(__dirname, '..', '..', 'src', 'protocol', 'requests')

/** Metadata response decoders, by the version the capture was negotiated at. */
const METADATA_DECODERS = { 12: decodeMetadataV12, 13: decodeMetadataV13 }

/**
 * The APIs `--print-versions` reports a broker ceiling for: the ones whose codecs this capture
 * run writes fixtures for. `FindCoordinator` is `apiKeys.GroupCoordinator` in this client.
 * @type {{ name: string, apiKey: number }[]}
 */
const VERSION_REPORT_APIS = [
  { name: 'Produce', apiKey: apiKeys.Produce },
  { name: 'ListOffsets', apiKey: apiKeys.ListOffsets },
  { name: 'Metadata', apiKey: apiKeys.Metadata },
  { name: 'OffsetCommit', apiKey: apiKeys.OffsetCommit },
  { name: 'OffsetFetch', apiKey: apiKeys.OffsetFetch },
  { name: 'FindCoordinator', apiKey: apiKeys.GroupCoordinator },
  { name: 'InitProducerId', apiKey: apiKeys.InitProducerId },
  { name: 'Fetch', apiKey: apiKeys.Fetch },
  { name: 'OffsetForLeaderEpoch', apiKey: apiKeys.OffsetForLeaderEpoch },
  { name: 'SaslAuthenticate', apiKey: apiKeys.SaslAuthenticate },
]

/**
 * The exact set of wire exchanges this script produces fixtures for, at the version this client
 * negotiates against a current broker. Anything else the client happens to send during the run
 * (SASL, other admin calls, metadata refreshes at other versions) is recorded internally for
 * matching purposes but never written to disk. A row whose version an older broker does not
 * advertise is reported and skipped, not fatal — see `run`.
 * @type {{ apiKey: number, apiVersion: number, dir: string, directions: ('request'|'response')[] }[]}
 */
const CAPTURE_LIST = [
  { apiKey: apiKeys.ApiVersions, apiVersion: 4, dir: 'apiVersions', directions: ['response'] },
  { apiKey: apiKeys.Metadata, apiVersion: 12, dir: 'metadata', directions: ['response'] },
  { apiKey: apiKeys.Metadata, apiVersion: 13, dir: 'metadata', directions: ['response'] },
  { apiKey: apiKeys.Produce, apiVersion: 11, dir: 'produce', directions: ['response'] },
  { apiKey: apiKeys.Produce, apiVersion: 13, dir: 'produce', directions: ['response'] },
  { apiKey: apiKeys.Fetch, apiVersion: 18, dir: 'fetch', directions: ['request', 'response'] },
  {
    apiKey: apiKeys.GroupCoordinator,
    apiVersion: 6,
    dir: 'findCoordinator',
    directions: ['request', 'response'],
  },
  {
    apiKey: apiKeys.OffsetCommit,
    apiVersion: 8,
    dir: 'offsetCommit',
    directions: ['request', 'response'],
  },
  {
    apiKey: apiKeys.OffsetFetch,
    apiVersion: 8,
    dir: 'offsetFetch',
    directions: ['request', 'response'],
  },
  {
    apiKey: apiKeys.ListOffsets,
    apiVersion: 9,
    dir: 'listOffsets',
    directions: ['request', 'response'],
  },
  {
    apiKey: apiKeys.InitProducerId,
    apiVersion: 5,
    dir: 'initProducerId',
    directions: ['request', 'response'],
  },
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

/**
 * Parses `--name value` pairs. A flag whose next token is another `--name`, or which ends the
 * argument list, is a boolean and becomes `true` — that is what makes `--print-versions` usable
 * alongside `--brokers host:port`.
 * @param {string[]} argv
 * @returns {{ [name: string]: string|true }}
 */
const parseArgs = (argv) => {
  const args = {}

  for (let i = 0; i < argv.length; i++) {
    const name = argv[i].replace(/^--/, '')
    const value = argv[i + 1]

    if (value === undefined || value.startsWith('--')) {
      args[name] = true
    } else {
      args[name] = value
      i++
    }
  }

  return args
}

/**
 * Finds the highest-version body captured for one api key and direction. The negotiated version
 * is not known up front — it is whatever the broker's `ApiVersions` allowed — so a caller that
 * needs to decode a capture (rather than write it verbatim) has to look it up.
 * @param {Map<string, Buffer>} captures keyed `${apiKey}/${apiVersion}/${direction}`
 * @param {number} apiKey
 * @param {'request'|'response'} direction
 * @returns {{ apiVersion: number, body: Buffer }|null} `null` when nothing was captured
 */
const findLatestCapture = (captures, apiKey, direction) => {
  let latest = null

  for (const [key, body] of captures) {
    const [capturedKey, capturedVersion, capturedDirection] = key.split('/')
    if (Number(capturedKey) !== apiKey || capturedDirection !== direction) {
      continue
    }

    const apiVersion = Number(capturedVersion)
    if (!latest || apiVersion > latest.apiVersion) {
      latest = { apiVersion, body }
    }
  }

  return latest
}

/**
 * Reduces a decoded `ApiVersions` response to the ceiling this broker advertises for each API
 * this capture run writes fixtures for.
 * @param {{ apiKey: number, minVersion: number, maxVersion: number }[]} apiVersions
 * @returns {{ name: string, apiKey: number, maxVersion: number|null }[]} `maxVersion` is `null`
 *   for an API the broker does not advertise at all
 */
const maxVersionsForCapturedApis = (apiVersions) =>
  VERSION_REPORT_APIS.map(({ name, apiKey }) => {
    const advertised = apiVersions.find((entry) => entry.apiKey === apiKey)
    return { name, apiKey, maxVersion: advertised ? advertised.maxVersion : null }
  })

/**
 * Drives an admin client through a fixed sequence of real broker calls — connect (ApiVersions),
 * list topics (Metadata), create the capture topic if missing, list topics again (Metadata with
 * the topic present), alter and list partition reassignments.
 * @param {import("../../types").Kafka} kafka
 * @param {Map<string, Buffer>} captures
 * @returns {Promise<{ clusterId: string, topicIds: string[] }>} the values `normalise` substitutes
 * @throws {Error} if the capture topic is missing from the cluster or from the captured metadata
 */
const driveAdmin = async (kafka, captures) => {
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

  const latest = findLatestCapture(captures, apiKeys.Metadata, 'response')
  const decodeMetadata = latest && METADATA_DECODERS[latest.apiVersion]
  if (!decodeMetadata) {
    throw new Error(`no Metadata response captured at a version this script can decode`)
  }

  const metadata = await decodeMetadata(latest.body)
  const topicMetadata = metadata.topicMetadata.find((entry) => entry.topic === CAPTURE_TOPIC)
  if (!topicMetadata) {
    throw new Error(`captured Metadata response has no entry for ${CAPTURE_TOPIC}`)
  }

  // Metadata does not guarantee partitions come back in partition-id order, so read both the
  // id and its replicas off the same entry rather than assuming index 0 is partition 0 — a
  // mismatched pair here would request a genuine reassignment instead of the intended no-op.
  const { partitionId, replicas } = topicMetadata.partitionMetadata[0]

  await admin.alterPartitionReassignments({
    topics: [{ topic: CAPTURE_TOPIC, partitionAssignment: [{ partition: partitionId, replicas }] }],
  })
  await admin.listPartitionReassignments({ topics: null })

  await admin.disconnect()

  return { clusterId: metadata.clusterId, topicIds: [topicMetadata.topicId] }
}

/**
 * Sends one record to the capture topic (Produce), then opens and aborts one transaction on a
 * second, transactional producer (FindCoordinator with key type TRANSACTION, then InitProducerId).
 * `connect()` alone initialises no producer id — the request is made by `transaction()` — so the
 * transaction is started and immediately aborted.
 * @param {import("../../types").Kafka} kafka
 * @returns {Promise<void>}
 */
const driveProducers = async (kafka) => {
  const producer = kafka.producer()
  await producer.connect()
  await producer.send({
    topic: CAPTURE_TOPIC,
    messages: [{ key: 'capture-key', value: 'capture-value' }],
  })
  await producer.disconnect()

  const transactionalProducer = kafka.producer({
    transactionalId: CAPTURE_TRANSACTIONAL_ID,
    idempotent: true,
    maxInFlightRequests: 1,
  })
  await transactionalProducer.connect()
  const transaction = await transactionalProducer.transaction()
  await transaction.abort()
  await transactionalProducer.disconnect()
}

/**
 * Joins a consumer group and consumes the record the producer phase wrote (FindCoordinator with
 * key type GROUP, OffsetFetch, ListOffsets), then commits an offset explicitly (OffsetCommit).
 * The run waits for the first delivered message, which only happens once the group has joined and
 * its offsets have been resolved, so no timing assumption is needed.
 * @param {import("../../types").Kafka} kafka
 * @returns {Promise<void>}
 */
const driveConsumer = async (kafka) => {
  const consumer = kafka.consumer({ groupId: CAPTURE_GROUP })
  await consumer.connect()
  await consumer.subscribe({ topic: CAPTURE_TOPIC, fromBeginning: true })

  let onMessage
  const consumed = new Promise((resolve) => {
    onMessage = resolve
  })

  await consumer.run({ eachMessage: async ({ partition }) => onMessage(partition) })
  const partition = await consumed

  await consumer.commitOffsets([{ topic: CAPTURE_TOPIC, partition, offset: '1' }])
  await consumer.disconnect()
}

/**
 * Connects an admin client and prints the maximum version the broker advertises for every API
 * this script captures, read straight off the `ApiVersions` response the socket tee recorded.
 * Nothing is written to disk.
 * @param {import("../../types").Kafka} kafka
 * @param {Map<string, Buffer>} captures
 * @returns {Promise<void>}
 * @throws {Error} if no `ApiVersions` response was captured
 */
const printVersions = async (kafka, captures) => {
  const admin = kafka.admin()
  await admin.connect()
  await admin.disconnect()

  const latest = findLatestCapture(captures, apiKeys.ApiVersions, 'response')
  if (!latest) {
    throw new Error('no ApiVersions response captured')
  }

  const { apiVersions } = await decodeApiVersions(latest.body)
  for (const { name, apiKey, maxVersion } of maxVersionsForCapturedApis(apiVersions)) {
    console.log(
      `${name} (apiKey ${apiKey}) -> maxVersion ${maxVersion === null ? 'absent' : maxVersion}`
    )
  }
}

/**
 * Drives an admin client, a producer and a consumer through a fixed sequence of real broker calls
 * while every request and response is captured, normalised, and written to the matching codec's
 * `fixtures/` directory as `v<version>_<request|response>_kafka<tag>.json`. A `CAPTURE_LIST` row
 * the broker never answered at that version — an older broker whose ceiling is below it — is
 * reported and skipped, so a legitimately absent capture is visible rather than fatal.
 *
 * With `--print-versions` the run stops after the handshake and only reports the broker's
 * per-API ceilings.
 * @returns {Promise<string[]>} the fixture files written, in write order
 * @throws {Error} if `--tag` is missing, or the capture topic cannot be created
 */
const run = async () => {
  const args = parseArgs(process.argv.slice(2))
  const brokers = String(args.brokers || 'localhost:9092').split(',')
  const tag = args.tag

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

  if (args['print-versions']) {
    await printVersions(kafka, captures)
    return []
  }

  if (!tag) {
    throw new Error('usage: node scripts/protocol/capture.js --brokers <host:port,...> --tag <tag>')
  }

  const { clusterId, topicIds } = await driveAdmin(kafka, captures)
  await driveProducers(kafka)
  await driveConsumer(kafka)

  const written = []
  for (const { apiKey, apiVersion, dir, directions } of CAPTURE_LIST) {
    for (const direction of directions) {
      const body = captures.get(`${apiKey}/${apiVersion}/${direction}`)
      if (!body) {
        console.warn(
          `skipped: no ${direction} captured for ${dir} v${apiVersion} (apiKey ${apiKey}) — this broker never negotiated that version`
        )
        continue
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

module.exports = {
  splitFrames,
  stripHeader,
  normalise,
  parseArgs,
  findLatestCapture,
  maxVersionsForCapturedApis,
  run,
}
