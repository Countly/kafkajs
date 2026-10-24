/**
 * The authentic librdkafka Snappy capture and what is read off it: the loader that refuses to run
 * without it, the replay envelope, and the public projection the capture's manifest records. The
 * capture is acquired from a real librdkafka producer and is never synthesized: JavaScript or Java
 * generated data does not replace it.
 */
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { isDeepStrictEqual } = require('util')
const { KafkaJSNotImplemented } = require('../../../../errors')
const crc32C = require('../../../recordBatch/crc32C')

/** Where the capture's three files live. */
const CAPTURE_DIR = path.join(__dirname, '..', '..', '..', 'fixtures', 'librdkafka')

/** The capture's files: the original batch bytes, its manifest, and the uncompressed records. */
const CAPTURE_FILES = Object.freeze({
  batch: 'snappy-record-batch.bin',
  manifest: 'snappy-record-batch.json',
  data: 'snappy-record-data.bin',
})

/**
 * Reads the authentic capture.
 * @param {string} [directory] where the files are; the capture's own directory by default
 * @returns {{ batch: Buffer, data: Buffer, manifest: object }} the original batch bytes, the
 *   independently acquired uncompressed record section, and the parsed manifest
 * @throws {Error} naming every missing file: without the capture nothing that needs it can pass
 */
const loadCapture = (directory = CAPTURE_DIR) => {
  const missing = Object.values(CAPTURE_FILES).filter(
    (name) => !fs.existsSync(path.join(directory, name))
  )
  if (missing.length > 0) {
    throw new Error(
      `The authentic librdkafka Snappy capture is missing (${missing.join(', ')}) from ${directory}. ` +
        'It is a prerequisite that is never synthesized.'
    )
  }
  const read = (name) => fs.readFileSync(path.join(directory, name))
  return {
    batch: read(CAPTURE_FILES.batch),
    data: read(CAPTURE_FILES.data),
    manifest: JSON.parse(read(CAPTURE_FILES.manifest).toString('utf8')),
  }
}

/**
 * @param {Buffer} bytes
 * @returns {string} its SHA-256 as lower-case hex
 */
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/**
 * The batch as it is replayed to a new broker: the first offset is 0, the leader epoch -1, and a
 * producer stamp a new broker would refuse becomes the non-idempotent one, with the CRC recomputed
 * only then. Compression bits, record bytes, timestamps, delta and count are never touched.
 * @param {Buffer} original one complete v2 record batch
 * @returns {{ bytes: Buffer, delta: string[] }} the replayed batch and the envelope fields changed
 */
const restamp = (original) => {
  const bytes = Buffer.from(original)
  const delta = []
  if (bytes.readBigInt64BE(0) !== 0n) {
    bytes.writeBigInt64BE(0n, 0)
    delta.push('baseOffset')
  }
  if (bytes.readInt32BE(12) !== -1) {
    bytes.writeInt32BE(-1, 12)
    delta.push('partitionLeaderEpoch')
  }
  if (
    bytes.readBigInt64BE(43) !== -1n ||
    bytes.readInt16BE(51) !== -1 ||
    bytes.readInt32BE(53) !== -1
  ) {
    bytes.writeBigInt64BE(-1n, 43)
    bytes.writeInt16BE(-1, 51)
    bytes.writeInt32BE(-1, 53)
    bytes.writeUInt32BE(crc32C(bytes.subarray(21)), 17)
    delta.push('producerStamp', 'crc')
  }
  return { bytes, delta }
}

/**
 * @param {Buffer|string|null} bytes
 * @returns {string|null} lower-case hex, null for null
 */
const hexOf = (bytes) => (bytes === null ? null : Buffer.from(bytes).toString('hex'))

/**
 * What a public consumer was given, as the manifest records the expected public projection: bytes
 * as hex, every header key once with its values in order, keys sorted by their UTF-8 bytes. It
 * makes no claim about the interleaving of different keys on the wire.
 * @param {object} message a consumer message
 * @returns {{ offset: string, timestamp: string, keyHex: string|null, valueHex: string|null,
 *   headers: Array<{ key: string, values: Array<string|null> }> }}
 */
const publicRecord = ({ offset, timestamp, key, value, headers }) => ({
  offset,
  timestamp,
  keyHex: hexOf(key),
  valueHex: hexOf(value),
  headers: Object.keys(headers ?? {})
    .sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)))
    .map((name) => ({ key: name, values: [].concat(headers[name]).map(hexOf) })),
})

/**
 * The manifest's expected public records at the offsets a replay was assigned.
 * @param {object} manifest
 * @param {bigint|string} assigned the base offset the broker gave the replayed batch
 * @returns {object[]} the same records with `offset` moved by `assigned - manifest.batch.baseOffset`
 */
const expectedRecords = (manifest, assigned) =>
  manifest.expectedPublicRecords.map((record) => ({
    ...record,
    offset: String(BigInt(assigned) + BigInt(record.offset) - BigInt(manifest.batch.baseOffset)),
  }))

/**
 * What the Snappy slot of a compression module does when asked for its codec.
 * @param {{ lookupCodec: Function, Types: Record<string, number> }} compression the module under
 *   test: `src/protocol/message/compression` of the tree the suite runs in
 * @returns {{ state: 'usable'|'absent'|'unimplemented', error: Error|null }} `usable` when a codec
 *   with `compress` and `decompress` comes back, `absent` when nothing does, `unimplemented` when
 *   the lookup throws `KafkaJSNotImplemented`
 * @throws {Error} any other failure of the lookup, which says nothing about the slot
 */
const snappySlot = ({ lookupCodec, Types }) => {
  let codec
  try {
    codec = lookupCodec(Types.Snappy)
  } catch (error) {
    if (error instanceof KafkaJSNotImplemented) return { state: 'unimplemented', error }
    throw error
  }
  const usable = typeof codec?.compress === 'function' && typeof codec?.decompress === 'function'
  return { state: usable ? 'usable' : 'absent', error: null }
}

/**
 * The first stage's precondition, which runs in the supplied no-built-in artifact: the tree is the
 * named artifact, and its Snappy slot holds no codec, whether it is absent or an unimplemented
 * placeholder. A codec a plugin registered would make the slot usable, so it fails here too.
 * @param {object} options
 * @param {string} options.root the tree the suite runs in
 * @param {string} options.version the `package.json` version the artifact must carry
 * @param {{ lookupCodec: Function, Types: Record<string, number> }} options.compression
 * @returns {{ version: string, state: 'absent'|'unimplemented' }}
 * @throws {Error} for another version, or a Snappy slot that returns a codec
 */
const assertBaselineArtifact = ({ root, version, compression }) => {
  const found = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8')).version
  if (found !== version) {
    throw new Error(`The baseline artifact is ${found}, not the pinned ${version}`)
  }
  const { state } = snappySlot(compression)
  if (state === 'usable') throw new Error('The baseline artifact already has a Snappy codec')
  return { version: found, state }
}

/**
 * The final stage's precondition: the tree the suite runs in has a usable Snappy codec.
 * @param {{ compression: { lookupCodec: Function, Types: Record<string, number> } }} options
 * @returns {void}
 * @throws {Error} when the slot returns no codec or throws
 */
const assertFinalArtifact = ({ compression }) => {
  const { state } = snappySlot(compression)
  if (state !== 'usable') throw new Error(`The final tree has no usable Snappy codec (${state})`)
}

/**
 * What the baseline stage pins of a failure: its class, message, `code` and, recursively, cause.
 * @param {*} error
 * @param {number} [depth=3] how many causes to follow
 * @returns {{ class: string, message: string, code: *, cause: object|null }|null} null for no error
 */
const diagnosticOf = (error, depth = 3) => {
  if (error === null || error === undefined) return null
  if (typeof error !== 'object') {
    return { class: typeof error, message: String(error), code: null, cause: null }
  }
  return {
    class: error.constructor?.name ?? 'Object',
    message: String(error.message),
    code: error.code ?? null,
    cause: depth > 0 ? diagnosticOf(error.cause, depth - 1) : null,
  }
}

/**
 * Holds the failure the first stage saw to the diagnostic the controller read from the artifact:
 * the exact class, message, code and cause chain, never a substring and never any other failure.
 * @param {*} error what the consumer failed with, null when it did not fail
 * @param {string} pinned the JSON of the pinned diagnostic, in the shape of `diagnosticOf`
 * @returns {void}
 * @throws {Error} when there was no failure, the pin is not that shape, or the failure differs
 */
const assertBaselineDiagnostic = (error, pinned) => {
  const expected = JSON.parse(pinned)
  const keys = Object.keys(expected ?? {}).sort()
  if (!isDeepStrictEqual(keys, ['cause', 'class', 'code', 'message'])) {
    throw new Error('The pinned diagnostic is not { class, message, code, cause }')
  }
  const actual = diagnosticOf(error)
  if (actual === null) throw new Error('The baseline stage saw no failure to pin')
  if (!isDeepStrictEqual(actual, expected)) {
    throw new Error(
      `The baseline failure is not the pinned diagnostic: ${JSON.stringify(actual)} versus ` +
        JSON.stringify(expected)
    )
  }
}

module.exports = {
  CAPTURE_DIR,
  CAPTURE_FILES,
  assertBaselineArtifact,
  assertBaselineDiagnostic,
  assertFinalArtifact,
  diagnosticOf,
  expectedRecords,
  hexOf,
  loadCapture,
  publicRecord,
  restamp,
  sha256,
  snappySlot,
}
