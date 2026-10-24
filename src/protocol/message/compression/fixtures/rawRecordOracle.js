/**
 * The independent oracle of the authentic librdkafka capture. It reads the acquired, uncompressed
 * record section with its own strict parser, derives the public projection from what it read, and
 * holds the capture's manifest to it. It imports no production record decoder or header formatter:
 * the client under test never supplies its own expectation. It proves the capture is internally
 * consistent and well formed; that the bytes really came from librdkafka rests on the capture's
 * recorded provenance, which no code can verify.
 */
const { isUtf8 } = require('buffer')
const { isDeepStrictEqual } = require('util')
const crc32C = require('../../../recordBatch/crc32C')
const { sha256 } = require('./librdkafkaCapture')

/** The bytes before a v2 batch's records. */
const BATCH_HEADER_BYTES = 61
/** The codec bits of a Snappy batch's attributes. */
const SNAPPY = 2
const DECIMAL = /^-?\d+$/
const SHA256_HEX = /^[0-9a-f]{64}$/

/**
 * Reads one buffer front to back and refuses to run past its end.
 */
class StrictReader {
  /** @param {Buffer} buffer */
  constructor(buffer) {
    this.buffer = buffer
    this.offset = 0
  }

  /** @returns {number} the bytes not read yet */
  get remaining() {
    return this.buffer.length - this.offset
  }

  /**
   * @returns {number} the next signed byte
   * @throws {Error} at the end of the buffer
   */
  int8() {
    if (this.remaining < 1) throw new Error('The record ends inside a field')
    const value = this.buffer.readInt8(this.offset)
    this.offset += 1
    return value
  }

  /**
   * Reads a zigzag varint within the byte bound of its width and the range of its width. A longer
   * encoding, or a final byte that carries bits beyond the width, is refused; a shorter-than-bound
   * encoding with padding is accepted, as the Java reader accepts it.
   * @param {32|64} bits the width of the value
   * @returns {bigint} the signed value
   * @throws {Error} at the end of the buffer, past the byte bound, or past the width
   */
  varint(bits) {
    const maxBytes = Math.ceil(bits / 7)
    let value = 0n
    for (let index = 0; index < maxBytes; index += 1) {
      if (this.remaining < 1) throw new Error('The record ends inside a varint')
      const byte = this.buffer[this.offset]
      this.offset += 1
      value |= BigInt(byte & 0x7f) << BigInt(7 * index)
      if ((byte & 0x80) === 0) {
        if (value >= 1n << BigInt(bits)) throw new Error(`A varint exceeds ${bits} bits`)
        return (value >> 1n) ^ -(value & 1n)
      }
    }
    throw new Error(`A varint is longer than ${maxBytes} bytes`)
  }

  /**
   * @param {number} length
   * @returns {Buffer} the next `length` bytes
   * @throws {Error} when fewer remain
   */
  bytes(length) {
    if (length > this.remaining)
      throw new Error('A length runs past the end of the data it sits in')
    const bytes = this.buffer.subarray(this.offset, this.offset + length)
    this.offset += length
    return bytes
  }

  /**
   * @returns {Buffer|null} a value behind a varint length, null for length -1
   * @throws {Error} for a length below -1 or past the record
   */
  nullableBytes() {
    const length = Number(this.varint(32))
    if (length === -1) return null
    if (length < -1) throw new Error('A value has a negative length')
    return this.bytes(length)
  }
}

/**
 * @param {Buffer|null} bytes
 * @returns {string|null} lower-case hex, null for null, '' for empty
 */
const hex = (bytes) => (bytes === null ? null : bytes.toString('hex'))

/**
 * Parses the uncompressed record section of one v2 batch.
 * @param {Buffer} data the records, back to back, with nothing around them
 * @param {number} [count] how many records it must hold; any number when absent
 * @returns {Array<{ offsetDelta: string, timestampDelta: string, keyHex: string|null,
 *   valueHex: string|null, headers: Array<{ keyHex: string, keyUtf8: string,
 *   valueHex: string|null }> }>} the records in wire order, header entries in wire order with
 *   every duplicate; deltas are decimal strings
 * @throws {Error} for a record whose declared length differs from what it holds, a varint beyond
 *   its bound, a negative count or header-key length, a non-zero record attribute, a header key
 *   that is not valid UTF-8, a section not used up exactly, or a different record count
 */
const parseRecordSection = (data, count) => {
  const section = new StrictReader(data)
  const records = []
  while (section.remaining > 0) {
    const length = Number(section.varint(32))
    if (length < 0) throw new Error('A record has a negative length')
    const record = new StrictReader(section.bytes(length))
    if (record.int8() !== 0) throw new Error('A record has a non-zero attribute')
    const timestampDelta = record.varint(64)
    const offsetDelta = record.varint(32)
    const key = record.nullableBytes()
    const value = record.nullableBytes()
    const headerCount = Number(record.varint(32))
    if (headerCount < 0) throw new Error('A record has a negative header count')
    if (headerCount > record.remaining) throw new Error('A header count runs past its record')
    const headers = []
    for (let index = 0; index < headerCount; index += 1) {
      const keyLength = Number(record.varint(32))
      if (keyLength < 0) throw new Error('A header key has a negative length')
      const keyBytes = record.bytes(keyLength)
      if (!isUtf8(keyBytes)) throw new Error('A header key is not valid UTF-8')
      const keyUtf8 = keyBytes.toString('utf8')
      if (!Buffer.from(keyUtf8, 'utf8').equals(keyBytes)) {
        throw new Error('A header key does not round-trip through UTF-8')
      }
      headers.push({ keyHex: hex(keyBytes), keyUtf8, valueHex: hex(record.nullableBytes()) })
    }
    if (record.remaining !== 0) throw new Error('A record is longer than its fields')
    records.push({
      offsetDelta: String(offsetDelta),
      timestampDelta: String(timestampDelta),
      keyHex: hex(key),
      valueHex: hex(value),
      headers,
    })
  }
  if (count !== undefined && records.length !== count) {
    throw new Error(`The record section holds ${records.length} records, not ${count}`)
  }
  return records
}

/**
 * The public projection of parsed records: absolute decimal offset and timestamp, bytes as hex,
 * and each distinct header key once, keys sorted by their UTF-8 bytes, its values in wire order
 * with null kept apart from empty. It says nothing of how different keys interleaved on the wire.
 * @param {ReturnType<typeof parseRecordSection>} records
 * @param {{ baseOffset: string, baseTimestamp: string }} batch
 * @returns {Array<{ offset: string, timestamp: string, keyHex: string|null, valueHex: string|null,
 *   headers: Array<{ key: string, values: Array<string|null> }> }>}
 */
const deriveProjection = (records, { baseOffset, baseTimestamp }) =>
  records.map((record) => {
    const grouped = new Map()
    for (const { keyUtf8, valueHex } of record.headers) {
      grouped.set(keyUtf8, [...(grouped.get(keyUtf8) ?? []), valueHex])
    }
    return {
      offset: String(BigInt(baseOffset) + BigInt(record.offsetDelta)),
      timestamp: String(BigInt(baseTimestamp) + BigInt(record.timestampDelta)),
      keyHex: record.keyHex,
      valueHex: record.valueHex,
      headers: [...grouped]
        .sort(([a], [b]) => Buffer.compare(Buffer.from(a), Buffer.from(b)))
        .map(([key, values]) => ({ key, values })),
    }
  })

/**
 * @param {*} value a manifest number: a safe integer or a decimal string
 * @param {string} name for the failure message
 * @returns {bigint}
 * @throws {Error} for anything else
 */
const integerOf = (value, name) => {
  if (typeof value === 'number' && Number.isSafeInteger(value)) return BigInt(value)
  if (typeof value === 'string' && DECIMAL.test(value)) return BigInt(value)
  throw new Error(`The manifest's ${name} is not an exact integer`)
}

/**
 * @param {*} value a manifest offset or timestamp
 * @param {string} name
 * @returns {string} the decimal string the plan requires
 * @throws {Error} for a number, or any string that is not decimal
 */
const decimalOf = (value, name) => {
  if (typeof value !== 'string' || !DECIMAL.test(value)) {
    throw new Error(`The manifest's ${name} is not an exact decimal string`)
  }
  return value
}

/**
 * @param {*} value
 * @param {string} name
 * @returns {void}
 * @throws {Error} unless it is a non-empty string
 */
const requireText = (value, name) => {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`The manifest's ${name} is missing`)
  }
}

/**
 * @param {object} manifest
 * @returns {void}
 * @throws {Error} naming the first field that is missing or not of its type
 */
const assertManifestShape = (manifest) => {
  const { producer, provenance, batch } = manifest
  if (producer === null || typeof producer !== 'object')
    throw new Error("The manifest's producer is missing")
  requireText(producer.name, 'producer.name')
  requireText(producer.version, 'producer.version')
  if (producer.configuration === undefined || producer.configuration === null) {
    throw new Error("The manifest's producer.configuration is missing")
  }
  if (provenance === null || typeof provenance !== 'object')
    throw new Error("The manifest's provenance is missing")
  for (const name of ['source', 'acquiredAt', 'releaseOrEnvironment']) {
    requireText(provenance[name], `provenance.${name}`)
  }
  if (!Array.isArray(provenance.commands) || provenance.commands.length === 0) {
    throw new Error("The manifest's provenance.commands is missing")
  }
  if (provenance.referenceDecoder === undefined || provenance.referenceDecoder === null) {
    throw new Error("The manifest's provenance.referenceDecoder is missing")
  }
  for (const name of ['sha256', 'compressedSha256', 'recordDataSha256']) {
    if (typeof manifest[name] !== 'string' || !SHA256_HEX.test(manifest[name])) {
      throw new Error(`The manifest's ${name} is not a lower-case SHA-256`)
    }
  }
  if (batch === null || typeof batch !== 'object')
    throw new Error("The manifest's batch is missing")
  requireText(batch.framing, 'batch.framing')
  decimalOf(batch.baseOffset, 'batch.baseOffset')
  decimalOf(batch.baseTimestamp, 'batch.baseTimestamp')
  for (const name of ['magic', 'attributes', 'recordCount', 'lastOffsetDelta']) {
    integerOf(batch[name], `batch.${name}`)
  }
  for (const name of ['rawRecords', 'expectedPublicRecords']) {
    if (!Array.isArray(manifest[name])) throw new Error(`The manifest's ${name} is missing`)
  }
}

/**
 * @param {string} what the list being compared, for the failure message
 * @param {Array<object>} actual
 * @param {Array<object>} expected
 * @returns {void}
 * @throws {Error} naming the first index that differs, with both entries
 */
const assertSameList = (what, actual, expected) => {
  const length = Math.max(actual.length, expected.length)
  for (let index = 0; index < length; index += 1) {
    if (!isDeepStrictEqual(actual[index], expected[index])) {
      throw new Error(
        `${what} differ at ${index}: read ${JSON.stringify(actual[index])}, ` +
          `the manifest records ${JSON.stringify(expected[index])}`
      )
    }
  }
}

/**
 * @param {object} record a manifest raw record
 * @returns {object} the same with its deltas as decimal strings, for comparison with a parse
 */
const normalizedRaw = (record) => ({
  ...record,
  offsetDelta: String(record.offsetDelta),
  timestampDelta: String(record.timestampDelta),
})

/**
 * Holds a capture to its own manifest: shape and digests, the batch's framing, CRC and attributes
 * (Snappy, CreateTime, not transactional, not a control batch), the record section parsed by the
 * strict parser against the manifest's raw records, and the public projection derived from that
 * parse against the manifest's expected public records.
 * @param {{ batch: Buffer, data: Buffer, manifest: object }} capture what `loadCapture` returns
 * @returns {{ body: Buffer, data: Buffer, manifest: object, records: object[] }} the compressed
 *   records section, the acquired reference section, the manifest and the parsed raw records
 * @throws {Error} naming the first inconsistency
 */
const validateCapture = ({ batch, data, manifest }) => {
  assertManifestShape(manifest)
  const header = manifest.batch
  if (sha256(batch) !== manifest.sha256)
    throw new Error('The batch does not match the manifest SHA-256')
  if (batch.length < BATCH_HEADER_BYTES || batch.readInt32BE(8) + 12 !== batch.length) {
    throw new Error('The capture is not exactly one record batch')
  }
  if (
    batch.readInt8(16) !== 2 ||
    BigInt(batch.readInt8(16)) !== integerOf(header.magic, 'batch.magic')
  ) {
    throw new Error('The batch is not magic 2, or not the magic the manifest records')
  }
  if (crc32C(batch.subarray(21)) !== batch.readUInt32BE(17))
    throw new Error('The batch CRC32C does not match')
  const attributes = batch.readInt16BE(21)
  if ((attributes & 7) !== SNAPPY) throw new Error('The batch is not Snappy compressed')
  if ((attributes & 8) !== 0) throw new Error('The batch uses LogAppendTime, not CreateTime')
  if ((attributes & 0x30) !== 0) throw new Error('The batch is transactional or a control batch')
  const facts = [
    ['attributes', BigInt(attributes), header.attributes],
    ['record count', BigInt(batch.readInt32BE(57)), header.recordCount],
    ['last offset delta', BigInt(batch.readInt32BE(23)), header.lastOffsetDelta],
  ]
  for (const [name, actual, recorded] of facts) {
    if (actual !== integerOf(recorded, `batch.${name}`)) {
      throw new Error(`The batch ${name} differs from the manifest`)
    }
  }
  if (String(batch.readBigInt64BE(0)) !== header.baseOffset) {
    throw new Error('The batch base offset differs from the manifest')
  }
  if (String(batch.readBigInt64BE(27)) !== header.baseTimestamp) {
    throw new Error('The batch base timestamp differs from the manifest')
  }
  const body = batch.subarray(BATCH_HEADER_BYTES)
  if (sha256(body) !== manifest.compressedSha256) {
    throw new Error('The compressed records do not match the manifest SHA-256')
  }
  if (sha256(data) !== manifest.recordDataSha256) {
    throw new Error('The acquired record section does not match the manifest SHA-256')
  }

  const count = Number(integerOf(header.recordCount, 'batch.recordCount'))
  const records = parseRecordSection(data, count)
  const deltas = records.map(({ offsetDelta }) => BigInt(offsetDelta))
  if (deltas.some((delta, index) => delta < 0n || (index > 0 && delta <= deltas[index - 1]))) {
    throw new Error('The offset deltas are not increasing from zero')
  }
  if (deltas.at(-1) !== integerOf(header.lastOffsetDelta, 'batch.lastOffsetDelta')) {
    throw new Error('The last record does not carry the last offset delta')
  }
  assertSameList('The raw records', records, manifest.rawRecords.map(normalizedRaw))
  assertSameList(
    'The public records',
    deriveProjection(records, header),
    manifest.expectedPublicRecords
  )
  return { body, data, manifest, records }
}

/**
 * Holds what a broker stored back to the validated capture: the compressed records byte for byte,
 * and, once a codec decompressed them, the acquired reference section byte for byte and by digest.
 * The reference section was parsed against the manifest's raw records when it was validated.
 * @param {ReturnType<typeof validateCapture>} validated
 * @param {Buffer} body the compressed records section of the batch the broker stored
 * @param {Buffer} [decompressed] what the built-in Snappy decoder made of `body`; omitted where
 *   there is no built-in decoder, which leaves only the compressed bytes to compare
 * @returns {void}
 * @throws {Error} naming the first difference
 */
const verifyStoredBody = (validated, body, decompressed) => {
  if (!body.equals(validated.body))
    throw new Error('The stored compressed records differ from the capture')
  if (decompressed === undefined) return
  if (!decompressed.equals(validated.data)) {
    throw new Error('The decompressed records differ from the acquired reference section')
  }
  if (sha256(decompressed) !== validated.manifest.recordDataSha256) {
    throw new Error('The decompressed records do not match the manifest SHA-256')
  }
}

module.exports = { deriveProjection, parseRecordSection, validateCapture, verifyStoredBody }
