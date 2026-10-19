const path = require('path')
const manifest = require('../../fixtures/fuzzVectors.json')

const PROTOCOL = path.join(__dirname, '..', '..')

/**
 * Where the fields of a v2 record batch are, from the start of the batch: the layout of the
 * message format, the same for every batch.
 * @see https://kafka.apache.org/43/implementation/message-format
 * @type {Record<'length'|'magic'|'count', { at: number, bytes: number }>}
 */
const BATCH_FIELDS = {
  length: { at: 8, bytes: 4 },
  magic: { at: 16, bytes: 1 },
  count: { at: 57, bytes: 4 },
}

/**
 * One response vector the corpus is built from.
 * @typedef {object} FuzzVector
 * @property {string} name
 * @property {string} [file] the body, a `{ type: 'Buffer', data }` JSON, relative to `src/protocol`
 * @property {string} [body] the body as hex, for a vector that is built and not captured
 * @property {number} version the Fetch version of the body
 * @property {string} evidence where its expected values come from
 * @property {object} control the values the unmutated body decodes to, written out by hand:
 *   what {@link project} returns for it
 * @property {{ sizeAt: number, sizeWidth: 'int32'|'uvarint', blockAt: number, blockLength: number,
 *   tail: string, batches: Array<{ bytes: number, offsets: string[] }> }} [records] where the
 *   RECORDS field of its one partition is, and what its batches hold
 */

/** @type {FuzzVector[]} */
const vectors = manifest

/**
 * @param {FuzzVector} vector
 * @returns {Buffer} the vector's body
 */
const bodyOf = (vector) =>
  vector.body
    ? Buffer.from(vector.body, 'hex')
    : Buffer.from(require(path.join(PROTOCOL, vector.file)))

/**
 * @param {FuzzVector} vector
 * @returns {{ decode: Function, parse: Function }} the codec of the vector's version
 */
const codecOf = ({ version }) => require(path.join(PROTOCOL, `requests/fetch/v${version}/response`))

/**
 * What a decoded Fetch response is, in the terms of a vector's `control`: the values a reader of
 * the bytes finds, without the record contexts and the shapes that are the decoder's own. A header
 * is kept under its own name, `__proto__` included.
 * @param {object} decoded what a Fetch response codec's `decode` returned
 * @returns {object}
 */
const project = (decoded) => ({
  errorCode: decoded.errorCode,
  nodeEndpoints: decoded.nodeEndpoints,
  topics: decoded.responses.map((topic) => ({
    ...(topic.topicName === undefined ? { id: topic.topicId } : { name: topic.topicName }),
    partitions: topic.partitions.map((partition) => ({
      partition: partition.partition,
      errorCode: partition.errorCode,
      highWatermark: partition.highWatermark,
      lastStableOffset: partition.lastStableOffset,
      messages: partition.messages.map(({ offset, key, value, headers }) => ({
        offset,
        key: key && key.toString(),
        value: value && value.toString(),
        headers: Object.fromEntries(
          Object.entries(headers).map(([name, content]) => [
            name,
            Array.isArray(content) ? content.map(String) : String(content),
          ])
        ),
      })),
    })),
  })),
})

/**
 * @param {number} value
 * @returns {Buffer} the unsigned varint of `value`
 */
const uvarint = (value) => {
  const bytes = []
  for (let rest = value; ; rest >>>= 7) {
    bytes.push(rest > 0x7f ? (rest & 0x7f) | 0x80 : rest)
    if (rest <= 0x7f) return Buffer.from(bytes)
  }
}

/**
 * The body with its RECORDS block cut to `bytes` bytes and the size written again to say so: a
 * block that ends where a fetch's `maxBytes` ended, inside a consistent response.
 * @param {FuzzVector} vector one with `records`
 * @param {number} bytes from 0 to the block's length
 * @returns {Buffer}
 */
const withRecordsCutTo = (vector, bytes) => {
  const { sizeAt, sizeWidth, blockAt, tail } = vector.records
  const body = bodyOf(vector)
  const size = sizeWidth === 'int32' ? Buffer.alloc(4) : uvarint(bytes + 1)
  if (sizeWidth === 'int32') {
    size.writeInt32BE(bytes)
  }

  return Buffer.concat([
    body.subarray(0, sizeAt),
    size,
    body.subarray(blockAt, blockAt + bytes),
    Buffer.from(tail, 'hex'),
  ])
}

/**
 * The offsets of the batches that lie whole inside the first `bytes` bytes of the block.
 * @param {FuzzVector} vector one with `records`
 * @param {number} bytes
 * @returns {string[]}
 */
const offsetsWithin = (vector, bytes) => {
  const offsets = []
  let end = 0
  for (const batch of vector.records.batches) {
    end += batch.bytes
    if (end > bytes) break
    offsets.push(...batch.offsets)
  }
  return offsets
}

/**
 * The body with one field of one batch written over.
 * @param {FuzzVector} vector one with `records`
 * @param {number} batchIndex
 * @param {'length'|'magic'|'count'} field
 * @param {number} value
 * @returns {Buffer}
 */
const withForgedField = (vector, batchIndex, field, value) => {
  const { blockAt, batches } = vector.records
  const start = batches.slice(0, batchIndex).reduce((at, { bytes }) => at + bytes, blockAt)
  const { at, bytes } = BATCH_FIELDS[field]
  const body = bodyOf(vector)
  if (bytes === 1) {
    body.writeInt8(value, start + at)
  } else {
    body.writeInt32BE(value, start + at)
  }
  return body
}

module.exports = {
  BATCH_FIELDS,
  vectors,
  bodyOf,
  codecOf,
  project,
  withRecordsCutTo,
  offsetsWithin,
  withForgedField,
}
