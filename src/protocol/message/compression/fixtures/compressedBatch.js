/**
 * Compressed record batches for the decompression specs, written from the documented v2 layout by
 * the batch builder that never reads a batch, with the body compressed by the codec's own
 * library: `node:zlib` for gzip and zstd, `lz4-napi` for LZ4, and the in-tree encoder for Snappy.
 */
const zlib = require('zlib')
const Decoder = require('../../../decoder')
const decodeMessages = require('../../../requests/fetch/v4/decodeMessages')
const { batch, record } = require('../../../requests/fetch/__tests__/fixtures/recordBatchBytes')
const snappy = require('../snappy')

const TYPES = { gzip: 1, snappy: 2, lz4: 3, zstd: 4 }

/** @type {Record<string, (plain: Buffer) => Promise<Buffer>>} */
const COMPRESSORS = {
  gzip: async (plain) => zlib.gzipSync(plain),
  // eslint-disable-next-line n/no-unsupported-features/node-builtins -- Node 22.15+, as the codec
  zstd: async (plain) => zlib.zstdCompressSync(plain),
  lz4: async (plain) => require('lz4-napi').compressFrame(plain),
  snappy: (plain) => snappy.compress({ buffer: plain }),
}

/**
 * @param {number} count
 * @returns {Buffer} `count` encoded records at offset deltas 0 to count - 1, keyed `k<n>` and
 *   valued `v<n>`
 */
const plainRecords = (count) =>
  Buffer.concat(
    Array.from({ length: count }, (_, n) =>
      record({ offsetDelta: n, key: `k${n}`, value: `v${n}` })
    )
  )

/**
 * A batch whose records are compressed, or plain when `codec` is `none`.
 * @param {object} [fields]
 * @param {'none'|'gzip'|'snappy'|'lz4'|'zstd'} [fields.codec='gzip']
 * @param {number} [fields.records=3] how many records the body holds
 * @param {number} [fields.count] the count the header declares; defaults to `records`
 * @param {Buffer} [fields.body] replaces the compressed body
 * @param {bigint|number} [fields.baseOffset=0n]
 * @param {number} [fields.lastOffsetDelta] defaults to `records - 1`
 * @param {number} [fields.crc] forges the CRC
 * @returns {Promise<Buffer>}
 */
const compressedBatch = async ({
  codec = 'gzip',
  records = 3,
  count = records,
  body,
  baseOffset = 0n,
  lastOffsetDelta = Math.max(records - 1, 0),
  crc,
} = {}) =>
  batch({
    baseOffset,
    attributes: codec === 'none' ? 0 : TYPES[codec],
    lastOffsetDelta,
    records: [
      body ??
        (codec === 'none'
          ? plainRecords(records)
          : await COMPRESSORS[codec](plainRecords(records))),
    ],
    count,
    crc,
  })

/**
 * @param {...Buffer} batches
 * @returns {Buffer} the RECORDS field of a classic Fetch partition: an int32 length, the batches
 */
const recordsField = (...batches) => {
  const block = Buffer.concat(batches)
  const size = Buffer.alloc(4)
  size.writeInt32BE(block.length)
  return Buffer.concat([size, block])
}

/**
 * Decodes a RECORDS block as a Fetch response partition does.
 * @param {Buffer} field from {@link recordsField}
 * @param {object} [options] `{ checkCrcs, maxDecompressedBatchBytes }`
 * @returns {Promise<{ messages: object[], corruption?: object }>}
 */
const decodeField = (field, options) => decodeMessages(new Decoder(field), options)

module.exports = { TYPES, COMPRESSORS, plainRecords, compressedBatch, recordsField, decodeField }
