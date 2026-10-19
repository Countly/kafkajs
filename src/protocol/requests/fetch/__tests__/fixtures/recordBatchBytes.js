/**
 * Record batches written byte by byte from the documented v2 layout, for specs that need a batch
 * the decoder never produced: a length, a magic byte, a record count or a control record that a
 * broker never sends. Nothing here reads a batch: a spec asserts what a decoder returns against the
 * values it gave to these functions.
 * @see https://kafka.apache.org/43/implementation/message-format
 */

const CASTAGNOLI_REFLECTED = 0x82f63b78

/**
 * @param {Buffer} buffer
 * @returns {number} the CRC-32C (Castagnoli) of `buffer`, as an unsigned 32-bit integer
 */
const crc32c = (buffer) => {
  let crc = 0xffffffff
  for (const byte of buffer) {
    crc ^= byte
    for (let bit = 0; bit < 8; bit++) {
      crc = crc & 1 ? (crc >>> 1) ^ CASTAGNOLI_REFLECTED : crc >>> 1
    }
  }
  return (crc ^ 0xffffffff) >>> 0
}

/**
 * @param {number|bigint} value
 * @returns {Buffer} the zig-zag varint of `value`, as a record writes its lengths and deltas
 */
const varint = (value) => {
  let zigzag = BigInt.asUintN(64, (BigInt(value) << 1n) ^ (BigInt(value) >> 63n))
  const bytes = []
  while (zigzag >= 0x80n) {
    bytes.push(Number(zigzag & 0x7fn) | 0x80)
    zigzag >>= 7n
  }
  bytes.push(Number(zigzag))
  return Buffer.from(bytes)
}

/**
 * @param {Buffer|string|null} bytes
 * @returns {Buffer} a varint length (-1 for null) and the bytes
 */
const sized = (bytes) => {
  if (bytes === null) return varint(-1)
  const buffer = Buffer.from(bytes)
  return Buffer.concat([varint(buffer.length), buffer])
}

/**
 * One record of a v2 batch.
 * @param {object} [fields]
 * @param {number} [fields.offsetDelta=0]
 * @param {number} [fields.timestampDelta=0]
 * @param {Buffer|string|null} [fields.key=null]
 * @param {Buffer|string|null} [fields.value=null]
 * @param {Array<[string, Buffer|string]>} [fields.headers=[]] key and value of each header
 * @returns {Buffer} the record, its length varint first
 */
const record = ({
  offsetDelta = 0,
  timestampDelta = 0,
  key = null,
  value = null,
  headers = [],
} = {}) => {
  const body = Buffer.concat([
    Buffer.from([0]),
    varint(timestampDelta),
    varint(offsetDelta),
    sized(key),
    sized(value),
    varint(headers.length),
    ...headers.flatMap(([name, content]) => [sized(name), sized(content)]),
  ])
  return Buffer.concat([varint(body.length), body])
}

/**
 * One record batch. `length`, `count` and `crc` default to the values that make the batch valid;
 * passing one forges it. The CRC covers the bytes after its own field, so it is computed last, from
 * the batch as forged.
 * @param {object} [fields]
 * @param {bigint|number} [fields.baseOffset=0n]
 * @param {number} [fields.partitionLeaderEpoch=0]
 * @param {number} [fields.magic=2]
 * @param {number} [fields.attributes=0] bit 0x10 transactional, 0x20 control, the low 3 bits compression
 * @param {number} [fields.lastOffsetDelta] defaults to the last record's offset delta
 * @param {bigint|number} [fields.firstTimestamp=0n]
 * @param {bigint|number} [fields.maxTimestamp=0n]
 * @param {bigint|number} [fields.producerId=-1n]
 * @param {number} [fields.producerEpoch=-1]
 * @param {number} [fields.baseSequence=-1]
 * @param {Buffer[]} [fields.records=[]] encoded records, see {@link record}
 * @param {number} [fields.count] the record count; defaults to `records.length`
 * @param {number} [fields.length] the batch length; defaults to 49 plus the records' bytes
 * @param {number} [fields.crc] the CRC; defaults to the CRC-32C of the bytes after it
 * @returns {Buffer}
 */
const batch = ({
  baseOffset = 0n,
  partitionLeaderEpoch = 0,
  magic = 2,
  attributes = 0,
  lastOffsetDelta,
  firstTimestamp = 0n,
  maxTimestamp = 0n,
  producerId = -1n,
  producerEpoch = -1,
  baseSequence = -1,
  records = [],
  count = records.length,
  length,
  crc,
} = {}) => {
  const recordBytes = Buffer.concat(records)
  const afterCrc = Buffer.alloc(40 + recordBytes.length)
  afterCrc.writeInt16BE(attributes, 0)
  afterCrc.writeInt32BE(lastOffsetDelta ?? Math.max(records.length - 1, 0), 2)
  afterCrc.writeBigInt64BE(BigInt(firstTimestamp), 6)
  afterCrc.writeBigInt64BE(BigInt(maxTimestamp), 14)
  afterCrc.writeBigInt64BE(BigInt(producerId), 22)
  afterCrc.writeInt16BE(producerEpoch, 30)
  afterCrc.writeInt32BE(baseSequence, 32)
  afterCrc.writeInt32BE(count, 36)
  recordBytes.copy(afterCrc, 40)

  const header = Buffer.alloc(21)
  header.writeBigInt64BE(BigInt(baseOffset), 0)
  header.writeInt32BE(length ?? 49 + recordBytes.length, 8)
  header.writeInt32BE(partitionLeaderEpoch, 12)
  header.writeInt8(magic, 16)
  header.writeUInt32BE(crc ?? crc32c(afterCrc), 17)
  return Buffer.concat([header, afterCrc])
}

/** The control record types of a transaction marker, by the type id in the record's key. */
const CONTROL_TYPES = { ABORT: 0, COMMIT: 1 }

/**
 * A control batch of one record: a transaction marker unless told otherwise.
 * @param {object} [fields]
 * @param {'ABORT'|'COMMIT'|number} [fields.type='COMMIT'] a name, or a raw type id
 * @param {Buffer} [fields.key] replaces the key (version int16, type int16)
 * @param {Buffer|null} [fields.value] replaces the value (version int16, coordinator epoch int32)
 * @param {bigint|number} [fields.baseOffset=0n]
 * @param {number} [fields.crc]
 * @returns {Buffer}
 */
const controlBatch = ({ type = 'COMMIT', key, value, baseOffset = 0n, crc } = {}) => {
  const typeId = typeof type === 'number' ? type : CONTROL_TYPES[type]
  return batch({
    baseOffset,
    attributes: 0x30,
    producerId: 103000n,
    producerEpoch: 2,
    records: [
      record({
        key: key ?? Buffer.from([0, 0, 0, typeId]),
        value: value === undefined ? Buffer.from([0, 0, 0, 0, 0, 117]) : value,
      }),
    ],
    crc,
  })
}

module.exports = { crc32c, varint, record, batch, controlBatch }
