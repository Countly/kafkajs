const { KafkaJSRecordTooLargeError } = require('../../errors')
const { Codecs, Types } = require('../../protocol/message/compression')
const snappy = require('../../protocol/message/compression/snappy')

/**
 * The sizes the producer charges against `bufferMemory`. A record's wire size is the Java client's
 * upper bound for it: `DefaultRecord.MAX_RECORD_OVERHEAD` (21) plus its key, value and headers with
 * their length varints, a null or absent field counting one byte. What the producer holds for it is
 * more: the record object, its settler share and the arrays it sits in, so its held size adds 64
 * bytes, and 16 per header. A send adds 1536 bytes (its holder, settlers and promises, and while it
 * waits for room its wait) and the
 * backing stores its Buffer views keep alive beyond the bytes it uses; a batch adds 1024 (its
 * object, timer and arrays). The constants are the measured fit of probes P7 and P12, refitted
 * only through `scripts/benchmarks/admissionAccounting.js`, never the formula. Records are those
 * the encoder accepts: keys, values and header values are strings, Buffers, null or absent.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecord.java
 */

/** `DefaultRecord.MAX_RECORD_OVERHEAD`: the most a record's length, attributes and deltas take. */
const RECORD_OVERHEAD = 21
/** What a held record costs beyond its wire size. */
const HELD_RECORD = 64
/** What each header of a held record costs beyond its wire size. */
const HELD_HEADER = 16
/** What a send costs beyond its records. */
const SEND_CHARGE = 1536
/** What a batch costs, charged without waiting when the accumulator creates it. */
const BATCH_DEBIT = 1024
/** A Buffer is copied when its backing store is larger than this and than 16 times the Buffer. */
const COPY_STORES_ABOVE = 16384
const COPY_STORES_TIMES = 16

/**
 * Java's `ByteUtils.sizeOfVarint` of a length: the bytes of its zigzag varint.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/utils/ByteUtils.java
 * @param {number} length from 0 to 2^31 - 1
 * @returns {number} from 1 to 5
 */
const varintSize = (length) =>
  length < 64 ? 1 : length < 8192 ? 2 : length < 1048576 ? 3 : length < 134217728 ? 4 : 5

/**
 * @param {string|Buffer|null|undefined} field
 * @returns {number} its bytes on the wire with their length varint: UTF-8 bytes for a string, 1
 *   for null or absent
 */
const fieldSize = (field) => {
  if (field == null) {
    return 1
  }
  const length = typeof field === 'string' ? Buffer.byteLength(field) : field.length
  return varintSize(length) + length
}

/**
 * A record's wire size plus `perRecord`, and `perHeader` for each header. Its headers are read as
 * the record encoder writes them: each own key, and an array value as one header per element.
 * @param {object} record
 * @param {number} perRecord
 * @param {number} perHeader
 * @returns {number}
 */
const sizeOf = ({ key, value, headers }, perRecord, perHeader) => {
  let size = RECORD_OVERHEAD + perRecord + fieldSize(key) + fieldSize(value)
  let count = 0
  if (headers != null) {
    for (const name of Object.keys(headers)) {
      const header = headers[name]
      const nameSize = fieldSize(name)
      if (!Array.isArray(header)) {
        count += 1
        size += perHeader + nameSize + fieldSize(header)
        continue
      }
      for (let i = 0; i < header.length; i++) {
        if (i in header) {
          count += 1
          size += perHeader + nameSize + fieldSize(header[i])
        }
      }
    }
  }
  return size + varintSize(count)
}

/**
 * The Java client's upper bound of a record's size in a batch (`recordSizeUpperBound`).
 * @param {object} record
 * @returns {number} bytes, at least 24
 */
const wire = (record) => sizeOf(record, 0, 0)

/**
 * What the producer holds for a record: its wire size, 64, and 16 per header.
 * @param {object} record
 * @returns {number} bytes, at least 88
 */
const held = (record) => sizeOf(record, HELD_RECORD, HELD_HEADER)

/**
 * Adds the Buffers among a record's key, value and header values to `views`, as the encoder
 * writes them: an array header value is one header per element, a hole none.
 * @param {object} record
 * @param {Buffer[]} views
 * @returns {void}
 */
const collectBuffers = ({ key, value, headers }, views) => {
  if (Buffer.isBuffer(key)) {
    views.push(key)
  }
  if (Buffer.isBuffer(value)) {
    views.push(value)
  }
  if (headers != null) {
    for (const name of Object.keys(headers)) {
      const header = headers[name]
      if (!Array.isArray(header)) {
        if (Buffer.isBuffer(header)) {
          views.push(header)
        }
        continue
      }
      for (let i = 0; i < header.length; i++) {
        if (i in header && Buffer.isBuffer(header[i])) {
          views.push(header[i])
        }
      }
    }
  }
}

/**
 * A record's share of its send's charge, released when the batch carrying it retires: its held
 * size without the bytes of its Buffers, which the send's shell pays for (see `chargeOf`).
 * @param {object} record
 * @returns {number}
 */
const share = (record) => {
  let size = held(record)
  if (Buffer.isBuffer(record.key)) {
    size -= record.key.length
  }
  if (Buffer.isBuffer(record.value)) {
    size -= record.value.length
  }
  if (record.headers != null) {
    const views = []
    collectBuffers({ headers: record.headers }, views)
    for (const view of views) {
      size -= view.length
    }
  }
  return size
}

/**
 * Adds the bytes from `start` to `end` of a backing store to those this send's earlier views of it
 * cover.
 * @param {number[]} spans the covered bytes, as sorted, disjoint `[start, end, start, end, ...]`
 *   pairs, merged in place
 * @param {number} start
 * @param {number} end
 * @returns {number} how many of those bytes no earlier view covered
 */
const cover = (spans, start, end) => {
  let low = 0
  let high = spans.length / 2
  while (low < high) {
    const middle = (low + high) >> 1
    if (spans[2 * middle + 1] < start) {
      low = middle + 1
    } else {
      high = middle
    }
  }
  const first = 2 * low
  let last = first
  let from = start
  let to = end
  let added = end - start
  for (; last < spans.length && spans[last] <= end; last += 2) {
    added -= Math.max(0, Math.min(end, spans[last + 1]) - Math.max(start, spans[last]))
    from = Math.min(from, spans[last])
    to = Math.max(to, spans[last + 1])
  }
  spans.splice(first, last - first, from, to)
  return added
}

/**
 * What one Buffer of a send changes in the bytes its backing stores keep alive beyond what its
 * views show, the store being the `ArrayBuffer` itself: the first view of a store adds the bytes it
 * does not show, and a later view takes back the bytes it shows that no earlier view did.
 * @param {Map<ArrayBufferLike, number[]>} stores by backing store, the bytes the send's views cover
 * @param {Buffer} view
 * @returns {number}
 */
const storeCharge = (stores, view) => {
  const start = view.byteOffset
  const end = start + view.length
  const spans = stores.get(view.buffer)
  if (spans === undefined) {
    stores.set(view.buffer, [start, end])
    return view.buffer.byteLength - view.length
  }
  return -cover(spans, start, end)
}

/**
 * What admitting a send charges: the held size of every record, 1536, and every backing store its
 * Buffers keep alive beyond the bytes they show. Views are grouped by store, the store being the
 * `ArrayBuffer` itself: a store several of the send's views share is counted once, and a store
 * other sends also use is counted by each of them. The send's shell is the 1536, the stores and the
 * bytes of its Buffers: the part no batch releases, held until the last batch carrying the send's
 * records retires and the last wait on it ends; the rest is the records' shares (see `share`).
 *
 * The records are charged in order, each with its stores, and the charge of every prefix is
 * checked against `limit` before the next record is read: a record never takes back more than its
 * own held size adds, so the charge only grows, and the first prefix above `limit` ends the pass and
 * is returned, for `acquire` to refuse as `scope: 'send'`. A total within `limit` is exact, and no
 * sum goes past `limit` by more than one record and its stores.
 * @param {{ topic: string, messages: object[] }[]} topicMessages
 * @param {number} limit the producer's `bufferMemory`
 * @returns {{ total: number, shell: number }} `total` is above `limit` when the send cannot fit
 * @throws {KafkaJSRecordTooLargeError} for a record whose held size alone is above `limit`
 */
const chargeOf = (topicMessages, limit) => {
  let total = SEND_CHARGE
  let stores = 0
  let buffered = 0
  /** @type {Map<ArrayBufferLike, number[]>|null} by backing store, the bytes the send's views cover */
  let covered = null
  /** @type {Buffer[]} the Buffers of the record being charged */
  const views = []
  for (const { topic, messages } of topicMessages) {
    for (const message of messages) {
      const size = held(message)
      if (size > limit) {
        throw new KafkaJSRecordTooLargeError(
          `A record of ${size} bytes for topic "${topic}" can never fit in bufferMemory (${limit} bytes)`,
          { reason: 'bufferMemory', scope: 'record', size, limit, topic }
        )
      }
      total += size
      collectBuffers(message, views)
      for (const view of views) {
        covered ??= new Map()
        const charge = storeCharge(covered, view)
        stores += charge
        total += charge
        buffered += view.length
      }
      views.length = 0
      if (total > limit) {
        return { total, shell: 0 }
      }
    }
  }
  return { total, shell: SEND_CHARGE + stores + buffered }
}

/**
 * The Buffer a send keeps for a key, value or header value: the caller's own, as in 2.2.4, so its
 * bytes still reach the wire if the caller changes them, unless it is a small view over a large
 * backing store (above 16 KiB and above 16 times the view), which is copied into a store of its
 * own size so the send does not keep the large one alive. Anything else is returned as it is.
 * @param {*} field
 * @returns {*}
 */
const ownedBuffer = (field) => {
  if (!Buffer.isBuffer(field)) {
    return field
  }
  const { byteLength } = field.buffer
  if (byteLength <= COPY_STORES_ABOVE || byteLength <= COPY_STORES_TIMES * field.length) {
    return field
  }
  const copy = Buffer.allocUnsafeSlow(field.length)
  field.copy(copy)
  return copy
}

/** `DefaultRecordBatch.RECORD_BATCH_OVERHEAD`: the header of a v2 record batch. */
const BATCH_OVERHEAD = 61
/** What one partition adds to a Produce request beside its batches: its index and lengths. */
const PARTITION_FRAMING = 16
/** The wire size of a record with no key, an empty value and no header. */
const EMPTY_RECORD = 24
/** The codecs this client registers itself; a factory assigned later is the caller's plugin. */
const BUILT_IN = { ...Codecs }

/**
 * @param {*} text a client id, transactional id or topic name; null and absent count 0
 * @returns {number} its UTF-8 bytes
 */
const textBytes = (text) => (text == null ? 0 : Buffer.byteLength(String(text)))

/**
 * What a Produce request costs beside its topics: the length prefix, the request header and the
 * body's own fields, at most 32 bytes in every version, plus the client id and transactional id.
 * @param {string|null} [clientId='kafkajs'] the connection's client id, its default when absent
 * @param {string|null} [transactionalId]
 * @returns {number}
 */
const framing = (clientId = 'kafkajs', transactionalId) =>
  32 + textBytes(clientId) + textBytes(transactionalId)

/**
 * What a topic costs in a Produce request beside its partitions: its name or id, its partition
 * array's length and tags, at most 32 bytes plus the name.
 * @param {string} name
 * @returns {number}
 */
const topicFraming = (name) => 32 + textBytes(name)

/**
 * The bytes of records as a batch carries them, uncompressed: their wire sizes.
 * @param {object[]} records
 * @returns {number}
 */
const recordsBytes = (records) => {
  let bytes = 0
  for (const record of records) {
    bytes += wire(record)
  }
  return bytes
}

/**
 * A v2 batch of `records`, uncompressed, at most: its header and every record's wire size.
 * @param {object[]} records
 * @returns {number}
 */
const batchBytes = (records) => BATCH_OVERHEAD + recordsBytes(records)

/**
 * The most `n` bytes of records can take once a built-in codec compressed them, from each codec's
 * own worst case; `n` for no compression, and for a codec the caller registered, whose bound is
 * unknown (the write-boundary guard catches what it adds).
 * @see https://github.com/madler/zlib/blob/v1.3.1/deflate.c (`deflateBound`, gzip wrapper)
 * @see https://github.com/facebook/zstd/blob/dev/lib/zstd.h (`ZSTD_COMPRESSBOUND`)
 * @see https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
 * @param {number} codec the request's `compression`, one of `Types`
 * @param {number} n uncompressed bytes
 * @returns {number}
 */
const encodedBound = (codec, n) => {
  if (Codecs[codec] === undefined || Codecs[codec] !== BUILT_IN[codec]) {
    return n
  }
  switch (codec) {
    case Types.GZIP:
      return n + Math.floor(n / 4096) + Math.floor(n / 16384) + Math.floor(n / 33554432) + 25
    case Types.ZSTD:
      return n + Math.floor(n / 256) + (n < 131072 ? Math.floor((131072 - n) / 2048) : 0)
    case Types.LZ4:
      return n + 8 * Math.ceil(n / 65536) + 27
    default:
      return snappy.encodedBound(n)
  }
}

/**
 * The smallest `maxRequestSize` a producer with these identifiers takes: its request framing, one
 * batch header and one empty record.
 * @param {string|null} [clientId]
 * @param {string|null} [transactionalId]
 * @returns {number}
 */
const minimumRequestSize = (clientId, transactionalId) =>
  framing(clientId, transactionalId) + BATCH_OVERHEAD + EMPTY_RECORD

/**
 * One producer's plan of its Produce requests under `maxRequestSize`, on the raw sizes of the
 * records plus each built-in codec's worst case (`encodedBound`): a request is planned as its
 * framing, each topic's framing, and per batch its partition's framing, its header and its
 * records' bound. The plan only ever overestimates a request the encoder builds from the same
 * batches, so a batch planned alone always fits; the write-boundary guard compares the real frame.
 * Without `maxRequestSize` nothing is limited.
 */
class RequestBudget {
  #framing
  /** @type {Map<string, number>} by codec, whether it is built in, and topic name bytes */
  #capacities = new Map()

  /**
   * @param {object} [options]
   * @param {number} [options.maxRequestSize=Infinity] the largest frame, its length prefix included
   * @param {string|null} [options.clientId] absent means the connection's default, `kafkajs`
   * @param {string|null} [options.transactionalId]
   */
  constructor({ maxRequestSize = Infinity, clientId, transactionalId } = {}) {
    this.maxRequestSize = maxRequestSize
    this.#framing = framing(clientId, transactionalId)
  }

  /** @returns {number} the bytes the topics and batches of one request may plan */
  get maxRequestBytes() {
    return this.maxRequestSize - this.#framing
  }

  /**
   * @param {string} topic
   * @returns {number} the planned bytes of the topic's framing in one request
   */
  topicBytes(topic) {
    return topicFraming(topic)
  }

  /**
   * @param {{ compression?: number, messages: object[], wire?: number }} batch `wire` is its
   *   records' wire bytes, computed from `messages` when absent
   * @returns {number} the planned bytes of the batch in a request, its topic's framing apart
   */
  batchBytes({ compression, messages, wire: bytes = recordsBytes(messages) }) {
    return PARTITION_FRAMING + BATCH_OVERHEAD + encodedBound(compression, bytes)
  }

  /**
   * The most wire bytes of records one batch of `topic` may carry for a request of that batch
   * alone to fit: the largest `R` whose plan, `framing + topicFraming + 16 + 61 +
   * encodedBound(codec, R)`, is at most `maxRequestSize`. Found by bisection over the monotone
   * bound, and kept per codec and topic name length.
   * @param {string} topic
   * @param {number} [codec]
   * @returns {number} Infinity without a limit; -1 when not even an empty batch fits
   */
  chunkCapacity(topic, codec) {
    if (this.maxRequestSize === Infinity) {
      return Infinity
    }
    const fixed = this.#framing + topicFraming(topic) + PARTITION_FRAMING + BATCH_OVERHEAD
    const key = `${codec}/${Codecs[codec] === BUILT_IN[codec]}/${fixed}`
    let capacity = this.#capacities.get(key)
    if (capacity === undefined) {
      let low = -1
      let high = Math.max(-1, this.maxRequestSize - fixed)
      while (low < high) {
        const middle = Math.ceil((low + high) / 2)
        if (fixed + encodedBound(codec, middle) <= this.maxRequestSize) {
          low = middle
        } else {
          high = middle - 1
        }
      }
      capacity = low
      this.#capacities.set(key, capacity)
    }
    return capacity
  }
}

module.exports = {
  wire,
  held,
  share,
  chargeOf,
  ownedBuffer,
  varintSize,
  framing,
  topicFraming,
  recordsBytes,
  batchBytes,
  encodedBound,
  minimumRequestSize,
  RequestBudget,
  RECORD_OVERHEAD,
  SEND_CHARGE,
  BATCH_DEBIT,
  BATCH_OVERHEAD,
  PARTITION_FRAMING,
}
